// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0
use std::sync::Arc;

use pgwire::api::Type;
use pgwire::api::results::Response;
use pgwire::error::{PgWireError, PgWireResult};
use trino_rust_client::{Client, Row};

use super::{build_response, json_str, text_field};

/// Starting OID for synthetic table entries (above PostgreSQL's system range).
const BASE_TABLE_OID: u32 = 16384;

/// Well-known namespace OID for the "public" schema in PostgreSQL.
const PUBLIC_NAMESPACE_OID: u32 = 2200;

/// Starting OID for synthetic namespace entries (schemas other than "public").
const BASE_NAMESPACE_OID: u32 = 20000;

/// djb2 hash, then map into `[base, i32::MAX)`. Used to derive synthetic
/// OIDs from string names.
///
/// We need a deterministic hash that produces the same OID every time the
/// gateway sees the same name — clients that reconnect must see stable
/// OIDs in pg_class so they can resolve them in pg_attribute. Rust's
/// `std::collections::hash_map::DefaultHasher` is *not* stability-
/// guaranteed across compiler releases (the standard library is allowed
/// to swap the algorithm), so we use the well-known djb2 hash which is
/// fixed and trivial.
fn djb2_oid(parts: &[&str], base: u32) -> u32 {
    let mut h: u32 = 5381;
    for (i, part) in parts.iter().enumerate() {
        if i > 0 {
            // Separator byte to avoid collisions between e.g. "abctable"
            // and "ab" + "ctable".
            h = h.wrapping_mul(33).wrapping_add(0xFF);
        }
        for b in part.bytes() {
            h = h.wrapping_mul(33).wrapping_add(u32::from(b));
        }
    }
    base + (h % (i32::MAX as u32 - base))
}

/// Compute a deterministic OID for a table given its schema and table name.
///
/// Uses the same hash as `namespace_oid`, so pg_attribute can reference
/// the same OID for `attrelid`.
pub fn table_oid(schema_name: &str, table_name: &str) -> u32 {
    djb2_oid(&[schema_name, table_name], BASE_TABLE_OID)
}

/// Compute a deterministic namespace OID for a schema name. The "public"
/// schema gets the well-known PostgreSQL OID (2200); everything else is
/// hashed.
pub fn namespace_oid(schema_name: &str) -> u32 {
    if schema_name == "public" {
        return PUBLIC_NAMESPACE_OID;
    }
    djb2_oid(&[schema_name], BASE_NAMESPACE_OID)
}

/// Map Trino's table_type string to PostgreSQL's relkind char.
fn relkind(table_type: &str) -> &'static str {
    match table_type.to_uppercase().as_str() {
        "BASE TABLE" => "r",
        "VIEW" => "v",
        _ => "r",
    }
}

/// Query Trino's information_schema.tables and return a pg_class-compatible response.
pub async fn respond_pg_class(client: &Arc<Client>) -> PgWireResult<Vec<Response>> {
    let sql = "SELECT table_schema, table_name, table_type \
               FROM information_schema.tables \
               WHERE table_schema NOT IN ('information_schema')"
        .to_owned();

    let dataset = client
        .get_all::<Row>(sql)
        .await
        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

    let schema = Arc::new(vec![
        text_field("oid", Type::INT4),
        text_field("relname", Type::VARCHAR),
        text_field("relnamespace", Type::INT4),
        text_field("relkind", Type::VARCHAR),
    ]);

    let mut rows = Vec::new();

    for trino_row in dataset.into_vec() {
        let values = trino_row.into_json();
        // columns: table_schema (0), table_name (1), table_type (2)
        let schema_name = json_str(&values, 0, "public");
        let tbl_name = json_str(&values, 1, "");
        let tbl_type = json_str(&values, 2, "BASE TABLE");

        let oid = table_oid(schema_name, tbl_name);
        let ns_oid = namespace_oid(schema_name);
        let kind = relkind(tbl_type);

        rows.push(vec![
            Some(oid.to_string()),
            Some(tbl_name.to_owned()),
            Some(ns_oid.to_string()),
            Some(kind.to_owned()),
        ]);
    }

    build_response(schema, rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_oid_is_deterministic() {
        let oid1 = table_oid("public", "users");
        let oid2 = table_oid("public", "users");
        assert_eq!(oid1, oid2);
    }

    #[test]
    fn table_oid_differs_for_different_tables() {
        let oid1 = table_oid("public", "users");
        let oid2 = table_oid("public", "orders");
        assert_ne!(oid1, oid2);
    }

    #[test]
    fn table_oid_differs_across_schemas() {
        let oid1 = table_oid("public", "users");
        let oid2 = table_oid("sales", "users");
        assert_ne!(oid1, oid2);
    }

    #[test]
    fn table_oid_above_base() {
        let oid = table_oid("public", "users");
        assert!(oid >= BASE_TABLE_OID);
    }

    #[test]
    fn namespace_oid_public() {
        assert_eq!(namespace_oid("public"), PUBLIC_NAMESPACE_OID);
    }

    #[test]
    fn namespace_oid_deterministic() {
        assert_eq!(namespace_oid("sales"), namespace_oid("sales"));
    }

    #[test]
    fn namespace_oid_differs() {
        assert_ne!(namespace_oid("sales"), namespace_oid("marketing"));
    }

    #[test]
    fn relkind_base_table() {
        assert_eq!(relkind("BASE TABLE"), "r");
    }

    #[test]
    fn relkind_view() {
        assert_eq!(relkind("VIEW"), "v");
    }

    #[test]
    fn relkind_default() {
        assert_eq!(relkind("MATERIALIZED VIEW"), "r");
    }
}
