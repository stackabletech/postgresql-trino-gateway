use std::sync::Arc;

use pgwire::api::Type;
use pgwire::api::results::{FieldFormat, FieldInfo, Response};
use pgwire::error::{PgWireError, PgWireResult};
use trino_rust_client::{Client, Row};

use super::build_response;

/// Starting OID for synthetic table entries (above PostgreSQL's system range).
const BASE_TABLE_OID: u32 = 16384;

/// Well-known namespace OID for the "public" schema in PostgreSQL.
const PUBLIC_NAMESPACE_OID: u32 = 2200;

/// Starting OID for synthetic namespace entries (schemas other than "public").
const BASE_NAMESPACE_OID: u32 = 20000;

/// Compute a deterministic OID for a table given its schema and table name.
///
/// Uses the same algorithm as `table_oid`, so pg_attribute can reference the
/// same OID for attrelid.
pub fn table_oid(schema_name: &str, table_name: &str) -> u32 {
    // Simple deterministic hash: we want stable OIDs across pg_class and
    // pg_attribute calls within the same session.  Using a basic hash that
    // fits in u32 and is offset from BASE_TABLE_OID.
    let mut h: u32 = 5381;
    for b in schema_name.bytes() {
        h = h.wrapping_mul(33).wrapping_add(u32::from(b));
    }
    // Separator to avoid collisions between "abctable" and "ab" + "ctable"
    h = h.wrapping_mul(33).wrapping_add(0xFF);
    for b in table_name.bytes() {
        h = h.wrapping_mul(33).wrapping_add(u32::from(b));
    }
    // Ensure it's at least BASE_TABLE_OID and positive (i32 range)
    BASE_TABLE_OID + (h % (i32::MAX as u32 - BASE_TABLE_OID))
}

/// Compute a deterministic namespace OID for a schema name.
pub fn namespace_oid(schema_name: &str) -> u32 {
    if schema_name == "public" {
        return PUBLIC_NAMESPACE_OID;
    }
    let mut h: u32 = 5381;
    for b in schema_name.bytes() {
        h = h.wrapping_mul(33).wrapping_add(u32::from(b));
    }
    BASE_NAMESPACE_OID + (h % (i32::MAX as u32 - BASE_NAMESPACE_OID))
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
        FieldInfo::new("oid".to_owned(), None, None, Type::INT4, FieldFormat::Text),
        FieldInfo::new(
            "relname".to_owned(),
            None,
            None,
            Type::VARCHAR,
            FieldFormat::Text,
        ),
        FieldInfo::new(
            "relnamespace".to_owned(),
            None,
            None,
            Type::INT4,
            FieldFormat::Text,
        ),
        FieldInfo::new(
            "relkind".to_owned(),
            None,
            None,
            Type::VARCHAR,
            FieldFormat::Text,
        ),
    ]);

    let mut rows = Vec::new();

    for trino_row in dataset.into_vec() {
        let values = trino_row.into_json();
        // columns: table_schema (0), table_name (1), table_type (2)
        let schema_name = values.first().and_then(|v| v.as_str()).unwrap_or("public");
        let tbl_name = values.get(1).and_then(|v| v.as_str()).unwrap_or("");
        let tbl_type = values
            .get(2)
            .and_then(|v| v.as_str())
            .unwrap_or("BASE TABLE");

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
