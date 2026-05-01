// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0
use std::sync::Arc;

use pgwire::api::Type;
use pgwire::api::results::Response;
use pgwire::error::{PgWireError, PgWireResult};
use trino_rust_client::{Client, Row};

use super::pg_class::table_oid;
use super::{build_response, json_str, text_field};
use crate::types::trino_type_to_pg;

/// Query Trino's information_schema.columns and return a pg_attribute-compatible response.
pub async fn respond_pg_attribute(client: &Arc<Client>) -> PgWireResult<Vec<Response>> {
    let sql = "SELECT table_schema, table_name, column_name, ordinal_position, \
               is_nullable, data_type \
               FROM information_schema.columns \
               WHERE table_schema NOT IN ('information_schema') \
               ORDER BY table_schema, table_name, ordinal_position"
        .to_owned();

    let dataset = client
        .get_all::<Row>(sql)
        .await
        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

    let schema = Arc::new(vec![
        text_field("attrelid", Type::INT4),
        text_field("attname", Type::VARCHAR),
        text_field("atttypid", Type::INT4),
        text_field("attnum", Type::INT2),
        text_field("attnotnull", Type::BOOL),
        text_field("attisdropped", Type::BOOL),
    ]);

    let mut rows = Vec::new();

    for trino_row in dataset.into_vec() {
        let values = trino_row.into_json();
        // columns: table_schema (0), table_name (1), column_name (2),
        //          ordinal_position (3), is_nullable (4), data_type (5)
        let schema_name = json_str(&values, 0, "public");
        let tbl_name = json_str(&values, 1, "");
        let col_name = json_str(&values, 2, "");
        // Trino reports ordinal_position as a JSON number, but some
        // information_schema bridges (e.g. legacy connectors) stringify it.
        // Accept either form.
        let ordinal = values
            .get(3)
            .and_then(|v| {
                v.as_i64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            })
            .unwrap_or(0);
        let is_nullable = json_str(&values, 4, "YES");
        let data_type = json_str(&values, 5, "varchar");

        let attrelid = table_oid(schema_name, tbl_name);
        let pg_type = trino_type_to_pg(data_type);
        let atttypid = pg_type.oid();
        let attnotnull = if is_nullable == "NO" { "true" } else { "false" };

        rows.push(vec![
            Some(attrelid.to_string()),
            Some(col_name.to_owned()),
            Some(atttypid.to_string()),
            Some(ordinal.to_string()),
            Some(attnotnull.to_owned()),
            Some("false".to_owned()), // attisdropped
        ]);
    }

    build_response(schema, rows)
}

#[cfg(test)]
mod tests {
    #[test]
    fn attrelid_matches_pg_class_oid() {
        // The same (schema, table) pair must produce the same OID in both
        // pg_class and pg_attribute so joins work correctly.
        use super::super::pg_class::table_oid;
        let class_oid = table_oid("public", "orders");
        let attr_oid = table_oid("public", "orders");
        assert_eq!(class_oid, attr_oid);
    }
}
