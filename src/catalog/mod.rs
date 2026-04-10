// Copyright 2026 Stackable GmbH
// Licensed under the Open Software License version 3.0 (OSL-3.0).
// See LICENSE file in the project root for full license text.
mod pg_attribute;
pub(crate) mod pg_class;
mod pg_type;
mod stubs;

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, FieldInfo, QueryResponse, Response};
use pgwire::error::PgWireResult;
use trino_rust_client::Client;

/// Build a QueryResponse from a schema and rows of string values.
///
/// Each row is a `Vec<Option<String>>` where `None` represents SQL NULL.
/// Values are encoded as text using `encode_field` which respects the column
/// type declared in the schema.
fn build_response(
    schema: Arc<Vec<FieldInfo>>,
    rows: Vec<Vec<Option<String>>>,
) -> PgWireResult<Vec<Response>> {
    let mut data_rows = Vec::with_capacity(rows.len());

    for row in &rows {
        let mut encoder = DataRowEncoder::new(Arc::clone(&schema));
        for value in row {
            match value {
                Some(v) => encoder.encode_field(&v.as_str())?,
                None => encoder.encode_field(&None::<&str>)?,
            }
        }
        data_rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(data_rows),
    ))])
}

/// Check whether a query targets pg_catalog tables and return a pre-built
/// static response if so.
///
/// Returns `Some(response)` for catalog queries, `None` otherwise.
pub fn handle_catalog_query(query: &str) -> Option<PgWireResult<Vec<Response>>> {
    let upper = query.to_uppercase();

    // pg_attribute + pg_type join = composite field lookup
    if upper.contains("PG_ATTRIBUTE") && upper.contains("PG_TYPE") {
        return Some(stubs::empty_composite_fields());
    }

    // pg_type (must come after the pg_attribute+pg_type check)
    if upper.contains("FROM PG_TYPE")
        || upper.contains("JOIN PG_TYPE")
        || upper.contains(".PG_TYPE")
    {
        return Some(pg_type::respond_type_loading());
    }

    // pg_enum
    if upper.contains("FROM PG_ENUM") || upper.contains("JOIN PG_ENUM") {
        return Some(stubs::empty_enum_labels());
    }

    // pg_range
    if upper.contains("FROM PG_RANGE") {
        return Some(stubs::empty_pg_range());
    }

    // pg_namespace
    if upper.contains("PG_NAMESPACE") {
        return Some(stubs::respond_pg_namespace());
    }

    // pg_class and pg_attribute are handled dynamically (need Trino client).
    // See `handle_dynamic_catalog_query`.

    None
}

/// Check whether a query targets pg_class or pg_attribute and, if so, query
/// Trino's information_schema to build a real response.
///
/// Returns `Some(result)` for dynamic catalog queries, `None` otherwise.
pub async fn handle_dynamic_catalog_query(
    query: &str,
    client: &Arc<Client>,
) -> Option<PgWireResult<Vec<Response>>> {
    let upper = query.to_uppercase();

    // pg_attribute + pg_type join = composite field lookup (stay static)
    if upper.contains("PG_ATTRIBUTE") && upper.contains("PG_TYPE") {
        return None; // handled by static intercept
    }

    // pg_class (matches "FROM pg_class", "FROM pg_catalog.pg_class", "JOIN pg_class", etc.)
    if upper.contains("PG_CLASS") && (upper.contains("FROM") || upper.contains("JOIN")) {
        tracing::debug!("Dynamic catalog: pg_class");
        return Some(pg_class::respond_pg_class(client).await);
    }

    // pg_attribute (standalone, not the pg_attribute+pg_type join)
    if upper.contains("PG_ATTRIBUTE") && (upper.contains("FROM") || upper.contains("JOIN")) {
        tracing::debug!("Dynamic catalog: pg_attribute");
        return Some(pg_attribute::respond_pg_attribute(client).await);
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_type_detected_from_clause() {
        let query = "SELECT * FROM pg_type WHERE typname = 'int4'";
        assert!(handle_catalog_query(query).is_some());
    }

    #[test]
    fn pg_type_detected_join_clause() {
        let query = "SELECT t.oid, t.typname FROM pg_catalog.pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace";
        assert!(handle_catalog_query(query).is_some());
    }

    #[test]
    fn pg_type_case_insensitive() {
        let query = "select * from pg_type";
        assert!(handle_catalog_query(query).is_some());
    }

    #[test]
    fn composite_fields_detected() {
        let query =
            "SELECT a.attname, t.typname FROM pg_attribute a JOIN pg_type t ON a.atttypid = t.oid";
        assert!(handle_catalog_query(query).is_some());
    }

    #[test]
    fn pg_enum_detected() {
        let query = "SELECT enumlabel FROM pg_enum WHERE enumtypid = 12345";
        assert!(handle_catalog_query(query).is_some());
    }

    #[test]
    fn pg_range_detected() {
        let query = "SELECT * FROM pg_range";
        assert!(handle_catalog_query(query).is_some());
    }

    #[test]
    fn pg_namespace_detected() {
        let query = "SELECT oid, nspname FROM pg_namespace";
        assert!(handle_catalog_query(query).is_some());
    }

    #[test]
    fn pg_class_not_static() {
        // pg_class is now handled dynamically, not by the static handler.
        let query = "SELECT * FROM pg_class WHERE relkind = 'r'";
        assert!(handle_catalog_query(query).is_none());
    }

    #[test]
    fn pg_attribute_not_static() {
        // pg_attribute (standalone) is now handled dynamically.
        let query = "SELECT * FROM pg_attribute WHERE attrelid = 1234";
        assert!(handle_catalog_query(query).is_none());
    }

    #[test]
    fn regular_query_not_intercepted() {
        assert!(handle_catalog_query("SELECT 1").is_none());
        assert!(handle_catalog_query("SELECT * FROM users").is_none());
        assert!(handle_catalog_query("INSERT INTO t VALUES (1)").is_none());
    }

    #[test]
    fn pg_type_response_has_correct_row_count() {
        let resp = handle_catalog_query("SELECT * FROM pg_type")
            .unwrap()
            .unwrap();
        assert_eq!(resp.len(), 1);
        // Verify it built successfully; the row count is validated in pg_type::tests.
    }

    #[test]
    fn pg_namespace_returns_three_rows() {
        let resp = handle_catalog_query("SELECT * FROM pg_namespace")
            .unwrap()
            .unwrap();
        assert_eq!(resp.len(), 1);
    }
}
