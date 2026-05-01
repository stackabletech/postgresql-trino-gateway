// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0
mod pg_attribute;
pub(crate) mod pg_class;
mod pg_type;
mod stubs;

use std::sync::Arc;

use futures::stream;
use pgwire::api::Type;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::error::PgWireResult;
use trino_rust_client::Client;

use crate::query_inspection::ParsedQuery;

/// Shorthand for the boilerplate that catalog and intercept responses
/// emit dozens of times: a `FieldInfo` with no table or column oid and
/// text wire format.
pub(crate) fn text_field(name: &str, ty: Type) -> FieldInfo {
    FieldInfo::new(name.to_owned(), None, None, ty, FieldFormat::Text)
}

/// Read a string column from a Trino row decoded as `serde_json::Value`s,
/// falling back to `default` when the index is out of range or the value
/// isn't a string. Used by pg_class / pg_attribute responders to keep the
/// row-decoding boilerplate to one line per column.
pub(crate) fn json_str<'a>(
    values: &'a [serde_json::Value],
    idx: usize,
    default: &'a str,
) -> &'a str {
    values.get(idx).and_then(|v| v.as_str()).unwrap_or(default)
}

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
/// Uses the unqualified `references_table`: any FROM/JOIN reference to a
/// pg_catalog name (`pg_type`, `pg_class`, `pg_namespace`, etc.) is treated
/// as the catalog table regardless of schema. The names are specific enough
/// that collisions with user tables are not a real concern, and the
/// alternative (requiring `pg_catalog.` prefix) would miss the unqualified
/// usage that JDBC and Npgsql actually emit.
pub fn handle_catalog_query(query: &ParsedQuery) -> Option<PgWireResult<Vec<Response>>> {
    // pg_attribute + pg_type join = composite field lookup.
    if query.references_table("pg_attribute") && query.references_table("pg_type") {
        return Some(stubs::empty_composite_fields());
    }

    // pg_enum must come before pg_type because the enum query joins pg_type.
    if query.references_table("pg_enum") {
        return Some(stubs::empty_enum_labels());
    }

    if query.references_table("pg_type") {
        return Some(pg_type::respond_type_loading());
    }

    if query.references_table("pg_range") {
        return Some(stubs::empty_pg_range());
    }

    if query.references_table("pg_namespace") {
        return Some(stubs::respond_pg_namespace());
    }

    // pg_class and pg_attribute are handled dynamically (need Trino client).
    None
}

/// Check whether a query targets pg_class or pg_attribute and, if so, query
/// Trino's information_schema to build a real response.
pub async fn handle_dynamic_catalog_query(
    query: &ParsedQuery,
    client: &Arc<Client>,
) -> Option<PgWireResult<Vec<Response>>> {
    // PostgreSQL composite types (`CREATE TYPE foo AS (a int, b text)`) are
    // looked up by joining `pg_attribute` to `pg_type` to recover the
    // composite's field list. Npgsql probes for these on every connection
    // even when the schema has no composites; we serve a static empty
    // response from `handle_catalog_query` and short-circuit here so the
    // dynamic path doesn't try to resolve a non-existent table list.
    if query.references_table("pg_attribute") && query.references_table("pg_type") {
        return None;
    }

    if query.references_table("pg_class") {
        tracing::debug!("Dynamic catalog: pg_class");
        return Some(pg_class::respond_pg_class(client).await);
    }

    if query.references_table("pg_attribute") {
        tracing::debug!("Dynamic catalog: pg_attribute");
        return Some(pg_attribute::respond_pg_attribute(client).await);
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dispatch(sql: &str) -> Option<PgWireResult<Vec<Response>>> {
        let query = ParsedQuery::new(sql);
        handle_catalog_query(&query)
    }

    #[test]
    fn pg_type_detected_from_clause() {
        assert!(dispatch("SELECT * FROM pg_type WHERE typname = 'int4'").is_some());
    }

    #[test]
    fn pg_type_detected_join_clause() {
        let q = "SELECT t.oid, t.typname FROM pg_catalog.pg_type t \
                 JOIN pg_namespace n ON n.oid = t.typnamespace";
        assert!(dispatch(q).is_some());
    }

    #[test]
    fn pg_type_case_insensitive() {
        assert!(dispatch("select * from pg_type").is_some());
    }

    #[test]
    fn composite_fields_detected() {
        let q = "SELECT a.attname, t.typname FROM pg_attribute a \
                 JOIN pg_type t ON a.atttypid = t.oid";
        assert!(dispatch(q).is_some());
    }

    #[test]
    fn pg_enum_detected() {
        assert!(dispatch("SELECT enumlabel FROM pg_enum WHERE enumtypid = 12345").is_some());
    }

    #[test]
    fn pg_range_detected() {
        assert!(dispatch("SELECT * FROM pg_range").is_some());
    }

    #[test]
    fn pg_namespace_detected() {
        assert!(dispatch("SELECT oid, nspname FROM pg_namespace").is_some());
    }

    /// pg_class is handled dynamically (needs Trino client).
    #[test]
    fn pg_class_not_static() {
        assert!(dispatch("SELECT * FROM pg_class WHERE relkind = 'r'").is_none());
    }

    /// pg_attribute on its own is also dynamic.
    #[test]
    fn pg_attribute_not_static() {
        assert!(dispatch("SELECT * FROM pg_attribute WHERE attrelid = 1234").is_none());
    }

    #[test]
    fn regular_query_not_intercepted() {
        assert!(dispatch("SELECT 1").is_none());
        assert!(dispatch("SELECT * FROM users").is_none());
        assert!(dispatch("INSERT INTO t VALUES (1)").is_none());
    }

    /// Regression: a string literal mentioning a catalog name must not be
    /// routed to the static stub.
    #[test]
    fn literal_with_catalog_name_not_intercepted() {
        assert!(dispatch("SELECT * FROM users WHERE notes LIKE '%pg_type%'").is_none());
        assert!(dispatch("SELECT 'pg_type' FROM users").is_none());
    }

    /// Regression: a column named pg_type must not be routed.
    #[test]
    fn column_named_like_catalog_not_intercepted() {
        assert!(dispatch("SELECT pg_type FROM my_table").is_none());
    }

    #[test]
    fn pg_type_response_has_correct_row_count() {
        let resp = dispatch("SELECT * FROM pg_type").unwrap().unwrap();
        assert_eq!(resp.len(), 1);
    }

    #[test]
    fn pg_namespace_returns_three_rows() {
        let resp = dispatch("SELECT * FROM pg_namespace").unwrap().unwrap();
        assert_eq!(resp.len(), 1);
    }
}
