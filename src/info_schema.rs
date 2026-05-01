// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0

//! Handling for `INFORMATION_SCHEMA` queries that Trino doesn't model the
//! same way Postgres does. Two responsibilities:
//!
//! 1. Synthesise empty result sets for `information_schema` tables Trino
//!    doesn't have (`referential_constraints`, `key_column_usage`, ...) so
//!    Power BI's relationship discovery proceeds without an error.
//! 2. Rewrite the Power BI-specific
//!    `CASE WHEN data_type LIKE '%unsigned%' ...` expression in queries
//!    against `information_schema.columns` to map Trino type names to
//!    PostgreSQL-style names before forwarding.

use std::sync::Arc;

use futures::stream;
use pgwire::api::Type;
use pgwire::api::results::{QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::catalog::text_field;
use crate::query_inspection::ParsedQuery;

/// Trino doesn't expose these `information_schema` tables. Power BI queries
/// them during relationship discovery; returning a zero-row result with the
/// right column shape lets the client proceed.
const MISSING_TABLES: &[&str] = &[
    "referential_constraints",
    "table_constraints",
    "key_column_usage",
    "constraint_column_usage",
    "constraint_table_usage",
    "check_constraints",
];

pub(crate) fn intercept_missing_information_schema(
    query: &ParsedQuery,
) -> Option<PgWireResult<Vec<Response>>> {
    for table in MISSING_TABLES {
        if query.references_table_in_schema("information_schema", table) {
            tracing::debug!(
                table,
                "Intercepting query for missing information_schema table"
            );
            return Some(empty_query_response(query));
        }
    }
    None
}

/// Empty result set whose schema mirrors the SELECT list of the input
/// query. Power BI's `RetrieveRelationshipsForTable` expects a typed result
/// (not just `CommandComplete`), even with zero rows.
fn empty_query_response(query: &ParsedQuery) -> PgWireResult<Vec<Response>> {
    let mut columns = query.select_column_names();
    if columns.is_empty() {
        // Last-resort fallback when the query failed to parse: give the
        // client one column so it can read the (zero-row) result without
        // tripping on an empty RowDescription.
        columns.push("column".to_owned());
    }

    let schema = Arc::new(
        columns
            .iter()
            .map(|name| text_field(name, Type::VARCHAR))
            .collect::<Vec<_>>(),
    );

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::empty(),
    ))])
}

/// Rewrite `INFORMATION_SCHEMA.columns` queries to translate Trino data-type
/// names into PostgreSQL-style equivalents before forwarding.
///
/// Power BI sends a query with a `CASE WHEN data_type LIKE '%unsigned%' ...`
/// expression. We replace it with a Trino CASE WHEN that maps type names
/// like `double` to `double precision`. Returns `None` when the query
/// doesn't target `INFORMATION_SCHEMA.columns`.
///
/// Implementation note: the rewrite uses byte-offset string splicing rather
/// than AST transformation. The Power BI marker is a fixed driver-emitted
/// pattern, not user input, so there is no injection risk; AST
/// round-tripping a CASE expression of this complexity loses formatting in
/// ways the rewrite isn't robust to.
///
/// `to_ascii_uppercase` is deliberate. Full Unicode `to_uppercase` can
/// change byte length (e.g. Turkish `ı` (U+0131, 2 bytes) maps to `I` (1
/// byte)), and the splice below uses byte offsets that must remain valid in
/// the original query. The marker is pure ASCII, so ASCII-only case folding
/// is sufficient and preserves byte alignment.
pub(crate) fn rewrite_info_schema_columns(
    query: &str,
    parsed_query: &ParsedQuery,
) -> Option<String> {
    if !parsed_query.references_table_in_schema("information_schema", "columns") {
        return None;
    }
    let upper = query.to_ascii_uppercase();

    let type_mapping = "\
        CASE \
        WHEN lower(data_type) = 'double' THEN 'double precision' \
        WHEN lower(data_type) LIKE 'varchar%' THEN 'character varying' \
        WHEN lower(data_type) LIKE 'char(%' THEN 'character' \
        WHEN lower(data_type) LIKE '%timestamp%with time zone%' THEN 'timestamp with time zone' \
        WHEN lower(data_type) LIKE 'timestamp%' THEN 'timestamp without time zone' \
        WHEN lower(data_type) LIKE '%time%with time zone%' THEN 'time with time zone' \
        WHEN lower(data_type) LIKE 'time%' THEN 'time without time zone' \
        WHEN lower(data_type) LIKE 'decimal%' THEN 'numeric' \
        WHEN lower(data_type) = 'varbinary' THEN 'bytea' \
        ELSE data_type END";

    let powerbi_marker = "CASE WHEN (DATA_TYPE LIKE '%UNSIGNED%')";
    if upper.contains(powerbi_marker) {
        let start = upper.find(powerbi_marker)?;
        let end_marker = "END AS DATA_TYPE";
        let end_pos = upper[start..].find(end_marker)?;
        let end = start + end_pos + end_marker.len();

        let before = &query[..start];
        let after = &query[end..];
        return Some(format!("{before}{type_mapping} AS DATA_TYPE{after}"));
    }

    // Query references information_schema.columns but doesn't match the
    // Power BI CASE WHEN pattern. Pass through unchanged so other clients
    // see unmodified results.
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrite_powerbi_pattern() {
        let query = "select COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, \
            case when (data_type like '%unsigned%') then DATA_TYPE || ' unsigned' else DATA_TYPE end as DATA_TYPE \
            from INFORMATION_SCHEMA.columns \
            where TABLE_SCHEMA = 'sf1' and TABLE_NAME = 'orders' \
            order by TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION";

        let parsed_query = ParsedQuery::new(query);
        let rewritten = rewrite_info_schema_columns(query, &parsed_query)
            .expect("should rewrite Power BI INFORMATION_SCHEMA.columns query");

        assert!(
            rewritten.contains("lower(data_type)"),
            "should contain type mapping: {rewritten}"
        );
        assert!(
            rewritten.contains("TABLE_SCHEMA = 'sf1'"),
            "should preserve WHERE clause: {rewritten}"
        );
        assert!(
            rewritten
                .to_uppercase()
                .contains("FROM INFORMATION_SCHEMA.COLUMNS"),
            "should preserve FROM: {rewritten}"
        );
        assert!(
            rewritten.contains("AS DATA_TYPE"),
            "should have DATA_TYPE alias: {rewritten}"
        );
    }

    #[test]
    fn does_not_rewrite_other_queries() {
        for q in [
            "SELECT * FROM INFORMATION_SCHEMA.tables WHERE TABLE_SCHEMA = 'sf1'",
            "SELECT * FROM pg_type",
            "SELECT 1",
        ] {
            let parsed_query = ParsedQuery::new(q);
            assert!(
                rewrite_info_schema_columns(q, &parsed_query).is_none(),
                "should not rewrite: {q}"
            );
        }
    }

    /// A user table named `columns` in another schema must not trip the
    /// rewrite — the AST inspector restricts the match to
    /// `information_schema.columns`.
    #[test]
    fn skips_other_schemas() {
        let q = "SELECT * FROM mycustom.columns";
        let parsed_query = ParsedQuery::new(q);
        assert!(rewrite_info_schema_columns(q, &parsed_query).is_none());
    }

    /// Regression: non-ASCII content in a string literal before the Power
    /// BI marker would shift byte offsets if we used Unicode `to_uppercase`
    /// (Turkish `ı` (U+0131, 2 bytes) maps to `I` (1 byte)). With
    /// `to_ascii_uppercase` indices stay aligned and the splice produces
    /// well-formed SQL.
    #[test]
    fn handles_non_ascii_literals() {
        let query = "select COLUMN_NAME, /* başlık ı ß */ \
            case when (data_type like '%unsigned%') then DATA_TYPE || ' unsigned' else DATA_TYPE end as DATA_TYPE \
            from INFORMATION_SCHEMA.columns where TABLE_NAME = 'orders'";

        let parsed_query = ParsedQuery::new(query);
        let rewritten = rewrite_info_schema_columns(query, &parsed_query)
            .expect("rewrite must trigger on INFORMATION_SCHEMA.columns");

        assert!(
            rewritten.contains("/* başlık ı ß */"),
            "non-ASCII content must round-trip intact: {rewritten}"
        );
        assert!(rewritten.contains("lower(data_type)"));
        assert!(rewritten.contains("AS DATA_TYPE"));
    }
}
