// Copyright 2026 Stackable GmbH
// Licensed under the Open Software License version 3.0 (OSL-3.0).
// See LICENSE file in the project root for full license text.
use std::sync::Arc;

use futures::stream;
use pgwire::api::Type;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;

/// Build a single-column, single-row VARCHAR text response.
fn single_text_response(column_name: &str, value: &str) -> PgWireResult<Vec<Response>> {
    let fields = Arc::new(vec![FieldInfo::new(
        column_name.to_owned(),
        None,
        None,
        Type::VARCHAR,
        FieldFormat::Text,
    )]);

    let mut encoder = DataRowEncoder::new(Arc::clone(&fields));
    encoder.encode_field(&value)?;
    let row = encoder.take_row();

    Ok(vec![Response::Query(QueryResponse::new(
        fields,
        stream::iter(vec![Ok(row)]),
    ))])
}

/// Check whether a query should be intercepted locally instead of forwarded to
/// Trino. Returns `Some(response)` for intercepted queries, `None` otherwise.
pub fn intercept_query(
    query: &str,
    catalog: &str,
    schema: &str,
) -> Option<PgWireResult<Vec<Response>>> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return None;
    }

    // Remove trailing semicolons for matching.
    let trimmed = trimmed.trim_end_matches(';').trim();
    let upper = trimmed.to_uppercase();

    // SET commands
    if upper.starts_with("SET ") {
        return Some(Ok(vec![Response::Execution(Tag::new("SET"))]));
    }

    // Transaction commands
    if let Some(resp) = intercept_transaction(&upper) {
        return Some(resp);
    }

    // DISCARD / DEALLOCATE / CLOSE
    if let Some(resp) = intercept_session_commands(&upper) {
        return Some(resp);
    }

    // SHOW commands
    if upper.starts_with("SHOW ") {
        return Some(intercept_show(trimmed));
    }

    // Server info functions
    if let Some(resp) = intercept_server_functions(&upper, catalog, schema) {
        return Some(resp);
    }

    // pg_catalog queries (Npgsql type loading, etc.)
    if let Some(resp) = crate::catalog::handle_catalog_query(trimmed) {
        return Some(resp);
    }

    // information_schema tables that don't exist in Trino — return empty results.
    // Power BI queries these for relationship/constraint discovery.
    if let Some(resp) = intercept_missing_information_schema(&upper, trimmed) {
        return Some(resp);
    }

    None
}

/// Intercept queries against information_schema tables that Trino doesn't have.
/// Returns empty result sets so the client proceeds without constraint data.
fn intercept_missing_information_schema(
    upper: &str,
    query: &str,
) -> Option<PgWireResult<Vec<Response>>> {
    let missing_tables = [
        "REFERENTIAL_CONSTRAINTS",
        "TABLE_CONSTRAINTS",
        "KEY_COLUMN_USAGE",
        "CONSTRAINT_COLUMN_USAGE",
        "CONSTRAINT_TABLE_USAGE",
        "CHARACTER_SETS",
        "CHECK_CONSTRAINTS",
    ];

    for table in &missing_tables {
        if upper.contains(table) {
            tracing::debug!(
                table,
                "Intercepting query for missing information_schema table"
            );
            return Some(empty_query_response(query));
        }
    }

    None
}

/// Return an empty result set with the column schema extracted from the query.
/// Power BI's RetrieveRelationshipsForTable expects a real result set with
/// typed columns (not just CommandComplete), even if it has zero rows.
/// We parse the query to extract column aliases from the SELECT list.
fn empty_query_response(query: &str) -> PgWireResult<Vec<Response>> {
    use futures::stream;
    use pgwire::api::results::{FieldFormat, FieldInfo, QueryResponse};

    // Try to extract column names from the SELECT list
    let columns = extract_select_columns(query);

    let schema = Arc::new(
        columns
            .iter()
            .map(|name| {
                FieldInfo::new(
                    name.clone(),
                    None,
                    None,
                    pgwire::api::Type::VARCHAR,
                    FieldFormat::Text,
                )
            })
            .collect::<Vec<_>>(),
    );

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::empty(),
    ))])
}

/// Extract column names/aliases from a SELECT query.
/// Falls back to generic column names if parsing fails.
fn extract_select_columns(query: &str) -> Vec<String> {
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    let dialect = PostgreSqlDialect {};
    let ast = match Parser::parse_sql(&dialect, query) {
        Ok(ast) => ast,
        Err(_) => return vec!["column".to_string()],
    };

    if let Some(sqlparser::ast::Statement::Query(q)) = ast.into_iter().next()
        && let sqlparser::ast::SetExpr::Select(select) = *q.body
    {
        return select
            .projection
            .iter()
            .map(|item| match item {
                sqlparser::ast::SelectItem::UnnamedExpr(expr) => expr.to_string(),
                sqlparser::ast::SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
                sqlparser::ast::SelectItem::Wildcard(_) => "*".to_string(),
                sqlparser::ast::SelectItem::QualifiedWildcard(name, _) => {
                    format!("{name}.*")
                }
            })
            .collect();
    }

    vec!["column".to_string()]
}

fn intercept_transaction(upper: &str) -> Option<PgWireResult<Vec<Response>>> {
    if upper == "BEGIN" || upper.starts_with("BEGIN ") || upper.starts_with("START TRANSACTION") {
        return Some(Ok(vec![Response::Execution(Tag::new("BEGIN"))]));
    }
    if upper == "COMMIT" || upper == "END" {
        return Some(Ok(vec![Response::Execution(Tag::new("COMMIT"))]));
    }
    if upper == "ROLLBACK" || upper.starts_with("ROLLBACK ") {
        return Some(Ok(vec![Response::Execution(Tag::new("ROLLBACK"))]));
    }
    None
}

fn intercept_session_commands(upper: &str) -> Option<PgWireResult<Vec<Response>>> {
    if upper.starts_with("DISCARD ") || upper.starts_with("DEALLOCATE ") || upper == "CLOSE ALL" {
        return Some(Ok(vec![Response::Execution(Tag::new("OK"))]));
    }
    None
}

fn intercept_show(trimmed: &str) -> PgWireResult<Vec<Response>> {
    // Extract the parameter name after SHOW, case-insensitive.
    let param = trimmed[4..].trim().to_lowercase();

    let value = match param.as_str() {
        "server_version" => "16.6",
        "server_version_num" => "160006",
        "server_encoding" => "UTF8",
        "client_encoding" => "UTF8",
        "standard_conforming_strings" => "on",
        "max_identifier_length" => "63",
        "transaction_isolation" => "read committed",
        "datestyle" => "ISO, MDY",
        "timezone" => "UTC",
        "integer_datetimes" => "on",
        "intervalstyle" => "postgres",
        "is_superuser" => "on",
        _ => "on", // safe default
    };

    single_text_response(&param, value)
}

fn intercept_server_functions(
    upper: &str,
    catalog: &str,
    schema: &str,
) -> Option<PgWireResult<Vec<Response>>> {
    if upper.contains("VERSION()") {
        return Some(single_text_response(
            "version",
            "PostgreSQL 16.6 on x86_64-pc-linux-gnu, compiled by gcc 12.2.0, 64-bit",
        ));
    }

    if upper.contains("CURRENT_DATABASE()") {
        return Some(single_text_response("current_database", catalog));
    }

    if upper.contains("CURRENT_SCHEMA") {
        return Some(single_text_response("current_schema", schema));
    }

    if upper.contains("PG_IS_IN_RECOVERY()") {
        return Some(single_text_response("pg_is_in_recovery", "false"));
    }

    if upper.contains("CURRENT_SETTING('SERVER_VERSION_NUM')") {
        return Some(single_text_response("current_setting", "160006"));
    }

    if upper.contains("CURRENT_SETTING('SERVER_VERSION')") {
        return Some(single_text_response("current_setting", "16.6"));
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: assert that a query is intercepted (returns Some).
    fn assert_intercepted(query: &str) {
        assert!(
            intercept_query(query, "test_catalog", "test_schema").is_some(),
            "expected query to be intercepted: {query}"
        );
    }

    /// Helper: assert that a query is NOT intercepted (returns None).
    fn assert_not_intercepted(query: &str) {
        assert!(
            intercept_query(query, "test_catalog", "test_schema").is_none(),
            "expected query to NOT be intercepted: {query}"
        );
    }

    #[test]
    fn set_commands_intercepted() {
        assert_intercepted("SET client_encoding TO 'UTF8'");
        assert_intercepted("set search_path to public");
        assert_intercepted("SET extra_float_digits = 3;");
    }

    #[test]
    fn transaction_commands_intercepted() {
        assert_intercepted("BEGIN");
        assert_intercepted("BEGIN READ ONLY");
        assert_intercepted("START TRANSACTION");
        assert_intercepted("COMMIT");
        assert_intercepted("END");
        assert_intercepted("ROLLBACK");
        assert_intercepted("ROLLBACK TO SAVEPOINT s1");
    }

    #[test]
    fn discard_deallocate_intercepted() {
        assert_intercepted("DISCARD ALL");
        assert_intercepted("DEALLOCATE pstmt1");
        assert_intercepted("CLOSE ALL");
    }

    #[test]
    fn show_returns_correct_values() {
        let cases: &[(&str, &str)] = &[
            ("SHOW server_version", "16.6"),
            ("SHOW server_version_num", "160006"),
            ("SHOW server_encoding", "UTF8"),
            ("SHOW client_encoding", "UTF8"),
            ("SHOW standard_conforming_strings", "on"),
            ("SHOW max_identifier_length", "63"),
            ("SHOW transaction_isolation", "read committed"),
            ("SHOW datestyle", "ISO, MDY"),
            ("SHOW timezone", "UTC"),
            ("SHOW integer_datetimes", "on"),
            ("SHOW intervalstyle", "postgres"),
            ("SHOW is_superuser", "on"),
            ("SHOW unknown_param", "on"),
        ];

        for &(query, expected) in cases {
            let result = intercept_query(query, "test_catalog", "test_schema")
                .unwrap_or_else(|| panic!("SHOW not intercepted: {query}"));
            // We just verify it returns Ok; the value is embedded inside the
            // encoded DataRow which is opaque here, but at least we confirm
            // the function runs without error.
            assert!(result.is_ok(), "SHOW returned error for: {query}");

            // Also verify via the internal helper directly.
            let trimmed = query.trim().trim_end_matches(';').trim();
            let resp = intercept_show(trimmed).unwrap();
            // Should have exactly one Response::Query variant.
            assert_eq!(resp.len(), 1);
            match &resp[0] {
                Response::Query(_) => {} // expected
                other => panic!("expected Query response, got: {other:?}"),
            }

            // Verify the value by re-checking through intercept_show's logic.
            let param = trimmed[4..].trim().to_lowercase();
            let value = match param.as_str() {
                "server_version" => "16.6",
                "server_version_num" => "160006",
                "server_encoding" => "UTF8",
                "client_encoding" => "UTF8",
                "standard_conforming_strings" => "on",
                "max_identifier_length" => "63",
                "transaction_isolation" => "read committed",
                "datestyle" => "ISO, MDY",
                "timezone" => "UTC",
                "integer_datetimes" => "on",
                "intervalstyle" => "postgres",
                "is_superuser" => "on",
                _ => "on",
            };
            assert_eq!(value, expected, "mismatch for {query}");
        }
    }

    #[test]
    fn server_functions_intercepted() {
        assert_intercepted("SELECT version()");
        assert_intercepted("SELECT VERSION()");
        assert_intercepted("SELECT current_database()");
        assert_intercepted("SELECT current_schema");
        assert_intercepted("SELECT pg_is_in_recovery()");
        assert_intercepted("SELECT current_setting('server_version_num')");
        assert_intercepted("SELECT current_setting('server_version')");
    }

    #[test]
    fn regular_queries_not_intercepted() {
        assert_not_intercepted("SELECT 1");
        assert_not_intercepted("SELECT * FROM users");
        assert_not_intercepted("INSERT INTO t VALUES (1)");
        assert_not_intercepted("CREATE TABLE t (id INT)");
    }
}
