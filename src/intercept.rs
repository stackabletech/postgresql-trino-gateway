// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0
use std::sync::Arc;

use futures::stream;
use pgwire::api::Type;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;

use crate::catalog::text_field;
use crate::query_inspection::ParsedQuery;

/// Build a single-column, single-row VARCHAR text response.
fn single_text_response(column_name: &str, value: &str) -> PgWireResult<Vec<Response>> {
    let fields = Arc::new(vec![text_field(column_name, Type::VARCHAR)]);

    let mut encoder = DataRowEncoder::new(Arc::clone(&fields));
    encoder.encode_field(&value)?;
    let row = encoder.take_row();

    Ok(vec![Response::Query(QueryResponse::new(
        fields,
        stream::iter(vec![Ok(row)]),
    ))])
}

/// If the query can be answered locally instead of forwarded to Trino,
/// build the response and return it. Returns `None` for queries that
/// should pass through to Trino.
///
/// `SET`, `DISCARD`, and `DEALLOCATE` are matched by raw keyword prefix
/// because sqlparser's `PostgreSqlDialect` either doesn't model them or
/// does so partially. `BEGIN`/`COMMIT`/`ROLLBACK` *are* modelled as
/// `Statement::StartTransaction`/`Commit`/`Rollback` and could be detected
/// via `ParsedQuery`, but the prefix path is simpler and consistent with
/// the SET/DISCARD case. The AST-based checks below (`references_table`,
/// `calls_function`) handle the cases where prefix matching would
/// misroute a user query that *contains* a catalog name as a literal or
/// column reference.
pub fn try_intercept(
    query: &str,
    parsed_query: &ParsedQuery,
    catalog: &str,
    schema: &str,
) -> Option<PgWireResult<Vec<Response>>> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return None;
    }

    // Strip a trailing `;` so the keyword-prefix matches below (e.g.
    // `upper.starts_with("SET ")`) work whether or not the client sent
    // a terminator. The AST checks (which use `parsed_query`) operate on
    // the original unmodified string and don't need this.
    let trimmed = trimmed.trim_end_matches(';').trim();
    let upper = trimmed.to_uppercase();

    if upper.starts_with("SET ") {
        tracing::trace!(query = trimmed, "Intercept: SET");
        return Some(Ok(vec![Response::Execution(Tag::new("SET"))]));
    }

    // Transaction commands. Trino has no client-managed transactions, so
    // BEGIN/COMMIT/ROLLBACK are silently acknowledged as no-ops. README
    // documents this for users who expect real transactional semantics.
    let txn_tag = if upper == "BEGIN"
        || upper.starts_with("BEGIN ")
        || upper.starts_with("START TRANSACTION")
    {
        Some("BEGIN")
    } else if upper == "COMMIT" || upper == "END" {
        Some("COMMIT")
    } else if upper == "ROLLBACK" || upper.starts_with("ROLLBACK ") {
        Some("ROLLBACK")
    } else {
        None
    };
    if let Some(tag) = txn_tag {
        tracing::trace!(query = trimmed, "Intercept: transaction");
        return Some(Ok(vec![Response::Execution(Tag::new(tag))]));
    }

    if upper.starts_with("DISCARD ") || upper.starts_with("DEALLOCATE ") || upper == "CLOSE ALL" {
        tracing::trace!(query = trimmed, "Intercept: session command");
        return Some(Ok(vec![Response::Execution(Tag::new("OK"))]));
    }

    if upper.starts_with("SHOW ") {
        tracing::trace!(query = trimmed, "Intercept: SHOW");
        return Some(intercept_show(trimmed));
    }

    if let Some(resp) = intercept_server_functions(parsed_query, catalog, schema) {
        tracing::trace!(query = trimmed, "Intercept: server function");
        return Some(resp);
    }

    if let Some(resp) = crate::catalog::handle_catalog_query(parsed_query) {
        tracing::trace!(query = trimmed, "Intercept: pg_catalog");
        return Some(resp);
    }

    // Power BI confirms its client encoding via INFORMATION_SCHEMA.character_sets;
    // real PostgreSQL returns one row, "UTF8".
    if parsed_query.references_table_in_schema("information_schema", "character_sets") {
        tracing::trace!(query = trimmed, "Intercept: CHARACTER_SETS");
        return Some(single_text_response("character_set_name", "UTF8"));
    }

    // information_schema tables Trino doesn't expose — return an empty
    // result with the right column shape so Power BI's relationship
    // discovery proceeds.
    if let Some(resp) = crate::info_schema::intercept_missing_information_schema(parsed_query) {
        return Some(resp);
    }

    None
}

fn intercept_show(trimmed: &str) -> PgWireResult<Vec<Response>> {
    let param = trimmed[4..].trim().to_lowercase();
    let value = crate::startup::SERVER_PARAMS
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(&param))
        .map(|(_, v)| *v)
        .unwrap_or("on"); // safe default for unknown params

    single_text_response(&param, value)
}

/// Intercept bare scalar-function probe queries that PG clients send during
/// session setup (`SELECT version()`, `SELECT current_schema`, etc.).
///
/// We only intercept when the query is `SELECT <single-call> [AS alias]` with
/// no FROM, WHERE, or other clauses; otherwise a column reference like
/// `WHERE current_schema = 'public'` or a multi-projection query like
/// `SELECT current_setting('a'), current_setting('b')` would be misrouted to
/// a single-row stub response.
fn intercept_server_functions(
    query: &ParsedQuery,
    catalog: &str,
    schema: &str,
) -> Option<PgWireResult<Vec<Response>>> {
    if !query.is_bare_scalar_select() {
        return None;
    }

    // TOOD: We're impersonating PostgreSQL 16 - are there any protocol changes for 17 or 18 and why did you pick 16? Do we rely on pgwire or sqlparser for version support?
    if query.calls_function("version") {
        return Some(single_text_response(
            "version",
            "PostgreSQL 16.6 on x86_64-pc-linux-gnu, compiled by gcc 12.2.0, 64-bit",
        ));
    }

    if query.calls_function("current_database") {
        return Some(single_text_response("current_database", catalog));
    }

    // `current_schema` is idiomatically written without parens in PostgreSQL.
    // Inside a bare scalar SELECT it can't be a column reference.
    if query.calls_function_or_keyword("current_schema") {
        return Some(single_text_response("current_schema", schema));
    }

    if query.calls_function("pg_is_in_recovery") {
        return Some(single_text_response("pg_is_in_recovery", "false"));
    }

    if let Some(setting) = query.function_string_arg("current_setting") {
        let value = match setting.as_str() {
            "server_version_num" => Some("160006"),
            "server_version" => Some("16.6"),
            _ => None,
        };
        if let Some(v) = value {
            return Some(single_text_response("current_setting", v));
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_intercepted(query: &str) {
        let parsed_query = ParsedQuery::new(query);
        assert!(
            try_intercept(query, &parsed_query, "test_catalog", "test_schema").is_some(),
            "expected query to be intercepted: {query}"
        );
    }

    fn assert_not_intercepted(query: &str) {
        let parsed_query = ParsedQuery::new(query);
        assert!(
            try_intercept(query, &parsed_query, "test_catalog", "test_schema").is_none(),
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
            let parsed_query = ParsedQuery::new(query);
            let result = try_intercept(query, &parsed_query, "test_catalog", "test_schema")
                .unwrap_or_else(|| panic!("SHOW not intercepted: {query}"));
            assert!(result.is_ok(), "SHOW returned error for: {query}");

            let trimmed = query.trim().trim_end_matches(';').trim();
            let resp = intercept_show(trimmed).unwrap();
            assert_eq!(resp.len(), 1);
            match &resp[0] {
                Response::Query(_) => {}
                other => panic!("expected Query response, got: {other:?}"),
            }

            // Cross-check against SERVER_PARAMS, the single source of truth.
            let param = trimmed[4..].trim().to_lowercase();
            let from_table = crate::startup::SERVER_PARAMS
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(&param))
                .map(|(_, v)| *v)
                .unwrap_or("on");
            assert_eq!(from_table, expected, "mismatch for {query}");
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

    /// Power BI's PK-discovery query has unaliased qualified columns like
    /// `ii.COLUMN_NAME`. PostgreSQL strips the qualifier in the result's
    /// RowDescription so the client sees `COLUMN_NAME`. If we return
    /// `ii.COLUMN_NAME` instead, Power BI's `RetrieveKeysForTable` looks up
    /// `COLUMN_NAME`, gets null, and throws NullReferenceException.
    #[test]
    fn powerbi_index_query_column_names_are_unqualified() {
        let query = "select i.CONSTRAINT_SCHEMA || '_' || i.CONSTRAINT_NAME as INDEX_NAME, \
             ii.COLUMN_NAME, ii.ORDINAL_POSITION, \
             case when i.CONSTRAINT_TYPE = 'PRIMARY KEY' then 'Y' else 'N' end as PRIMARY_KEY \
             from INFORMATION_SCHEMA.table_constraints i \
             inner join INFORMATION_SCHEMA.key_column_usage ii \
                 on i.CONSTRAINT_SCHEMA = ii.CONSTRAINT_SCHEMA \
             where i.TABLE_SCHEMA = 'sf1' and i.TABLE_NAME = 'nation'";

        let cols = ParsedQuery::new(query).select_column_names();
        assert_eq!(
            cols,
            vec![
                "INDEX_NAME",
                "COLUMN_NAME",
                "ORDINAL_POSITION",
                "PRIMARY_KEY"
            ],
            "column names must be unqualified to match PostgreSQL behavior"
        );
    }

    /// Regression: a literal containing `pg_type` must not match the catalog
    /// dispatch. This is the bug fix that motivated `query_inspection`.
    #[test]
    fn user_query_with_pg_type_in_literal_not_intercepted() {
        assert_not_intercepted("SELECT * FROM customers WHERE notes LIKE '%pg_type%'");
        assert_not_intercepted("SELECT 'pg_type' AS sentinel");
    }

    /// Regression: a column named `version` in a SELECT must not trigger the
    /// version() server-function intercept.
    #[test]
    fn user_query_with_version_column_not_intercepted() {
        assert_not_intercepted("SELECT version FROM releases");
    }
}
