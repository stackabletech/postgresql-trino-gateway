// Copyright 2026 Stackable GmbH
// Licensed under the Open Software License version 3.0 (OSL-3.0).
// See LICENSE file in the project root for full license text.
use std::sync::Arc;

use pgwire::api::results::{QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use trino_rust_client::Client as TrinoClient;

use crate::config::Config;
use crate::query_inspection::ParsedQuery;
use crate::trino_stream::execute_trino_query;

/// Core query processing pipeline.
///
/// PostgreSQL's simple-query protocol allows a single message to carry
/// multiple statements separated by semicolons; the server is expected to
/// run each statement and reply with a `CommandComplete` per statement.
/// Trino's REST endpoint accepts only a single statement per request, so we
/// split here and route each one through the per-statement pipeline
/// (intercept -> catalog -> rewrite -> execute), returning one `Response`
/// per statement.
///
/// Quote and comment handling is delegated to `sqlparser`, so a semicolon
/// inside a string literal (`SET foo = 'a;b'`) does not cause a split. When
/// the parser cannot tokenise the SQL (`DISCARD ALL`, dialect-only DDL,
/// etc.), fall back to processing the original string as one statement —
/// matching the rewriter's existing parse-failure behaviour.
pub(crate) async fn process_query(
    query: &str,
    trino_client: &Arc<TrinoClient>,
    config: &Arc<Config>,
) -> PgWireResult<Vec<Response>> {
    tracing::trace!(query, "Pipeline: enter");

    let pieces = split_statements(query);
    if pieces.len() <= 1 {
        return process_single_statement(query, trino_client, config).await;
    }

    tracing::trace!(count = pieces.len(), "Pipeline: multi-statement input");
    let mut out = Vec::with_capacity(pieces.len());
    for stmt in &pieces {
        let mut responses = process_single_statement(stmt, trino_client, config).await?;
        out.append(&mut responses);
    }
    Ok(out)
}

/// Split `query` into individual statement strings via `sqlparser`. Returns
/// a single-element vector wrapping the original input when the parser
/// cannot tokenise the SQL (matching the rewriter's passthrough behaviour)
/// or when the input is already a single statement.
fn split_statements(query: &str) -> Vec<String> {
    match Parser::parse_sql(&PostgreSqlDialect {}, query) {
        Ok(stmts) if stmts.len() > 1 => stmts.into_iter().map(|s| s.to_string()).collect(),
        _ => vec![query.to_owned()],
    }
}

/// Process exactly one statement. The multi-statement entrypoint above is
/// responsible for splitting; this function never recurses on its input.
async fn process_single_statement(
    query: &str,
    trino_client: &Arc<TrinoClient>,
    config: &Arc<Config>,
) -> PgWireResult<Vec<Response>> {
    let inspect = ParsedQuery::new(query);

    // Static catalog interception (pg_type, pg_enum, pg_range, pg_namespace, etc.)
    if let Some(result) = crate::intercept::intercept_query(
        query,
        &inspect,
        &config.trino_catalog,
        &config.trino_schema,
    ) {
        tracing::trace!("Pipeline: static intercept matched");
        return result;
    }

    // Dynamic catalog interception (pg_class, pg_attribute -- needs Trino client)
    if let Some(result) = crate::catalog::handle_dynamic_catalog_query(&inspect, trino_client).await
    {
        tracing::trace!("Pipeline: dynamic catalog matched");
        return result;
    }

    // Rewrite INFORMATION_SCHEMA.columns DATA_TYPE to PostgreSQL-style type names.
    let rewritten_columns = crate::intercept::rewrite_info_schema_columns(query, &inspect);
    if rewritten_columns.is_some() {
        tracing::trace!("Pipeline: rewrote INFORMATION_SCHEMA.columns");
    }
    let query = rewritten_columns
        .map(std::borrow::Cow::Owned)
        .unwrap_or(std::borrow::Cow::Borrowed(query));
    let query: &str = query.as_ref();

    let rewritten = crate::rewrite::rewrite_sql(query);
    if rewritten != query {
        tracing::trace!(trino_sql = %rewritten, "Pipeline: SQL rewritten for Trino");
    }
    tracing::debug!(original = query, rewritten = %rewritten, "Rewritten query");

    let (schema, row_stream) = execute_trino_query(trino_client, rewritten).await?;

    if schema.is_empty() {
        tracing::trace!("Pipeline: Trino returned no schema — treating as DDL/DML");
        // DDL/DML -- no result set
        Ok(vec![Response::Execution(Tag::new("OK"))])
    } else {
        tracing::trace!(
            columns = ?schema.iter().map(|f| f.name()).collect::<Vec<&str>>(),
            "Pipeline: Trino returned schema"
        );
        Ok(vec![Response::Query(QueryResponse::new(
            schema, row_stream,
        ))])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_statements_preserves_single_select() {
        let pieces = split_statements("SELECT 1");
        assert_eq!(pieces.len(), 1);
    }

    #[test]
    fn split_statements_separates_two_selects() {
        let pieces = split_statements("SELECT 1; SELECT 2");
        assert_eq!(pieces.len(), 2);
        assert!(pieces[0].to_uppercase().contains("SELECT"));
        assert!(pieces[1].to_uppercase().contains("SELECT"));
    }

    #[test]
    fn split_statements_separates_begin_select_commit() {
        let pieces = split_statements("BEGIN; SELECT 1; COMMIT");
        assert_eq!(pieces.len(), 3);
    }

    #[test]
    fn split_statements_does_not_split_on_semicolon_inside_literal() {
        // The semicolon is inside a single-quoted string and must not cause
        // a split. sqlparser parses this as one SetVariable statement.
        let pieces = split_statements("SET application_name = 'a; b; c'");
        assert_eq!(pieces.len(), 1);
        assert!(pieces[0].contains("a; b; c"));
    }

    #[test]
    fn split_statements_falls_back_on_parse_failure() {
        // DISCARD is parsed by sqlparser; pick something it doesn't model.
        // If sqlparser ever learns the syntax, this test still passes — the
        // contract is "single-element vec on parse failure or single stmt."
        let pieces = split_statements("LISTEN my_channel");
        assert_eq!(pieces.len(), 1);
    }

    #[test]
    fn split_statements_handles_empty_input() {
        let pieces = split_statements("");
        assert_eq!(pieces.len(), 1);
    }

    #[test]
    fn split_statements_separates_multiple_set_commands() {
        let pieces = split_statements("SET a = '1'; SET b = '2'");
        assert_eq!(pieces.len(), 2);
    }
}
