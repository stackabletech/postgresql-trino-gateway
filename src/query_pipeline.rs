use std::sync::Arc;

use pgwire::api::results::{QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;
use trino_rust_client::Client as TrinoClient;

use crate::config::Config;
use crate::trino_stream::execute_trino_query;

/// Core query processing pipeline: intercept -> catalog -> rewrite -> execute.
/// Returns Vec<Response> (one for DDL/DML Execution, one for SELECT Query).
pub(crate) async fn process_query(
    query: &str,
    trino_client: &Arc<TrinoClient>,
    config: &Arc<Config>,
) -> PgWireResult<Vec<Response>> {
    // Static catalog interception (pg_type, pg_enum, pg_range, pg_namespace, etc.)
    if let Some(result) =
        crate::intercept::intercept_query(query, &config.trino_catalog, &config.trino_schema)
    {
        return result;
    }

    // Dynamic catalog interception (pg_class, pg_attribute -- needs Trino client)
    if let Some(result) = crate::catalog::handle_dynamic_catalog_query(query, trino_client).await {
        return result;
    }

    let rewritten = crate::rewrite::rewrite_sql(query);
    tracing::debug!(original = query, rewritten = %rewritten, "Rewritten query");

    let (schema, row_stream) = execute_trino_query(trino_client, rewritten).await?;

    if schema.is_empty() {
        // DDL/DML -- no result set
        Ok(vec![Response::Execution(Tag::new("OK"))])
    } else {
        Ok(vec![Response::Query(QueryResponse::new(
            schema, row_stream,
        ))])
    }
}
