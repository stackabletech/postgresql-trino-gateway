// Copyright 2026 Stackable GmbH
// Licensed under the Open Software License version 3.0 (OSL-3.0).
// See LICENSE file in the project root for full license text.
use std::sync::Arc;

use pgwire::api::results::{QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;
use trino_rust_client::Client as TrinoClient;

use crate::config::Config;
use crate::query_inspection::ParsedQuery;
use crate::trino_stream::execute_trino_query;

/// Core query processing pipeline: intercept -> catalog -> rewrite -> execute.
/// Returns Vec<Response> (one for DDL/DML Execution, one for SELECT Query).
pub(crate) async fn process_query(
    query: &str,
    trino_client: &Arc<TrinoClient>,
    config: &Arc<Config>,
) -> PgWireResult<Vec<Response>> {
    tracing::trace!(query, "Pipeline: enter");

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
