use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Sink;
use pgwire::api::results::{QueryResponse, Response};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use trino_rust_client::Client as TrinoClient;

use pgwire::api::query::SimpleQueryHandler;

use crate::trino_stream::execute_trino_query;

/// Handles simple query protocol messages.
#[derive(Debug)]
pub struct GatewayQueryHandler;

#[async_trait]
impl SimpleQueryHandler for GatewayQueryHandler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        tracing::debug!(query, "Received query");

        // Static catalog interception (pg_type, pg_enum, pg_range, pg_namespace, etc.)
        if let Some(result) = crate::intercept::intercept_query(query) {
            return result;
        }

        let trino_client: Arc<TrinoClient> = client
            .session_extensions()
            .get::<TrinoClient>()
            .ok_or_else(|| PgWireError::ApiError("No Trino client in session".into()))?;

        // Dynamic catalog interception (pg_class, pg_attribute — needs Trino client)
        if let Some(result) =
            crate::catalog::handle_dynamic_catalog_query(query, &trino_client).await
        {
            return result;
        }

        let rewritten = crate::rewrite::rewrite_sql(query);
        tracing::debug!(original = query, rewritten = %rewritten, "Rewritten query");

        let (schema, row_stream) =
            execute_trino_query(&trino_client, rewritten).await?;

        Ok(vec![Response::Query(QueryResponse::new(schema, row_stream))])
    }
}
