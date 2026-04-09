use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Sink;
use pgwire::api::results::Response;
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use trino_rust_client::Client as TrinoClient;

use pgwire::api::query::SimpleQueryHandler;

use crate::config::Config;
use crate::query_pipeline::process_query;

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

        let trino_client: Arc<TrinoClient> = client
            .session_extensions()
            .get::<TrinoClient>()
            .ok_or_else(|| PgWireError::ApiError("No Trino client in session".into()))?;

        let config: Arc<Config> = client
            .session_extensions()
            .get::<Config>()
            .ok_or_else(|| PgWireError::ApiError("No Config in session".into()))?;

        process_query(query, &trino_client, &config).await
    }
}
