// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Sink;
use pgwire::api::results::Response;
use pgwire::api::{ClientInfo, ClientPortalStore};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;

use pgwire::api::query::SimpleQueryHandler;

use crate::query_pipeline::process_query;
use crate::session;

#[derive(Debug)]
pub struct GatewayQueryHandler;

#[async_trait]
impl SimpleQueryHandler for GatewayQueryHandler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        tracing::debug!(query, "Simple query received");

        let conn_id = client
            .metadata()
            .get(session::CONNECTION_ID_KEY)
            .ok_or_else(|| PgWireError::ApiError("No connection ID in metadata".into()))?
            .clone();
        let conn_state = session::get_connection(&conn_id)
            .ok_or_else(|| PgWireError::ApiError("Connection state not found".into()))?;
        let trino_client = Arc::clone(&conn_state.trino_client);
        let config = Arc::clone(&conn_state.config);
        let active_query_id = Arc::clone(&conn_state.active_query_id);
        // `conn_state` is a `dashmap::Ref` guard holding a read-lock on its
        // shard; the guard is not `Send`, so it cannot be held across the
        // `process_query(...).await` below. Drop it explicitly after we've
        // cloned out the Arcs we need; otherwise the borrow-checker rejects
        // the await.
        drop(conn_state);

        let result = process_query(query, &trino_client, &config, Some(&active_query_id)).await;
        match &result {
            Ok(responses) => tracing::trace!(
                conn_id = %conn_id,
                response_count = responses.len(),
                "Simple query processed"
            ),
            Err(e) => tracing::debug!(conn_id = %conn_id, error = %e, "Simple query failed"),
        }
        result
    }
}
