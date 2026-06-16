// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0
use std::fmt::Debug;

use async_trait::async_trait;
use futures::Sink;
use pgwire::api::ClientInfo;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::Response;
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;

use crate::query_pipeline::process_query;
use crate::session::ConnectionState;

#[derive(Debug)]
pub struct GatewayQueryHandler;

#[async_trait]
impl SimpleQueryHandler for GatewayQueryHandler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        tracing::debug!(query, "Simple query received");

        let conn_state = client
            .session_extensions()
            .get::<ConnectionState>()
            .ok_or_else(|| PgWireError::ApiError("Connection state not found".into()))?;

        // The simple-query protocol always uses text wire format; it never
        // negotiates per-column binary results.
        let result = process_query(
            query,
            &conn_state.trino_client,
            &conn_state.config,
            Some(&conn_state.active_query_id),
            None,
        )
        .await;
        match &result {
            Ok(responses) => {
                tracing::trace!(response_count = responses.len(), "Simple query processed")
            }
            Err(e) => tracing::debug!(error = %e, "Simple query failed"),
        }
        result
    }
}
