// Copyright 2026 Stackable GmbH
// Licensed under the Open Software License version 3.0 (OSL-3.0).
// See LICENSE file in the project root for full license text.
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};
use pgwire::api::PgWireConnectionState;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, send_execution_response, send_query_response};
use pgwire::api::results::{DescribePortalResponse, DescribeStatementResponse, Response};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, DEFAULT_NAME, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;

use crate::query_pipeline::process_query;
use crate::session;

/// Handles the extended query protocol (Parse/Bind/Describe/Execute).
///
/// Power BI uses Npgsql 4.0.17 which pipelines multiple Parse/Bind/Describe/Execute
/// sequences for type loading. We override `on_execute` to always include
/// RowDescription with the data rows (`send_describe: true`). This is critical
/// because Npgsql can lose track of result set boundaries in pipelined responses,
/// causing "Field not found" errors when it reads the next result set's schema
/// instead of the current one.
#[derive(Debug)]
pub struct GatewayExtendedQueryHandler;

#[async_trait]
impl ExtendedQueryHandler for GatewayExtendedQueryHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(NoopQueryParser)
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let query = &portal.statement.statement;
        tracing::debug!(query, "Extended query execute");

        let conn_id = client
            .metadata()
            .get(session::connection_id_key())
            .ok_or_else(|| PgWireError::ApiError("No connection ID in metadata".into()))?
            .clone();
        let conn_state = session::get_connection(&conn_id)
            .ok_or_else(|| PgWireError::ApiError("Connection state not found".into()))?;
        let trino_client = Arc::clone(&conn_state.trino_client);
        let config = Arc::clone(&conn_state.config);
        drop(conn_state);

        let responses = process_query(query, &trino_client, &config).await?;
        responses
            .into_iter()
            .next()
            .ok_or_else(|| PgWireError::ApiError("Empty pipeline response".into()))
    }

    /// Always send RowDescription with Execute responses.
    ///
    /// pgwire's default `on_execute` uses `send_describe: false`, relying on
    /// the client to cache the schema from Describe Portal. But Npgsql 4.0.17
    /// can misparse pipelined responses and lose the cached schema. Sending
    /// RowDescription redundantly is safe per the PG protocol spec.
    async fn on_execute<C>(
        &self,
        client: &mut C,
        message: pgwire::messages::extendedquery::Execute,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let portal_name = message.name.as_deref().unwrap_or(DEFAULT_NAME);
        let max_rows = message.max_rows as usize;

        // For the common case (first execution, fetch all rows), handle it
        // ourselves with send_describe=true. For partial fetch or resumed
        // portals, delegate to pgwire's default.
        let portal = client
            .portal_store()
            .get_portal(portal_name)
            .ok_or_else(|| PgWireError::PortalNotFound(portal_name.to_owned()))?;

        let is_initial = {
            let state = portal.state();
            let guard = state.lock().await;
            matches!(*guard, pgwire::api::portal::PortalExecutionState::Initial)
        };

        if is_initial && max_rows == 0 {
            if !matches!(client.state(), PgWireConnectionState::ReadyForQuery) {
                return Err(PgWireError::NotReadyForQuery);
            }
            let mut transaction_status = client.transaction_status();
            client.set_state(PgWireConnectionState::QueryInProgress);

            match self.do_query(client, portal.as_ref(), max_rows).await? {
                Response::Query(results) => {
                    let col_names: Vec<&str> =
                        results.row_schema.iter().map(|f| f.name()).collect();
                    tracing::trace!(
                        columns = ?col_names,
                        "Extended query execute: sending RowDescription + DataRows"
                    );
                    send_query_response(client, results, true).await?;
                }
                Response::Execution(tag) => {
                    tracing::trace!("Extended query execute: sending Execution tag");
                    send_execution_response(client, tag).await?;
                }
                Response::Error(err) => {
                    tracing::trace!(error = %err, "Extended query execute: sending Error");
                    client
                        .send(PgWireBackendMessage::ErrorResponse((*err).into()))
                        .await?;
                    transaction_status = transaction_status.to_error_state();
                }
                _ => {} // EmptyQuery, Transaction, Copy — rare for Trino gateway
            }

            // Do NOT set portal state to Finished or remove the portal.
            // Npgsql pipelines multiple Parse/Bind/Execute sequences reusing
            // the unnamed portal. Each Parse/Bind overwrites the previous one.
            // pgwire's Sync handler cleans up portals at the end of the pipeline.

            client.set_state(PgWireConnectionState::ReadyForQuery);
            client.set_transaction_status(transaction_status);

            Ok(())
        } else {
            self._on_execute(client, message).await
        }
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let param_types = stmt
            .parameter_types
            .iter()
            .map(|t| t.clone().unwrap_or(Type::TEXT))
            .collect();
        Ok(DescribeStatementResponse::new(param_types, vec![]))
    }

    async fn do_describe_portal<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // Run the full query pipeline to extract the real column schema.
        // This is called by pgjdbc BEFORE Execute, so the client gets
        // RowDescription from here and then DataRow from Execute.
        let query = &portal.statement.statement;
        tracing::debug!(query, "Extended query describe portal");

        let conn_id = client
            .metadata()
            .get(session::connection_id_key())
            .ok_or_else(|| PgWireError::ApiError("No connection ID in metadata".into()))?
            .clone();
        let conn_state = session::get_connection(&conn_id)
            .ok_or_else(|| PgWireError::ApiError("Connection state not found".into()))?;
        let trino_client = Arc::clone(&conn_state.trino_client);
        let config = Arc::clone(&conn_state.config);
        drop(conn_state);

        let responses = process_query(query, &trino_client, &config).await?;
        let fields = match responses.into_iter().next() {
            Some(Response::Query(qr)) => {
                let cols: Vec<&str> = qr.row_schema.iter().map(|f| f.name()).collect();
                tracing::trace!(columns = ?cols, "Describe portal: returning RowDescription");
                qr.row_schema.as_ref().clone()
            }
            _ => vec![], // DDL/DML — no columns
        };

        Ok(DescribePortalResponse::new(fields))
    }
}
