// Copyright 2026 Stackable GmbH
// Licensed under the Open Software License version 3.0 (OSL-3.0).
// See LICENSE file in the project root for full license text.
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Sink;
use pgwire::api::portal::Portal;
use pgwire::api::query::ExtendedQueryHandler;
use pgwire::api::results::{DescribePortalResponse, DescribeStatementResponse, Response};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;

use crate::query_pipeline::process_query;
use crate::session;

/// Handles the extended query protocol (Parse/Bind/Describe/Execute).
///
/// Power BI uses Npgsql 4.0.17 which pipelines multiple Parse/Bind/Describe/Execute
/// sequences for type loading. We rely on pgwire's default `on_execute` which sends
/// DataRow WITHOUT RowDescription (`send_describe: false`). Npgsql expects this —
/// it gets RowDescription from Describe Portal and rejects a second one during Execute
/// with "Received unexpected backend message RowDescription".
///
/// Our `do_describe_portal` runs the full query pipeline to return the real column
/// schema. This means Trino queries execute twice (Describe + Execute), but ensures
/// the client always has the correct schema before data arrives.
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

        let conn_id = client
            .metadata()
            .get(session::connection_id_key())
            .ok_or_else(|| PgWireError::ApiError("No connection ID in metadata".into()))?
            .clone();
        tracing::debug!(conn_id = %conn_id, query, "Extended query execute");

        let conn_state = session::get_connection(&conn_id)
            .ok_or_else(|| PgWireError::ApiError("Connection state not found".into()))?;
        let trino_client = Arc::clone(&conn_state.trino_client);
        let config = Arc::clone(&conn_state.config);
        drop(conn_state);

        let responses = process_query(query, &trino_client, &config).await?;
        let response = responses
            .into_iter()
            .next()
            .ok_or_else(|| PgWireError::ApiError("Empty pipeline response".into()))?;
        match &response {
            pgwire::api::results::Response::Query(qr) => {
                tracing::trace!(conn_id = %conn_id, columns = qr.row_schema.len(), "Extended query execute: query response");
            }
            pgwire::api::results::Response::Execution(_tag) => {
                tracing::trace!(conn_id = %conn_id, "Extended query execute: execution response");
            }
            _ => {}
        }
        Ok(response)
    }

    // No on_execute override — pgwire's default sends DataRow WITHOUT
    // RowDescription (send_describe=false), which is correct for Npgsql 4.0.17.
    // Npgsql gets RowDescription from Describe Portal and expects Execute to
    // send only DataRow + CommandComplete.

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
        tracing::trace!(statement = %stmt.statement, "Describe statement");
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

        let conn_id = client
            .metadata()
            .get(session::connection_id_key())
            .ok_or_else(|| PgWireError::ApiError("No connection ID in metadata".into()))?
            .clone();
        tracing::debug!(conn_id = %conn_id, query, "Extended query describe portal");
        let conn_state = session::get_connection(&conn_id)
            .ok_or_else(|| PgWireError::ApiError("Connection state not found".into()))?;
        let trino_client = Arc::clone(&conn_state.trino_client);
        let config = Arc::clone(&conn_state.config);
        drop(conn_state);

        let responses = process_query(query, &trino_client, &config).await?;
        let fields = match responses.into_iter().next() {
            Some(Response::Query(qr)) => {
                tracing::trace!(
                    columns = ?qr.row_schema.iter().map(|f| f.name()).collect::<Vec<&str>>(),
                    "Describe portal: returning RowDescription"
                );
                qr.row_schema.as_ref().clone()
            }
            _ => vec![], // DDL/DML — no columns
        };

        Ok(DescribePortalResponse::new(fields))
    }
}
