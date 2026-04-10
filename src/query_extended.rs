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
use trino_rust_client::Client as TrinoClient;

use crate::config::Config;
use crate::query_pipeline::process_query;

/// Handles the extended query protocol (Parse/Bind/Describe/Execute).
///
/// The JDBC PostgreSQL driver (pgjdbc, used by Power BI) always does
/// Describe Portal before Execute. Our `do_describe_portal` runs the
/// query pipeline to return the real column schema. Then `do_query`
/// (called by the default `on_execute`) runs it again to get the data.
///
/// We do NOT override `on_execute` — pgwire's default sends DataRow
/// without RowDescription (`send_describe: false`), which is correct
/// because the client already received RowDescription from Describe Portal.
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

        let trino_client: Arc<TrinoClient> = client
            .session_extensions()
            .get::<TrinoClient>()
            .ok_or_else(|| PgWireError::ApiError("No Trino client in session".into()))?;

        let config: Arc<Config> = client
            .session_extensions()
            .get::<Config>()
            .ok_or_else(|| PgWireError::ApiError("No Config in session".into()))?;

        let responses = process_query(query, &trino_client, &config).await?;
        responses
            .into_iter()
            .next()
            .ok_or_else(|| PgWireError::ApiError("Empty pipeline response".into()))
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

        let trino_client: Arc<TrinoClient> = client
            .session_extensions()
            .get::<TrinoClient>()
            .ok_or_else(|| PgWireError::ApiError("No Trino client in session".into()))?;

        let config: Arc<Config> = client
            .session_extensions()
            .get::<Config>()
            .ok_or_else(|| PgWireError::ApiError("No Config in session".into()))?;

        let responses = process_query(query, &trino_client, &config).await?;
        let fields = match responses.into_iter().next() {
            Some(Response::Query(qr)) => qr.row_schema.as_ref().clone(),
            _ => vec![], // DDL/DML — no columns
        };

        Ok(DescribePortalResponse::new(fields))
    }
}
