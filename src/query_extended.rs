use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, send_query_response};
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
/// Npgsql and other drivers use this for all parameterized queries. Power BI
/// DirectQuery generates queries like `SELECT "col" FROM "table" WHERE "col" = $1::text`.
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

    /// Override on_execute to always send RowDescription with query results.
    ///
    /// The default pgwire on_execute calls `send_query_response(client, results, false)`,
    /// skipping RowDescription. Npgsql and some JDBC drivers skip Describe Portal and go
    /// straight to Execute, so they need RowDescription included with the data.
    ///
    /// We delegate to the default `_on_execute` for most of the protocol handling, then
    /// intercept `Response::Query` to send RowDescription. For non-Query responses
    /// (Execution, EmptyQuery, etc.), the default behavior is correct.
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
        // We can't easily patch _on_execute because it uses pgwire-internal types
        // (EmptyQueryResponse, NoData) that we can't construct from outside the crate.
        //
        // Instead, we intercept at the do_query level: run the query ourselves,
        // and if it's a Query response, send it with RowDescription included.
        // For all other response types, delegate to the default implementation.

        if !matches!(
            client.state(),
            pgwire::api::PgWireConnectionState::ReadyForQuery
        ) {
            return Err(PgWireError::NotReadyForQuery);
        }

        let portal_name = message.name.as_deref().unwrap_or("");
        let max_rows = message.max_rows as usize;

        let portal = client
            .portal_store()
            .get_portal(portal_name)
            .ok_or_else(|| PgWireError::PortalNotFound(portal_name.to_owned()))?;

        // Check if portal is in Initial state (first execution)
        let is_initial = {
            let state = portal.state();
            let guard = state.lock().await;
            matches!(*guard, pgwire::api::portal::PortalExecutionState::Initial)
        };

        if is_initial && max_rows == 0 {
            // This is the common case: first execution, fetch all rows.
            // We handle it ourselves to include RowDescription.
            client.set_state(pgwire::api::PgWireConnectionState::QueryInProgress);
            let mut transaction_status = client.transaction_status();

            match self.do_query(client, portal.as_ref(), max_rows).await? {
                Response::Query(results) => {
                    // KEY: send_describe=true so RowDescription is always sent
                    send_query_response(client, results, true).await?;
                }
                other => {
                    // For Execution, Error, Transaction, etc. — handle normally.
                    // We need to send the appropriate message ourselves since we
                    // took over the flow.
                    match other {
                        Response::Execution(tag) => {
                            pgwire::api::query::send_execution_response(client, tag).await?;
                        }
                        Response::Error(err) => {
                            client
                                .send(PgWireBackendMessage::ErrorResponse((*err).into()))
                                .await?;
                            transaction_status = transaction_status.to_error_state();
                        }
                        _ => {
                            // EmptyQuery, TransactionStart/End, Copy — fall through.
                            // These are rare in practice for a Trino gateway.
                        }
                    }
                }
            }

            // Mark portal as finished
            {
                let state = portal.state();
                let mut guard = state.lock().await;
                *guard = pgwire::api::portal::PortalExecutionState::Finished;
            }

            client.set_state(pgwire::api::PgWireConnectionState::ReadyForQuery);
            client.set_transaction_status(transaction_status);

            if portal_name.is_empty() {
                client.portal_store().rm_portal(portal_name);
            }

            Ok(())
        } else {
            // Partial fetch or resumed portal — delegate to default implementation
            // which handles Suspended/Finished states correctly.
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
        // Run the query pipeline to extract column metadata for clients that
        // do Describe Portal before Execute (DBeaver, some JDBC drivers).
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
            _ => vec![],
        };

        Ok(DescribePortalResponse::new(fields))
    }
}
