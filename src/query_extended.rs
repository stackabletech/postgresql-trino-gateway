// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0
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
use crate::session::{self, CachedPortalResponse, MAX_CACHED_PORTALS, PortalCache};

/// Handles the extended query protocol (Parse/Bind/Describe/Execute).
///
/// # Background
///
/// PostgreSQL has two query protocols. The simple-query protocol (handled in
/// `query_simple.rs`) sends one `Query` message containing a SQL string and
/// gets back a result set; that's the path `psql` typically uses. The
/// extended-query protocol splits that into four messages so that drivers can
/// reuse a parsed statement, bind different parameters to it, and stream
/// large result sets:
///
/// - `Parse` — the client sends SQL text and an optional name; the server
///   parses it once into a *prepared statement*.
/// - `Bind` — the client supplies parameter values and an optional name;
///   the server creates a *portal*, which is a runnable instance of the
///   prepared statement with concrete parameters bound.
/// - `Describe` — the client asks for the column-shape (RowDescription) of
///   either a prepared statement or a portal, before fetching rows.
/// - `Execute` — the client tells the server to actually run the portal and
///   stream rows back.
///
/// The same prepared statement can be `Bind`-ed to many portals; the same
/// portal can be `Execute`-ed multiple times. Drivers (Npgsql, pgjdbc) use
/// this protocol to load type metadata at connection time and to support
/// real prepared statements at the application level.
///
/// For the full message-flow reference, see
/// <https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY>.
///
/// # Why the gateway needs `do_describe_portal` to run the pipeline
///
/// Power BI uses Npgsql 4.0.17 (released 2019), which pipelines multiple
/// Parse/Bind/Describe/Execute sequences during type loading. We rely on
/// pgwire's default `on_execute` (which sends DataRow WITHOUT
/// RowDescription, `send_describe: false`); Npgsql gets the RowDescription
/// from Describe Portal and rejects a second one during Execute with
/// "Received unexpected backend message RowDescription".
///
/// `do_describe_portal` runs the query pipeline to obtain the real column
/// schema and stashes the result in the per-connection `portals` map;
/// `do_query` takes the stashed response so the query runs against Trino
/// exactly once per Describe+Execute pair instead of twice. Critically this
/// means side-effecting statements (INSERT, UPDATE, CREATE) issued via
/// prepared statement run only once.
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
            .get(session::CONNECTION_ID_KEY)
            .ok_or_else(|| PgWireError::ApiError("No connection ID in metadata".into()))?
            .clone();
        tracing::debug!(conn_id = %conn_id, query, "Extended query execute");

        let conn_state = session::get_connection(&conn_id)
            .ok_or_else(|| PgWireError::ApiError("Connection state not found".into()))?;

        // Take any response stashed by do_describe_portal. If the cached
        // entry was generated for a different statement (the client re-bound
        // the portal name without an intervening Describe), discard it and
        // run the pipeline fresh.
        let cached_entry = take_cached(&conn_state.portals, &portal.name)?;
        if let Some(entry) = cached_entry {
            if entry.query == *query {
                tracing::trace!(conn_id = %conn_id, portal = %portal.name, "Extended query execute: served from describe cache");
                return Ok(entry.response);
            }
            tracing::debug!(
                conn_id = %conn_id,
                portal = %portal.name,
                "Discarding stale describe-cache entry — portal was re-bound"
            );
        }

        let trino_client = Arc::clone(&conn_state.trino_client);
        let config = Arc::clone(&conn_state.config);
        let active_query_id = Arc::clone(&conn_state.active_query_id);
        drop(conn_state);

        let responses =
            process_query(query, &trino_client, &config, Some(&active_query_id)).await?;
        let response = responses
            .into_iter()
            .next()
            .ok_or_else(|| PgWireError::ApiError("Empty pipeline response".into()))?;
        match &response {
            Response::Query(qr) => {
                tracing::trace!(
                    conn_id = %conn_id,
                    portal = %portal.name,
                    columns = qr.row_schema.len(),
                    "Extended query execute: query response (no cache)"
                );
            }
            Response::Execution(_tag) => {
                tracing::trace!(
                    conn_id = %conn_id,
                    portal = %portal.name,
                    "Extended query execute: execution response (no cache)"
                );
            }
            _ => {}
        }
        Ok(response)
    }

    // No on_execute override. pgwire's default sends DataRow WITHOUT
    // RowDescription (send_describe=false), which is what Npgsql 4.0.17
    // expects: it gets RowDescription from Describe Portal and Execute
    // should produce only DataRow + CommandComplete.

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
        // Run the pipeline once, return the schema, and stash the response so
        // do_query can serve Execute without re-running.
        let query = &portal.statement.statement;

        let conn_id = client
            .metadata()
            .get(session::CONNECTION_ID_KEY)
            .ok_or_else(|| PgWireError::ApiError("No connection ID in metadata".into()))?
            .clone();
        tracing::debug!(conn_id = %conn_id, query, "Extended query describe portal");
        let conn_state = session::get_connection(&conn_id)
            .ok_or_else(|| PgWireError::ApiError("Connection state not found".into()))?;
        let trino_client = Arc::clone(&conn_state.trino_client);
        let config = Arc::clone(&conn_state.config);
        // Clone the Arcs so we can use them after the .await below;
        // conn_state is a DashMap ref-guard that can't be held across awaits.
        let portals = Arc::clone(&conn_state.portals);
        let active_query_id = Arc::clone(&conn_state.active_query_id);
        drop(conn_state);

        let responses =
            process_query(query, &trino_client, &config, Some(&active_query_id)).await?;
        let response = responses
            .into_iter()
            .next()
            .ok_or_else(|| PgWireError::ApiError("Empty pipeline response".into()))?;

        let fields = match &response {
            Response::Query(qr) => {
                tracing::trace!(
                    columns = ?qr.row_schema.iter().map(|f| f.name()).collect::<Vec<&str>>(),
                    "Describe portal: returning RowDescription"
                );
                qr.row_schema.as_ref().clone()
            }
            _ => vec![], // DDL/DML — no columns
        };

        // Stash for do_query. Drops any orphaned entry for this portal name.
        // Trino-side query state for the dropped Response remains alive until
        // its server-side TTL — the trino-rust-client doesn't issue a DELETE
        // on the nextUri when its stream is dropped.
        // TODO(cancel): wire PG CancelRequest and dropped-cache cleanup to
        // Trino's `DELETE /v1/statement/{queryId}` so abandoned queries
        // release Trino resources promptly.
        insert_cached(
            &portals,
            portal.name.clone(),
            CachedPortalResponse {
                query: query.clone(),
                response,
            },
        )?;

        Ok(DescribePortalResponse::new(fields))
    }
}

fn take_cached(cache: &PortalCache, name: &str) -> PgWireResult<Option<CachedPortalResponse>> {
    let mut map = cache
        .lock()
        .map_err(|_| PgWireError::ApiError("portal cache mutex poisoned".into()))?;
    Ok(map.remove(name))
}

fn insert_cached(
    cache: &PortalCache,
    name: String,
    entry: CachedPortalResponse,
) -> PgWireResult<()> {
    let mut map = cache
        .lock()
        .map_err(|_| PgWireError::ApiError("portal cache mutex poisoned".into()))?;
    if map.len() >= MAX_CACHED_PORTALS && !map.contains_key(&name) {
        // Refuse to grow past the cap. The Response we received is dropped
        // here; the next Execute on this portal will re-run the pipeline.
        tracing::warn!(
            portal = %name,
            cached = map.len(),
            cap = MAX_CACHED_PORTALS,
            "Portal cache full — dropping describe response"
        );
        return Ok(());
    }
    map.insert(name, entry);
    Ok(())
}
