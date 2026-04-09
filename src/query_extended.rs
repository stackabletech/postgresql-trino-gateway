use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Sink;
use pgwire::api::portal::Portal;
use pgwire::api::query::ExtendedQueryHandler;
use pgwire::api::results::{
    DescribePortalResponse, DescribeStatementResponse, QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use trino_rust_client::Client as TrinoClient;

use crate::trino_stream::execute_trino_query;

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

        // Static catalog interception (pg_type, pg_enum, pg_range, pg_namespace, etc.)
        if let Some(result) = crate::intercept::intercept_query(query) {
            // intercept_query returns PgWireResult<Vec<Response>>; take the first.
            let responses = result?;
            return responses
                .into_iter()
                .next()
                .ok_or_else(|| PgWireError::ApiError("Empty intercept response".into()));
        }

        let trino_client: Arc<TrinoClient> = client
            .session_extensions()
            .get::<TrinoClient>()
            .ok_or_else(|| PgWireError::ApiError("No Trino client in session".into()))?;

        // Dynamic catalog interception (pg_class, pg_attribute — needs Trino client)
        if let Some(result) =
            crate::catalog::handle_dynamic_catalog_query(query, &trino_client).await
        {
            let responses = result?;
            return responses
                .into_iter()
                .next()
                .ok_or_else(|| PgWireError::ApiError("Empty dynamic catalog response".into()));
        }

        let rewritten = crate::rewrite::rewrite_sql(query);
        tracing::debug!(original = query, rewritten = %rewritten, "Rewritten extended query");

        let (schema, row_stream) = execute_trino_query(&trino_client, rewritten).await?;

        if schema.is_empty() {
            Ok(Response::Execution(Tag::new("OK")))
        } else {
            Ok(Response::Query(QueryResponse::new(schema, row_stream)))
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
        // Report all parameters as TEXT. Trino handles the actual type coercion,
        // and Npgsql is happy as long as it gets valid OIDs back.
        let param_types = stmt
            .parameter_types
            .iter()
            .map(|t| t.clone().unwrap_or(Type::TEXT))
            .collect();

        // We cannot know the result columns without executing the query, so
        // return an empty field list. The client will get the real schema from
        // the RowDescription sent during Execute.
        Ok(DescribeStatementResponse::new(param_types, vec![]))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        _portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Ok(DescribePortalResponse::new(vec![]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handler_can_be_constructed() {
        let handler = GatewayExtendedQueryHandler;
        // Verify the query parser is wired up correctly.
        let parser = handler.query_parser();
        assert_eq!(format!("{:?}", *parser), "NoopQueryParser");
    }
}
