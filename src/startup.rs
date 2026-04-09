use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Sink;
use pgwire::api::ClientInfo;
use pgwire::api::auth::{
    ServerParameterProvider, StartupHandler, finish_authentication, protocol_negotiation,
    save_startup_parameters_to_metadata,
};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use crate::config::Config;

/// Server parameter provider that returns PostgreSQL-compatible parameters.
#[derive(Debug)]
pub struct GatewayParameterProvider;

impl ServerParameterProvider for GatewayParameterProvider {
    fn server_parameters<C>(&self, _client: &C) -> Option<HashMap<String, String>>
    where
        C: ClientInfo,
    {
        let mut params = HashMap::new();
        params.insert("server_version".to_owned(), "16.6".to_owned());
        params.insert("server_encoding".to_owned(), "UTF8".to_owned());
        params.insert("client_encoding".to_owned(), "UTF8".to_owned());
        params.insert("DateStyle".to_owned(), "ISO, MDY".to_owned());
        params.insert("integer_datetimes".to_owned(), "on".to_owned());
        params.insert("standard_conforming_strings".to_owned(), "on".to_owned());
        params.insert("TimeZone".to_owned(), "UTC".to_owned());
        Some(params)
    }
}

/// Handles the startup/authentication phase of a PostgreSQL connection.
#[derive(Debug)]
pub struct GatewayStartupHandler {
    pub config: Arc<Config>,
}

#[async_trait]
impl StartupHandler for GatewayStartupHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if let PgWireFrontendMessage::Startup(ref startup) = message {
            protocol_negotiation(client, startup).await?;
            save_startup_parameters_to_metadata(client, startup);
            finish_authentication(client, &GatewayParameterProvider).await?;

            let mut builder = trino_rust_client::ClientBuilder::new(
                &self.config.trino_user,
                &self.config.trino_host,
            )
            .port(self.config.trino_port)
            .catalog(&self.config.trino_catalog)
            .schema(&self.config.trino_schema);

            if self.config.trino_ssl {
                builder = builder.secure(true);
            }
            if self.config.trino_ssl_insecure {
                builder = builder.no_verify(true);
            }

            let trino_client = builder
                .build()
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

            client.session_extensions().insert(trino_client);
            client.session_extensions().insert((*self.config).clone());

            tracing::info!(
                addr = %client.socket_addr(),
                "client connected",
            );
        } else {
            return Err(PgWireError::ApiError(
                "Expected Startup message during connection setup".into(),
            ));
        }

        Ok(())
    }
}
