// Copyright 2026 Stackable GmbH
// Licensed under the Open Software License version 3.0 (OSL-3.0).
// See LICENSE file in the project root for full license text.
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};
use pgwire::api::auth::{
    ServerParameterProvider, StartupHandler, finish_authentication, protocol_negotiation,
    save_startup_parameters_to_metadata,
};
use pgwire::api::{ClientInfo, PgWireConnectionState};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::startup::{Authentication, SecretKey};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use trino_rust_client::auth::Auth;

use crate::config::Config;
use crate::session::{self, ConnectionState};

/// Monotonically-increasing connection counter used as the BackendKeyData PID.
/// Starts at 1 so it is never zero (PID 0 means "not received" in Npgsql).
static CONNECTION_PID: AtomicI32 = AtomicI32::new(1);

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
///
/// Two modes:
/// - `config.auth == false`: No password required. Connects to Trino with the
///   configured --trino-user and no auth.
/// - `config.auth == true`: Requests cleartext password from the PG client.
///   Forwards username + password to Trino as HTTP Basic auth. If Trino rejects
///   the credentials, the PG connection is refused.
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
        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                protocol_negotiation(client, startup).await?;
                save_startup_parameters_to_metadata(client, startup);

                if self.config.auth {
                    // Request password from PG client
                    client.set_state(PgWireConnectionState::AuthenticationInProgress);
                    client
                        .send(PgWireBackendMessage::Authentication(
                            Authentication::CleartextPassword,
                        ))
                        .await?;
                } else {
                    // No auth — create Trino client immediately
                    let trino_client = self.build_trino_client(None, None)?;
                    let conn_id =
                        format!("{}_{}", client.socket_addr(), client.pid_and_secret_key().0);
                    client
                        .metadata_mut()
                        .insert(session::connection_id_key().to_owned(), conn_id.clone());
                    session::register_connection(
                        conn_id,
                        ConnectionState {
                            trino_client: Arc::new(trino_client),
                            config: self.config.clone(),
                        },
                    );
                    client.set_pid_and_secret_key(
                        CONNECTION_PID.fetch_add(1, Ordering::Relaxed),
                        SecretKey::I32(0),
                    );
                    finish_authentication(client, &GatewayParameterProvider).await?;
                    tracing::info!(addr = %client.socket_addr(), "client connected (no auth)");
                }
            }
            PgWireFrontendMessage::PasswordMessageFamily(pwd) => {
                let password = pwd.into_password()?;
                let user = client.metadata().get("user").cloned().unwrap_or_default();

                // Build Trino client with the PG client's credentials
                let trino_client =
                    self.build_trino_client(Some(&user), Some(&password.password))?;

                // Validate credentials by running a lightweight query against Trino.
                // If Trino rejects the auth, we reject the PG connection immediately
                // rather than letting the first real query fail with a confusing error.
                if let Err(e) = trino_client
                    .get::<trino_rust_client::Row>("SELECT 1".to_owned())
                    .await
                {
                    let msg = e.to_string();
                    tracing::warn!(
                        addr = %client.socket_addr(),
                        user = %user,
                        "Trino authentication failed: {msg}"
                    );
                    return Err(PgWireError::InvalidPassword(user));
                }

                let conn_id = format!("{}_{}", client.socket_addr(), client.pid_and_secret_key().0);
                client
                    .metadata_mut()
                    .insert(session::connection_id_key().to_owned(), conn_id.clone());
                session::register_connection(
                    conn_id,
                    ConnectionState {
                        trino_client: Arc::new(trino_client),
                        config: self.config.clone(),
                    },
                );
                client.set_pid_and_secret_key(
                    CONNECTION_PID.fetch_add(1, Ordering::Relaxed),
                    SecretKey::I32(0),
                );
                finish_authentication(client, &GatewayParameterProvider).await?;
                tracing::info!(addr = %client.socket_addr(), user = %user, "client connected");
            }
            _ => {
                return Err(PgWireError::ApiError(
                    "Expected Startup message during connection setup".into(),
                ));
            }
        }

        Ok(())
    }
}

impl GatewayStartupHandler {
    /// Build a Trino client, optionally with Basic auth credentials from the PG client.
    fn build_trino_client(
        &self,
        user: Option<&str>,
        password: Option<&str>,
    ) -> PgWireResult<trino_rust_client::Client> {
        let trino_user = user.unwrap_or(&self.config.trino_user);

        let mut builder =
            trino_rust_client::ClientBuilder::new(trino_user, &self.config.trino_host)
                .port(self.config.trino_port)
                .catalog(&self.config.trino_catalog)
                .schema(&self.config.trino_schema);

        if self.config.trino_ssl {
            builder = builder.secure(true);
        }
        if self.config.trino_ssl_insecure {
            builder = builder.no_verify(true);
        }

        // Forward PG credentials to Trino as Basic auth
        if let Some(pw) = password {
            builder = builder
                .auth(Auth::new_basic(trino_user, Some(pw)))
                .auth_http_insecure(self.config.trino_ssl_insecure);
        }

        builder
            .build()
            .map_err(|e| PgWireError::ApiError(Box::new(e)))
    }
}
