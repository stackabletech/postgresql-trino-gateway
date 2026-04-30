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
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::startup::{Authentication, SecretKey};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use rand::Rng;
use trino_rust_client::auth::Auth;

use crate::config::Config;
use crate::session::{self, ConnectionState};

/// Monotonically-increasing connection counter used as the BackendKeyData PID.
/// Starts at 1 so it is never zero (PID 0 means "not received" in Npgsql).
static CONNECTION_PID: AtomicI32 = AtomicI32::new(1);

/// Allocate a fresh `(pid, secret_key)` pair for a new connection. The pid
/// is a sequential counter for human-readable logging; the secret_key is a
/// CSPRNG-generated 32-bit value so an attacker can't guess the cancel
/// authorisation token for somebody else's connection.
fn new_pid_and_secret_key() -> (i32, SecretKey) {
    let pid = CONNECTION_PID.fetch_add(1, Ordering::Relaxed);
    let secret = rand::thread_rng().r#gen::<i32>();
    (pid, SecretKey::I32(secret))
}

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
        params.insert("IntervalStyle".to_owned(), "postgres".to_owned());
        params.insert("in_hot_standby".to_owned(), "off".to_owned());
        params.insert("search_path".to_owned(), "\"$user\", public".to_owned());
        params.insert("is_superuser".to_owned(), "on".to_owned());
        params.insert("default_transaction_read_only".to_owned(), "off".to_owned());
        params.insert("application_name".to_owned(), "".to_owned());
        Some(params)
    }
}

/// Handles the startup/authentication phase of a PostgreSQL connection.
///
/// Two modes:
/// - `config.auth == false`: no password challenge; connects to Trino with
///   `--trino-user` and no per-client credentials.
/// - `config.auth == true`: requests a cleartext password from the PG
///   client and forwards (username, password) to Trino as HTTP Basic auth.
///   If Trino rejects the credentials, the PG connection is refused.
///
/// SCRAM-SHA-256 is intentionally not implemented. SCRAM is a server-side
/// challenge-response that does not expose the cleartext password to the
/// server, but the gateway needs the cleartext password to authenticate to
/// Trino on the client's behalf. To make `--auth` safe over the network,
/// the gateway requires TLS termination — `policy::validate` enforces this
/// at startup before any client can connect.
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
                tracing::trace!(
                    addr = %client.socket_addr(),
                    "Startup message received"
                );
                protocol_negotiation(client, startup).await?;
                save_startup_parameters_to_metadata(client, startup);

                // pgwire's negotiate_tls upgrades only if the client first
                // sent an SslRequest; clients that skip the upgrade reach
                // here over plaintext. When auth+TLS are configured, refuse
                // such clients before issuing a cleartext-password challenge.
                if self.config.auth && self.tls_required() && !client.is_secure() {
                    tracing::warn!(
                        addr = %client.socket_addr(),
                        "rejecting plaintext client — --auth with TLS configured requires SslRequest"
                    );
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "FATAL".to_owned(),
                        "28000".to_owned(), // invalid_authorization_specification
                        "TLS is required for password authentication on this server".to_owned(),
                    ))));
                }

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
                    let trino_client = Arc::new(self.build_trino_client(None, None)?);
                    let (pid, secret_key) = new_pid_and_secret_key();
                    client.set_pid_and_secret_key(pid, secret_key.clone());
                    let active_query_id = session::register_cancel(
                        pid,
                        secret_key.clone(),
                        Arc::clone(&trino_client),
                    );
                    let conn_id = format!("{}_{}", client.socket_addr(), pid);
                    client
                        .metadata_mut()
                        .insert(session::connection_id_key().to_owned(), conn_id.clone());
                    session::register_connection(
                        conn_id,
                        ConnectionState {
                            trino_client,
                            config: self.config.clone(),
                            portals: Default::default(),
                            active_query_id,
                            cancel_key: (pid, secret_key),
                        },
                    );
                    finish_authentication(client, &GatewayParameterProvider).await?;
                    tracing::info!(addr = %client.socket_addr(), "client connected (no auth)");
                }
            }
            PgWireFrontendMessage::PasswordMessageFamily(pwd) => {
                tracing::trace!(
                    addr = %client.socket_addr(),
                    "Password message received (contents redacted)"
                );
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

                let trino_client = Arc::new(trino_client);
                let (pid, secret_key) = new_pid_and_secret_key();
                client.set_pid_and_secret_key(pid, secret_key.clone());
                let active_query_id = session::register_cancel(
                    pid,
                    secret_key.clone(),
                    Arc::clone(&trino_client),
                );
                let conn_id = format!("{}_{}", client.socket_addr(), pid);
                client
                    .metadata_mut()
                    .insert(session::connection_id_key().to_owned(), conn_id.clone());
                session::register_connection(
                    conn_id,
                    ConnectionState {
                        trino_client,
                        config: self.config.clone(),
                        portals: Default::default(),
                        active_query_id,
                        cancel_key: (pid, secret_key),
                    },
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
    /// True iff the operator configured TLS termination (`--tls-cert` and
    /// `--tls-key` both present). When auth is also on, plaintext clients
    /// are refused; see `policy::AuthPosture::CleartextRequiresTls`.
    fn tls_required(&self) -> bool {
        self.config.tls_cert.is_some() && self.config.tls_key.is_some()
    }

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
        if self.config.trino_tls_no_verify {
            builder = builder.no_verify(true);
        }

        // Forward PG credentials to Trino as Basic auth
        if let Some(pw) = password {
            builder = builder
                .auth(Auth::new_basic(trino_user, Some(pw)))
                .auth_http_insecure(self.config.trino_allow_plaintext_auth);
        }

        builder
            .build()
            .map_err(|e| PgWireError::ApiError(Box::new(e)))
    }
}
