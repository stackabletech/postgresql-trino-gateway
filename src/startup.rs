// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0
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
    let secret = rand::random::<i32>();
    (pid, SecretKey::I32(secret))
}

/// PostgreSQL-compatible server parameters announced at startup AND
/// returned by `SHOW <name>`. Single source of truth so the two answers
/// can't drift.
pub const SERVER_PARAMS: &[(&str, &str)] = &[
    ("server_version", "16.6"),
    ("server_version_num", "160006"),
    ("server_encoding", "UTF8"),
    ("client_encoding", "UTF8"),
    ("DateStyle", "ISO, MDY"),
    ("integer_datetimes", "on"),
    ("standard_conforming_strings", "on"),
    ("TimeZone", "UTC"),
    ("IntervalStyle", "postgres"),
    ("in_hot_standby", "off"),
    ("search_path", "\"$user\", public"),
    ("is_superuser", "on"),
    ("default_transaction_read_only", "off"),
    ("application_name", ""),
    ("max_identifier_length", "63"),
    ("transaction_isolation", "read committed"),
];

/// Server parameter provider that returns PostgreSQL-compatible parameters.
#[derive(Debug)]
pub struct GatewayParameterProvider;

impl ServerParameterProvider for GatewayParameterProvider {
    fn server_parameters<C>(&self, _client: &C) -> Option<HashMap<String, String>>
    where
        C: ClientInfo,
    {
        Some(
            SERVER_PARAMS
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
        )
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
                    // No auth — create Trino client immediately and finish.
                    let trino_client = Arc::new(self.build_trino_client(None, None)?);
                    self.establish_session(client, trino_client, None).await?;
                }
            }
            PgWireFrontendMessage::PasswordMessageFamily(pwd) => {
                tracing::trace!(
                    addr = %client.socket_addr(),
                    "Password message received (contents redacted)"
                );
                let password = pwd.into_password()?;
                let user = client.metadata().get("user").cloned().unwrap_or_default();

                // Build Trino client with the PG client's credentials.
                let trino_client =
                    self.build_trino_client(Some(&user), Some(&password.password))?;

                // Validate credentials by running a lightweight query against
                // Trino. We reject the PG connection here on any failure
                // (auth rejection, network error, malformed reply, ...) so
                // the client gets a clear failure during startup rather than
                // a confusing error on its first real query. The error
                // message is logged at the gateway with the underlying
                // cause; the client gets the standard `InvalidPassword`
                // response — distinguishing auth-failure from other failures
                // requires inspecting `trino-rust-client`'s error enum and
                // is tracked separately.
                if let Err(e) = trino_client
                    .get::<trino_rust_client::Row>("SELECT 1".to_owned())
                    .await
                {
                    tracing::warn!(
                        addr = %client.socket_addr(),
                        user = %user,
                        error = %e,
                        "rejecting PG connection — Trino credential check failed (auth rejection, network error, or unreachable Trino)"
                    );
                    return Err(PgWireError::InvalidPassword(user));
                }

                self.establish_session(client, Arc::new(trino_client), Some(user.as_str()))
                    .await?;
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

    /// Common per-connection setup once the Trino client has been built
    /// and (for `--auth=true`) credentials have been verified. Allocates a
    /// fresh `(pid, secret_key)`, registers the connection in the cancel
    /// registry and the per-connection state map, and finishes the PG
    /// startup handshake. `user` is `Some` only on the auth=true path,
    /// where it's logged for operator visibility.
    async fn establish_session<C>(
        &self,
        client: &mut C,
        trino_client: Arc<trino_rust_client::Client>,
        user: Option<&str>,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let (pid, secret_key) = new_pid_and_secret_key();
        client.set_pid_and_secret_key(pid, secret_key.clone());
        let active_query_id =
            session::register_cancel(pid, secret_key.clone(), Arc::clone(&trino_client));
        client.session_extensions().insert(ConnectionState {
            trino_client,
            config: self.config.clone(),
            portals: Default::default(),
            active_query_id,
            cancel_key: (pid, secret_key),
        });
        finish_authentication(client, &GatewayParameterProvider).await?;
        match user {
            Some(u) => {
                tracing::info!(addr = %client.socket_addr(), user = %u, "client connected")
            }
            None => tracing::info!(addr = %client.socket_addr(), "client connected (no auth)"),
        }
        Ok(())
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
