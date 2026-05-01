// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0
use std::path::PathBuf;

use clap::Parser;

/// PostgreSQL-to-Trino gateway configuration.
// WARNING: Debug is derived for clap compatibility. If credential fields
// (password, token) are ever added, implement Debug manually to redact them.
#[derive(Clone, Debug, Parser)]
#[command(name = "postgresql-trino-gateway")]
pub struct Config {
    /// Address to listen on for PostgreSQL connections.
    #[arg(long, default_value = "127.0.0.1:5432")]
    pub listen_addr: String,

    /// PEM-encoded TLS certificate chain for the listening socket. When set,
    /// `--tls-key` is also required. Without these flags the gateway speaks
    /// plaintext PG protocol; the auth/TLS posture matrix in `policy.rs`
    /// refuses plaintext + `--auth` on a non-loopback bind at startup.
    #[arg(long, requires = "tls_key")]
    pub tls_cert: Option<PathBuf>,

    /// PEM-encoded TLS private key for the listening socket. PKCS#8, RSA,
    /// or SEC1 EC keys are supported. Required if `--tls-cert` is set.
    #[arg(long, requires = "tls_cert")]
    pub tls_key: Option<PathBuf>,

    /// Trino host to connect to.
    #[arg(long, default_value = "localhost")]
    pub trino_host: String,

    /// Trino port to connect to.
    #[arg(long, default_value_t = 8080)]
    pub trino_port: u16,

    /// Trino catalog to use.
    #[arg(long, default_value = "memory")]
    pub trino_catalog: String,

    /// Trino schema to use.
    #[arg(long, default_value = "default")]
    pub trino_schema: String,

    /// Trino user to authenticate as.
    #[arg(long, default_value = "trino")]
    pub trino_user: String,

    /// Use HTTPS to connect to Trino.
    #[arg(long, default_value_t = false)]
    pub trino_ssl: bool,

    /// Skip TLS certificate-chain and hostname verification on the Trino
    /// connection. Useful with self-signed certs in a trusted network.
    /// Only meaningful with `--trino-ssl`.
    #[arg(long, default_value_t = false)]
    pub trino_tls_no_verify: bool,

    /// Allow forwarding the PG client's password to Trino over plain
    /// HTTP. Required when `--auth` is on and Trino is reached over HTTP
    /// (`--trino-ssl=false`); without it the Trino client refuses to send
    /// credentials. The password crosses the network in cleartext, so use
    /// only with a loopback or otherwise-trusted Trino endpoint.
    #[arg(long, default_value_t = false)]
    pub trino_allow_plaintext_auth: bool,

    /// Require password authentication from PG clients.
    /// Credentials are forwarded to Trino as HTTP Basic auth.
    /// When disabled, connects to Trino with the --trino-user and no password.
    #[arg(long, default_value_t = false)]
    pub auth: bool,

    /// Acknowledge that the gateway should accept unauthenticated PG
    /// connections on a non-loopback listen address. Without this flag,
    /// `--auth=false` is allowed only on loopback binds; with this flag,
    /// every network-reachable client gets unauthenticated access to
    /// Trino as `--trino-user`. Use only when Trino itself enforces
    /// authentication or the network is otherwise trusted.
    #[arg(long, default_value_t = false)]
    pub allow_insecure_listener: bool,

    /// Maximum number of concurrent PG client connections. Excess
    /// connections are accepted on the TCP socket and immediately closed
    /// with a logged warning, so an unauthenticated client cannot exhaust
    /// the gateway's file descriptors or memory by piling on connections.
    /// Each in-flight connection holds one slot until its task ends.
    #[arg(long, default_value_t = 256)]
    pub max_connections: usize,
}
