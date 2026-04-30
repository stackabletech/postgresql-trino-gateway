// Copyright 2026 Stackable GmbH
// Licensed under the Open Software License version 3.0 (OSL-3.0).
// See LICENSE file in the project root for full license text.
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
    /// plaintext PG protocol; do not pair plaintext with `--auth` on a
    /// non-loopback bind. (Operator-side discipline for now — automatic
    /// refusal is tracked under publication-checklist item A11.)
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

    /// Skip TLS certificate verification (for self-signed certs).
    #[arg(long, default_value_t = false)]
    pub trino_ssl_insecure: bool,

    /// Require password authentication from PG clients.
    /// Credentials are forwarded to Trino as HTTP Basic auth.
    /// When disabled, connects to Trino with the --trino-user and no password.
    #[arg(long, default_value_t = false)]
    pub auth: bool,
}
