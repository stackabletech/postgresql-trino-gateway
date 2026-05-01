// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0

//! Shared test infrastructure used by both `integration_test.rs`
//! (tokio-postgres-driven) and `psql_integration_test.rs` (real psql
//! subprocess-driven).

#![allow(dead_code)] // not every helper is used by every test file

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;

use postgresql_trino_gateway::config::Config;
use postgresql_trino_gateway::handler::GatewayHandlerFactory;
use postgresql_trino_gateway::query_extended::GatewayExtendedQueryHandler;
use postgresql_trino_gateway::query_simple::GatewayQueryHandler;
use postgresql_trino_gateway::startup::GatewayStartupHandler;

/// Bind a fresh in-process gateway to `127.0.0.1:0` and return the
/// resulting `SocketAddr`. The gateway runs in a background tokio task
/// and accepts connections until the test binary exits.
pub async fn start_gateway(config: Config) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let config = Arc::new(config);
    let factory = Arc::new(GatewayHandlerFactory::new(
        Arc::new(GatewayStartupHandler {
            config: config.clone(),
        }),
        Arc::new(GatewayQueryHandler),
        Arc::new(GatewayExtendedQueryHandler),
    ));
    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            let factory = factory.clone();
            tokio::spawn(async move {
                let _ = pgwire::tokio::process_socket(socket, None, factory).await;
            });
        }
    });
    addr
}

/// Default Config for tests that don't reach Trino. Used by the
/// gateway-only tests in `integration_test.rs` (intercepts, catalog
/// stubs, server-function probes).
pub fn test_config() -> Config {
    Config {
        listen_addr: "127.0.0.1:0".to_string(),
        tls_cert: None,
        tls_key: None,
        trino_host: "localhost".to_string(),
        trino_port: 8080,
        trino_catalog: "memory".to_string(),
        trino_schema: "default".to_string(),
        trino_user: "trino".to_string(),
        trino_ssl: false,
        trino_tls_no_verify: false,
        trino_allow_plaintext_auth: false,
        auth: false,
        allow_insecure_listener: false,
        max_connections: 256,
    }
}

/// Build a Config pointing at a real Trino, sourced from the
/// `TRINO_HOST` / `TRINO_PORT` / `TRINO_SSL` / `TRINO_TLS_NO_VERIFY` /
/// `TRINO_CATALOG` / `TRINO_SCHEMA` env vars. Returns `None` when
/// `TRINO_HOST` is unset, so test functions can early-return with a
/// "skipping" message.
pub fn trino_config() -> Option<Config> {
    let host = std::env::var("TRINO_HOST").ok()?;
    let port: u16 = std::env::var("TRINO_PORT").ok()?.parse().ok()?;
    let ssl = std::env::var("TRINO_SSL").ok().is_some_and(|v| v == "true");
    let tls_no_verify = std::env::var("TRINO_TLS_NO_VERIFY")
        .ok()
        .is_some_and(|v| v == "true");
    let catalog = std::env::var("TRINO_CATALOG").unwrap_or_else(|_| "tpch".to_string());
    let schema = std::env::var("TRINO_SCHEMA").unwrap_or_else(|_| "sf1".to_string());
    Some(Config {
        listen_addr: "127.0.0.1:0".to_string(),
        tls_cert: None,
        tls_key: None,
        trino_host: host,
        trino_port: port,
        trino_catalog: catalog,
        trino_schema: schema,
        trino_user: "trino".to_string(),
        trino_ssl: ssl,
        trino_tls_no_verify: tls_no_verify,
        trino_allow_plaintext_auth: false,
        auth: false,
        allow_insecure_listener: false,
        max_connections: 256,
    })
}
