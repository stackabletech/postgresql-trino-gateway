// Copyright 2026 Stackable GmbH
// Licensed under the Open Software License version 3.0 (OSL-3.0).
// See LICENSE file in the project root for full license text.
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tokio::net::TcpListener;
use tokio::signal::unix::{SignalKind, signal};
use tokio::task::JoinSet;

use postgresql_trino_gateway::config::Config;
use postgresql_trino_gateway::handler::GatewayHandlerFactory;
use postgresql_trino_gateway::query_extended::GatewayExtendedQueryHandler;
use postgresql_trino_gateway::query_simple::GatewayQueryHandler;
use postgresql_trino_gateway::session;
use postgresql_trino_gateway::startup::GatewayStartupHandler;

/// Maximum time to wait for in-flight connections to complete after a
/// shutdown signal before aborting them.
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config = Arc::new(Config::parse());

    let factory = Arc::new(GatewayHandlerFactory::new(
        Arc::new(GatewayStartupHandler {
            config: config.clone(),
        }),
        Arc::new(GatewayQueryHandler),
        Arc::new(GatewayExtendedQueryHandler),
    ));

    let listener = TcpListener::bind(&config.listen_addr).await?;
    tracing::info!(
        addr = %config.listen_addr,
        version = concat!(env!("CARGO_PKG_VERSION"), "-", env!("BUILD_GIT_HASH")),
        built = env!("BUILD_TIMESTAMP"),
        "listening for PostgreSQL connections"
    );

    let mut sigterm = signal(SignalKind::terminate())?;
    let mut tasks: JoinSet<()> = JoinSet::new();

    loop {
        tokio::select! {
            biased;
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("received SIGINT, beginning graceful shutdown");
                break;
            }
            _ = sigterm.recv() => {
                tracing::info!("received SIGTERM, beginning graceful shutdown");
                break;
            }
            accept = listener.accept() => {
                let (socket, peer_addr) = accept?;
                let factory = Arc::clone(&factory);
                tasks.spawn(async move {
                    // Drop guard so per-connection state is removed even if
                    // process_socket panics or the task is cancelled.
                    struct Cleanup(std::net::SocketAddr);
                    impl Drop for Cleanup {
                        fn drop(&mut self) {
                            session::remove_connections_for_addr(self.0);
                        }
                    }
                    let _guard = Cleanup(peer_addr);

                    if let Err(e) = pgwire::tokio::process_socket(socket, None, factory).await {
                        tracing::error!(error = %e, "connection error");
                    }
                });
            }
        }
    }

    drop(listener);
    drain(tasks).await;
    Ok(())
}

/// Wait up to `SHUTDOWN_DRAIN_TIMEOUT` for in-flight connection tasks to
/// complete. After the timeout, abort whatever is still running so the
/// process can exit instead of hanging.
async fn drain(mut tasks: JoinSet<()>) {
    let in_flight = tasks.len();
    if in_flight == 0 {
        return;
    }
    tracing::info!(in_flight, "draining connections");

    let drain_loop = async {
        while let Some(res) = tasks.join_next().await {
            if let Err(e) = res
                && !e.is_cancelled()
            {
                tracing::error!(error = %e, "connection task failed during drain");
            }
        }
    };

    if tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, drain_loop)
        .await
        .is_err()
    {
        let still_running = tasks.len();
        tracing::warn!(
            still_running,
            timeout_secs = SHUTDOWN_DRAIN_TIMEOUT.as_secs(),
            "drain timeout exceeded, aborting remaining connections"
        );
        tasks.abort_all();
        // Give aborted tasks a moment to unwind cleanly.
        let _ = tokio::time::timeout(Duration::from_secs(1), async {
            while tasks.join_next().await.is_some() {}
        })
        .await;
    } else {
        tracing::info!("all connections drained");
    }
}
