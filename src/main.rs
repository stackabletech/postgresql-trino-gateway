// Copyright 2026 Stackable GmbH
// Licensed under the Open Software License version 3.0 (OSL-3.0).
// See LICENSE file in the project root for full license text.
use std::net::SocketAddr;
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

/// Default shutdown-drain budget. Kept under K8s's default
/// `terminationGracePeriodSeconds = 30` so the gateway has a chance to abort
/// stuck connections cleanly before the kubelet sends SIGKILL.
const DEFAULT_SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(25);

/// Env var that overrides the drain budget at startup.
const SHUTDOWN_DRAIN_TIMEOUT_ENV: &str = "GATEWAY_SHUTDOWN_DRAIN_TIMEOUT_SECS";

/// After `abort_all`, how long to wait for cancelled tasks to finish
/// unwinding before letting the process exit.
const ABORT_UNWIND_GRACE: Duration = Duration::from_secs(1);

/// Drop guard that removes per-connection state when the connection task
/// ends, including on panic or cancellation. Defined at module scope so the
/// accept loop body stays readable.
struct ConnectionCleanup(SocketAddr);
impl Drop for ConnectionCleanup {
    fn drop(&mut self) {
        session::remove_connections_for_addr(self.0);
    }
}

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

    let drain_timeout = drain_timeout_from_env();

    let listener = TcpListener::bind(&config.listen_addr).await?;
    tracing::info!(
        addr = %config.listen_addr,
        version = concat!(env!("CARGO_PKG_VERSION"), "-", env!("BUILD_GIT_HASH")),
        built = env!("BUILD_TIMESTAMP"),
        drain_timeout_secs = drain_timeout.as_secs(),
        "listening for PostgreSQL connections"
    );

    let mut sigterm = signal(SignalKind::terminate())?;
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);
    let mut tasks: JoinSet<()> = JoinSet::new();

    loop {
        // `biased` ensures shutdown signals are observed before another
        // accept fires when several branches are simultaneously ready.
        // Signal branches return Pending when no signal is queued, so the
        // accept branch is not starved under normal load.
        tokio::select! {
            biased;
            _ = &mut ctrl_c => {
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
                    let _guard = ConnectionCleanup(peer_addr);
                    if let Err(e) = pgwire::tokio::process_socket(socket, None, factory).await {
                        tracing::error!(error = %e, "connection error");
                    }
                });
            }
        }
    }

    drop(listener);
    drain(tasks, drain_timeout).await;
    Ok(())
}

/// Resolve the drain timeout from the environment, falling back to the
/// compile-time default. Invalid or missing values are logged and replaced.
fn drain_timeout_from_env() -> Duration {
    match std::env::var(SHUTDOWN_DRAIN_TIMEOUT_ENV) {
        Ok(v) => match v.parse::<u64>() {
            Ok(secs) => Duration::from_secs(secs),
            Err(e) => {
                tracing::warn!(
                    var = SHUTDOWN_DRAIN_TIMEOUT_ENV,
                    value = %v,
                    error = %e,
                    fallback_secs = DEFAULT_SHUTDOWN_DRAIN_TIMEOUT.as_secs(),
                    "ignoring unparseable shutdown drain timeout, using default"
                );
                DEFAULT_SHUTDOWN_DRAIN_TIMEOUT
            }
        },
        Err(_) => DEFAULT_SHUTDOWN_DRAIN_TIMEOUT,
    }
}

/// Wait up to `timeout` for in-flight connection tasks to complete. After
/// the timeout, abort whatever is still running so the process can exit
/// instead of hanging.
async fn drain(mut tasks: JoinSet<()>, timeout: Duration) {
    let in_flight = tasks.len();
    if in_flight == 0 {
        return;
    }
    tracing::info!(in_flight, timeout_secs = timeout.as_secs(), "draining connections");

    let drain_loop = async {
        while let Some(res) = tasks.join_next().await {
            log_task_result(res);
        }
    };

    if tokio::time::timeout(timeout, drain_loop).await.is_err() {
        let still_running = tasks.len();
        tracing::warn!(
            still_running,
            timeout_secs = timeout.as_secs(),
            "drain timeout exceeded, aborting remaining connections"
        );
        tasks.abort_all();
        let _ = tokio::time::timeout(ABORT_UNWIND_GRACE, async {
            while let Some(res) = tasks.join_next().await {
                log_task_result(res);
            }
        })
        .await;
    } else {
        tracing::info!("all connections drained");
    }
}

fn log_task_result(res: Result<(), tokio::task::JoinError>) {
    if let Err(e) = res {
        if e.is_panic() {
            tracing::error!(error = %e, "connection task panicked");
        } else if !e.is_cancelled() {
            tracing::error!(error = %e, "connection task failed during drain");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn drain_returns_immediately_when_empty() {
        let tasks: JoinSet<()> = JoinSet::new();
        let start = std::time::Instant::now();
        drain(tasks, Duration::from_secs(60)).await;
        assert!(start.elapsed() < Duration::from_millis(100));
    }

    #[tokio::test]
    async fn drain_waits_for_short_tasks_to_finish() {
        let mut tasks: JoinSet<()> = JoinSet::new();
        let counter = Arc::new(AtomicUsize::new(0));
        for _ in 0..3 {
            let c = Arc::clone(&counter);
            tasks.spawn(async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                c.fetch_add(1, Ordering::SeqCst);
            });
        }
        drain(tasks, Duration::from_secs(5)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn drain_aborts_long_tasks_after_timeout() {
        let mut tasks: JoinSet<()> = JoinSet::new();
        let completed = Arc::new(AtomicUsize::new(0));
        for _ in 0..2 {
            let c = Arc::clone(&completed);
            tasks.spawn(async move {
                tokio::time::sleep(Duration::from_secs(60)).await;
                c.fetch_add(1, Ordering::SeqCst);
            });
        }
        let start = std::time::Instant::now();
        drain(tasks, Duration::from_millis(50)).await;
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "drain should return quickly after abort"
        );
        assert_eq!(
            completed.load(Ordering::SeqCst),
            0,
            "tasks should have been aborted before completing"
        );
    }

    #[test]
    fn drain_timeout_falls_back_on_unparseable_env() {
        // Use a unique env var so this test doesn't race with itself.
        // SAFETY: tests in this module that touch GATEWAY_SHUTDOWN_DRAIN_TIMEOUT_SECS
        // would need #[serial], but we only read the public function, which
        // takes the var name as a constant — exercise it directly.
        let prev = std::env::var(SHUTDOWN_DRAIN_TIMEOUT_ENV).ok();
        // SAFETY: single-threaded test; no concurrent env access here.
        unsafe { std::env::set_var(SHUTDOWN_DRAIN_TIMEOUT_ENV, "not-a-number") };
        assert_eq!(drain_timeout_from_env(), DEFAULT_SHUTDOWN_DRAIN_TIMEOUT);
        // restore
        unsafe {
            match prev {
                Some(v) => std::env::set_var(SHUTDOWN_DRAIN_TIMEOUT_ENV, v),
                None => std::env::remove_var(SHUTDOWN_DRAIN_TIMEOUT_ENV),
            }
        }
    }
}
