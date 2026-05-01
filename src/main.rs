// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tokio::net::TcpListener;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use postgresql_trino_gateway::config::Config;
use postgresql_trino_gateway::handler::GatewayHandlerFactory;
use postgresql_trino_gateway::policy;
use postgresql_trino_gateway::query_extended::GatewayExtendedQueryHandler;
use postgresql_trino_gateway::query_simple::GatewayQueryHandler;
use postgresql_trino_gateway::session;
use postgresql_trino_gateway::startup::GatewayStartupHandler;
use postgresql_trino_gateway::tls;

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

    // Refuse to start with a configuration that would cross the network
    // with cleartext credentials. Logs the chosen posture as a side effect.
    policy::validate(&config)?;

    let factory = Arc::new(GatewayHandlerFactory::new(
        Arc::new(GatewayStartupHandler {
            config: config.clone(),
        }),
        Arc::new(GatewayQueryHandler),
        Arc::new(GatewayExtendedQueryHandler),
    ));

    let drain_timeout = drain_timeout_from_env();

    let tls_acceptor = match (&config.tls_cert, &config.tls_key) {
        (Some(cert), Some(key)) => Some(tls::build_acceptor(cert, key)?),
        (None, None) => None,
        // Clap's `requires` enforces both-or-neither at parse time; if a
        // caller bypassed clap (programmatic Config construction in tests)
        // and supplied only one path, log loudly and treat as no-TLS rather
        // than silently dropping the cert.
        (Some(cert), None) => {
            tracing::warn!(
                tls_cert = %cert.display(),
                "tls_cert set without tls_key — TLS disabled (use both flags)"
            );
            None
        }
        (None, Some(key)) => {
            tracing::warn!(
                tls_key = %key.display(),
                "tls_key set without tls_cert — TLS disabled (use both flags)"
            );
            None
        }
    };

    if config.max_connections == 0 {
        anyhow::bail!(
            "--max-connections 0 would refuse every connection. \
             Use a positive value (default 256)."
        );
    }
    let connection_limit = Arc::new(Semaphore::new(config.max_connections));

    let listener = TcpListener::bind(&config.listen_addr).await?;
    tracing::info!(
        addr = %config.listen_addr,
        version = concat!(env!("CARGO_PKG_VERSION"), "-", env!("BUILD_GIT_HASH")),
        built = env!("BUILD_TIMESTAMP"),
        drain_timeout_secs = drain_timeout.as_secs(),
        tls = tls_acceptor.is_some(),
        max_connections = config.max_connections,
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
                // Take a permit before spawning the connection task. When
                // the cap is reached we drop the socket immediately rather
                // than queue the connection, so a flood of incoming SYNs
                // can't pile up FD/memory pressure waiting for a slot.
                //
                // The PG protocol *does* allow a graceful refusal — an
                // `ErrorResponse` (SQLSTATE 53300, `too_many_connections`)
                // followed by a close — but issuing it requires reading
                // the client's StartupMessage first, which itself consumes
                // resources and defeats the SYN-flood mitigation above.
                // Clients see a TCP close instead; the warning below makes
                // the cause visible operator-side.
                let permit = match Arc::clone(&connection_limit).try_acquire_owned() {
                    Ok(p) => p,
                    Err(_) => {
                        tracing::warn!(
                            addr = %peer_addr,
                            cap = config.max_connections,
                            "rejecting connection — max_connections reached"
                        );
                        drop(socket);
                        continue;
                    }
                };
                let factory = Arc::clone(&factory);
                let tls = tls_acceptor.clone();
                tasks.spawn(async move {
                    let _permit = permit; // released on task exit
                    let _guard = ConnectionCleanup(peer_addr);
                    if let Err(e) = pgwire::tokio::process_socket(socket, tls, factory).await {
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
/// compile-time default.
fn drain_timeout_from_env() -> Duration {
    parse_drain_timeout(std::env::var(SHUTDOWN_DRAIN_TIMEOUT_ENV).ok())
}

/// Pure parsing helper for `drain_timeout_from_env`. Extracted so tests can
/// exercise the unparseable-fallback path without mutating the process-wide
/// environment.
fn parse_drain_timeout(raw: Option<String>) -> Duration {
    let Some(v) = raw else {
        return DEFAULT_SHUTDOWN_DRAIN_TIMEOUT;
    };
    match v.parse::<u64>() {
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
    tracing::info!(
        in_flight,
        timeout_secs = timeout.as_secs(),
        "draining connections"
    );

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

    /// Smoke test for the connection-limiting primitive: with a 2-permit
    /// semaphore, two `try_acquire_owned` calls succeed and the third
    /// fails. After dropping one permit, a new acquisition succeeds.
    /// Pins the semaphore behaviour the accept loop depends on.
    #[tokio::test]
    async fn semaphore_caps_concurrent_permits() {
        let sem = Arc::new(Semaphore::new(2));
        let p1 = Arc::clone(&sem).try_acquire_owned().unwrap();
        let _p2 = Arc::clone(&sem).try_acquire_owned().unwrap();
        assert!(Arc::clone(&sem).try_acquire_owned().is_err());
        drop(p1);
        assert!(Arc::clone(&sem).try_acquire_owned().is_ok());
    }

    #[test]
    fn drain_timeout_parser_handles_inputs() {
        assert_eq!(parse_drain_timeout(None), DEFAULT_SHUTDOWN_DRAIN_TIMEOUT);
        assert_eq!(
            parse_drain_timeout(Some("not-a-number".to_owned())),
            DEFAULT_SHUTDOWN_DRAIN_TIMEOUT
        );
        assert_eq!(
            parse_drain_timeout(Some("0".to_owned())),
            Duration::from_secs(0)
        );
        assert_eq!(
            parse_drain_timeout(Some("60".to_owned())),
            Duration::from_secs(60)
        );
    }
}
