// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0

//! Accept loop: one task per PG connection, capped at `max_connections`.

use std::future::Future;
use std::io;
use std::sync::Arc;

use pgwire::tokio::TlsAcceptor;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::handler::GatewayHandlerFactory;

/// Accept PG connections until `shutdown` resolves, then return the
/// connection tasks that are still tracked so the caller can drain them.
/// The listener is dropped on return, so no new connections are accepted
/// while draining.
pub async fn serve<S>(
    listener: TcpListener,
    factory: Arc<GatewayHandlerFactory>,
    tls_acceptor: Option<TlsAcceptor>,
    max_connections: usize,
    shutdown: S,
) -> io::Result<JoinSet<()>>
where
    S: Future<Output = ()>,
{
    let connection_limit = Arc::new(Semaphore::new(max_connections));
    let mut tasks: JoinSet<()> = JoinSet::new();
    tokio::pin!(shutdown);

    loop {
        // `biased` ensures shutdown is observed before another accept fires
        // when several branches are simultaneously ready. The shutdown
        // future returns Pending until a signal arrives, so the accept
        // branch is not starved under normal load. To clean up finished
        // connections, they need to be joined - this is positioned before
        // accept, so that we actually reap instead of always accepting new
        // connections.
        tokio::select! {
            biased;
            _ = &mut shutdown => break,
            Some(res) = tasks.join_next(), if !tasks.is_empty() => log_task_result(res),
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
                            cap = max_connections,
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
                    if let Err(e) = pgwire::tokio::process_socket(socket, tls, factory).await {
                        tracing::error!(addr = %peer_addr, error = %e, "connection error");
                    }
                });
            }
        }
    }

    Ok(tasks)
}

pub fn log_task_result(res: Result<(), tokio::task::JoinError>) {
    if let Err(e) = res {
        if e.is_panic() {
            tracing::error!(error = %e, "connection task panicked");
        } else if !e.is_cancelled() {
            tracing::error!(error = %e, "connection task failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use clap::Parser;
    use tokio::net::TcpStream;
    use tokio::sync::oneshot;

    use super::*;
    use crate::config::Config;
    use crate::query_extended::GatewayExtendedQueryHandler;
    use crate::query_simple::GatewayQueryHandler;
    use crate::startup::GatewayStartupHandler;

    fn test_factory() -> Arc<GatewayHandlerFactory> {
        let config = Arc::new(Config::parse_from(["test"]));
        Arc::new(GatewayHandlerFactory::new(
            Arc::new(GatewayStartupHandler { config }),
            Arc::new(GatewayQueryHandler),
            Arc::new(GatewayExtendedQueryHandler),
        ))
    }

    /// Kubelet `tcpSocket` probes and pooling BI clients open and close
    /// connections all day. A closed connection's task must not stay in the
    /// set, or the gateway keeps one task allocation per connection it has
    /// ever accepted and eventually runs out of memory.
    #[tokio::test]
    async fn closed_connections_are_not_retained() {
        const CONNECTIONS: usize = 50;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop, stopped) = oneshot::channel::<()>();
        let server = tokio::spawn(serve(listener, test_factory(), None, 256, async move {
            let _ = stopped.await;
        }));

        for _ in 0..CONNECTIONS {
            drop(TcpStream::connect(addr).await.unwrap());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        stop.send(()).unwrap();

        let tasks = server.await.unwrap().unwrap();
        assert_eq!(
            tasks.len(),
            0,
            "{} of {CONNECTIONS} closed connections are still held by the JoinSet",
            tasks.len()
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
}
