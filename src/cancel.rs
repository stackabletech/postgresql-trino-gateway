// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0

//! PostgreSQL `CancelRequest` handling.
//!
//! When a PG client sends a `CancelRequest` on a separate TCP connection
//! with a matching `(pid, secret_key)` pair, the gateway looks up the
//! originating connection's currently-running Trino query id and asks
//! Trino to cancel it (`DELETE /v1/query/{id}`). The cancel connection
//! itself is one-shot — pgwire closes it after this handler returns.

use async_trait::async_trait;
use pgwire::api::cancel::CancelHandler;
use pgwire::messages::cancel::CancelRequest;

use crate::session;

/// Cancel handler that maps PG `(pid, secret_key)` pairs back to the
/// originating connection's active Trino query id and asks Trino to cancel
/// it. A cancel for an unknown pair, or one whose connection isn't running
/// a query right now, is silently ignored — that's the same behaviour real
/// PostgreSQL exhibits.
#[derive(Debug)]
pub struct GatewayCancelHandler;

#[async_trait]
impl CancelHandler for GatewayCancelHandler {
    async fn on_cancel_request(&self, req: CancelRequest) {
        let entry = match session::lookup_cancel(req.pid, &req.secret_key) {
            Some(e) => e,
            None => {
                tracing::debug!(
                    pid = req.pid,
                    "CancelRequest for unknown (pid, secret_key) — ignored"
                );
                return;
            }
        };

        // Snapshot the query id and trino-client handle, then drop the
        // DashMap ref guard before the async cancel call — guards aren't
        // Send and can't be held across awaits.
        let trino_client = std::sync::Arc::clone(&entry.trino_client);
        let query_id = match entry.active_query_id.lock() {
            Ok(g) => g.clone(),
            Err(p) => p.into_inner().clone(),
        };
        drop(entry);

        let Some(qid) = query_id else {
            tracing::debug!(
                pid = req.pid,
                "CancelRequest hit a connection with no active Trino query — ignored"
            );
            return;
        };

        tracing::info!(pid = req.pid, query_id = %qid, "cancelling Trino query");
        if let Err(e) = trino_client.cancel(&qid).await {
            // Best-effort: a Trino cancel for an already-finished query
            // returns 404, and we have nowhere to surface other errors —
            // log and move on. The PG protocol does not acknowledge cancel
            // outcome to the client.
            tracing::warn!(
                pid = req.pid,
                query_id = %qid,
                error = %e,
                "Trino cancel failed (query may have already finished)"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgwire::messages::startup::SecretKey;
    use std::sync::Arc;
    use trino_rust_client::ClientBuilder;

    /// CancelRequest for a (pid, secret_key) pair that was never registered
    /// is silently ignored, not panicked or errored.
    #[tokio::test]
    async fn cancel_for_unknown_keypair_is_a_noop() {
        let req = CancelRequest::new(-1, SecretKey::I32(0));
        // Must complete without panicking. We can't observe the "ignored"
        // state directly, but lookup_cancel returning None drives the
        // early-return path.
        GatewayCancelHandler.on_cancel_request(req).await;
    }

    /// CancelRequest for a registered keypair whose active_query_id is
    /// `None` (no query running) is also a no-op — no Trino call.
    #[tokio::test]
    async fn cancel_for_idle_connection_is_a_noop() {
        let client = Arc::new(ClientBuilder::new("u", "h").build().unwrap());
        let pid = i32::MIN + 7; // unique sentinel for this test
        let secret = SecretKey::I32(424242);
        let _slot = session::register_cancel(pid, secret.clone(), client);
        // active_query_id remains None; on_cancel_request must not call
        // trino_client.cancel (which would try to hit the network).
        GatewayCancelHandler
            .on_cancel_request(CancelRequest::new(pid, secret.clone()))
            .await;
        session::unregister_cancel(pid, &secret);
    }
}
