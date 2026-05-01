// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0

//! Per-connection state and the cross-connection cancel registry.
//!
//! Per-connection state (`ConnectionState`) lives in pgwire's
//! `SessionExtensions` map (added in pgwire 0.39). The map is a typed
//! `Arc`-store hung off `ClientInfo`, so each handler can pull the same
//! `Arc<ConnectionState>` for the connection it's serving without a
//! global keyed lookup.
//!
//! The cancel registry stays global because a `CancelRequest` arrives on
//! a separate TCP connection that has its own `SessionExtensions` —
//! routing it back to the originating connection's running Trino query
//! requires a process-wide map keyed by the `(pid, secret_key)` pair
//! pgwire sends in `BackendKeyData`.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use pgwire::api::results::Response;
use pgwire::messages::startup::SecretKey;
use trino_rust_client::Client as TrinoClient;

use crate::config::Config;

/// Trino query id of the connection's currently-running query, shared
/// between the streaming bridge (writer) and the cancel handler (reader).
/// `None` when the connection is idle or hasn't yet sent a query to Trino.
pub type ActiveQueryId = Arc<Mutex<Option<String>>>;

/// Routing entry for a PG `CancelRequest`. Created at connection startup
/// and removed when the originating `ConnectionState` is dropped.
pub struct CancelEntry {
    pub trino_client: Arc<TrinoClient>,
    pub active_query_id: ActiveQueryId,
}

/// Map keyed by `(pid, secret_key)` (the pair pgwire sends in
/// `BackendKeyData` and matches in a subsequent `CancelRequest`).
///
/// Cleanup runs via `Drop for ConnectionState` (below) — when the last
/// `Arc<ConnectionState>` is dropped (which happens automatically when
/// pgwire tears down the connection's `SessionExtensions`), the
/// `unregister_cancel` call removes our entry.
static CANCEL_REGISTRY: LazyLock<Mutex<HashMap<(i32, SecretKey), CancelEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub fn register_cancel(
    pid: i32,
    secret_key: SecretKey,
    trino_client: Arc<TrinoClient>,
) -> ActiveQueryId {
    let active_query_id = Arc::new(Mutex::new(None));
    let mut registry = CANCEL_REGISTRY.lock().unwrap_or_else(|p| p.into_inner());
    registry.insert(
        (pid, secret_key),
        CancelEntry {
            trino_client,
            active_query_id: Arc::clone(&active_query_id),
        },
    );
    active_query_id
}

/// Look up a cancel entry by `(pid, secret_key)` and clone out the bits
/// the cancel handler needs. Returns `None` when no matching entry exists.
///
/// We clone rather than return a reference so the caller can `await`
/// across the resulting Trino-cancel call without holding the registry's
/// mutex.
pub fn lookup_cancel(
    pid: i32,
    secret_key: &SecretKey,
) -> Option<(Arc<TrinoClient>, ActiveQueryId)> {
    let registry = CANCEL_REGISTRY.lock().unwrap_or_else(|p| p.into_inner());
    registry
        .get(&(pid, secret_key.clone()))
        .map(|e| (Arc::clone(&e.trino_client), Arc::clone(&e.active_query_id)))
}

pub fn unregister_cancel(pid: i32, secret_key: &SecretKey) {
    let mut registry = CANCEL_REGISTRY.lock().unwrap_or_else(|p| p.into_inner());
    registry.remove(&(pid, secret_key.clone()));
}

/// A cached pipeline response keyed by the SQL text it was produced for.
///
/// We can't key just on portal name: the PostgreSQL extended protocol allows
/// a client to re-Bind an existing portal name to a different statement, and
/// pgwire doesn't expose a Bind hook we could use to invalidate. The query
/// text is checked at retrieval and the entry is discarded on mismatch.
pub struct CachedPortalResponse {
    pub query: String,
    pub response: Response,
}

/// Pipeline responses produced by `do_describe_portal` and consumed by
/// `do_query`, keyed by portal name.
///
/// Wrapped in `Arc<Mutex<...>>` (rather than a `DashMap`) because `Response`
/// contains a `dyn Stream + Send` that isn't `Sync`. Concurrent access within
/// one connection isn't required — pgwire processes a connection's messages
/// serially — so a single mutex is fine.
pub type PortalCache = Arc<Mutex<HashMap<String, CachedPortalResponse>>>;

/// Maximum number of cached portal responses per connection.
///
/// A misbehaving or adversarial client can issue Describe for many distinct
/// named portals without ever sending Execute, and each cached entry holds a
/// live Trino query open server-side. Cap the cache so the gateway can't be
/// pushed into unbounded memory or Trino-side query growth by one connection.
pub const MAX_CACHED_PORTALS: usize = 64;

/// Per-connection state stored in pgwire's `SessionExtensions` map. Each
/// PG connection holds a single `Arc<ConnectionState>`; pgwire drops it
/// when the connection closes (normal exit, error, panic, drain abort),
/// which fires `Drop for ConnectionState` to clean up the cancel-registry
/// entry.
pub struct ConnectionState {
    pub trino_client: Arc<TrinoClient>,
    pub config: Arc<Config>,
    /// Pipeline result produced by `do_describe_portal`, taken by `do_query`
    /// so the query runs against Trino once per Describe+Execute pair instead
    /// of twice. Orphaned entries (Describe with no Execute) are freed when
    /// the connection's state is removed.
    pub portals: PortalCache,
    /// Slot the streaming bridge writes when Trino returns the initial
    /// response for a query. Read by the `CancelHandler` to issue a
    /// `DELETE /v1/query/{id}` against Trino on a PG `CancelRequest`.
    pub active_query_id: ActiveQueryId,
    /// `(pid, secret_key)` registered with `CANCEL_REGISTRY` at startup.
    /// `Drop` uses this to clean the registry entry when the connection
    /// state is removed (panic, normal close, drain abort).
    pub cancel_key: (i32, SecretKey),
}

impl Drop for ConnectionState {
    fn drop(&mut self) {
        unregister_cancel(self.cancel_key.0, &self.cancel_key.1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use trino_rust_client::ClientBuilder;

    /// Counter so each call gets a unique cancel_key — the dummy state's
    /// Drop unregisters from the global registry, so reusing keys across
    /// tests would race.
    fn next_test_pid() -> i32 {
        use std::sync::atomic::{AtomicI32, Ordering};
        static N: AtomicI32 = AtomicI32::new(900_000);
        N.fetch_add(1, Ordering::Relaxed)
    }

    /// Cancel registry register / lookup / drop-driven cleanup all work
    /// across the new `Mutex<HashMap>` storage.
    #[test]
    fn cancel_registry_lifecycle() {
        let client = Arc::new(ClientBuilder::new("u", "h").build().unwrap());
        let pid = next_test_pid();
        let secret = SecretKey::I32(pid);

        let _slot = register_cancel(pid, secret.clone(), Arc::clone(&client));
        assert!(lookup_cancel(pid, &secret).is_some());

        unregister_cancel(pid, &secret);
        assert!(lookup_cancel(pid, &secret).is_none());
    }
}
