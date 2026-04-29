// Copyright 2026 Stackable GmbH
// Licensed under the Open Software License version 3.0 (OSL-3.0).
// See LICENSE file in the project root for full license text.
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, LazyLock, Mutex};

use dashmap::DashMap;
use pgwire::api::results::Response;
use trino_rust_client::Client as TrinoClient;

use crate::config::Config;

/// Pipeline responses produced by `do_describe_portal` and consumed by
/// `do_query`, keyed by portal name.
///
/// Wrapped in `Arc<Mutex<...>>` (rather than a `DashMap`) because `Response`
/// contains a `dyn Stream + Send` that isn't `Sync`. Concurrent access within
/// one connection isn't required — pgwire processes a connection's messages
/// serially — so a single mutex is fine.
pub type PortalCache = Arc<Mutex<HashMap<String, Response>>>;

/// Per-connection state keyed by `{peer_addr}_{pid}` in `CONNECTIONS`.
///
/// pgwire's `ClientInfo::metadata()` is `HashMap<String, String>`, so it can't
/// hold an `Arc<TrinoClient>`. Once pgwire's `SessionExtensions` is released,
/// this global map can be replaced.
pub struct ConnectionState {
    pub trino_client: Arc<TrinoClient>,
    pub config: Arc<Config>,
    /// Pipeline result produced by `do_describe_portal`, taken by `do_query`
    /// so the query runs against Trino once per Describe+Execute pair instead
    /// of twice. Orphaned entries (Describe with no Execute) are freed when
    /// the connection's state is removed.
    pub portals: PortalCache,
}

static CONNECTIONS: LazyLock<DashMap<String, ConnectionState>> = LazyLock::new(DashMap::new);

const CONNECTION_ID_KEY: &str = "_conn_id";

/// Metadata key under which the conn_id is stored on the pgwire `ClientInfo`.
pub fn connection_id_key() -> &'static str {
    CONNECTION_ID_KEY
}

pub fn register_connection(conn_id: String, state: ConnectionState) {
    CONNECTIONS.insert(conn_id, state);
}

pub fn get_connection(
    conn_id: &str,
) -> Option<dashmap::mapref::one::Ref<'_, String, ConnectionState>> {
    CONNECTIONS.get(conn_id)
}

/// Remove every entry whose key has `{addr}_` as a prefix.
///
/// Called via a drop guard in the per-connection spawn task so the entry is
/// removed whether `process_socket` returns Ok, returns Err, or panics. The
/// `(peer_ip, source_port)` tuple is unique among currently-established TCP
/// connections, which is what makes the prefix match safe — the kernel will
/// not reuse a tuple while a connection holding it is still active. After
/// close, the tuple may sit in TIME_WAIT for tens of seconds before becoming
/// reusable; cleanup here runs on connection-task exit, well before reuse.
pub fn remove_connections_for_addr(addr: SocketAddr) {
    let prefix = format!("{addr}_");
    CONNECTIONS.retain(|key, _| !key.starts_with(&prefix));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use std::sync::Arc;
    use trino_rust_client::ClientBuilder;

    fn dummy_state() -> ConnectionState {
        let client = ClientBuilder::new("u", "h").build().unwrap();
        ConnectionState {
            trino_client: Arc::new(client),
            config: Arc::new(Config {
                listen_addr: "127.0.0.1:5432".to_owned(),
                trino_host: "h".to_owned(),
                trino_port: 8080,
                trino_catalog: "c".to_owned(),
                trino_schema: "s".to_owned(),
                trino_user: "u".to_owned(),
                trino_ssl: false,
                trino_ssl_insecure: false,
                auth: false,
            }),
            portals: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Addresses from RFC 5737 documentation ranges so the test won't collide
    /// with real or future test entries in the global CONNECTIONS map.
    #[test]
    fn remove_by_addr_strips_only_matching_prefix() {
        let a: SocketAddr = "192.0.2.1:11111".parse().unwrap();
        let b: SocketAddr = "192.0.2.2:22222".parse().unwrap();
        register_connection(format!("{a}_1"), dummy_state());
        register_connection(format!("{a}_2"), dummy_state());
        register_connection(format!("{b}_1"), dummy_state());

        remove_connections_for_addr(a);

        assert!(get_connection(&format!("{a}_1")).is_none());
        assert!(get_connection(&format!("{a}_2")).is_none());
        assert!(get_connection(&format!("{b}_1")).is_some());

        remove_connections_for_addr(b);
    }
}
