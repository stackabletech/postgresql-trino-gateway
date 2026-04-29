// Copyright 2026 Stackable GmbH
// Licensed under the Open Software License version 3.0 (OSL-3.0).
// See LICENSE file in the project root for full license text.
use std::net::SocketAddr;
use std::sync::{Arc, LazyLock};

use dashmap::DashMap;
use trino_rust_client::Client as TrinoClient;

use crate::config::Config;

/// Per-connection state keyed by `{peer_addr}_{pid}` in `CONNECTIONS`.
///
/// pgwire's `ClientInfo::metadata()` is `HashMap<String, String>`, so it can't
/// hold an `Arc<TrinoClient>`. Once pgwire's `SessionExtensions` is released,
/// this global map can be replaced.
pub struct ConnectionState {
    pub trino_client: Arc<TrinoClient>,
    pub config: Arc<Config>,
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
/// Called from the spawn task after `pgwire::tokio::process_socket` returns,
/// when we no longer have access to the conn_id but do still have the peer
/// address from accept(). Source-port-uniqueness of concurrent TCP connections
/// makes this safe.
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
        }
    }

    #[test]
    fn remove_by_addr_strips_only_matching_prefix() {
        let a: SocketAddr = "10.0.0.1:1111".parse().unwrap();
        let b: SocketAddr = "10.0.0.2:2222".parse().unwrap();
        register_connection(format!("{a}_1"), dummy_state());
        register_connection(format!("{a}_2"), dummy_state());
        register_connection(format!("{b}_1"), dummy_state());

        remove_connections_for_addr(a);

        assert!(get_connection(&format!("{a}_1")).is_none());
        assert!(get_connection(&format!("{a}_2")).is_none());
        assert!(get_connection(&format!("{b}_1")).is_some());

        // cleanup
        remove_connections_for_addr(b);
    }
}
