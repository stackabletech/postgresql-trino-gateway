use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

use postgresql_trino_gateway::config::Config;
use postgresql_trino_gateway::handler::GatewayHandlerFactory;
use postgresql_trino_gateway::query_extended::GatewayExtendedQueryHandler;
use postgresql_trino_gateway::query_simple::GatewayQueryHandler;
use postgresql_trino_gateway::startup::GatewayStartupHandler;

/// Start a gateway on a random port, return the address.
/// The gateway runs as a background tokio task.
async fn start_gateway(config: Config) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let config = Arc::new(config);
    let factory = Arc::new(GatewayHandlerFactory {
        startup: Arc::new(GatewayStartupHandler {
            config: config.clone(),
        }),
        query: Arc::new(GatewayQueryHandler),
        extended_query: Arc::new(GatewayExtendedQueryHandler),
    });

    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((socket, _)) => {
                    let factory = factory.clone();
                    tokio::spawn(async move {
                        let _ = pgwire::tokio::process_socket(socket, None, factory).await;
                    });
                }
                Err(_) => break,
            }
        }
    });

    addr
}

/// Connect to the gateway with tokio-postgres.
async fn connect(addr: SocketAddr) -> Client {
    let conn_str = format!(
        "host={} port={} user=trino dbname=test",
        addr.ip(),
        addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    client
}

/// Extract data rows from simple_query results.
fn extract_rows(messages: Vec<SimpleQueryMessage>) -> Vec<tokio_postgres::SimpleQueryRow> {
    messages
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect()
}

/// Default config for tests that don't need Trino.
fn test_config() -> Config {
    Config {
        listen_addr: "127.0.0.1:0".to_string(),
        trino_host: "localhost".to_string(),
        trino_port: 8080,
        trino_catalog: "memory".to_string(),
        trino_schema: "default".to_string(),
        trino_user: "trino".to_string(),
        trino_ssl: false,
        trino_ssl_insecure: false,
    }
}

/// Config pointing at real Trino (from env vars), or None if not available.
fn trino_config() -> Option<Config> {
    let host = std::env::var("TRINO_HOST").ok()?;
    let port: u16 = std::env::var("TRINO_PORT").ok()?.parse().ok()?;
    let ssl = std::env::var("TRINO_SSL")
        .ok()
        .map(|v| v == "true")
        .unwrap_or(false);
    let ssl_insecure = std::env::var("TRINO_SSL_INSECURE")
        .ok()
        .map(|v| v == "true")
        .unwrap_or(false);
    let catalog = std::env::var("TRINO_CATALOG").unwrap_or_else(|_| "tpch".to_string());
    let schema = std::env::var("TRINO_SCHEMA").unwrap_or_else(|_| "sf1".to_string());
    Some(Config {
        listen_addr: "127.0.0.1:0".to_string(),
        trino_host: host,
        trino_port: port,
        trino_catalog: catalog,
        trino_schema: schema,
        trino_user: "trino".to_string(),
        trino_ssl: ssl,
        trino_ssl_insecure: ssl_insecure,
    })
}

// ---------------------------------------------------------------------------
// Intercept tests (no Trino needed)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_select_version() {
    let addr = start_gateway(test_config()).await;
    let client = connect(addr).await;
    let rows = extract_rows(client.simple_query("SELECT version()").await.unwrap());
    let version = rows[0].get(0).unwrap();
    assert!(
        version.contains("PostgreSQL 16.6"),
        "version() = '{version}'"
    );
}

#[tokio::test]
async fn test_show_server_version() {
    let addr = start_gateway(test_config()).await;
    let client = connect(addr).await;
    let rows = extract_rows(client.simple_query("SHOW server_version").await.unwrap());
    let version = rows[0].get(0).unwrap();
    assert_eq!(version, "16.6");
}

#[tokio::test]
async fn test_set_command() {
    let addr = start_gateway(test_config()).await;
    let client = connect(addr).await;
    client.batch_execute("SET extra_float_digits = 3").await.unwrap();
    client.batch_execute("SET DateStyle = 'ISO, MDY'").await.unwrap();
    client.batch_execute("SET client_encoding = 'UTF8'").await.unwrap();
}

#[tokio::test]
async fn test_transaction_commands() {
    let addr = start_gateway(test_config()).await;
    let client = connect(addr).await;
    client.batch_execute("BEGIN").await.unwrap();
    client.batch_execute("COMMIT").await.unwrap();
    client.batch_execute("BEGIN READ ONLY").await.unwrap();
    client.batch_execute("ROLLBACK").await.unwrap();
}

#[tokio::test]
async fn test_current_database() {
    let addr = start_gateway(test_config()).await;
    let client = connect(addr).await;
    let rows = extract_rows(client.simple_query("SELECT current_database()").await.unwrap());
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn test_pg_is_in_recovery() {
    let addr = start_gateway(test_config()).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query("SELECT pg_catalog.pg_is_in_recovery()")
            .await
            .unwrap(),
    );
    let val = rows[0].get(0).unwrap();
    assert_eq!(val, "false");
}

#[tokio::test]
async fn test_show_standard_conforming_strings() {
    let addr = start_gateway(test_config()).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query("SHOW standard_conforming_strings")
            .await
            .unwrap(),
    );
    let val = rows[0].get(0).unwrap();
    assert_eq!(val, "on");
}

#[tokio::test]
async fn test_pg_type_catalog() {
    let addr = start_gateway(test_config()).await;
    let client = connect(addr).await;
    // The gateway returns all pg_type columns regardless of the SELECT list:
    //   nspname(0), oid(1), typname(2), typtype(3), typnotnull(4), elemtypoid(5)
    let rows = extract_rows(
        client
            .simple_query("SELECT * FROM pg_type")
            .await
            .unwrap(),
    );
    assert!(
        rows.len() > 40,
        "Expected at least 40 types, got {}",
        rows.len()
    );

    // typname is at index 2
    let type_names: Vec<&str> = rows.iter().map(|r| r.get(2).unwrap()).collect();
    assert!(type_names.contains(&"bool"));
    assert!(type_names.contains(&"int4"));
    assert!(type_names.contains(&"varchar"));
    assert!(type_names.contains(&"_int4")); // array type
}

#[tokio::test]
async fn test_pg_namespace_catalog() {
    let addr = start_gateway(test_config()).await;
    let client = connect(addr).await;
    // The gateway returns all pg_namespace columns: oid(0), nspname(1)
    let rows = extract_rows(
        client
            .simple_query("SELECT * FROM pg_namespace")
            .await
            .unwrap(),
    );
    // nspname is at index 1
    let names: Vec<&str> = rows.iter().map(|r| r.get(1).unwrap()).collect();
    assert!(names.contains(&"pg_catalog"));
    assert!(names.contains(&"public"));
}

#[tokio::test]
async fn test_discard_and_deallocate() {
    let addr = start_gateway(test_config()).await;
    let client = connect(addr).await;
    client.batch_execute("DISCARD ALL").await.unwrap();
    client.batch_execute("DEALLOCATE ALL").await.unwrap();
}

#[tokio::test]
async fn test_show_various_params() {
    let addr = start_gateway(test_config()).await;
    let client = connect(addr).await;

    for (param, expected) in [
        ("server_encoding", "UTF8"),
        ("client_encoding", "UTF8"),
        ("integer_datetimes", "on"),
        ("datestyle", "ISO, MDY"),
        ("timezone", "UTC"),
        ("intervalstyle", "postgres"),
        ("max_identifier_length", "63"),
        ("is_superuser", "on"),
    ] {
        let query = format!("SHOW {}", param);
        let rows = extract_rows(client.simple_query(&query).await.unwrap());
        let val = rows[0].get(0).unwrap();
        assert_eq!(
            val, expected,
            "SHOW {} returned '{}', expected '{}'",
            param, val, expected
        );
    }
}

#[tokio::test]
async fn test_current_setting() {
    let addr = start_gateway(test_config()).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query("SELECT current_setting('server_version_num')")
            .await
            .unwrap(),
    );
    let val = rows[0].get(0).unwrap();
    assert_eq!(val, "160006");
}

// ---------------------------------------------------------------------------
// Trino pass-through tests (need TRINO_HOST env var)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_select_one_via_trino() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(client.simple_query("SELECT 1 AS num").await.unwrap());
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn test_trino_with_limit() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query("SELECT name FROM nation LIMIT 5")
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 5);
}

#[tokio::test]
async fn test_trino_count() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query("SELECT count(*) FROM nation")
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn test_trino_join() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query("SELECT n.name, r.name AS region FROM nation n JOIN region r ON n.regionkey = r.regionkey LIMIT 3")
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 3);
}

#[tokio::test]
async fn test_trino_aggregation() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query("SELECT r.name, count(*) as cnt FROM nation n JOIN region r ON n.regionkey = r.regionkey GROUP BY r.name ORDER BY r.name")
            .await
            .unwrap(),
    );
    assert!(!rows.is_empty());
}

#[tokio::test]
async fn test_trino_error_handling() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    // Query a non-existent table -- should get an error, not a crash
    let result = client
        .simple_query("SELECT * FROM nonexistent_table_xyz")
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_pg_class_dynamic_catalog() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query(
                "SELECT relname, relkind FROM pg_catalog.pg_class WHERE relkind IN ('r', 'v')",
            )
            .await
            .unwrap(),
    );
    assert!(!rows.is_empty(), "pg_class should return tables from Trino");
}

#[tokio::test]
async fn test_information_schema_passthrough() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query("SELECT table_name FROM information_schema.tables LIMIT 5")
            .await
            .unwrap(),
    );
    assert!(!rows.is_empty());
}

#[tokio::test]
async fn test_multiple_queries_same_connection() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;

    // Run multiple queries on the same connection
    client
        .batch_execute("SET extra_float_digits = 3")
        .await
        .unwrap();
    let rows = extract_rows(client.simple_query("SELECT version()").await.unwrap());
    assert_eq!(rows.len(), 1);
    let rows = extract_rows(client.simple_query("SELECT 1").await.unwrap());
    assert_eq!(rows.len(), 1);
    let rows = extract_rows(
        client
            .simple_query("SELECT name FROM nation LIMIT 2")
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 2);
}

// ---------------------------------------------------------------------------
// Basic SELECT variations
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_trino_select_column_aliases() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query("SELECT name AS country_name FROM nation LIMIT 3")
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 3);
    // Every row should have a non-empty name
    for row in &rows {
        assert!(!row.get(0).unwrap().is_empty());
    }
}

#[tokio::test]
async fn test_trino_select_expressions() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query("SELECT nationkey, nationkey * 2 AS doubled FROM nation LIMIT 3")
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 3);
    for row in &rows {
        let key: i64 = row.get(0).unwrap().parse().unwrap();
        let doubled: i64 = row.get(1).unwrap().parse().unwrap();
        assert_eq!(doubled, key * 2);
    }
}

#[tokio::test]
async fn test_trino_select_distinct() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query("SELECT DISTINCT regionkey FROM nation")
            .await
            .unwrap(),
    );
    // TPC-H nation has 5 distinct regionkeys (0..4)
    assert_eq!(rows.len(), 5);
}

#[tokio::test]
async fn test_trino_where_multiple_conditions() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query(
                "SELECT name FROM nation WHERE regionkey = 1 AND name > 'B' ORDER BY name",
            )
            .await
            .unwrap(),
    );
    assert!(!rows.is_empty());
    for row in &rows {
        assert!(row.get(0).unwrap() > "B");
    }
}

#[tokio::test]
async fn test_trino_where_in_list() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query("SELECT regionkey, name FROM nation WHERE regionkey IN (1, 2, 3)")
            .await
            .unwrap(),
    );
    assert!(!rows.is_empty());
    for row in &rows {
        let rk: i64 = row.get(0).unwrap().parse().unwrap();
        assert!(rk == 1 || rk == 2 || rk == 3);
    }
}

#[tokio::test]
async fn test_trino_where_between() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query("SELECT nationkey, name FROM nation WHERE nationkey BETWEEN 5 AND 10")
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 6); // 5,6,7,8,9,10
    for row in &rows {
        let nk: i64 = row.get(0).unwrap().parse().unwrap();
        assert!((5..=10).contains(&nk));
    }
}

#[tokio::test]
async fn test_trino_where_is_null_is_not_null() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    // comment column can have NULLs in TPC-H nation, but all rows have a name
    let rows = extract_rows(
        client
            .simple_query("SELECT name FROM nation WHERE name IS NOT NULL")
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 25); // TPC-H has 25 nations
    let rows_null = extract_rows(
        client
            .simple_query("SELECT name FROM nation WHERE name IS NULL")
            .await
            .unwrap(),
    );
    assert_eq!(rows_null.len(), 0);
}

#[tokio::test]
async fn test_trino_where_like() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query("SELECT name FROM nation WHERE name LIKE 'A%' ORDER BY name")
            .await
            .unwrap(),
    );
    assert!(!rows.is_empty());
    for row in &rows {
        assert!(row.get(0).unwrap().starts_with('A'));
    }
}

#[tokio::test]
async fn test_trino_where_or_conditions() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query(
                "SELECT nationkey, name FROM nation WHERE nationkey = 0 OR nationkey = 24",
            )
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_trino_where_not() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query("SELECT name FROM nation WHERE NOT regionkey = 0")
            .await
            .unwrap(),
    );
    // 25 nations total, regionkey=0 has 5 nations => 20 remaining
    assert_eq!(rows.len(), 20);
}

// ---------------------------------------------------------------------------
// Aggregate functions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_trino_aggregate_functions() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query(
                "SELECT count(*) AS cnt, count(DISTINCT regionkey) AS dist_regions, \
                 sum(nationkey) AS total, avg(nationkey) AS average, \
                 min(nationkey) AS mn, max(nationkey) AS mx \
                 FROM nation",
            )
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 1);
    let cnt: i64 = rows[0].get(0).unwrap().parse().unwrap();
    assert_eq!(cnt, 25);
    let dist: i64 = rows[0].get(1).unwrap().parse().unwrap();
    assert_eq!(dist, 5);
    let mn: i64 = rows[0].get(4).unwrap().parse().unwrap();
    assert_eq!(mn, 0);
    let mx: i64 = rows[0].get(5).unwrap().parse().unwrap();
    assert_eq!(mx, 24);
}

#[tokio::test]
async fn test_trino_group_by_having() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query(
                "SELECT regionkey, count(*) AS cnt FROM nation \
                 GROUP BY regionkey HAVING count(*) >= 5 ORDER BY regionkey",
            )
            .await
            .unwrap(),
    );
    assert!(!rows.is_empty());
    for row in &rows {
        let cnt: i64 = row.get(1).unwrap().parse().unwrap();
        assert!(cnt >= 5);
    }
}

// ---------------------------------------------------------------------------
// Sorting and pagination
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_trino_order_by_asc_desc() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;

    // ASC
    let rows = extract_rows(
        client
            .simple_query("SELECT nationkey FROM nation ORDER BY nationkey ASC LIMIT 3")
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 3);
    let keys: Vec<i64> = rows.iter().map(|r| r.get(0).unwrap().parse().unwrap()).collect();
    assert_eq!(keys, vec![0, 1, 2]);

    // DESC
    let rows = extract_rows(
        client
            .simple_query("SELECT nationkey FROM nation ORDER BY nationkey DESC LIMIT 3")
            .await
            .unwrap(),
    );
    let keys: Vec<i64> = rows.iter().map(|r| r.get(0).unwrap().parse().unwrap()).collect();
    assert_eq!(keys, vec![24, 23, 22]);
}

#[tokio::test]
async fn test_trino_order_by_multiple_columns() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query(
                "SELECT regionkey, name FROM nation ORDER BY regionkey ASC, name ASC LIMIT 5",
            )
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 5);
    // First rows should be regionkey=0, names sorted alphabetically
    let rk: i64 = rows[0].get(0).unwrap().parse().unwrap();
    assert_eq!(rk, 0);
}

#[tokio::test]
async fn test_trino_limit_offset() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    // Get nationkeys 5..9 via OFFSET using FETCH FIRST syntax (Trino compatible)
    let rows = extract_rows(
        client
            .simple_query(
                "SELECT nationkey FROM nation ORDER BY nationkey \
                 OFFSET 5 ROWS FETCH FIRST 5 ROWS ONLY",
            )
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 5);
    let keys: Vec<i64> = rows.iter().map(|r| r.get(0).unwrap().parse().unwrap()).collect();
    assert_eq!(keys, vec![5, 6, 7, 8, 9]);
}

#[tokio::test]
async fn test_trino_top_n_pattern() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    // Power BI Top N: ORDER BY + LIMIT
    let rows = extract_rows(
        client
            .simple_query(
                "SELECT name FROM nation ORDER BY nationkey DESC LIMIT 5",
            )
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 5);
}

// ---------------------------------------------------------------------------
// JOINs
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_trino_inner_join() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query(
                "SELECT n.name, r.name AS region_name \
                 FROM nation n INNER JOIN region r ON n.regionkey = r.regionkey \
                 ORDER BY n.name LIMIT 5",
            )
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 5);
    for row in &rows {
        assert!(!row.get(0).unwrap().is_empty());
        assert!(!row.get(1).unwrap().is_empty());
    }
}

#[tokio::test]
async fn test_trino_left_join() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query(
                "SELECT r.name, n.name AS nation_name \
                 FROM region r LEFT JOIN nation n ON r.regionkey = n.regionkey \
                 ORDER BY r.name, n.name LIMIT 10",
            )
            .await
            .unwrap(),
    );
    assert!(!rows.is_empty());
}

#[tokio::test]
async fn test_trino_multi_table_join() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    // 3-table join: customer -> nation -> region
    let rows = extract_rows(
        client
            .simple_query(
                "SELECT c.name AS customer, n.name AS nation, r.name AS region \
                 FROM customer c \
                 JOIN nation n ON c.nationkey = n.nationkey \
                 JOIN region r ON n.regionkey = r.regionkey \
                 LIMIT 5",
            )
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 5);
    for row in &rows {
        assert!(!row.get(0).unwrap().is_empty());
        assert!(!row.get(1).unwrap().is_empty());
        assert!(!row.get(2).unwrap().is_empty());
    }
}

#[tokio::test]
async fn test_trino_self_join() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    // Self-join: nations in the same region
    let rows = extract_rows(
        client
            .simple_query(
                "SELECT a.name, b.name AS same_region_nation \
                 FROM nation a JOIN nation b ON a.regionkey = b.regionkey \
                 WHERE a.nationkey < b.nationkey \
                 ORDER BY a.name LIMIT 5",
            )
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 5);
}

#[tokio::test]
async fn test_trino_join_with_aggregation() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query(
                "SELECT r.name, count(*) AS nation_count \
                 FROM nation n JOIN region r ON n.regionkey = r.regionkey \
                 GROUP BY r.name ORDER BY r.name",
            )
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 5); // 5 regions
    for row in &rows {
        let cnt: i64 = row.get(1).unwrap().parse().unwrap();
        assert_eq!(cnt, 5); // each region has 5 nations in TPC-H
    }
}

// ---------------------------------------------------------------------------
// Subqueries
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_trino_subquery_in_where() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query(
                "SELECT name FROM nation \
                 WHERE regionkey IN (SELECT regionkey FROM region WHERE name = 'EUROPE') \
                 ORDER BY name",
            )
            .await
            .unwrap(),
    );
    assert!(!rows.is_empty());
    assert_eq!(rows.len(), 5); // 5 nations per region
}

#[tokio::test]
async fn test_trino_subquery_in_from() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query(
                "SELECT sub.cnt FROM (SELECT count(*) AS cnt FROM nation) sub",
            )
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 1);
    let cnt: i64 = rows[0].get(0).unwrap().parse().unwrap();
    assert_eq!(cnt, 25);
}

#[tokio::test]
async fn test_trino_correlated_subquery() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query(
                "SELECT r.name, \
                 (SELECT count(*) FROM nation n WHERE n.regionkey = r.regionkey) AS cnt \
                 FROM region r ORDER BY r.name",
            )
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 5);
    for row in &rows {
        let cnt: i64 = row.get(1).unwrap().parse().unwrap();
        assert_eq!(cnt, 5);
    }
}

// ---------------------------------------------------------------------------
// Type handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_trino_integer_types() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query(
                "SELECT CAST(1 AS INTEGER), CAST(2 AS BIGINT), CAST(3 AS SMALLINT)",
            )
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0).unwrap(), "1");
    assert_eq!(rows[0].get(1).unwrap(), "2");
    assert_eq!(rows[0].get(2).unwrap(), "3");
}

#[tokio::test]
async fn test_trino_float_double() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query("SELECT 1.5, CAST(2.5 AS DOUBLE)")
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 1);
    let v1: f64 = rows[0].get(0).unwrap().parse().unwrap();
    assert!((v1 - 1.5).abs() < 0.01);
    let v2: f64 = rows[0].get(1).unwrap().parse().unwrap();
    assert!((v2 - 2.5).abs() < 0.01);
}

#[tokio::test]
async fn test_trino_string_functions() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query(
                "SELECT length('hello'), upper('hello'), lower('HELLO'), \
                 trim('  hi  '), substr('hello', 2, 3), concat('a', 'b', 'c')",
            )
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0).unwrap(), "5");
    assert_eq!(rows[0].get(1).unwrap(), "HELLO");
    assert_eq!(rows[0].get(2).unwrap(), "hello");
    assert_eq!(rows[0].get(3).unwrap(), "hi");
    assert_eq!(rows[0].get(4).unwrap(), "ell");
    assert_eq!(rows[0].get(5).unwrap(), "abc");
}

#[tokio::test]
async fn test_trino_date_time() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query("SELECT DATE '2024-01-01', current_date")
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0).unwrap(), "2024-01-01");
    // current_date should be a valid date string
    assert!(rows[0].get(1).unwrap().contains('-'));
}

#[tokio::test]
async fn test_trino_boolean_values() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query("SELECT true, false")
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0).unwrap(), "true");
    assert_eq!(rows[0].get(1).unwrap(), "false");
}

#[tokio::test]
async fn test_trino_null_handling() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query(
                "SELECT COALESCE(NULL, 'fallback'), NULLIF('a', 'a'), NULLIF('a', 'b'), \
                 CASE WHEN NULL IS NULL THEN 'yes' ELSE 'no' END",
            )
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0).unwrap(), "fallback");
    // NULLIF('a','a') returns NULL which is None in simple_query
    assert!(rows[0].get(1).is_none());
    assert_eq!(rows[0].get(2).unwrap(), "a");
    assert_eq!(rows[0].get(3).unwrap(), "yes");
}

// ---------------------------------------------------------------------------
// CASE expressions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_trino_case_simple() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query(
                "SELECT regionkey, \
                 CASE regionkey \
                   WHEN 0 THEN 'AFRICA' \
                   WHEN 1 THEN 'AMERICA' \
                   WHEN 2 THEN 'ASIA' \
                   WHEN 3 THEN 'EUROPE' \
                   WHEN 4 THEN 'MIDDLE EAST' \
                 END AS region_label \
                 FROM nation ORDER BY nationkey LIMIT 5",
            )
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 5);
    for row in &rows {
        assert!(!row.get(1).unwrap().is_empty());
    }
}

#[tokio::test]
async fn test_trino_case_searched() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query(
                "SELECT name, \
                 CASE WHEN regionkey > 2 THEN 'high' ELSE 'low' END AS category \
                 FROM nation ORDER BY name LIMIT 5",
            )
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 5);
    for row in &rows {
        let cat = row.get(1).unwrap();
        assert!(cat == "high" || cat == "low");
    }
}

// ---------------------------------------------------------------------------
// String operations
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_trino_string_concatenation() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query("SELECT 'hello' || ' ' || 'world'")
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0).unwrap(), "hello world");
}

#[tokio::test]
async fn test_trino_string_operations_on_data() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query(
                "SELECT upper(name), lower(name), length(name), substr(name, 1, 3) \
                 FROM nation WHERE name = 'BRAZIL'",
            )
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0).unwrap(), "BRAZIL");
    assert_eq!(rows[0].get(1).unwrap(), "brazil");
    assert_eq!(rows[0].get(2).unwrap(), "6");
    assert_eq!(rows[0].get(3).unwrap(), "BRA");
}

#[tokio::test]
async fn test_trino_string_comparison() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query(
                "SELECT name FROM nation WHERE name >= 'U' ORDER BY name",
            )
            .await
            .unwrap(),
    );
    assert!(!rows.is_empty());
    for row in &rows {
        assert!(row.get(0).unwrap() >= "U");
    }
}

// ---------------------------------------------------------------------------
// Window functions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_trino_row_number() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query(
                "SELECT name, ROW_NUMBER() OVER (ORDER BY name) AS rn \
                 FROM nation ORDER BY name LIMIT 5",
            )
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 5);
    for (i, row) in rows.iter().enumerate() {
        let rn: i64 = row.get(1).unwrap().parse().unwrap();
        assert_eq!(rn, (i + 1) as i64);
    }
}

#[tokio::test]
async fn test_trino_row_number_partition() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query(
                "SELECT regionkey, name, \
                 ROW_NUMBER() OVER (PARTITION BY regionkey ORDER BY name) AS rn \
                 FROM nation ORDER BY regionkey, name LIMIT 10",
            )
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 10);
    // First row in each partition should have rn=1
    let rn_first: i64 = rows[0].get(2).unwrap().parse().unwrap();
    assert_eq!(rn_first, 1);
}

#[tokio::test]
async fn test_trino_rank_dense_rank() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query(
                "SELECT regionkey, \
                 RANK() OVER (ORDER BY regionkey) AS rnk, \
                 DENSE_RANK() OVER (ORDER BY regionkey) AS drnk \
                 FROM nation ORDER BY nationkey LIMIT 10",
            )
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 10);
    // All rows should have non-empty rank values
    for row in &rows {
        let _rnk: i64 = row.get(1).unwrap().parse().unwrap();
        let _drnk: i64 = row.get(2).unwrap().parse().unwrap();
    }
}

#[tokio::test]
async fn test_trino_window_aggregate() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query(
                "SELECT regionkey, name, \
                 COUNT(*) OVER (PARTITION BY regionkey) AS region_count, \
                 SUM(nationkey) OVER (PARTITION BY regionkey) AS region_sum \
                 FROM nation ORDER BY regionkey, name LIMIT 10",
            )
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 10);
    // Each region has 5 nations
    for row in &rows {
        let cnt: i64 = row.get(2).unwrap().parse().unwrap();
        assert_eq!(cnt, 5);
    }
}

// ---------------------------------------------------------------------------
// Common Table Expressions (CTE)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_trino_cte_simple() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query(
                "WITH european_nations AS ( \
                   SELECT n.name FROM nation n \
                   JOIN region r ON n.regionkey = r.regionkey \
                   WHERE r.name = 'EUROPE' \
                 ) \
                 SELECT name FROM european_nations ORDER BY name",
            )
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 5);
}

#[tokio::test]
async fn test_trino_cte_multiple() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query(
                "WITH region_counts AS ( \
                   SELECT regionkey, count(*) AS cnt FROM nation GROUP BY regionkey \
                 ), \
                 region_names AS ( \
                   SELECT regionkey, name FROM region \
                 ) \
                 SELECT rn.name, rc.cnt \
                 FROM region_counts rc JOIN region_names rn ON rc.regionkey = rn.regionkey \
                 ORDER BY rn.name",
            )
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 5);
    for row in &rows {
        let cnt: i64 = row.get(1).unwrap().parse().unwrap();
        assert_eq!(cnt, 5);
    }
}

// ---------------------------------------------------------------------------
// UNION / INTERSECT / EXCEPT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_trino_union_all() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query(
                "SELECT name FROM nation WHERE regionkey = 0 \
                 UNION ALL \
                 SELECT name FROM nation WHERE regionkey = 1",
            )
            .await
            .unwrap(),
    );
    // 5 nations per region, so 10 total
    assert_eq!(rows.len(), 10);
}

#[tokio::test]
async fn test_trino_union_distinct() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let rows = extract_rows(
        client
            .simple_query(
                "SELECT regionkey FROM nation WHERE regionkey IN (0, 1) \
                 UNION \
                 SELECT regionkey FROM nation WHERE regionkey IN (1, 2)",
            )
            .await
            .unwrap(),
    );
    // UNION deduplicates: should get 0, 1, 2
    assert_eq!(rows.len(), 3);
}

// ---------------------------------------------------------------------------
// SQL rewrite verification
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_trino_ilike_rewrite() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    // ILIKE should be rewritten to lower() LIKE lower() transparently
    let rows = extract_rows(
        client
            .simple_query("SELECT name FROM nation WHERE name ILIKE '%united%' ORDER BY name")
            .await
            .unwrap(),
    );
    assert!(!rows.is_empty());
    for row in &rows {
        assert!(row.get(0).unwrap().to_lowercase().contains("united"));
    }
}

#[tokio::test]
async fn test_trino_cast_rewrite() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    // ::text should be rewritten to CAST(... AS VARCHAR)
    let rows = extract_rows(
        client
            .simple_query("SELECT nationkey::text FROM nation LIMIT 3")
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 3);
    for row in &rows {
        // Should be valid string representations of integers
        let _: i64 = row.get(0).unwrap().parse().unwrap();
    }
}

// ---------------------------------------------------------------------------
// Error cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_trino_error_syntax() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let result = client.simple_query("SELEC broken syntax here").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_trino_error_nonexistent_column() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let result = client
        .simple_query("SELECT nonexistent_column_xyz FROM nation")
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_trino_error_division_by_zero() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let result = client.simple_query("SELECT 1 / 0").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_trino_error_type_mismatch() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;
    let result = client
        .simple_query("SELECT CAST('not_a_number' AS INTEGER)")
        .await;
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// Multiple statements / session behavior
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_trino_session_set_then_query() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;

    // Power BI typical startup sequence
    client.batch_execute("SET extra_float_digits = 3").await.unwrap();
    client.batch_execute("SET DateStyle = 'ISO, MDY'").await.unwrap();
    client.batch_execute("SET client_encoding = 'UTF8'").await.unwrap();

    // Then run real queries
    let rows = extract_rows(
        client
            .simple_query("SELECT name FROM nation LIMIT 5")
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 5);
}

#[tokio::test]
async fn test_trino_begin_query_commit() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;

    // Power BI wraps queries in transactions
    client.batch_execute("BEGIN").await.unwrap();
    let rows = extract_rows(
        client
            .simple_query("SELECT count(*) FROM nation")
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 1);
    let cnt: i64 = rows[0].get(0).unwrap().parse().unwrap();
    assert_eq!(cnt, 25);
    client.batch_execute("COMMIT").await.unwrap();
}

#[tokio::test]
async fn test_trino_many_queries_same_connection() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;

    // Run 10 queries on the same connection
    for i in 0..10 {
        let rows = extract_rows(
            client
                .simple_query(&format!("SELECT {} AS val", i))
                .await
                .unwrap(),
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0).unwrap(), i.to_string());
    }
}
