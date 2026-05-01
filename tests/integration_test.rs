// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0

// This whole crate is test code; clippy doesn't recognise the async helper
// functions as `#[test]`-annotated, so the test-allowance config in
// clippy.toml doesn't apply. Re-allow the specific lints here.
#![allow(clippy::panic, clippy::unwrap_used)]

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

use postgresql_trino_gateway::config::Config;
use postgresql_trino_gateway::handler::GatewayHandlerFactory;
use postgresql_trino_gateway::query_extended::GatewayExtendedQueryHandler;
use postgresql_trino_gateway::query_simple::GatewayQueryHandler;
use postgresql_trino_gateway::startup::GatewayStartupHandler;

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

async fn start_gateway(config: Config) -> SocketAddr {
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

async fn connect(addr: SocketAddr) -> Client {
    connect_as(addr, "trino", None).await
}

async fn connect_as(addr: SocketAddr, user: &str, password: Option<&str>) -> Client {
    let mut conn_str = format!(
        "host={} port={} user={} dbname=test",
        addr.ip(),
        addr.port(),
        user,
    );
    if let Some(pw) = password {
        conn_str.push_str(&format!(" password={pw}"));
    }
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    client
}

fn extract_rows(messages: Vec<SimpleQueryMessage>) -> Vec<tokio_postgres::SimpleQueryRow> {
    messages
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect()
}

fn test_config() -> Config {
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

fn trino_config() -> Option<Config> {
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

/// A test case: SQL to execute and how to check the result.
enum Check {
    /// Query returns rows. `min_rows` is the minimum expected count.
    Rows { min_rows: usize },
    /// Query returns rows, and the first row/column matches this value.
    Value { value: &'static str },
    /// Query returns rows, and the first row/column contains this substring.
    Contains { substring: &'static str },
    /// The command executes without error (for SET, BEGIN, etc.).
    Executes,
    /// The query should fail (syntax error, missing table, etc.).
    Fails,
}

/// Run a batch of test cases against a shared gateway + connection.
async fn run_cases(client: &Client, cases: &[(&str, &str, Check)]) {
    for (name, sql, check) in cases {
        match check {
            Check::Executes => {
                client
                    .batch_execute(sql)
                    .await
                    .unwrap_or_else(|e| panic!("[{name}] expected success, got: {e}"));
            }
            Check::Fails => {
                let result = client.simple_query(sql).await;
                assert!(result.is_err(), "[{name}] expected error, query succeeded");
            }
            Check::Rows { min_rows } => {
                let rows = extract_rows(
                    client
                        .simple_query(sql)
                        .await
                        .unwrap_or_else(|e| panic!("[{name}] query failed: {e}")),
                );
                assert!(
                    rows.len() >= *min_rows,
                    "[{name}] expected >= {min_rows} rows, got {}",
                    rows.len()
                );
            }
            Check::Value { value } => {
                let rows = extract_rows(
                    client
                        .simple_query(sql)
                        .await
                        .unwrap_or_else(|e| panic!("[{name}] query failed: {e}")),
                );
                assert!(!rows.is_empty(), "[{name}] expected rows, got none");
                let actual = rows[0].get(0).unwrap_or("");
                assert_eq!(
                    actual, *value,
                    "[{name}] expected '{value}', got '{actual}'"
                );
            }
            Check::Contains { substring } => {
                let rows = extract_rows(
                    client
                        .simple_query(sql)
                        .await
                        .unwrap_or_else(|e| panic!("[{name}] query failed: {e}")),
                );
                assert!(!rows.is_empty(), "[{name}] expected rows, got none");
                let actual = rows[0].get(0).unwrap_or("");
                assert!(
                    actual.contains(substring),
                    "[{name}] expected '{actual}' to contain '{substring}'"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Intercept tests — no Trino needed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_intercept_queries() {
    let addr = start_gateway(test_config()).await;
    let client = connect(addr).await;

    run_cases(
        &client,
        &[
            // SET commands
            (
                "set extra_float_digits",
                "SET extra_float_digits = 3",
                Check::Executes,
            ),
            (
                "set datestyle",
                "SET DateStyle = 'ISO, MDY'",
                Check::Executes,
            ),
            (
                "set client_encoding",
                "SET client_encoding = 'UTF8'",
                Check::Executes,
            ),
            (
                "set statement_timeout",
                "SET statement_timeout = 0",
                Check::Executes,
            ),
            (
                "set search_path",
                "SET search_path = '\"$user\", public'",
                Check::Executes,
            ),
            // Transactions
            ("begin", "BEGIN", Check::Executes),
            ("commit", "COMMIT", Check::Executes),
            ("begin read only", "BEGIN READ ONLY", Check::Executes),
            ("rollback", "ROLLBACK", Check::Executes),
            // Session cleanup
            ("discard all", "DISCARD ALL", Check::Executes),
            ("deallocate all", "DEALLOCATE ALL", Check::Executes),
            // Server info functions
            (
                "version()",
                "SELECT version()",
                Check::Contains {
                    substring: "PostgreSQL 16.6",
                },
            ),
            (
                "current_database()",
                "SELECT current_database()",
                Check::Rows { min_rows: 1 },
            ),
            (
                "pg_is_in_recovery",
                "SELECT pg_catalog.pg_is_in_recovery()",
                Check::Value { value: "false" },
            ),
            (
                "current_setting version_num",
                "SELECT current_setting('server_version_num')",
                Check::Value { value: "160006" },
            ),
            (
                "character_sets_utf8",
                "SELECT character_set_name FROM INFORMATION_SCHEMA.character_sets",
                Check::Value { value: "UTF8" },
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_show_params() {
    let addr = start_gateway(test_config()).await;
    let client = connect(addr).await;

    let params: &[(&str, &str)] = &[
        ("server_version", "16.6"),
        ("server_encoding", "UTF8"),
        ("client_encoding", "UTF8"),
        ("integer_datetimes", "on"),
        ("datestyle", "ISO, MDY"),
        ("timezone", "UTC"),
        ("intervalstyle", "postgres"),
        ("max_identifier_length", "63"),
        ("is_superuser", "on"),
        ("standard_conforming_strings", "on"),
        ("transaction_isolation", "read committed"),
        ("in_hot_standby", "off"),
        ("default_transaction_read_only", "off"),
        ("search_path", "\"$user\", public"),
        ("application_name", ""),
    ];

    for (param, expected) in params {
        let sql = format!("SHOW {param}");
        let rows = extract_rows(client.simple_query(&sql).await.unwrap());
        let val = rows[0].get(0).unwrap();
        assert_eq!(val, *expected, "SHOW {param}: '{val}' != '{expected}'");
    }
}

#[tokio::test]
async fn test_pg_type_catalog() {
    let addr = start_gateway(test_config()).await;
    let client = connect(addr).await;

    // pg_type returns columns: nspname(0), oid(1), typname(2), typtype(3), typnotnull(4), elemtypoid(5)
    let rows = extract_rows(client.simple_query("SELECT * FROM pg_type").await.unwrap());
    assert!(rows.len() > 40, "expected >40 types, got {}", rows.len());

    // typname is at column index 1 (schema: nspname, typname, oid, typrelid, typbasetype, type, elemoid, ord)
    let type_names: Vec<&str> = rows.iter().map(|r| r.get(1).unwrap()).collect();
    for expected in [
        "bool",
        "int2",
        "int4",
        "int8",
        "float4",
        "float8",
        "varchar",
        "text",
        "date",
        "timestamp",
        "timestamptz",
        "numeric",
        "uuid",
        "jsonb",
        "_int4",
        "_varchar",
        "_bool",
        "_text",
        "record",
        "void",
        "unknown",
    ] {
        assert!(
            type_names.contains(&expected),
            "missing pg_type: {expected}"
        );
    }
}

#[tokio::test]
async fn test_pg_namespace_catalog() {
    let addr = start_gateway(test_config()).await;
    let client = connect(addr).await;

    let rows = extract_rows(
        client
            .simple_query("SELECT * FROM pg_namespace")
            .await
            .unwrap(),
    );
    let names: Vec<&str> = rows.iter().map(|r| r.get(1).unwrap()).collect();
    for expected in ["pg_catalog", "public", "information_schema"] {
        assert!(names.contains(&expected), "missing namespace: {expected}");
    }
}

// ---------------------------------------------------------------------------
// Trino pass-through tests — gated behind TRINO_HOST env var
// ---------------------------------------------------------------------------

macro_rules! trino_tests {
    ($test_name:ident, $cases:expr) => {
        #[tokio::test]
        async fn $test_name() {
            let config = match trino_config() {
                Some(c) => c,
                None => {
                    eprintln!("Skipping {}: TRINO_HOST not set", stringify!($test_name));
                    return;
                }
            };
            let addr = start_gateway(config).await;
            let client = connect(addr).await;
            run_cases(&client, $cases).await;
        }
    };
}

trino_tests!(
    test_basic_selects,
    &[
        ("select 1", "SELECT 1 AS num", Check::Rows { min_rows: 1 }),
        (
            "column alias",
            "SELECT name AS country_name FROM nation LIMIT 3",
            Check::Rows { min_rows: 3 }
        ),
        (
            "expression",
            "SELECT nationkey * 2 AS doubled FROM nation LIMIT 3",
            Check::Rows { min_rows: 3 }
        ),
        (
            "distinct",
            "SELECT DISTINCT regionkey FROM nation",
            Check::Rows { min_rows: 1 }
        ),
    ]
);

trino_tests!(
    test_where_clauses,
    &[
        (
            "and",
            "SELECT name FROM nation WHERE regionkey = 1 AND nationkey > 5",
            Check::Rows { min_rows: 1 }
        ),
        (
            "or",
            "SELECT name FROM nation WHERE regionkey = 0 OR regionkey = 4",
            Check::Rows { min_rows: 1 }
        ),
        (
            "not",
            "SELECT name FROM nation WHERE NOT regionkey = 0",
            Check::Rows { min_rows: 1 }
        ),
        (
            "in list",
            "SELECT name FROM nation WHERE regionkey IN (1, 2, 3)",
            Check::Rows { min_rows: 1 }
        ),
        (
            "between",
            "SELECT name FROM nation WHERE nationkey BETWEEN 5 AND 10",
            Check::Rows { min_rows: 1 }
        ),
        (
            "like",
            "SELECT name FROM nation WHERE name LIKE 'A%'",
            Check::Rows { min_rows: 1 }
        ),
        (
            "is null",
            "SELECT name FROM nation WHERE comment IS NOT NULL",
            Check::Rows { min_rows: 1 }
        ),
    ]
);

trino_tests!(
    test_aggregates,
    &[
        (
            "count",
            "SELECT count(*) FROM nation",
            Check::Rows { min_rows: 1 }
        ),
        (
            "count distinct",
            "SELECT count(DISTINCT regionkey) FROM nation",
            Check::Rows { min_rows: 1 }
        ),
        (
            "sum/avg/min/max",
            "SELECT sum(nationkey), avg(nationkey), min(nationkey), max(nationkey) FROM nation",
            Check::Rows { min_rows: 1 }
        ),
        (
            "group by",
            "SELECT regionkey, count(*) FROM nation GROUP BY regionkey",
            Check::Rows { min_rows: 3 }
        ),
        (
            "having",
            "SELECT regionkey, count(*) AS cnt FROM nation GROUP BY regionkey HAVING count(*) > 3",
            Check::Rows { min_rows: 1 }
        ),
    ]
);

trino_tests!(
    test_sorting_pagination,
    &[
        (
            "order by asc",
            "SELECT name FROM nation ORDER BY name ASC LIMIT 3",
            Check::Rows { min_rows: 3 }
        ),
        (
            "order by desc",
            "SELECT name FROM nation ORDER BY name DESC LIMIT 3",
            Check::Rows { min_rows: 3 }
        ),
        (
            "order by multi",
            "SELECT regionkey, name FROM nation ORDER BY regionkey, name LIMIT 5",
            Check::Rows { min_rows: 5 }
        ),
        (
            "top n",
            "SELECT name FROM nation ORDER BY nationkey LIMIT 5",
            Check::Rows { min_rows: 5 }
        ),
        (
            "offset",
            "SELECT name FROM nation ORDER BY nationkey OFFSET 5 ROWS FETCH FIRST 3 ROWS ONLY",
            Check::Rows { min_rows: 3 }
        ),
    ]
);

trino_tests!(
    test_joins,
    &[
        (
            "inner join",
            "SELECT n.name, r.name FROM nation n JOIN region r ON n.regionkey = r.regionkey LIMIT 5",
            Check::Rows { min_rows: 5 }
        ),
        (
            "left join",
            "SELECT r.name, n.name FROM region r LEFT JOIN nation n ON r.regionkey = n.regionkey LIMIT 5",
            Check::Rows { min_rows: 5 }
        ),
        (
            "3-table join",
            "SELECT c.name, n.name, r.name FROM customer c JOIN nation n ON c.nationkey = n.nationkey JOIN region r ON n.regionkey = r.regionkey LIMIT 3",
            Check::Rows { min_rows: 3 }
        ),
        (
            "join + agg",
            "SELECT r.name, count(*) FROM nation n JOIN region r ON n.regionkey = r.regionkey GROUP BY r.name",
            Check::Rows { min_rows: 3 }
        ),
    ]
);

trino_tests!(
    test_subqueries,
    &[
        (
            "in subquery",
            "SELECT name FROM nation WHERE regionkey IN (SELECT regionkey FROM region WHERE name = 'AFRICA')",
            Check::Rows { min_rows: 1 }
        ),
        (
            "derived table",
            "SELECT sub.cnt FROM (SELECT count(*) AS cnt FROM nation) sub",
            Check::Rows { min_rows: 1 }
        ),
        (
            "correlated",
            "SELECT n.name FROM nation n WHERE n.nationkey = (SELECT min(n2.nationkey) FROM nation n2 WHERE n2.regionkey = n.regionkey)",
            Check::Rows { min_rows: 1 }
        ),
    ]
);

trino_tests!(
    test_types,
    &[
        (
            "integer cast",
            "SELECT CAST(1 AS INTEGER)",
            Check::Rows { min_rows: 1 }
        ),
        (
            "bigint cast",
            "SELECT CAST(9999999999 AS BIGINT)",
            Check::Rows { min_rows: 1 }
        ),
        (
            "double",
            "SELECT CAST(3.14 AS DOUBLE)",
            Check::Rows { min_rows: 1 }
        ),
        (
            "boolean true",
            "SELECT true",
            Check::Value { value: "true" }
        ),
        (
            "boolean false",
            "SELECT false",
            Check::Value { value: "false" }
        ),
        (
            "date literal",
            "SELECT DATE '2024-01-15'",
            Check::Value {
                value: "2024-01-15"
            }
        ),
        (
            "current_date",
            "SELECT current_date",
            Check::Rows { min_rows: 1 }
        ),
        (
            "null handling",
            "SELECT COALESCE(NULL, 'fallback')",
            Check::Value { value: "fallback" }
        ),
        ("nullif", "SELECT NULLIF(1, 1)", Check::Rows { min_rows: 1 }),
    ]
);

trino_tests!(
    test_string_ops,
    &[
        (
            "concat",
            "SELECT 'hello' || ' ' || 'world'",
            Check::Value {
                value: "hello world"
            }
        ),
        (
            "upper",
            "SELECT upper('hello')",
            Check::Value { value: "HELLO" }
        ),
        (
            "lower",
            "SELECT lower('HELLO')",
            Check::Value { value: "hello" }
        ),
        (
            "length",
            "SELECT length('hello')",
            Check::Value { value: "5" }
        ),
        (
            "substr",
            "SELECT substr('hello world', 1, 5)",
            Check::Value { value: "hello" }
        ),
        (
            "trim",
            "SELECT trim('  hello  ')",
            Check::Value { value: "hello" }
        ),
    ]
);

trino_tests!(
    test_case_expressions,
    &[
        (
            "simple case",
            "SELECT CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END",
            Check::Value { value: "one" }
        ),
        (
            "searched case",
            "SELECT CASE WHEN 1 > 2 THEN 'no' ELSE 'yes' END",
            Check::Value { value: "yes" }
        ),
        (
            "case on data",
            "SELECT name, CASE WHEN regionkey < 2 THEN 'low' ELSE 'high' END AS tier FROM nation LIMIT 3",
            Check::Rows { min_rows: 3 }
        ),
    ]
);

trino_tests!(
    test_window_functions,
    &[
        (
            "row_number",
            "SELECT name, ROW_NUMBER() OVER (ORDER BY name) AS rn FROM nation LIMIT 5",
            Check::Rows { min_rows: 5 }
        ),
        (
            "row_number partition",
            "SELECT name, ROW_NUMBER() OVER (PARTITION BY regionkey ORDER BY name) AS rn FROM nation LIMIT 5",
            Check::Rows { min_rows: 5 }
        ),
        (
            "rank",
            "SELECT name, RANK() OVER (ORDER BY regionkey) AS rnk FROM nation LIMIT 5",
            Check::Rows { min_rows: 5 }
        ),
        (
            "sum over",
            "SELECT name, SUM(nationkey) OVER (PARTITION BY regionkey) AS total FROM nation LIMIT 5",
            Check::Rows { min_rows: 5 }
        ),
    ]
);

trino_tests!(
    test_cte,
    &[
        (
            "single cte",
            "WITH counts AS (SELECT regionkey, count(*) AS cnt FROM nation GROUP BY regionkey) SELECT * FROM counts",
            Check::Rows { min_rows: 3 }
        ),
        (
            "multi cte",
            "WITH r AS (SELECT * FROM region), n AS (SELECT * FROM nation) SELECT n.name, r.name FROM n JOIN r ON n.regionkey = r.regionkey LIMIT 3",
            Check::Rows { min_rows: 3 }
        ),
    ]
);

trino_tests!(
    test_set_ops,
    &[
        (
            "union all",
            "SELECT name FROM nation WHERE regionkey = 0 UNION ALL SELECT name FROM nation WHERE regionkey = 1",
            Check::Rows { min_rows: 2 }
        ),
        (
            "union distinct",
            "SELECT regionkey FROM nation UNION SELECT regionkey FROM nation",
            Check::Rows { min_rows: 3 }
        ),
    ]
);

trino_tests!(
    test_sql_rewrites,
    &[
        (
            "ilike rewrite",
            "SELECT name FROM nation WHERE name ILIKE '%united%'",
            Check::Rows { min_rows: 1 }
        ),
        // ::text cast should be rewritten to CAST(... AS VARCHAR) transparently
        (
            "cast rewrite",
            "SELECT nationkey::text FROM nation LIMIT 1",
            Check::Rows { min_rows: 1 }
        ),
    ]
);

trino_tests!(
    test_errors,
    &[
        ("syntax error", "SELECTT 1", Check::Fails),
        (
            "bad column",
            "SELECT nonexistent_col FROM nation",
            Check::Fails
        ),
        (
            "bad table",
            "SELECT * FROM nonexistent_table_xyz_123",
            Check::Fails
        ),
        (
            "type mismatch",
            "SELECT nationkey + 'not_a_number' FROM nation",
            Check::Fails
        ),
    ]
);

trino_tests!(
    test_session_behavior,
    &[(
        "set then query",
        "SET extra_float_digits = 3",
        Check::Executes
    ),]
);

#[tokio::test]
async fn test_multi_query_session() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;

    // Mix intercepted and Trino queries on the same connection
    client
        .batch_execute("SET extra_float_digits = 3")
        .await
        .unwrap();
    client.batch_execute("BEGIN").await.unwrap();

    let rows = extract_rows(client.simple_query("SELECT version()").await.unwrap());
    assert!(rows[0].get(0).unwrap().contains("PostgreSQL"));

    let rows = extract_rows(client.simple_query("SELECT 1").await.unwrap());
    assert_eq!(rows.len(), 1);

    let rows = extract_rows(
        client
            .simple_query("SELECT name FROM nation LIMIT 2")
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 2);

    let rows = extract_rows(
        client
            .simple_query("SELECT count(*) FROM nation")
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 1);

    client.batch_execute("COMMIT").await.unwrap();
}

// ---------------------------------------------------------------------------
// DDL/DML tests (need Trino with a writable catalog like 'memory')
// Gated behind TRINO_WRITE_CATALOG env var — skip if not set.

fn writable_trino_config() -> Option<Config> {
    let write_catalog = std::env::var("TRINO_WRITE_CATALOG").ok()?;
    let mut config = trino_config()?;
    config.trino_catalog = write_catalog;
    config.trino_schema =
        std::env::var("TRINO_WRITE_SCHEMA").unwrap_or_else(|_| "default".to_string());
    Some(config)
}

#[tokio::test]
async fn test_create_insert_select_drop() {
    let config = match writable_trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_WRITE_CATALOG not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;

    // CREATE TABLE
    client
        .simple_query("CREATE TABLE IF NOT EXISTS test_gateway_ddl (id INTEGER, name VARCHAR)")
        .await
        .unwrap_or_else(|e| panic!("CREATE TABLE failed: {e}"));

    // INSERT
    client
        .simple_query(
            "INSERT INTO test_gateway_ddl VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')",
        )
        .await
        .unwrap_or_else(|e| panic!("INSERT failed: {e}"));

    // SELECT back
    let rows = extract_rows(
        client
            .simple_query("SELECT id, name FROM test_gateway_ddl ORDER BY id")
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 3, "expected 3 rows after insert");
    assert_eq!(rows[0].get(1).unwrap(), "alice");
    assert_eq!(rows[1].get(1).unwrap(), "bob");
    assert_eq!(rows[2].get(1).unwrap(), "charlie");

    // SELECT with WHERE
    let rows = extract_rows(
        client
            .simple_query("SELECT name FROM test_gateway_ddl WHERE id > 1 ORDER BY id")
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 2);

    // DROP TABLE
    client
        .simple_query("DROP TABLE IF EXISTS test_gateway_ddl")
        .await
        .unwrap_or_else(|e| panic!("DROP TABLE failed: {e}"));
}

#[tokio::test]
async fn test_create_table_as_select() {
    let config = match writable_trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_WRITE_CATALOG not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;

    // Drop if exists from prior run
    let _ = client
        .simple_query("DROP TABLE IF EXISTS test_gateway_ctas")
        .await;

    // CTAS
    client
        .simple_query("CREATE TABLE test_gateway_ctas AS SELECT 1 AS id, 'hello' AS val")
        .await
        .unwrap_or_else(|e| panic!("CTAS failed: {e}"));

    let rows = extract_rows(
        client
            .simple_query("SELECT id, val FROM test_gateway_ctas")
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(1).unwrap(), "hello");

    client
        .simple_query("DROP TABLE IF EXISTS test_gateway_ctas")
        .await
        .unwrap();
}

// Dynamic catalog tests (need Trino)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pg_class_from_trino() {
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

    // pg_class returns: oid(0), relname(1), relnamespace(2), relkind(3)
    let table_names: Vec<&str> = rows.iter().map(|r| r.get(1).unwrap()).collect();
    // TPC-H tables should be present
    assert!(
        table_names.contains(&"nation") || table_names.contains(&"region"),
        "expected TPC-H tables, got: {table_names:?}"
    );
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
    assert!(
        !rows.is_empty(),
        "information_schema.tables should return results"
    );
}

// ---------------------------------------------------------------------------
// Authentication tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_auth_disabled_allows_any_connection() {
    let config = test_config(); // auth: false
    let addr = start_gateway(config).await;

    // Should connect without any password
    let client = connect(addr).await;
    let rows = extract_rows(client.simple_query("SELECT version()").await.unwrap());
    assert!(!rows.is_empty());
}

#[tokio::test]
async fn test_auth_enabled_requires_password() {
    let mut config = test_config();
    config.auth = true;
    let addr = start_gateway(config).await;

    // Connecting without a password should fail (server requests password,
    // but tokio-postgres sends empty password which Trino will reject).
    // The connection itself should fail or the first query should fail.
    let conn_str = format!(
        "host={} port={} user=trino dbname=test",
        addr.ip(),
        addr.port()
    );
    let result = tokio_postgres::connect(&conn_str, NoTls).await;
    // With auth enabled and no reachable Trino, this should fail during
    // the auth handshake (credential validation query to Trino fails).
    assert!(
        result.is_err(),
        "Expected connection to fail without valid credentials"
    );
}

/// This test requires Trino to have password authentication configured.
/// Set TRINO_AUTH_ENABLED=true along with TRINO_HOST to run it.
#[tokio::test]
async fn test_auth_passthrough_to_trino() {
    if std::env::var("TRINO_AUTH_ENABLED").ok().as_deref() != Some("true") {
        eprintln!("Skipping: TRINO_AUTH_ENABLED not set (Trino needs password auth configured)");
        return;
    }
    let config = match trino_config() {
        Some(mut c) => {
            c.auth = true;
            c
        }
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;

    // Connect with valid credentials. The actual user/password depend on
    // how Trino's password authentication is configured.
    let user = std::env::var("TRINO_AUTH_USER").unwrap_or_else(|_| "trino".to_string());
    let password = std::env::var("TRINO_AUTH_PASSWORD").unwrap_or_else(|_| "trino".to_string());
    let client = connect_as(addr, &user, Some(&password)).await;
    let rows = extract_rows(client.simple_query("SELECT 1").await.unwrap());
    assert_eq!(rows.len(), 1);
}

// ---------------------------------------------------------------------------
// Extended query protocol (Parse / Bind / Describe / Execute)
//
// `simple_query` uses the simple-query protocol; `query` and `prepare`
// drive the extended-query handler in `query_extended.rs`. Until these
// were added, the extended path had zero direct test coverage.
// ---------------------------------------------------------------------------

/// Prepare-and-execute drives Parse/Bind/Describe/Execute end-to-end. The
/// portal-cache path in query_extended is exercised here: do_describe_portal
/// runs the query and stashes the response, do_query takes the stash.
#[tokio::test]
async fn test_extended_prepared_select() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;

    let stmt = client.prepare("SELECT 1::int4 AS one").await.unwrap();
    let rows = client.query(&stmt, &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let one: i32 = rows[0].get(0);
    assert_eq!(one, 1);
}

/// A second Execute on the same prepared statement: the portal cache from
/// the first Describe is consumed, so the second Execute re-runs through
/// the pipeline. Checks the cache-miss fallback path.
#[tokio::test]
async fn test_extended_re_execute() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;

    let stmt = client.prepare("SELECT 42::int4").await.unwrap();
    for _ in 0..3 {
        let rows = client.query(&stmt, &[]).await.unwrap();
        assert_eq!(rows.len(), 1);
        let v: i32 = rows[0].get(0);
        assert_eq!(v, 42);
    }
}

/// Two distinct prepared statements on one connection — both must work
/// without colliding on the unnamed portal or interfering with each other's
/// active_query_id slot.
#[tokio::test]
async fn test_extended_two_prepared_statements() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;

    let stmt_a = client.prepare("SELECT 1::int4").await.unwrap();
    let stmt_b = client.prepare("SELECT 2::int4").await.unwrap();

    let rows_a = client.query(&stmt_a, &[]).await.unwrap();
    let rows_b = client.query(&stmt_b, &[]).await.unwrap();

    assert_eq!(rows_a[0].get::<_, i32>(0), 1);
    assert_eq!(rows_b[0].get::<_, i32>(0), 2);
}

/// Catalog-emulation queries reach Trino through the extended path too —
/// Npgsql and pgjdbc drive type loading via prepared statements. Confirm
/// the static intercept still answers correctly via that path.
#[tokio::test]
async fn test_extended_catalog_intercept() {
    let config = match trino_config() {
        Some(c) => c,
        None => {
            eprintln!("Skipping: TRINO_HOST not set");
            return;
        }
    };
    let addr = start_gateway(config).await;
    let client = connect(addr).await;

    let stmt = client
        .prepare("SELECT oid, typname FROM pg_type LIMIT 5")
        .await
        .unwrap();
    let rows = client.query(&stmt, &[]).await.unwrap();
    assert!(!rows.is_empty(), "pg_type intercept should return rows");
}
