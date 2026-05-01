// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0

//! Integration tests driven by the *real* `psql` command-line client.
//!
//! These exercise the same wire-protocol path as `integration_test.rs`
//! but with a different driver — `psql` (libpq) instead of
//! `tokio-postgres`. Catches client-specific issues that the embedded
//! driver wouldn't surface: startup negotiation quirks, notice
//! handling, and the simple-query-with-CommandComplete flow that
//! psql's `-c` mode uses by default.
//!
//! A representative subset of cases (not the full ~200 from
//! `integration_test.rs`); the goal is wire-protocol confidence with
//! a real binary, not test parity. Subprocess overhead is ~50ms per
//! query, so we cap at ~10–15 cases.
//!
//! Skips the entire suite when `psql` is not on `PATH` (e.g. minimal
//! CI images without postgresql-client).

#![allow(clippy::panic, clippy::unwrap_used)]

use std::net::SocketAddr;
use std::process::Stdio;

use tokio::process::Command;

mod common;
use common::{start_gateway, trino_config};

/// Result of running one psql command.
struct PsqlOutput {
    stdout: String,
    stderr: String,
    success: bool,
}

/// Run `psql -A -t -F$'\t' -c "{sql}"` against the gateway and return the
/// captured stdout/stderr/exit. `-A` (no align), `-t` (tuples-only) and
/// `-F` (deterministic field separator) give parseable output.
async fn run_psql(addr: SocketAddr, catalog: &str, sql: &str) -> PsqlOutput {
    let port = addr.port().to_string();
    let host = addr.ip().to_string();
    let args: Vec<&str> = vec![
        "--no-psqlrc",
        "-h",
        &host,
        "-p",
        &port,
        "-U",
        "trino",
        "-d",
        catalog,
        "-A", // no aligned output
        "-t", // tuples-only (no header / footer)
        "-F",
        "\t",
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        sql,
    ];

    // Print the command before running so a hung test reveals what's
    // actually being invoked. Shell-quote each arg so the line can be
    // copy-pasted verbatim.
    let quoted = args
        .iter()
        .map(|a| {
            if a.chars()
                .all(|c| c.is_ascii_alphanumeric() || "-_=.,/:".contains(c))
            {
                (*a).to_string()
            } else {
                format!("'{}'", a.replace('\'', r"'\''"))
            }
        })
        .collect::<Vec<_>>()
        .join(" ");
    println!("$ PGPASSWORD='' psql {quoted}");

    // Async tokio::process::Command, not std::process::Command. The
    // sync version's `.output()` blocks the runtime thread; with
    // `#[tokio::test]`'s default single-threaded runtime, that
    // deadlocks against the in-process gateway's accept loop (which
    // needs the same thread to schedule). The async `.output().await`
    // parks the future so the runtime can keep serving the gateway
    // while psql runs.
    let out = Command::new("psql")
        .args(&args)
        .env("PGPASSWORD", "") // auth is disabled in test_config
        // Without explicit `Stdio::null()` for stdin, psql inherits the
        // test process's stdin and blocks reading from it forever —
        // even with `-c`. Closing stdin makes psql exit as soon as the
        // `-c` query finishes.
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .expect("failed to spawn psql; install postgresql-client");
    PsqlOutput {
        stdout: String::from_utf8_lossy(&out.stdout).to_string(),
        stderr: String::from_utf8_lossy(&out.stderr).to_string(),
        success: out.status.success(),
    }
}

fn psql_available() -> bool {
    // One-time sync probe at test start; no async needed and no
    // deadlock risk (gateway hasn't been started yet).
    std::process::Command::new("psql")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Skip the whole suite if `psql` isn't installed. Returns true when we
/// should bail out of the calling test.
fn skip_if_no_psql() -> bool {
    if psql_available() {
        return false;
    }
    eprintln!("Skipping psql test: `psql` not on PATH");
    true
}

fn skip_if_no_trino() -> Option<postgresql_trino_gateway::config::Config> {
    match trino_config() {
        Some(c) => Some(c),
        None => {
            eprintln!("Skipping psql test: TRINO_HOST not set");
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Tests against a non-Trino-dependent gateway (intercepts only)
// ---------------------------------------------------------------------------

/// `SELECT 1` is the canonical "does the wire protocol work" probe.
/// With our gateway pointing at a (running) Trino, psql's startup +
/// simple-query path should round-trip cleanly.
#[tokio::test]
async fn psql_select_one() {
    if skip_if_no_psql() {
        return;
    }
    let Some(config) = skip_if_no_trino() else {
        return;
    };
    let addr = start_gateway(config).await;
    let out = run_psql(addr, "tpch", "SELECT 1").await;
    assert!(
        out.success,
        "psql failed: {}\nstderr: {}",
        out.stdout, out.stderr
    );
    assert_eq!(out.stdout.trim(), "1");
}

/// `SHOW server_version` exercises the SHOW intercept path. The gateway
/// answers locally without touching Trino. Verifies the intercept's
/// RowDescription + DataRow flow with a real psql.
#[tokio::test]
async fn psql_show_server_version() {
    if skip_if_no_psql() {
        return;
    }
    let Some(config) = skip_if_no_trino() else {
        return;
    };
    let addr = start_gateway(config).await;
    let out = run_psql(addr, "tpch", "SHOW server_version").await;
    assert!(out.success, "psql failed: {}", out.stderr);
    assert_eq!(out.stdout.trim(), "16.6");
}

/// `SELECT version()` exercises the bare-scalar-SELECT intercept. Same
/// flow as above but through a different intercept branch.
#[tokio::test]
async fn psql_select_version() {
    if skip_if_no_psql() {
        return;
    }
    let Some(config) = skip_if_no_trino() else {
        return;
    };
    let addr = start_gateway(config).await;
    let out = run_psql(addr, "tpch", "SELECT version()").await;
    assert!(out.success, "psql failed: {}", out.stderr);
    assert!(
        out.stdout.contains("PostgreSQL 16.6"),
        "expected version string, got: {}",
        out.stdout
    );
}

/// `BEGIN; ... ; COMMIT` — Trino has no client-managed transactions, so
/// these are intercepted as no-ops. psql's `-c` runs the whole batch in
/// one simple-query message and expects per-statement CommandComplete.
#[tokio::test]
async fn psql_transaction_no_op_batch() {
    if skip_if_no_psql() {
        return;
    }
    let Some(config) = skip_if_no_trino() else {
        return;
    };
    let addr = start_gateway(config).await;
    let out = run_psql(addr, "tpch", "BEGIN; SELECT 1; COMMIT").await;
    assert!(
        out.success,
        "psql failed: {}\nstderr: {}",
        out.stdout, out.stderr
    );
    // The middle SELECT yields its row; psql concatenates the per-
    // statement output with newlines. With `-A -t` the row body is
    // just "1".
    assert!(
        out.stdout.contains('1'),
        "middle SELECT should yield 1: {}",
        out.stdout
    );
}

/// `SELECT * FROM pg_type LIMIT 1` exercises the static catalog
/// intercept that Power BI / Npgsql / pgjdbc rely on for type loading.
/// Real psql receives the prebuilt static response.
#[tokio::test]
async fn psql_pg_type_intercept() {
    if skip_if_no_psql() {
        return;
    }
    let Some(config) = skip_if_no_trino() else {
        return;
    };
    let addr = start_gateway(config).await;
    let out = run_psql(addr, "tpch", "SELECT typname FROM pg_type LIMIT 1").await;
    assert!(out.success, "psql failed: {}", out.stderr);
    assert!(
        !out.stdout.trim().is_empty(),
        "pg_type intercept should produce at least one row"
    );
}

// ---------------------------------------------------------------------------
// Tests against Trino (require TRINO_HOST)
// ---------------------------------------------------------------------------

/// A real Trino-forwarded SELECT against TPC-H. Confirms the streaming
/// bridge produces wire-correct DataRows for psql.
#[tokio::test]
async fn psql_trino_tpch_nation_count() {
    if skip_if_no_psql() {
        return;
    }
    let Some(config) = skip_if_no_trino() else {
        return;
    };
    let addr = start_gateway(config).await;
    let out = run_psql(addr, "tpch", "SELECT COUNT(*) FROM tpch.sf1.nation").await;
    assert!(
        out.success,
        "psql failed: {}\nstderr: {}",
        out.stdout, out.stderr
    );
    assert_eq!(
        out.stdout.trim(),
        "25",
        "TPC-H sf1 has 25 nations; got: {}",
        out.stdout
    );
}

/// SQL rewrite — `ILIKE` becomes `lower() LIKE lower()`. psql sees the
/// rewritten output but the query result is what matters.
#[tokio::test]
async fn psql_trino_ilike_rewrite() {
    if skip_if_no_psql() {
        return;
    }
    let Some(config) = skip_if_no_trino() else {
        return;
    };
    let addr = start_gateway(config).await;
    let out = run_psql(
        addr,
        "tpch",
        "SELECT name FROM tpch.sf1.nation WHERE name ILIKE 'gER%'",
    )
    .await;
    assert!(
        out.success,
        "psql failed: {}\nstderr: {}",
        out.stdout, out.stderr
    );
    assert_eq!(
        out.stdout.trim(),
        "GERMANY",
        "ILIKE 'gER%' should match GERMANY; got: {}",
        out.stdout
    );
}

/// Multi-row, multi-column SELECT — verifies row separator and field
/// separator handling at the psql side.
#[tokio::test]
async fn psql_trino_multi_row_multi_col() {
    if skip_if_no_psql() {
        return;
    }
    let Some(config) = skip_if_no_trino() else {
        return;
    };
    let addr = start_gateway(config).await;
    let out = run_psql(
        addr,
        "tpch",
        "SELECT regionkey, name FROM tpch.sf1.region ORDER BY regionkey",
    )
    .await;
    assert!(
        out.success,
        "psql failed: {}\nstderr: {}",
        out.stdout, out.stderr
    );
    let lines: Vec<&str> = out.stdout.trim().split('\n').collect();
    assert_eq!(
        lines.len(),
        5,
        "TPC-H sf1.region has 5 rows; got: {:?}",
        lines
    );
    assert!(lines[0].contains("AFRICA"), "row 0: {}", lines[0]);
}

/// Cast syntax — `x::int4` is rewritten to `CAST(x AS INTEGER)`. The
/// rewriter operates on the AST; a real psql client confirms the
/// rewritten SQL still produces a correct wire response.
#[tokio::test]
async fn psql_trino_cast_rewrite() {
    if skip_if_no_psql() {
        return;
    }
    let Some(config) = skip_if_no_trino() else {
        return;
    };
    let addr = start_gateway(config).await;
    let out = run_psql(addr, "tpch", "SELECT (1 + 2)::int4 AS result").await;
    assert!(
        out.success,
        "psql failed: {}\nstderr: {}",
        out.stdout, out.stderr
    );
    assert_eq!(out.stdout.trim(), "3");
}

/// Bad SQL — Trino returns a syntax error, the gateway sanitises and
/// forwards as ErrorResponse. psql exits non-zero with the message in
/// stderr. Confirms the error-mapping path doesn't crash psql.
#[tokio::test]
async fn psql_trino_syntax_error_surfaces() {
    if skip_if_no_psql() {
        return;
    }
    let Some(config) = skip_if_no_trino() else {
        return;
    };
    let addr = start_gateway(config).await;
    let out = run_psql(addr, "tpch", "SELECT * FROM no_such_table_at_all").await;
    assert!(
        !out.success,
        "expected non-zero exit; stdout: {}\nstderr: {}",
        out.stdout, out.stderr
    );
    assert!(
        out.stderr.to_lowercase().contains("error"),
        "expected an ERROR in stderr; got: {}",
        out.stderr
    );
}
