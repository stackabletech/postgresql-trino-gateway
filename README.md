# PostgreSQL-Trino Gateway

A PostgreSQL wire-protocol shim in front of Trino. Power BI, Npgsql, JDBC,
and `psql` connect to the gateway as if it were a PostgreSQL server; the
gateway translates the queries and forwards them to Trino's REST API.

Copyright 2026 Stackable GmbH. Licensed under OSL-3.0.

## Status

Pre-1.0. The protocol surface is what Power BI Report Server exercises in
DirectQuery mode against TPC-H. Other PG clients (Npgsql, pgjdbc, `psql`)
work for read queries; INSERT/UPDATE/CREATE TABLE work via simple-query
and via prepared statements. Multi-statement batches are split and run one
at a time. Wire format is text only.

What is intentionally not implemented:

- SCRAM-SHA-256 (the gateway holds the cleartext password to forward as
  HTTP Basic auth to Trino, which doesn't compose with SCRAM).
- Binary wire format.
- Per-IP rate limiting (only a global concurrent-connection cap).
- Cancel of statements in a multi-statement batch other than the
  most-recently-submitted one.

## Build

```bash
./scripts/build.sh                   # release, dynamically linked
./scripts/build-static.sh            # static, via musl (needs musl-tools)
```

`rust-toolchain.toml` pins Rust 1.93; `rustup` will install it on first
build.

## Run

The gateway needs a Trino host. The simplest invocation, against a
loopback Trino on plain HTTP:

```bash
./target/release/postgresql-trino-gateway \
    --listen-addr 127.0.0.1:15432 \
    --trino-host localhost \
    --trino-port 8080 \
    --trino-catalog tpch \
    --trino-schema sf1
```

Connect with `psql`:

```bash
psql -h 127.0.0.1 -p 15432 -U trino -d tpch
```

`scripts/start.sh` reads `TRINO_HOST`, `TRINO_PORT`, `TRINO_CATALOG`,
`TRINO_SCHEMA`, `LISTEN_ADDR`, and `RUST_LOG` from the environment and
runs the binary. `scripts/connect.sh` is a thin wrapper around `psql`.

## Authentication and transport security

The gateway has two security planes:

- **Listener side (gateway accepts PG connections).** With `--auth=false`
  (the default) any client connection runs queries as the configured
  `--trino-user`. With `--auth=true` the gateway issues a CleartextPassword
  challenge and forwards the credentials to Trino as HTTP Basic auth.
  SCRAM-SHA-256 is not implemented; see [Status](#status).
- **Trino side (gateway connects to Trino).** Plain HTTP or HTTPS, with
  optional certificate-verification skip for self-signed setups.

The startup-time policy refuses configurations where credentials would
cross the network in cleartext:

| Listener | `--auth` | TLS configured | Bind | Outcome |
|---|---|---|---|---|
| any | off | n/a | loopback | OK (silent) |
| any | off | n/a | non-loopback | refused unless `--allow-insecure-listener` |
| any | on | yes | any | OK; plaintext clients refused at handshake |
| any | on | no | loopback | OK with a startup warning |
| any | on | no | non-loopback | refused |

For `--auth=true` over Trino HTTP (no `--trino-ssl`), the operator must
also pass `--trino-allow-plaintext-auth` to acknowledge that the password
will be forwarded over plain HTTP. Without it, startup fails.

## Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--listen-addr` | `127.0.0.1:5432` | PG listener address |
| `--trino-host` | `localhost` | Trino server hostname |
| `--trino-port` | `8080` | Trino server port |
| `--trino-catalog` | `memory` | Default Trino catalog |
| `--trino-schema` | `default` | Default Trino schema |
| `--trino-user` | `trino` | Trino user when `--auth=false` |
| `--trino-ssl` | off | Use HTTPS for Trino requests |
| `--trino-tls-no-verify` | off | Skip Trino TLS cert verification |
| `--trino-allow-plaintext-auth` | off | Forward credentials over HTTP to Trino |
| `--auth` | off | Require password from PG clients |
| `--allow-insecure-listener` | off | Permit `--auth=false` on a non-loopback bind |
| `--max-connections` | `256` | Concurrent connection cap |
| `--tls-cert` | none | PEM cert chain for the PG listener (requires `--tls-key`) |
| `--tls-key` | none | PEM private key for the PG listener (requires `--tls-cert`) |

The `GATEWAY_SHUTDOWN_DRAIN_TIMEOUT_SECS` environment variable overrides
the default 25-second connection drain on SIGTERM/SIGINT.

`RUST_LOG=postgresql_trino_gateway=debug` logs every query and rewrite;
`...=trace` adds the protocol-level decisions (intercept hits, response
shapes). Row contents are never logged.

## What the gateway does to your SQL

Most queries pass through unchanged. The rewriter handles the cases that
break Trino's strict-PostgreSQL parsing:

- `x::int4` becomes `CAST(x AS INTEGER)`.
- `ILIKE` becomes `lower(x) LIKE lower(pattern)`.
- A small set of functions are renamed: `string_agg` → `listagg`,
  `log` → `log10`, `trunc` → `truncate`.
- PostgreSQL type names map to Trino: `text` → `VARCHAR`, `int4` →
  `INTEGER`, and so on.

A handful of queries are intercepted and answered locally rather than
forwarded:

- `SET`, `SHOW`, `BEGIN`, `COMMIT`, `ROLLBACK`, `DISCARD`, `DEALLOCATE`.
- `version()`, `current_database()`, `current_schema`,
  `pg_is_in_recovery()`, `current_setting('server_version[_num]')`.
- `pg_catalog` queries used by Npgsql/pgjdbc for type loading
  (`pg_type`, `pg_enum`, `pg_class`, `pg_attribute`, `pg_namespace`,
  `pg_range`).
- `INFORMATION_SCHEMA` tables Trino doesn't expose
  (`referential_constraints`, `table_constraints`, `key_column_usage`,
  ...) return zero rows with the right column shape.

## Test

```bash
cargo test                                          # unit tests, no Trino
cargo clippy --all-targets -- -D warnings           # lint
cargo fmt --check                                   # format

# With Trino (read-only):
TRINO_HOST=... TRINO_PORT=... TRINO_SSL=true TRINO_TLS_NO_VERIFY=true \
    TRINO_CATALOG=tpch TRINO_SCHEMA=sf1 \
    cargo test

# With DDL tests (needs a writable catalog like 'memory'):
TRINO_WRITE_CATALOG=memory TRINO_WRITE_SCHEMA=default \
    cargo test
```

`pre-commit run --all-files` runs the full battery (fmt, clippy, tests,
cargo-deny, shellcheck, markdownlint) and is the gating check.

## Project layout

```
src/
  main.rs              CLI, TCP listener, accept loop, graceful shutdown
  config.rs            Config (clap)
  policy.rs            Startup auth/TLS/listener-policy validation
  startup.rs           PG startup handler, auth, Trino client construction
  tls.rs               TLS termination for the PG listener
  handler.rs           PgWireServerHandlers factory
  query_simple.rs      Simple query protocol handler
  query_extended.rs    Extended query protocol handler (Parse/Bind/Describe/Execute)
  query_pipeline.rs    Per-connection pipeline; multi-statement splitting
  query_inspection.rs  AST-based dispatch (references_table, calls_function)
  intercept.rs         SET/SHOW/transaction/server-function interception
  cancel.rs            PG CancelRequest -> Trino DELETE /v1/query/{id}
  session.rs           Per-connection state, cancel registry, portal cache
  catalog/             pg_catalog emulation
  rewrite/             SQL rewriting visitors
  types.rs             Trino-to-PG type mapping and value encoding
  trino_stream.rs      Streaming bridge (poll Trino, yield PG DataRow)
  error_mapping.rs     Trino errors -> PG SQLSTATE codes
tests/
  integration_test.rs  Data-driven tests against a real Trino
```

`pgwire` and `trino-rust-client` come from crates.io; nothing is vendored.
