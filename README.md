# PostgreSQL-Trino Gateway

A Rust service that speaks PostgreSQL wire protocol on the frontend and translates
queries to Trino's REST API on the backend. Enables Power BI and other PostgreSQL
clients to query Trino via DirectQuery.

Copyright 2026 Stackable GmbH. Licensed under OSL-3.0.

## Quick Start

### Prerequisites

- [Rust](https://rustup.rs/) (1.85+ recommended)
- A running Trino instance
- For static builds: `musl-tools` (see `build-static.sh`)

### Build

```bash
# Standard build (dynamically linked)
./build.sh

# Static build (no glibc dependency, runs on any Linux)
./build-static.sh
```

### Run

```bash
# Configure and start
export TRINO_HOST=your-trino-host
export TRINO_PORT=8443
export TRINO_CATALOG=tpch
export TRINO_SCHEMA=sf1
./start.sh

# Or run directly
./target/release/postgresql-trino-gateway \
    --listen-addr 0.0.0.0:15432 \
    --trino-host your-trino-host \
    --trino-port 8443 \
    --trino-ssl --trino-ssl-insecure \
    --trino-catalog tpch \
    --trino-schema sf1
```

### Connect

```bash
# psql
psql -h gateway-host -p 15432 -U trino -d tpch

# Or use the helper script
./connect.sh
```

## Configuration

All options can be passed as CLI flags or set via environment variables in `start.sh`.

| Flag | Default | Description |
|------|---------|-------------|
| `--listen-addr` | `127.0.0.1:5432` | Address to listen for PostgreSQL connections |
| `--trino-host` | `localhost` | Trino server hostname |
| `--trino-port` | `8080` | Trino server port |
| `--trino-catalog` | `memory` | Default Trino catalog |
| `--trino-schema` | `default` | Default Trino schema |
| `--trino-user` | `trino` | Trino user (used when --auth is disabled) |
| `--trino-ssl` | off | Use HTTPS to connect to Trino |
| `--trino-ssl-insecure` | off | Skip TLS certificate verification (self-signed certs) |
| `--auth` | off | Require password authentication from clients (forwarded to Trino as Basic auth) |

### Logging

```bash
# Info level (connections, auth)
RUST_LOG=postgresql_trino_gateway=info ./start.sh

# Debug level (every query, SQL rewrites)
RUST_LOG=postgresql_trino_gateway=debug ./start.sh
```

### Authentication

When `--auth` is enabled:
1. Client connects and is prompted for a password
2. Gateway forwards username + password to Trino as HTTP Basic auth
3. If Trino rejects the credentials, the connection is refused

Trino must have password authentication configured (e.g., file-based or LDAP).

## Architecture

```
Power BI / psql / DBeaver
        |
        | PostgreSQL wire protocol (port 15432)
        v
+------------------+
|     Gateway      |
|                  |
|  Intercept layer | -- SET, SHOW, BEGIN/COMMIT, version()
|  pg_catalog      | -- Fakes pg_type, pg_class, pg_attribute
|  SQL rewriter    | -- ::cast, ILIKE, function names
|  Trino client    | -- REST API, streaming results
+------------------+
        |
        | HTTPS (Trino REST API)
        v
    Trino Cluster
```

## Features

- **PostgreSQL wire protocol** (simple + extended query protocols)
- **Streaming results** from Trino (no full-result buffering)
- **pg_catalog emulation** for type discovery (pg_type, pg_class, pg_attribute, pg_namespace)
- **SQL rewriting** (PostgreSQL dialect to Trino):
  - `x::type` to `CAST(x AS type)`
  - `ILIKE` to `lower() LIKE lower()`
  - Function translation: `string_agg` to `listagg`, `log` to `log10`, `trunc` to `truncate`
  - PostgreSQL type names to Trino: `text` to `VARCHAR`, `int4` to `INTEGER`, etc.
- **Authentication pass-through** (PG password forwarded to Trino as Basic auth)
- **Error mapping** (Trino errors to PostgreSQL SQLSTATE codes)
- **DDL/DML support** (CREATE TABLE, INSERT, DROP)

## Scripts

| Script | Description |
|--------|-------------|
| `build.sh` | Build release binary (dynamically linked) |
| `build-static.sh` | Build static binary with musl (runs on any Linux) |
| `start.sh` | Start the gateway (configure via env vars) |
| `connect.sh` | Connect with psql for testing |

## Testing

```bash
# Unit tests (no Trino needed)
cargo test

# Full integration tests (needs Trino)
TRINO_HOST=... TRINO_PORT=... TRINO_SSL=true TRINO_SSL_INSECURE=true \
    TRINO_CATALOG=tpch TRINO_SCHEMA=sf1 \
    cargo test

# With DDL tests (needs writable catalog like 'memory')
TRINO_WRITE_CATALOG=memory TRINO_WRITE_SCHEMA=default \
    cargo test
```

## Project Structure

```
gateway/
  src/
    main.rs              # CLI, TCP listener
    config.rs            # Configuration (clap)
    startup.rs           # PG connection startup, auth, Trino client creation
    handler.rs           # PgWireServerHandlers factory
    query_simple.rs      # Simple query protocol
    query_extended.rs    # Extended query protocol (Parse/Bind/Execute)
    query_pipeline.rs    # Shared query processing pipeline
    intercept.rs         # SET/SHOW/transaction interception
    catalog/             # pg_catalog emulation
    rewrite/             # SQL rewriting (casts, ILIKE, functions)
    types.rs             # Trino-to-PG type mapping
    trino_stream.rs      # Streaming bridge (Trino polling -> PG DataRow)
    error_mapping.rs     # Trino error -> PG SQLSTATE mapping
  tests/
    integration_test.rs  # Data-driven integration tests
  vendor/
    pgwire/              # PostgreSQL wire protocol library
    trino-rust-client/   # Trino REST API client
```
