# PostgreSQL-Trino Gateway

A Rust service that speaks PostgreSQL wire protocol on the frontend and translates
queries to Trino's REST API on the backend. Designed to enable Power BI Report Server
to use Trino as a DirectQuery source.

## Quick Start

```bash
# Start a Trino instance
docker run -d --name trino -p 8080:8080 trinodb/trino:latest

# Build and run the gateway
cargo build --release --manifest-path gateway/Cargo.toml
./gateway/target/release/postgresql-trino-gateway \
  --listen-addr 0.0.0.0:5432 \
  --trino-host localhost \
  --trino-port 8080 \
  --trino-catalog memory \
  --trino-schema default \
  --trino-user trino

# Connect with psql
psql -h 127.0.0.1 -p 5432 -U trino -d memory
```

## Features

- PostgreSQL wire protocol (simple and extended query protocols)
- Trino REST API backend with streaming results
- pg_catalog emulation for Npgsql/Power BI type discovery
- SQL rewriting (PostgreSQL dialect -> Trino dialect)
  - `::` cast syntax -> `CAST()`
  - `ILIKE` -> `lower() LIKE lower()`
  - Function name translation (string_agg -> listagg, etc.)
- SET/SHOW/transaction command interception
- Trino error -> PostgreSQL SQLSTATE mapping

## Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--listen-addr` | `127.0.0.1:5432` | PostgreSQL listen address |
| `--trino-host` | `localhost` | Trino hostname |
| `--trino-port` | `8080` | Trino port |
| `--trino-catalog` | `memory` | Default Trino catalog |
| `--trino-schema` | `default` | Default Trino schema |
| `--trino-user` | `trino` | Trino user |

## Architecture

```
Power BI --TCP--> [Gateway] --HTTP--> Trino
                     |
                     +- Intercept (SET, SHOW, transactions)
                     +- pg_catalog emulation
                     +- SQL rewriting (PG -> Trino dialect)
                     +- Streaming bridge (poll Trino, yield PG rows)
```
