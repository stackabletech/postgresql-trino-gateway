# PostgreSQL-Trino Gateway

A PostgreSQL wire-protocol shim in front of Trino. Power BI, Npgsql, JDBC,
and `psql` connect to the gateway as if it were a PostgreSQL server; the
gateway translates the queries and forwards them to Trino's REST API.

## Status

Pre-1.0. The protocol surface is what Power BI Report Server exercises in
DirectQuery mode against TPC-H. Other PG clients (Npgsql, pgjdbc, `psql`)
work for read queries; INSERT/UPDATE/CREATE TABLE work via simple-query
and via prepared statements. Multi-statement batches are split and run one
at a time. Wire format is text only.

What is intentionally not implemented for now:

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

## Logging and information disclosure

The gateway distinguishes two output paths:

- **What the client sees on error.** Trino error messages are sanitised
  before being returned over the PG protocol: Java stack traces are
  dropped, exception class FQNs (`io.trino.spi.TrinoException: ...`)
  are stripped, hostnames inside `http://` / `https://` URLs are
  replaced with `<trino>`, and the result is capped at 512 bytes. This
  is to avoid leaking Trino-internal topology to PG clients that may
  be on a less-trusted network.
- **What goes into the gateway's own log stream.** Errors are logged
  with the *original* (unsanitised) Trino message at `warn`/`error`
  level so operators can debug. At `debug` and `trace` levels, the
  gateway also logs every incoming SQL statement, every rewrite
  decision, and the SQL it forwarded to Trino.

What this means in practice: **the gateway log stream can contain
sensitive information**. Trino error messages embed literal values from
the failing query (`Cannot cast '2024-foo' as DATE`). Trace-level logs
contain the query text itself, which may include identifiers or
literals. Treat the gateway's stdout/stderr with the same operational
sensitivity as Trino's own server log — restrict access, avoid
forwarding it to general-purpose log aggregators without scrubbing,
and review your log retention policy if your queries reference PII.

The gateway never logs row contents (returned data values), only query
text and Trino-side error messages. This is a deliberate boundary
documented in `AGENTS.md`.

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
  Trino has no client-managed transactions, so `BEGIN`/`COMMIT`/`ROLLBACK`
  are silently acknowledged as no-ops. Each statement runs in its own
  Trino-side transaction; you cannot group multiple statements atomically.
- `version()`, `current_database()`, `current_schema`,
  `pg_is_in_recovery()`, `current_setting('server_version[_num]')`.
- `pg_catalog` queries used by Npgsql/pgjdbc for type loading
  (`pg_type`, `pg_enum`, `pg_class`, `pg_attribute`, `pg_namespace`,
  `pg_range`).
- `INFORMATION_SCHEMA` tables Trino doesn't expose
  (`referential_constraints`, `table_constraints`, `key_column_usage`,
  ...) return zero rows with the right column shape.

## Wire format

The PostgreSQL frontend/backend protocol supports two wire encodings for
column values: **text** and **binary**. Both are negotiated per column
on each query — the client tells the server which format it wants for
each result column, and the server obliges. Text format renders every
value as its canonical PostgreSQL string representation (e.g. `42`,
`true`, `2026-04-30`); binary format uses a compact, type-specific byte
layout (e.g. a big-endian 4-byte integer for `int4`).

This gateway emits **text only**. Two reasons:

- **Compatibility.** Power BI Report Server uses Npgsql 4.0.17, whose
  decoder for many composite/array types in binary format expects the
  exact byte layout PostgreSQL 9.x produces. Trino's REST API hands us
  values as JSON; reconstructing PostgreSQL's binary layout faithfully
  for every type would be a large amount of error-prone code, and the
  text form is what Npgsql's text decoder is happy with.
- **Limited upside.** Binary format saves CPU and a small amount of
  bytes for tight numeric workloads, but the gateway's bottleneck is
  the Trino REST round-trip and JSON decode, not the wire encoding of
  the final row. The gain wouldn't be visible in a real query path.

The cost is that values cross the wire as decimal strings, ISO-format
dates, and so on; clients that only support binary format won't work
against the gateway. None of our documented clients (Power BI, Npgsql,
pgjdbc, `psql`) require binary.

If a future deployment needs binary format, the natural place to
implement it is in `types.rs::encode_value` (per-column branch on the
client-requested format code) and the `FieldFormat::Text` constants in
`trino_stream.rs::build_pg_schema` and the catalog responders.

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
