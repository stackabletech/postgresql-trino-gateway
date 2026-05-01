# PostgreSQL-Trino Gateway

Rust service speaking the PostgreSQL wire protocol on the listening side and
forwarding queries to Trino's REST API on the backend. Lets PG clients
(Power BI, Npgsql, psql, JDBC) reach Trino as a DirectQuery source.

## Architecture

- `pgwire` frontend accepts PG client connections.
- The intercept layer answers `SET`, `SHOW`, `BEGIN`/`COMMIT`, and
  `pg_catalog` queries locally; everything else is forwarded.
- The SQL rewriter transforms PG-dialect SQL to Trino-compatible SQL
  (`::cast`, `ILIKE`, function-name remaps, type-name normalisation).
- The Trino backend forwards rewritten queries via the REST API and streams
  result pages back as PG wire-protocol DataRows.
- Catalog emulation fakes `pg_type`, `pg_class`, `pg_attribute`, and a few
  others from Trino's `information_schema`.

## Project Structure

- `src/` — main binary crate
  - `main.rs` — CLI parsing, TCP listener, accept loop, graceful shutdown
  - `config.rs` — `Config` struct (clap derive)
  - `policy.rs` — startup auth/TLS/listener-policy validation
  - `startup.rs` — PG startup handler, server params, Trino client construction
  - `tls.rs` — TLS termination for the listening socket
  - `handler.rs` — `PgWireServerHandlers` factory
  - `query_simple.rs` — simple query protocol handler
  - `query_extended.rs` — extended query protocol (Parse/Bind/Describe/Execute)
  - `query_pipeline.rs` — shared pipeline; multi-statement splitting
  - `query_inspection.rs` — AST-based dispatch (`references_table`, `calls_function`)
  - `intercept.rs` — `SET`, `SHOW`, transaction, server-function interception
  - `cancel.rs` — PG `CancelRequest` to Trino `DELETE /v1/query/{id}`
  - `session.rs` — per-connection state, cancel registry, portal cache
  - `catalog/` — `pg_catalog` emulation (`pg_type`, `pg_class`, `pg_attribute`, stubs)
  - `rewrite/` — SQL rewriting (casts, predicates, functions)
  - `types.rs` — Trino-to-PG type mapping and value encoding
  - `trino_stream.rs` — streaming bridge (poll Trino, yield PG DataRow)
  - `error_mapping.rs` — Trino errors to PG SQLSTATE codes
- `tests/integration_test.rs` — data-driven integration tests against real Trino

## Build & Test

```bash
cargo build
cargo test                          # unit tests only
cargo clippy --all-targets          # lint
cargo fmt --check                   # format

# With Trino (read-only tests):
TRINO_HOST=... TRINO_PORT=... TRINO_SSL=true TRINO_TLS_NO_VERIFY=true \
  TRINO_CATALOG=tpch TRINO_SCHEMA=sf1 \
  cargo test

# With writable catalog (DDL tests):
... TRINO_WRITE_CATALOG=memory TRINO_WRITE_SCHEMA=default \
  cargo test

# Coverage (requires `cargo install cargo-llvm-cov`):
cargo llvm-cov --all-targets --html
# HTML report at target/llvm-cov/html/index.html
```

## Quality Rules

### Before every commit

- `cargo build` and `cargo clippy --all-targets` produce no warnings.
  `#[allow(dead_code)]` is not a fix; remove the unused code.
- `cargo fmt --check` passes.
- `cargo test` passes. New functionality has tests.

### Security

- No `unwrap()` or `panic!()` in production paths. Tests and proven
  invariants are exempt; document the invariant with a comment.
- No SQL injection. The rewriter operates on the AST, never on raw strings,
  with one documented exception for the Power BI INFORMATION_SCHEMA.columns
  CASE rewrite (driver-emitted fixed pattern, no user input interpolated).
- No credential leaks. Passwords, tokens, and connection strings never
  appear in logs, error messages, or debug output.
- Input validation at boundaries. Malformed wire-protocol messages must
  not crash the gateway.
- Fail closed. On unexpected state, return an error to the client rather
  than proceeding.

### Code shape

- Names carry the meaning. Comments explain *why*, not *what*.
- Functions fit on a screen.
- Three near-identical patterns means extract a helper.
- Catalog handlers and rewriter visitors follow the same structure as
  their siblings; consistency matters.

## Debugging protocol issues

For client compatibility issues (Power BI, Npgsql, JDBC, psql failing
against the gateway but working against real PostgreSQL), enable
protocol-level tracing:

```bash
RUST_LOG=postgresql_trino_gateway=trace cargo run -- ...
```

Trace output, per connection:

- Startup message and auth flow (passwords redacted).
- Every simple-query and extended-query message with the SQL text.
- Which intercept branch matched (`SET`, `SHOW`, `pg_catalog`,
  `info_schema`, ...) or whether the query was forwarded to Trino.
- The rewritten SQL sent to Trino, if a rewrite applied.
- Trino's response shape (column count, row count). Row contents are
  never logged. If you need to see values, run against a pre-production
  Trino catalog with synthetic data.

The trace output is structured. Pipe through `jq` or grep for `conn_id=`
to filter by connection.

## Conventions

### Code style

- Per-connection state lives in the `session` module's `CONNECTIONS` map
  until pgwire's `SessionExtensions` ships.
- Streaming bridges use `async_stream::stream!`.
- All response columns use text wire format (format code 0).
- Catalog queries return pre-built static responses; they don't go
  through `sqlparser`.
- SQL rewriting uses `sqlparser-rs` with `PostgreSqlDialect` and falls
  back to passthrough on parse failure.
- Integration tests are data-driven: `(name, sql, Check)` tuples with
  shared fixtures.

### Adding a new intercepted query

1. Add pattern detection in `intercept.rs` (or `catalog/mod.rs` for
   `pg_catalog` tables).
2. Build a response with `single_text_response()` or `build_response()`.
3. Add a test case to the appropriate test function.

### Adding a new SQL rewrite

1. Add a visitor or AST walker in the appropriate `rewrite/` submodule.
2. Add a unit test in `rewrite/mod.rs`.
3. Add an integration-test case in `integration_test.rs`.
