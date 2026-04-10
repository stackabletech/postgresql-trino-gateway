#!/usr/bin/env bash
# Start the PostgreSQL-Trino Gateway.
# Configure via environment variables or edit the defaults below.
set -euo pipefail

LISTEN_ADDR="${LISTEN_ADDR:-0.0.0.0:15432}"
TRINO_HOST="${TRINO_HOST:?Set TRINO_HOST environment variable}"
TRINO_PORT="${TRINO_PORT:-8443}"
TRINO_CATALOG="${TRINO_CATALOG:-tpch}"
TRINO_SCHEMA="${TRINO_SCHEMA:-sf1}"
LOG_LEVEL="${RUST_LOG:-postgresql_trino_gateway=info}"

# Optional flags (set to empty string to disable)
SSL_FLAGS="${SSL_FLAGS:---trino-ssl --trino-ssl-insecure}"
AUTH_FLAG="${AUTH_FLAG:-}"  # Set to "--auth" to require passwords

BINARY="./target/release/postgresql-trino-gateway"
if [ ! -f "$BINARY" ]; then
    BINARY="./target/debug/postgresql-trino-gateway"
fi
if [ ! -f "$BINARY" ]; then
    echo "Error: Binary not found. Run ./build.sh first." >&2
    exit 1
fi

echo "Starting gateway on $LISTEN_ADDR -> Trino at $TRINO_HOST:$TRINO_PORT ($TRINO_CATALOG.$TRINO_SCHEMA)"

# shellcheck disable=SC2086
RUST_LOG="$LOG_LEVEL" exec "$BINARY" \
    --listen-addr "$LISTEN_ADDR" \
    --trino-host "$TRINO_HOST" \
    --trino-port "$TRINO_PORT" \
    --trino-catalog "$TRINO_CATALOG" \
    --trino-schema "$TRINO_SCHEMA" \
    $SSL_FLAGS $AUTH_FLAG
