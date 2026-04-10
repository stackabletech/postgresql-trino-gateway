#!/usr/bin/env bash
# Connect to the gateway with psql for testing.
set -euo pipefail

HOST="${GATEWAY_HOST:-127.0.0.1}"
PORT="${GATEWAY_PORT:-15432}"
USER="${GATEWAY_USER:-trino}"
DB="${GATEWAY_DB:-tpch}"

exec psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB"
