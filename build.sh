#!/usr/bin/env bash
# Build the gateway binary.
# Produces a dynamically-linked binary at target/release/postgresql-trino-gateway.
set -euo pipefail

cd "$(dirname "$0")"

echo "Building postgresql-trino-gateway (release)..."
cargo build --release
echo "Binary: target/release/postgresql-trino-gateway"
ls -lh target/release/postgresql-trino-gateway
