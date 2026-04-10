#!/usr/bin/env bash
# Build a statically-linked binary using musl.
# The resulting binary runs on any Linux (no glibc dependency).
#
# Prerequisites:
#   rustup target add x86_64-unknown-linux-musl
#   # On Arch:  sudo pacman -S musl
#   # On Ubuntu: sudo apt install musl-tools
#   # On RHEL:  sudo dnf install musl-gcc (from EPEL)
set -euo pipefail

cd "$(dirname "$0")"

TARGET="x86_64-unknown-linux-musl"

if ! rustup target list --installed | grep -q "$TARGET"; then
    echo "Installing musl target..."
    rustup target add "$TARGET"
fi

echo "Building postgresql-trino-gateway (static, musl)..."
cargo build --release --target "$TARGET"

BINARY="target/$TARGET/release/postgresql-trino-gateway"
echo "Binary: $BINARY"
ls -lh "$BINARY"
file "$BINARY"
