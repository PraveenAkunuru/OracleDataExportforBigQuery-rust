#!/bin/bash
set -e

# Detect script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

echo "[Build] Building Docker build image..."
docker build -t oracle-rust-builder -f "$ROOT_DIR/docker/Dockerfile.build" "$ROOT_DIR"

echo "[Build] Compiling release binary in container..."
# We run a container to copy the artifact out
# A simple way is to use `docker run -v` but that messes with permissions.
# Easier: `docker create`, `docker cp`, `docker rm`.
CONTAINER_ID=$(docker create oracle-rust-builder)
docker cp "$CONTAINER_ID:/app/target/release/oracle_rust_exporter" "$ROOT_DIR/"
docker rm "$CONTAINER_ID"

echo "[Build] Done. Binary is at: $ROOT_DIR/oracle_rust_exporter"
echo "        Check compatibility with: ldd oracle_rust_exporter"
