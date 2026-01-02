#!/bin/bash
set -euo pipefail

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"

cd "$PROJECT_ROOT"

# Found Oracle Instant Client and libaio libraries here
export LD_LIBRARY_PATH="$PROJECT_ROOT/lib/instantclient_19_10:$PROJECT_ROOT/lib:${LD_LIBRARY_PATH:-}"
export RUST_LOG=info

CONFIG_FILE="tests/configs/integration/config_docker_test.yaml"
OUTPUT_DIR="./export_docker"

echo "Running Oracle Data Exporter (Rust) with Docker Test Config..."
./target/release/oracle_rust_exporter --config "$CONFIG_FILE"

# Verify results using the python script
echo "Verifying export results..."
python3 tests/scripts/verify_export.py "$OUTPUT_DIR"
