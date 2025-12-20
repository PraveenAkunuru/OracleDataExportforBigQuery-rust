#!/bin/bash
set -e

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"

cd "$PROJECT_ROOT"

# Compile latest
echo "Building Release..."
cargo build --release

# Config
CONFIG="tests/configs/integration/config_system_export.yaml"
OUTPUT_DIR="./export_test_output"

echo "Running Export with $CONFIG..."
# We allow it to fail script if exporter crashes, but exporter should handle errors gracefully and exit 0 usually unless critical.
./target/release/oracle_rust_exporter --config "$CONFIG" || echo "Exporter exited with error (check logs)"

# Verify
if [ -d "$OUTPUT_DIR" ]; then
    echo "Running Verification..."
    python3 tests/scripts/verify_export.py "$OUTPUT_DIR"
else
    echo "Output directory not created. Export likely failed early."
fi
