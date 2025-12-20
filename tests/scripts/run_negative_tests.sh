#!/bin/bash

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"

BIN="$PROJECT_ROOT/target/release/oracle_rust_exporter"
CONFIG_DIR="$PROJECT_ROOT/tests/configs/negative"

cd "$PROJECT_ROOT"

echo "=== Test 1: Invalid Config Path ==="
$BIN --config ./valid_path_but_no_file.yaml
if [ $? -eq 1 ]; then echo "PASS: Exited with 1"; else echo "FAIL: Exited with $?"; fi
echo ""

echo "=== Test 2: Zero Parallelism ==="
$BIN --config "$CONFIG_DIR/config_zero_parallel.yaml"
echo "Exit code: $?"
echo ""

echo "=== Test 3: Unreachable DB ==="
# We expect this to fail fast if host is clearly invalid or timeout if just unreachable
timeout 10s $BIN --config "$CONFIG_DIR/config_unreachable.yaml"
echo "Exit code: $?"
echo ""

echo "=== Test 4: Bad Delimiter ==="
$BIN --config "$CONFIG_DIR/config_bad_delim.yaml"
echo "Exit code: $?"
echo ""
