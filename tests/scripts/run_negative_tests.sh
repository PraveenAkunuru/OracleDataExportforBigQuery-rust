#!/bin/bash
set -euo pipefail

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"

BIN="$PROJECT_ROOT/target/release/oracle_rust_exporter"
CONFIG_DIR="$PROJECT_ROOT/tests/resources/configs/negative"

# Found Oracle Instant Client and libaio libraries here
export LD_LIBRARY_PATH="$PROJECT_ROOT/lib/instantclient_19_10:$PROJECT_ROOT/lib:${LD_LIBRARY_PATH:-}"

cd "$PROJECT_ROOT"

echo "=== Test 1: Invalid Config Path ==="
if $BIN --config ./valid_path_but_no_file.yaml; then
    echo "FAIL: Should have exited with error"
    exit 1
else
    echo "PASS: Exited with error"
fi
echo ""

echo "=== Test 2: Zero Parallelism ==="
if $BIN --config "$CONFIG_DIR/config_zero_parallel.yaml"; then
    echo "FAIL: Should have exited with error"
    exit 1
else
    echo "PASS: Exited with error"
fi
echo ""

echo "=== Test 3: Unreachable DB ==="
# We expect this to fail fast if host is clearly invalid or timeout if just unreachable
if timeout 10s $BIN --config "$CONFIG_DIR/config_unreachable.yaml"; then
    echo "FAIL: Should have exited with error"
    exit 1
else
    echo "PASS: Exited with error"
fi
echo ""

echo "=== Test 4: Bad Delimiter ==="
if $BIN --config "$CONFIG_DIR/config_bad_delim.yaml"; then
    echo "FAIL: Should have exited with error"
    exit 1
else
    echo "PASS: Exited with error"
fi
echo ""
