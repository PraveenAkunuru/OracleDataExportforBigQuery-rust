#!/bin/bash
# Copyright 2026 Google LLC
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

echo "=== Test 5: Invalid Password ==="
# We expect this to fail gracefully (e.g. ORA-01017)
if $BIN --config "$CONFIG_DIR/config_bad_creds.yaml"; then
    echo "FAIL: Should have exited with error"
    exit 1
else
    echo "PASS: Exited with error"
fi
echo ""
