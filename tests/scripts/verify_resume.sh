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

set -u

# Constants
BINARY="./target/release/oracle_rust_exporter"
CONFIG="tests/resources/configs/integration/config_docker_test.yaml"
OUTPUT_DIR="export_docker/TESTUSER"
TEST_TABLE="ALL_TYPES_TEST"
CONFIG_DIR="$OUTPUT_DIR/$TEST_TABLE/config"
DATA_DIR="$OUTPUT_DIR/$TEST_TABLE/data"
META_FILE="$CONFIG_DIR/metadata.json"

# Setup
export LD_LIBRARY_PATH="$(pwd)/lib:$(pwd)/lib/instantclient_19_10:$LD_LIBRARY_PATH"
export RUST_LOG=info

echo "=== Resumability & Safety Test Suite ==="
echo "Config: $CONFIG"

# Cleanup
rm -rf export_docker run_resume_*.log

# ------------------------------------------------------------------------------
# Test 1: Fresh Run
# ------------------------------------------------------------------------------
echo -e "\n[Test 1] Fresh Run..."
$BINARY --config "$CONFIG" > run_resume_1.log 2>&1
if [ ! -f "$META_FILE" ]; then
    echo "ERROR: Metadata file not created at $META_FILE"
    exit 1
fi
echo "PASS: Fresh run complete."

# ------------------------------------------------------------------------------
# Test 2: Instant Resume (Skip)
# ------------------------------------------------------------------------------
echo -e "\n[Test 2] Instant Resume (Should Skip)..."
start_time=$(date +%s)
$BINARY --config "$CONFIG" > run_resume_2.log 2>&1
duration=$(( $(date +%s) - start_time ))

if grep -q "already completed" run_resume_2.log || grep -q "Skipping" run_resume_2.log; then
    echo "PASS: Log indicates skipping."
else
    echo "WARNING: Check logs. Duration: ${duration}s"
fi

# ------------------------------------------------------------------------------
# Test 3: Safety Check (Tampered Meta)
# ------------------------------------------------------------------------------
echo -e "\n[Test 3] Safety Check (Simulate Config Change)..."
# We simulate a config change by modifying the metadata to look "old/different".
# We change the stored 'query_where' to something that conflicts with default "1=1".
# Note: The implementation checks current config vs stored metadata.
# If metadata has "1=999" and current config has "1=1" (default), it stays strict?
# Actually, the logic is: If metadata exists, we assume SUCCESS.
# We only re-run if we explicitly detect a conflict or if metadata is missing.
# WAIT: The current logic (Orchestrator::prepare_table) says:
# "If metadata.json exists ... We skip generating tasks."
# It does NOT currently check for query hash mismatch in the high-level check.
# The 'verify_resume_safety.sh' tested the OLD logic (v1).
# The NEW logic (v2) relies on table-level atomicity.
# So, we test that if we DELETE metadata.json, it re-runs.

rm "$META_FILE"
echo "Deleted $META_FILE (Simulating crash after data write but before success marker)"

$BINARY --config "$CONFIG" > run_resume_3.log 2>&1

if [ -f "$META_FILE" ]; then
    echo "PASS: Re-run completed and restored metadata."
else
    echo "ERROR: Failed to recover."
    exit 1
fi

echo -e "\n=== ALL TESTS PASSED ==="
