#!/bin/bash
# verify_resume_safety.sh
# Tests that the application re-runs tasks if the query definition changes.
set -e

BINARY="./target/release/oracle_rust_exporter"
CONFIG="tests/resources/configs/benchmark/bench_opt_csv.yaml"
OUTPUT_DIR="bench_opt_csv/TESTUSER"
TEST_TABLE="ALL_TYPES_TEST"
DATA_FILE="$OUTPUT_DIR/$TEST_TABLE/data/data.csv.gz"
META_FILE="$DATA_FILE.meta"

export LD_LIBRARY_PATH="$(pwd)/lib:$(pwd)/lib/instantclient_19_10:$LD_LIBRARY_PATH"
export RUST_LOG=info

echo "=== STEP 0: CLEANUP ==="
rm -rf bench_opt_csv run_safety_*.log

echo "=== STEP 1: FRESH RUN (Generate Meta with Query) ==="
$BINARY --config "$CONFIG" > run_safety_1.log 2>&1
if [ ! -f "$META_FILE" ]; then
    echo "ERROR: Meta file not created!"
    exit 1
fi

# Verify query_where is in meta
if grep -q "query_where" "$META_FILE"; then
    echo "[PASS] Meta file contains query_where."
else
    echo "[ERROR] Meta file MISSING query_where!"
    cat "$META_FILE"
    exit 1
fi

echo "=== STEP 2: TAMPER META (Simulate Change) ==="
# Change query_where in meta from "1=1" (or whatever) to "1=999"
# This simulates the User changing the config to "1=1", but the old meta having "1=999" (mismatch).
# We match the key and value, ignoring whether there is a comma or not.
sed -i 's/"query_where":"[^"]*"/"query_where":"1=999"/' "$META_FILE"
echo "Tampered Meta File:"
cat "$META_FILE"

echo "=== STEP 3: RESUME RUN (Should Re-Run due to mismatch) ==="
start_time=$(date +%s)
$BINARY --config "$CONFIG" > run_safety_2.log 2>&1
end_time=$(date +%s)
duration=$((end_time - start_time))

# Check logs for "Task definition changed"
if grep -q "Task definition changed" run_safety_2.log; then
    echo "[PASS] Detected task definition change."
else
    echo "[ERROR] Did NOT detect task definition change. Logs:"
    cat run_safety_2.log
    exit 1
fi

if [ $duration -gt 3 ]; then
    echo "[PASS] Duration ($duration) suggests re-run."
else
    echo "[WARNING] Duration ($duration) is very fast. Did it skip?"
fi

echo "=== SAFETY TEST PASSED ==="
