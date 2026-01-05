#!/bin/bash
# verify_resume.sh
set -x

BINARY="./target/release/oracle_rust_exporter"
CONFIG="tests/resources/configs/benchmark/bench_opt_csv.yaml"
# We'll use a specific table to isolate noise, but the config runs all.
# We can filter by env var if the app supports it, or just let it run all (it's fast).
# Actually, bench_opt_csv runs all 23 tables. That takes ~17s. Fine.

export LD_LIBRARY_PATH="$(pwd)/lib:$(pwd)/lib/instantclient_19_10:$LD_LIBRARY_PATH"
export RUST_LOG=info

OUTPUT_DIR="bench_opt_csv/TESTUSER"
TEST_TABLE="ALL_TYPES_TEST"
DATA_FILE="$OUTPUT_DIR/$TEST_TABLE/data/data.csv.gz"
META_FILE="$DATA_FILE.meta"

echo "=== STEP 0: CLEANUP ==="
rm -rf bench_opt_csv

echo "=== STEP 1: FRESH RUN ==="
$BINARY --config $CONFIG > run1.log 2>&1
if [ ! -f "$META_FILE" ]; then
    echo "ERROR: Meta file $META_FILE not created!"
    exit 1
fi
echo "Run 1 Complete. Meta file exists."

echo "=== STEP 2: RESUME RUN (Should Skip) ==="
start_time=$(date +%s)
$BINARY --config $CONFIG > run2.log 2>&1
end_time=$(date +%s)
duration=$((end_time - start_time))

if grep -q "Skipping task" run2.log; then
    echo "SUCCESS: Log indicates skipping."
else
    echo "WARNING: Logs don't explicitly say 'Skipping' (check log level). Checking duration."
fi

# It should be very fast.
if [ $duration -gt 5 ]; then
    echo "WARNING: Resume took $duration seconds. Might have re-exported?"
else
    echo "SUCCESS: Resume took $duration seconds (Fast)."
fi

echo "=== STEP 3: SIMULATE CRASH (Delete Meta) ==="
rm "$META_FILE"
# Data file still exists, but meta is gone. Should detect partial and re-run.

$BINARY --config $CONFIG > run3.log 2>&1

if [ ! -f "$META_FILE" ]; then
    echo "ERROR: Meta file not recreated after crash simulation!"
    exit 1
fi
if grep -q "Found partial data file" run3.log || grep -q "Cleaning up" run3.log; then
    echo "SUCCESS: Detected partial file and cleaned up."
else
    echo "INFO: Helper check - did it re-export?"
fi

echo "=== VERIFICATION COMPLETE ==="
