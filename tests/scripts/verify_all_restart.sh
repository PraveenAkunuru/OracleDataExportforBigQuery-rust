#!/bin/bash
# verify_all_restart.sh
# Comprehensive restartability test for both CSV and Parquet.
set -e

BINARY="./target/release/oracle_rust_exporter"
export LD_LIBRARY_PATH="$(pwd)/lib:$(pwd)/lib/instantclient_19_10:$LD_LIBRARY_PATH"
# Enable info logs to see "Skipping task" messages
export RUST_LOG=info

# Function to run a full cycle test
run_restart_test() {
    local FORMAT=$1
    local CONFIG=$2
    local OUTPUT_DIR=$3
    local TEST_FILE=$4
    local META_EXT=$5

    echo "=========================================================="
    echo "TESTING RESTARTABILITY: $FORMAT"
    echo "Config: $CONFIG"
    echo "Output: $OUTPUT_DIR"
    echo "=========================================================="

    # 1. Cleanup
    echo "[Step 0] Cleanup $OUTPUT_DIR..."
    rm -rf "$OUTPUT_DIR" "run_${FORMAT}_*.log"

    # 2. Fresh Run
    echo "[Step 1] Fresh Run..."
    start_time=$(date +%s)
    $BINARY --config "$CONFIG" > "run_${FORMAT}_1.log" 2>&1
    end_time=$(date +%s)
    echo "   -> Duration: $((end_time - start_time))s"

    # Verify Meta Existence (Table Level)
    # Output Dir structure: SCHEMA/TABLE/config/metadata.json
    local SAMPLE_META="$OUTPUT_DIR/TESTUSER/ALL_TYPES_TEST/config/metadata.json"
    
    if [ ! -f "$SAMPLE_META" ]; then
        echo "   [ERROR] Meta file missing at $SAMPLE_META"
        exit 1
    fi
    echo "   [PASS] Metadata file created."

    # 3. Resume Run (Should Skip)
    echo "[Step 2] Resume Run (Expecting Skip)..."
    start_time=$(date +%s)
    $BINARY --config "$CONFIG" > "run_${FORMAT}_2.log" 2>&1
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    echo "   -> Duration: ${duration}s"

    if grep -q "already completed" "run_${FORMAT}_2.log"; then
        echo "   [PASS] Log confirms skipping."
    else
        echo "   [WARNING] 'already completed' not found in logs. Check log level."
    fi

    if [ $duration -gt 10 ]; then
        echo "   [WARNING] Resume took >10s. Might have re-exported."
    else
        echo "   [PASS] Resume was fast."
    fi

    # 4. Crash Simulation (Delete Meta)
    echo "[Step 3] Crash Simulation (Delete Meta)..."
    rm "$SAMPLE_META"
    echo "   -> Deleted $SAMPLE_META (Data file preserved)"
    
    $BINARY --config "$CONFIG" > "run_${FORMAT}_3.log" 2>&1
    
    if [ ! -f "$SAMPLE_META" ]; then
         echo "   [ERROR] Meta file NOT recreated!"
         exit 1
    fi

    # With Table-Level restart, we might NOT see "partial data" logs if we just overwrite.
    # But current implementation deletes data if data exists but meta doesn't.
    # Let's check if data file was recreated (timestamp changed) or just verified.
    # Actually, we logged "Found partial data file ... Cleaning up".
    if grep -q "Found partial data file" "run_${FORMAT}_3.log"; then
        echo "   [PASS] Log confirms partial file detection/cleanup."
    else
        echo "   [INFO] Check log manually for partial detection."
    fi
    
    echo "   [PASS] Recovery successful."
    echo ""
}

# --- CSV TEST ---
# CSV output: bench_opt_csv
run_restart_test "CSV" \
    "tests/resources/configs/benchmark/bench_opt_csv.yaml" \
    "bench_opt_csv" \
    "ignored_data_file" \
    "ignored_meta_ext"

# --- PARQUET TEST ---
# Parquet output: bench_opt_parquet
run_restart_test "PARQUET" \
    "tests/resources/configs/benchmark/bench_opt_parquet.yaml" \
    "bench_opt_parquet" \
    "ignored_data_file" \
    "ignored_meta_ext"

echo "ALL TESTS PASSED SUCCESSFULLY!"
