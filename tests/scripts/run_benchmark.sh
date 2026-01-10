#!/bin/bash
set -u

# ==============================================================================
# Safe Benchmark Suite (Implicit Parallelism)
# ==============================================================================
# Runs benchmarks using the "Safe Mode" configurations which rely on 
# auto-detected parallelism (80% of DB CPU).
# ==============================================================================

# Constants
PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SCRIPTS_DIR="${PROJECT_ROOT}/tests/scripts"
BENCH_CONFIG_DIR="${PROJECT_ROOT}/tests/resources/configs/benchmark"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

# Ensure we have the binary built (Release)
cd "${PROJECT_ROOT}"
if [ ! -f "target/release/oracle_rust_exporter" ]; then
    echo "Building Release Binary..."
    cargo build --release
fi

echo -e "${GREEN}Starting Safe Benchmark Suite...${NC}"
echo "----------------------------------------------------------------------------------------------------------------"
printf "%-20s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s\n" "Scenario" "Duration" "Rows" "MB/s" "App CPU%" "App Mem" "DB CPU%" "DB Mem"
echo "----------------------------------------------------------------------------------------------------------------------------------"

run_scenario() {
    local config_name="$1"
    local friendly_name="$2"
    local config_path="${BENCH_CONFIG_DIR}/${config_name}"
    
    if [ ! -f "$config_path" ]; then
        echo "Error: Config $config_path not found"
        return
    fi
    
    # Run and capture output
    output=$("${SCRIPTS_DIR}/measure_full_stack.sh" ./target/release/oracle_rust_exporter --config "$config_path")
    
    # Parse Resource Metrics
    app_cpu=$(echo "$output" | grep "APP \[Host\] Peak CPU" | awk '{print $5}' | tr -d '%')
    app_mem=$(echo "$output" | grep "APP \[Host\] Peak Mem" | awk '{print $5}')
    db_cpu=$(echo "$output" | grep "DB  \[Docker\] Peak CPU" | awk '{print $5}' | tr -d '%')
    db_mem=$(echo "$output" | grep "DB  \[Docker\] Peak Mem" | awk '{print $5}')
    
    # Parse Throughput from JSON Report
    output_dir=$(grep "output_dir" "$config_path" | awk -F'"' '{print $2}')
    if [ -z "$output_dir" ]; then
        output_dir=$(grep "output_dir" "$config_path" | awk '{print $2}')
    fi
    
    # Locate report
    report=$(ls -t "${output_dir}"/report_*.json 2>/dev/null | head -n 1)
    
    duration="N/A"
    rows="N/A"
    mbs="N/A"
    
    if [ -f "$report" ]; then
        duration=$(grep '"total_duration_seconds":' "$report" | awk '{print $2}' | tr -d ',')
        rows=$(grep '"total_rows":' "$report" | awk '{print $2}' | tr -d ',')
        mbs=$(grep '"total_mb_per_sec":' "$report" | awk '{print $2}' | tr -d ',')
    fi
    
    # Print Row
    printf "%-20s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s\n" \
        "$friendly_name" "$duration" "$rows" "$mbs" "${app_cpu}%" "${app_mem}MB" "${db_cpu}%" "${db_mem}MB"
        
    # Archive logs
    mv resource_full.log "${output_dir}/resource_usage_${friendly_name}.log" 2>/dev/null || true
}

run_scenario "bench_csv_nohash.yaml" "CSV (Safe)"
run_scenario "bench_parquet_nohash.yaml" "Parquet (Safe)"
run_scenario "bench_csv_hash.yaml" "CSV+Hash (Safe)"
run_scenario "bench_parquet_hash.yaml" "Parquet+Hash (Safe)"

echo "----------------------------------------------------------------------------------------------------------------"
echo -e "${GREEN}Safe Benchmark Complete.${NC}"
