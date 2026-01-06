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

# Ensure we are in project root
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"
cd "$PROJECT_ROOT"

# Setup Environment
export LD_LIBRARY_PATH="$PROJECT_ROOT/lib/instantclient_19_10:$PROJECT_ROOT/lib:${LD_LIBRARY_PATH:-}"
export RUST_LOG=info

run_bench() {
    local config="$1"
    local name="$2"
    echo "----------------------------------------------------------------"
    echo "BENCHMARK: $name"
    echo "CONFIG: $config"
    echo "----------------------------------------------------------------"
    
    # Run Exporter
    ./target/release/oracle_rust_exporter --config "$config"
    
    # Extract Metrics
    local dir=$(grep "output_dir" "$config" | awk -F'"' '{print $2}')
    local report=$(ls -t "$dir"/report_*.json | head -n 1)
    
    if [ -f "$report" ]; then
        echo "REPORT_FILE: $report"
        grep -A 10 '"summary":' "$report"
    else
        echo "ERROR: No report found in $dir"
    fi
    echo ""
}

echo "Starting Thread Sweep..."
echo "Date: $(date)"

run_bench "tests/resources/configs/benchmark/bench_sweep_2.yaml" "Threads: 2"
run_bench "tests/resources/configs/benchmark/bench_sweep_4.yaml" "Threads: 4"
run_bench "tests/resources/configs/benchmark/bench_sweep_8.yaml" "Threads: 8"
run_bench "tests/resources/configs/benchmark/bench_sweep_16.yaml" "Threads: 16"
run_bench "tests/resources/configs/benchmark/bench_sweep_32.yaml" "Threads: 32"

echo "Thread Sweep Complete."
