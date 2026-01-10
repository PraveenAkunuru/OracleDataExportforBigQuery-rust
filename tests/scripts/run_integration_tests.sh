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

# ==============================================================================
# Oracle Data Exporter - Consolidated Integration Test Runner
# ==============================================================================
# Usage: ./run_integration_tests.sh [mode]
# Modes:
#   csv (default)   : Runs standard CSV export test
#   parquet         : Runs Parquet export test
#   resume          : Runs restartability/resume test
#   full            : Runs all modes
# ==============================================================================

# Constants
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"
CONFIG_DIR="${PROJECT_ROOT}/tests/resources/configs/integration"
BINARY="${PROJECT_ROOT}/target/release/oracle_rust_exporter"

# Setup Environment
cd "$PROJECT_ROOT"
export LD_LIBRARY_PATH="$PROJECT_ROOT/lib/instantclient_19_10:$PROJECT_ROOT/lib:${LD_LIBRARY_PATH:-}"
export RUST_LOG=info

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

run_test() {
    local config="$1"
    local output_dir="$2"
    local description="$3"
    
    echo -e "\n${BLUE}=== Running Test: ${description} ===${NC}"
    echo "Config: ${config}"
    
    # Cleanup previous run
    rm -rf "${output_dir}"
    
    if [ ! -f "${BINARY}" ]; then
        echo "Building Release Binary..."
        cargo build --release
    fi

    echo "Executing Exporter..."
    "${BINARY}" --config "${config}"
    
    echo "Verifying Results..."
    python3 tests/scripts/verify_export.py "${output_dir}"
    echo -e "${GREEN}Test Passed: ${description}${NC}"
}

MODE="${1:-csv}"

case "$MODE" in
    csv)
        run_test "${CONFIG_DIR}/config_docker_test.yaml" "./export_docker" "Standard CSV Export"
        ;;
    parquet)
        run_test "${CONFIG_DIR}/config_docker_parquet_no_hash.yaml" "./export_docker_parquet" "Parquet Export"
        ;;
    resume)
        echo "Resume testing requires customized orchestration. Running legacy verify_resume script for now."
        bash tests/scripts/verify_resume.sh
        ;;
    full)
        run_test "${CONFIG_DIR}/config_docker_test.yaml" "./export_docker" "Standard CSV Export"
        run_test "${CONFIG_DIR}/config_docker_parquet_no_hash.yaml" "./export_docker_parquet" "Parquet Export"
        bash tests/scripts/verify_resume.sh
        ;;
    *)
        echo "Unknown mode: $MODE"
        echo "Usage: $0 [csv|parquet|resume|full]"
        exit 1
        ;;
esac
