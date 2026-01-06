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

cd "$PROJECT_ROOT"

# Found Oracle Instant Client and libaio libraries here
export LD_LIBRARY_PATH="$PROJECT_ROOT/lib/instantclient_19_10:$PROJECT_ROOT/lib:${LD_LIBRARY_PATH:-}"
export RUST_LOG=info

CONFIG_FILE="tests/resources/configs/integration/config_docker_test.yaml"
OUTPUT_DIR="./export_docker"

echo "Running Oracle Data Exporter (Rust) with Docker Test Config..."
./target/release/oracle_rust_exporter --config "$CONFIG_FILE"

# Verify results using the python script
echo "Verifying export results..."
python3 tests/scripts/verify_export.py "$OUTPUT_DIR"
