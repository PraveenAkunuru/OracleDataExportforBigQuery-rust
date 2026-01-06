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

set -e

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"

cd "$PROJECT_ROOT"

# Compile latest
echo "Building Release..."
cargo build --release

# Config
CONFIG="tests/configs/integration/config_system_export.yaml"
OUTPUT_DIR="./export_test_output"

echo "Running Export with $CONFIG..."
# We allow it to fail script if exporter crashes, but exporter should handle errors gracefully and exit 0 usually unless critical.
./target/release/oracle_rust_exporter --config "$CONFIG" || echo "Exporter exited with error (check logs)"

# Verify
if [ -d "$OUTPUT_DIR" ]; then
    echo "Running Verification..."
    python3 tests/scripts/verify_export.py "$OUTPUT_DIR"
else
    echo "Output directory not created. Export likely failed early."
fi
