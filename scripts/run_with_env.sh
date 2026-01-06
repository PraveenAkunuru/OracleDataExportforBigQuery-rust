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

# scripts/run_with_env.sh
# Automatically sets up Oracle environment variables and runs the command.
# Usage: ./scripts/run_with_env.sh cargo run --release -- --config ...

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Load .env if it exists
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
fi

# Detect Library Paths
LIB_DIR="$PROJECT_ROOT/lib"
INSTANT_CLIENT="$LIB_DIR/instantclient_19_10"

# Check if libraries exist
if [ ! -d "$INSTANT_CLIENT" ]; then
    echo "⚠️  WARNING: Oracle Instant Client not found at $INSTANT_CLIENT"
    echo "    Please ensure you have unzipped the instant client into lib/"
fi

# Prepend code-local libs to LD_LIBRARY_PATH
export LD_LIBRARY_PATH="$INSTANT_CLIENT:$LIB_DIR:$LD_LIBRARY_PATH"

# Run the passed command
exec "$@"
