#!/bin/bash
# scripts/run_with_env.sh
# Automatically sets up Oracle environment variables and runs the command.
# Usage: ./scripts/run_with_env.sh cargo run --release -- --config ...

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

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
