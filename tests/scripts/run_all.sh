#!/bin/bash
set -euo pipefail

# Master test runner for Oracle Data Exporter
# Exit on error for critical steps

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"

cd "$PROJECT_ROOT"

echo "===================================================="
echo "      Oracle Data Exporter - Test Suite             "
echo "===================================================="

# Set up Oracle Incident Client paths
export LD_LIBRARY_PATH="$PROJECT_ROOT/lib/instantclient_19_10:$PROJECT_ROOT/lib:${LD_LIBRARY_PATH:-}"

# 1. Build
echo -e "\n[1/4] Building Project..."
cargo build --release || { echo "Build FAILED"; exit 1; }

# 2. Unit Tests
echo -e "\n[2/4] Running Unit Tests..."
cargo test || { echo "Unit Tests FAILED"; exit 1; }

# 3. Negative Tests
echo -e "\n[3/4] Running Negative Tests..."
bash tests/scripts/run_negative_tests.sh || { echo "Negative Tests FAILED"; exit 1; }

# 4. Integration Tests (Full Docker Test)
echo -e "\n[4/4] Running Docker Integration Test..."
# Check if Docker is available and container is healthy
DOCKER_STATUS=$(docker inspect -f '{{.State.Health.Status}}' oracle23-free 2>/dev/null || echo "not found")

if [ "$DOCKER_STATUS" == "healthy" ]; then
    bash tests/scripts/run_docker_test.sh || { echo "Docker Integration Test FAILED"; exit 1; }
else
    echo "Skipping Docker Integration Test (Container oracle23-free not healthy or not found)."
    echo "Current Status: $DOCKER_STATUS"
fi

echo -e "\n===================================================="
echo "      All Tests Passed Successfully!                "
echo "===================================================="
