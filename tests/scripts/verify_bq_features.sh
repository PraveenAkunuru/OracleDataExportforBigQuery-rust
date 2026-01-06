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

PROJECT="pakunuru-1119-20250930202256"
DATASET="TESTUSER"

# Ensure dataset exists
bq show $PROJECT:$DATASET || bq mk $PROJECT:$DATASET

tables=("TEST_BOOLEAN" "TEST_INTERVALS" "TEST_SPATIAL" "TEST_VIRTUAL_COLS")

for t in "${tables[@]}"; do
    echo "Processing $t..."
    CONFIG_DIR="features_test_output/TESTUSER/$t/config"
    
    # 1. Run DDL (Pipe to bq query)
    echo "  Running DDL..."
    # Warning: bigquery.ddl contains multiple statements. bq query via stdin usually runs first?
    # We will split by semicolon if needed, but let's try assuming script support.
    # We use --nouse_legacy_sql which enables standard SQL (scripting supported).
    cat "$CONFIG_DIR/bigquery.ddl" | bq query --nouse_legacy_sql --project_id=$PROJECT
    
    # 2. Run Load
    echo "  Running Load..."
    (cd "$CONFIG_DIR" && ./load_command.sh)
    
    # 3. Verify
    echo "  Verifying..."
    # For Virtual Cols, check the View. For others, check the table.
    QUERY_TABLE="$t"
    bq query --nouse_legacy_sql --format=prettyjson --project_id=$PROJECT "SELECT count(*) as count FROM \`$PROJECT.$DATASET.$QUERY_TABLE\`"
done
