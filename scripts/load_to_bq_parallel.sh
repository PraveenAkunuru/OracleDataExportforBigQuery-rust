#!/bin/bash
set -e

EXPORT_DIR=$1
DATASET=$2
GCS_BUCKET=$3

if [ -z "$EXPORT_DIR" ] || [ -z "$DATASET" ] || [ -z "$GCS_BUCKET" ]; then
    echo "Usage: $0 <export_dir> <dataset> <gcs_bucket>"
    exit 1
fi

echo "🚀 Loading tables from $EXPORT_DIR into $DATASET..."

for table_dir in "$EXPORT_DIR"/TESTUSER/*; do
    if [ -d "$table_dir" ]; then
        table_name=$(basename "$table_dir")
        echo "--- Processing table: $table_name ---"
        
        # 1. Execute DDL
        echo "  Executing DDL..."
        bq query --use_legacy_sql=false < "$table_dir/config/bigquery.ddl"
        
        # 2. Upload Data
        echo "  Uploading data to GCS..."
        gcloud storage cp "$table_dir/data/"* "$GCS_BUCKET/$table_name/data/"
        
        # 3. Load Data from GCS
        echo "  Running bq load..."
        # Determine if we should target _PHYSICAL or the base name
        target_name="$table_name"
        if grep -q "CREATE OR REPLACE VIEW" "$table_dir/config/bigquery.ddl"; then
            target_name="${table_name}_PHYSICAL"
        fi

        if ls "$table_dir/data/"*.parquet >/dev/null 2>&1; then
            fmt="PARQUET"
            flags=""
            src="$GCS_BUCKET/$table_name/data/*.parquet"
        else
            fmt="CSV"
            # Use printf to get the actual hex character 0x10
            delim=$(printf '\x10')
            flags="--field_delimiter=$delim --skip_leading_rows=1"
            src="$GCS_BUCKET/$table_name/data/*.csv.gz"
        fi
        
        bq load --source_format="$fmt" $flags --replace "pakunuru-1119-20250930202256:$DATASET.$target_name" "$src"
    fi
done

echo "✅ All tables loaded successfully into $DATASET!"
