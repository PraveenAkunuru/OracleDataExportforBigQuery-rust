#!/bin/bash
set -e
echo "Starting batch load (Parquet)..."
for d in export_full_parquet/TESTUSER/*; do
  if [[ "$d" == *"ALL_TYPES_TEST2" ]]; then
    continue
  fi
  if [ -d "$d/config" ]; then
    echo "Loading $d..."
    (cd "$d/config" && ./load_command.sh)
  fi
done
echo "Batch load complete."
