# Operational Handbook

This guide covers how to keep the exporter running smoothly, handle failures, and verify your data.

## 1. Architecture Overview (Mental Model)

*   **Coordinator**: The "Brain". It connects to Oracle, queries `ALL_TABLES`, and plans the work. If a table is >1GB, it splits it into chunks.
*   **Workers**: The "Muscle". They take a task (Table + Chunk range), execute the SQL, and stream the result to Gzipped CSVs.
*   **Validation**: Runs *after* export. It queries Oracle for `COUNT(*)` and column sums, and compares them to the exported file stats.

## 2. Resuming Interrupted Exports

I built the coordinator to be idempotent. If the process crashes or you kill it:

1.  **Fix the issue** (e.g., free up disk space).
2.  **Re-run the same command**: `./run.sh --config config.yaml`.

The coordinator checks if `data_chunk_X.csv.gz` exists.
*   **Exists**: SKIPPED (It assumes the chunk is done).
*   **Missing**: STARTED.

**Force Restart**: If you suspect a file is corrupt, delete the specific `TABLE_NAME` folder (or just the `.csv.gz` files) and re-run.

## 3. Data Verification

Every export generates a `metadata.json` and a `validation.sql` in the `config/` subfolder.

### How to Verify
1.  **Quick Check**: Look at `summary_report.json` in the root of the export. It lists every table and whether it SUCCEEDED or FAILED.
2.  **Deep Dive**:
    *   Open `export_data/SCHEMA/TABLE/config/metadata.json`.
    *   Compare `oracle_row_count` vs `exported_row_count`.
3.  **BigQuery Check**:
    Run the queries found in `validation.sql` inside the BigQuery console after loading. It checks for:
    *   Row Count Exact Match.
    *   Numeric Column Sum Matches (to catch precision loss).

## 4. Tuning Performance

*   **CPU usage**: By default, I cap usage at 50% of cores. You can increase this by setting `cpu_percent: 80` in `config.yaml`.
*   **Hashing**: `use_client_hash: true` is faster but uses more client CPU. `enable_row_hash: false` is the fastest if you don't need the checksums.
*   **Network**: The exporter is usually network-bound. If throughput is low, check your distance to the DB.

## 5. Troubleshooting Common Errors

### "DPI-1047: Cannot locate Oracle Client library"
*   **Cause**: The application can't find `libclntsh.so`.
*   **Fix**: Always use `./run.sh`. It sets `LD_LIBRARY_PATH`. Do not run the binary directly unless you set that var yourself.

### "ORA-12514: TNS:listener does not currently know of service"
*   **Cause**: Wrong `service` name in `config.yaml`.
*   **Fix**: Check `lsnrctl status` on the DB server to get the exact Service Name (PDB Name).
