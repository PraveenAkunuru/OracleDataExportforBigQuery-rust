# Troubleshooting Guide

This guide helps diagnose issues with the **Oracle Data Exporter for BigQuery**.

## 🔍 Enable Debug Logging

The application uses `env_logger`. To see detailed internal logs (SQL queries, row counts, memory usage):

```bash
# Enable debug logs for the application code only
export RUST_LOG=oracle_rust_exporter=debug

# Enable trace logs (very verbose)
export RUST_LOG=oracle_rust_exporter=trace

# Run the application
./target/release/oracle_rust_exporter --config config.yaml
```

**Note:** Debug logs may contain sensitive query info (though values are generally parameter placeholders, `query_where` clauses are visible).

## ⚠️ Common Errors

### 1. `ODPI-C unavailable` or `Library not loading`
**Symptom:** The application crashes immediately with an error about loading `libclntsh.so`.
**Fix:** Ensure `LD_LIBRARY_PATH` includes the Instant Client directory.
```bash
export LD_LIBRARY_PATH=/path/to/instantclient_19_10:$LD_LIBRARY_PATH
```
Use the provided wrapper script:
```bash
./scripts/run_with_env.sh ./target/release/oracle_rust_exporter ...
```

### 2. `Type Mismatch` (BigQuery Load)
**Symptom:** `bq load` fails with "Reading column X of type BYTE_ARRAY ... expecting INT64".
**Fix:** This means the Parquet file type doesn't match the BQ Table Schema.
-   Check `src/domain/mapping.rs` overrides.
-   **Known Issue:** Oracle `NUMBER` with undefined precision maps to `STRING` to avoid overflow. `TimestampLTZ` maps to `TIMESTAMP` (converted to UTC).

### 3. `Exit Code 0` but Incomplete Export
**Symptom:** Application exits silently.
**Fix:** This was a known issue with ODPI-C fetching LOBs.
-   Ensure you are using the latest binary causing `query_where="1=1"` injection.
-   Check `dmesg` for OOM kills (though unlikely with this Rust version).

## 📝 Inspection Reports
The application generates a `report.json` in the output directory (if configured). Use this to see which tables failed.

## 🐛 Reporting Bugs
When reporting issues, please include:
1.  Output with `RUST_LOG=debug`.
2.  The generated `metadata.json` for the failing table.
3.  The `load_command.sh` generated.
