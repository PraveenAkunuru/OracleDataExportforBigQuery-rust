# AI Handover Guide: Oracle Data Exporter (Rust)

**DO NOT IGNORE THIS FILE.**
This document contains the *verified ground truth* for this session. Use it to re-orient yourself immediately.

## 1. The Mission
We have rewritten a legacy Python Oracle-to-BigQuery exporter in **Rust**.
*   **Status**: Feature Complete & Production Ready.
*   **Current Branch**: `feature/bq-sql-translator`.
*   **Primary Goal**: Maintenance, optimizations, or shipping to production.

## 2. Critical Environment Details (Sandbox)

The "Model Amnesia" usually happens here. **Memorize these:**

### Database (Dockerized Oracle XE)
*   **Container**: `oracle-xe` (or similar, check `docker ps`).
*   **Host**: `localhost` (mapped via network host or port forwarding).
*   **Port**: `1521`.
*   **Service Name**: `FREEPDB1` (Start here) or `ORCLPDB`.
*   **User**: `TESTUSER`.
*   **Password**: `testpassword`.
*   **Admin User**: `SYSTEM` / `password`.
*   **Connection String**: `//localhost:1521/FREEPDB1`.

### Runtime Requirements (The "It Won't Run" Fix)
You **CANNOT** run the binary without setting `LD_LIBRARY_PATH`.
*   **Library Path**: `/usr/local/google/home/pakunuru/OracleDataExportforBigQuery-rust/lib`
*   **Command Pattern**:
    ```bash
    export LD_LIBRARY_PATH=$(pwd)/lib:$LD_LIBRARY_PATH
    cargo run --release ...
    ```

## 3. "Golden Path" Verification
To prove the system works, run the **Client-Side Hash** integration test. This tests connection, query, hashing, and CSV writing.

**Config File**: `tests/configs/integration/config_perf_client_hash.yaml` (If missing, recreate it based on `README.md` example).

**Run Command**:
```bash
export LD_LIBRARY_PATH=$(pwd)/lib/instantclient_19_10:$(pwd)/lib:$LD_LIBRARY_PATH
cargo run --release --bin oracle_rust_exporter -- --config tests/configs/integration/config_perf_client_hash.yaml
```

**Expected Output**:
*   Logs showing "Coordinator starting..."
*   "Splitting ... into chunks"
*   throughput metrics (~30-50 MB/s).
*   Final "Report: Total=X, Success=Y".

## 4. Codebase Navigation (Shortcuts)

| Feature | File | Note |
| :--- | :--- | :--- |
| **Main Loop / Chunking** | `src/export_coordinator.rs` | Look for `plan_table_export` and `execute_task`. |
| **SQL Generation / Query** | `src/oracle_data_extractor.rs` | `build_select_query` creates the SQL. |
| **Hashing Logic** | `src/oracle_data_extractor.rs` | Look for `Sha256::new()` inside the row loop. |
| **Config Structs** | `src/config.rs` | `AppConfig`, `ExportConfig`. |
| **BigQuery Types** | `src/bigquery_schema_mapper.rs` | Mappings for Oracle -> BQ types. |

## 5. Recent Changes (What we just finished)
1.  **Client-Side Hashing**: Implemented pure Rust `sha2` hashing to offload Oracle CPU. Enabled via `use_client_hash: true`.
2.  **Concurrency**: Fixed `chunk_count` logic to ensure we actually use `parallel: 8` threads.
3.  **Documentation**: Rewrote `README.md` and `docs/*.md` to have a personal, engineer-to-engineer tone. **Do not revert this tone.**

## 6. If You Are Stuck...
*   **"DPI-1047 Error"**: You forgot `LD_LIBRARY_PATH`. See Section 2.
*   **"ORA-12514"**: Wrong Service Name. Try `FREEPDB1`.
*   **"UnequalLengths" Panic**: You might be messing with the CSV header generation in `oracle_data_extractor.rs`. We just fixed a bug where `ROW_HASH` header was missing.
