# Architecture & Code Map

## Overview
This project is a high-performance **Oracle to BigQuery** data exporter written in **Rust**. It is designed for scalability, correctness, and speed.

## Module Map

### Core Application
| File | Role | Description |
|------|------|-------------|
| [`src/main.rs`](src/main.rs) | **Entry Point** | Parses CLI arguments, initializes logging, and decides between **Coordinator Mode** (config-based) or **Ad-hoc Worker Mode** (single table). |
| [`src/config.rs`](src/config.rs) | **Configuration** | Defines `AppConfig`, `DatabaseConfig`, `ExportConfig`. Handles YAML/JSON parsing and CLI overrides. |
| [`src/export_coordinator.rs`](src/export_coordinator.rs) | **The "Brain"** | Orchestrates the export. Discovers tables, plans chunking strategies, spawns thread pools, and generates the final report. Implements **Adaptive Parallelism**. |

### Export Logic
| File | Role | Description |
|------|------|-------------|
| [`src/oracle_data_extractor.rs`](src/oracle_data_extractor.rs) | **The "Worker"** | Connects to Oracle, executes SQL, and streams data to gzip-compressed CSVs. Handles **Client-Side Hashing**, XML stripping, and type conversion. |
| [`src/oracle_metadata.rs`](src/oracle_metadata.rs) | **Metadata** | Queries Oracle system views (`ALL_TABLES`, `ALL_TAB_COLUMNS`, `DBMS_METADATA`) to fetch DDL, Primary Keys, and calculate ROWID chunks for parallel extraction. |
| [`src/data_validator.rs`](src/data_validator.rs) | **Quality Control** | Validates table accessibility, row counts, and performs basic sanity checks before/after export. |

### Utilities & Integration
| File | Role | Description |
|------|------|-------------|
| [`src/artifact_generator.rs`](src/artifact_generator.rs) | **Artifacts** | Generates sidecar files: `bigquery.ddl`, `schema.json`, `metadata.json`, `load_command.sh`, and `validation.sql`. |
| [`src/bigquery_schema_mapper.rs`](src/bigquery_schema_mapper.rs) | **Type Mapping** | Maps Oracle data types (NUMBER, DATE, XMLTYPE) to BigQuery types (NUMERIC, TIMESTAMP, STRING). |
| [`src/bigquery_ingestion.rs`](src/bigquery_ingestion.rs) | **Ingestion** | (Optional) Helper to run the generated `bq load` commands directly. |
| [`src/sql_generator_utils.rs`](src/sql_generator_utils.rs) | **SQL Helper** | Generates complex SQL fragments properly quoted identifiers and hash functions. |

## Key Flows

### 1. Coordinator Flow (`src/export_coordinator.rs`)
1.  **Resolve Config**: Merge YAML and CLI args.
2.  **Adaptive Sizing**: Check `v$osstat` (via `oracle_metadata`) to set safe thread count.
3.  **Discovery**: List tables in schema.
4.  **Planning**:
    -   Fetch Row Count.
    -   If > 1GB (approx), fetch **ROWID Chunks** (`oracle_metadata`).
    -   Create `ExportTask` (one per table or one per chunk).
5.  **Execution**:
    -   Spawn Worker Threads.
    -   Workers run `oracle_data_extractor::export_table`.
6.  **Reporting**: Aggregates results into `report.json`.

### 2. Artifact Generation (`src/artifact_generator.rs`)
For *every* table exported, we generate a `config/` directory containing:
-   `bigquery.ddl`: Ready-to-run DDL.
-   `batch_load.sh`: Script to upload to GCS and load to BQ.
-   `validation.sql`: SQL to verify row counts in BQ.
-   `metadata.json`: Machine-readable stats for the control plane.

## Design Patterns
-   **No Shared State**: Each worker has its own Oracle connection.
-   **Streaming**: Data is streamed from Oracle Cursor -> CSV Writer -> Gzip Encoder -> Disk. RAM usage is minimal.
-   **Adaptive Concurrency**: Threads = `min(Local_CPUS, Oracle_CPUS) * user_percent`.

---

# Advanced Usage & Defaults

## 1. Directory Structure

We use a standard layout for all exports so processes can be automated easily.

```text
export_dir/
├── TESTUSER/                     # Schema Name
│   ├── EMPLOYEES/                # Table Name
│   │   ├── config/
│   │   │   ├── bigquery_schema.json    # JSON Schema for 'bq load'
│   │   │   ├── bigquery.ddl            # SQL to Create the Table
│   │   │   ├── oracle_ddl.sql          # Original DDL (for reference)
│   │   │   ├── load_command.sh         # One-click load script
│   │   │   ├── metadata.json           # Detailed stats (Rows, Bytes, Duration)
│   │   │   └── validation.sql          # Queries to run in BQ to verify data
│   │   └── data/
│   │       ├── data_chunk_0000.csv.gz  # The actual data (Gzipped)
│   │       └── ...
│   └── ...
└── summary_report.json           # Top-level success/failure report
```

### Why this structure?
*   **Separation**: Configs and Data are separate. You can zip `config/` separately for auditing.
*   **Automation**: The `load_command.sh` is generated with the exact path and schema settings for that specific table.

## 2. BigQuery Data Type Mapping

We hardcode these mappings to ensure safety and compliance with BigQuery standards.

| Oracle Type | Logic used | BigQuery Type |
| :--- | :--- | :--- |
| **VARCHAR2 / CHAR** | Standard String. | `STRING` |
| **NUMBER(P,0)** | if P <= 19 -> INT64. Else -> NUMERIC. | `INT64` / `NUMERIC` |
| **NUMBER(P,S)** | Fixed precision decimal. | `NUMERIC` / `BIGNUMERIC` |
| **FLOAT** | Standard 64-bit Float. | `FLOAT64` |
| **DATE** | Format: `YYYY-MM-DD HH:MM:SS`. | `DATETIME` |
| **TIMESTAMP** | Format: `YYYY-MM-DD HH:MM:SS.FFFFFF`. | `DATETIME` |
| **TIMESTAMP WITH TZ**| Converted to UTC: `YYYY-MM-DDTHH:MM:SS.FFFFFFZ`. | `TIMESTAMP` |
| **CLOB** | Exported as text. | `STRING` |
| **BLOB / RAW** | Base64 Encoded. | `BYTES` |
| **XMLTYPE** | Newlines stripped (to avoid breaking CSV). | `STRING` |

*Note: For XML, we replace `\n` and `\r` with spaces to preserve CSV integrity. If exact whitespace is required, consider exporting as Base64/BLOB.*

## 3. Configuration Reference
All available options for the `export` section can be found in `config.rs`, but here are the key ones:
```yaml
export:
  parallel: 8                 # Threads. High impact on RAM/Net.
  cpu_percent: 50             # Auto-throttle if host is busy.
  prefetch_rows: 5000         # Oracle fetch size. 5000 is usually sweet spot.
  field_delimiter: "|"        # Default is \x10 (Ctrl+P) to be safe.
  enable_row_hash: true       # Calculate SHA256 per row?
  use_client_hash: true       # Use Rust (Client) vs Oracle (Server) for hashing.
```

---

# Operational Handbook

## 1. Resuming Interrupted Exports
The coordinator is idempotent. If the process crashes or you kill it:
1.  **Fix the issue** (e.g., free up disk space).
2.  **Re-run the same command**: `./run.sh --config config.yaml`.

The coordinator checks if `data_chunk_X.csv.gz` exists.
*   **Exists**: SKIPPED (It assumes the chunk is done).
*   **Missing**: STARTED.

**Force Restart**: If you suspect a file is corrupt, delete the specific `TABLE_NAME` folder (or just the `.csv.gz` files) and re-run.

## 2. Data Verification
Every export generates a `metadata.json` and a `validation.sql` in the `config/` subfolder.
1.  **Quick Check**: Look at `summary_report.json` in the root of the export.
2.  **Deep Dive**: Compare `oracle_row_count` vs `exported_row_count` in `metadata.json`.
3.  **BigQuery Check**: Run the queries found in `validation.sql` inside the BigQuery console after loading.

---

# Performance Tuning Guide

## 1. Handling Slow Networks (High Latency)
If you are exporting from a remote Oracle database (e.g., Cloud to On-Prem, or across regions), **Network Latency** is often the biggest bottleneck.

### The Solution: `prefetch_rows`
Oracle defaults to fetching 1 row at a time or small batches. Over a high-latency link (e.g., 50ms), this kills performance.
We allow you to control the "Prefetch Size" — the number of rows fetched in a single network round-trip.

**Configuration:**
```yaml
export:
  prefetch_rows: 10000  # Default: 5000
```
-   **Low Latency (LAN)**: 1000 - 5000 is usually fine.
-   **High Latency (WAN)**: Increase to 10000 or even 50000.
    -   *Trade-off*: Higher memory usage per thread (buffer size depends on row width).

## 2. Handling Low Bandwidth
If you have a slow link (e.g., 100 Mbps VPN), throwing 64 threads at it will cause packet loss and retries.

### The Solution: Limit Parallelism
Use **Adaptive Parallelism** (enabled by default) or force a low thread count.
```yaml
export:
  # Force a specific number of threads
  parallel: 4 
```

## 3. CPU Constraints
### Oracle Server is Overloaded
1.  **Enable Client-Side Hashing**: Offloads SHA256 computation to the Rust exporter.
    ```yaml
    export:
      use_client_hash: true
    ```
2.  **Reduce `cpu_percent`**:
    ```yaml
    export:
      cpu_percent: 25  # Default 50
    ```

### Local Exporter is Overloaded
1.  **Disable Gzip** (Not currently exposed via config, but defaults to `Fast` compression).
2.  **Reduce Parallelism**.

## 4. Large Objects (LOBs)
The exporter automatically optimizes `XMLTYPE` by stripping newlines in Rust (faster than Oracle `REPLACE`).
To speed up LOBs, ensure you are **not** using `SELECT *`. The exporter automatically expands columns to avoid implicit LOB conversions where possible.

---

# Deployment & Build Guide

## 1. Deployment Strategy ("Zero-Install")
We designed this application to be "Zero-Install" for the production environment. You do **NOT** need Docker, Root Access, or a Rust toolchain on the target Oracle server.
You just need a standard Linux user and the folder we generate.

### The `run.sh` Wrapper
The `run.sh` script is critical. It sets `LD_LIBRARY_PATH` to the local `./lib` folder so the application finds the Oracle libraries immediately.
```bash
#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export LD_LIBRARY_PATH="$DIR/lib:$LD_LIBRARY_PATH"
exec "$DIR/oracle_rust_exporter" "$@"
```

## 2. Cross-Platform Builds

| OS | Architecture | Status | Build Method |
| :--- | :--- | :--- | :--- |
| **Linux** | x86-64 | ✅ **Supported** | `./build_bundle.sh` (Docker) |
| **Linux** | ARM64 (aarch64) | ⚠️ **Available** | Docker (requires ARM config) |
| **Windows**| x86-64 | ✅ **Supported** | `.\build_windows.ps1` |
| **Solaris**| SPARC / x86 | 🚧 **Manual** | Compile on host (Tier 2 Rust) |
| **AIX** | POWER (ppc64) | 🚧 **Manual** | Compile on host (Tier 3 Rust) |

---

# AI Handover Guide (Ground Truth)

## 1. The Mission
We have rewritten a legacy Python Oracle-to-BigQuery exporter in **Rust**.
*   **Status**: Feature Complete & Production Ready.
*   **Current Branch**: `feature/bq-sql-translator` (or latest).
*   **Primary Goal**: Maintenance, optimizations, or shipping to production.

## 2. Critical Environment Details (Sandbox)
**Memorize these:**

### Database (Dockerized Oracle XE)
*   **Container**: `oracle-xe` (or similar).
*   **Host**: `localhost`.
*   **Port**: `1521`.
*   **Service Name**: `FREEPDB1` or `ORCLPDB`.
*   **User**: `TESTUSER` / `testpassword`.
*   **Connection String**: `//localhost:1521/FREEPDB1`.

### Runtime Requirements
You **CANNOT** run the binary without setting `LD_LIBRARY_PATH`.
*   **Library Path**: `./lib` (relative to project root).
*   **Command Pattern**:
    ```bash
    export LD_LIBRARY_PATH=$(pwd)/lib/instantclient_19_10:$(pwd)/lib:$LD_LIBRARY_PATH
    cargo run --release ...
    ```

## 3. "Golden Path" Verification
To prove the system works, run the **Client-Side Hash** integration test.
**Config File**: `tests/configs/integration/config_perf_client_hash.yaml`

**Run Command**:
```bash
export LD_LIBRARY_PATH=$(pwd)/lib/instantclient_19_10:$(pwd)/lib:$LD_LIBRARY_PATH
cargo run --release --bin oracle_rust_exporter -- --config tests/configs/integration/config_perf_client_hash.yaml
```

## 4. Troubleshooting
*   **"DPI-1047 Error"**: You forgot `LD_LIBRARY_PATH`.
*   **"ORA-12514"**: Wrong Service Name. Try `FREEPDB1`.
*   **"UnequalLengths" Panic**: Fix CSV header generation or hashing logic.
