# Rust Exporter Walkthrough

We have built and verified a high-performance **Rust Exporter** (`oracle_rust_exporter`) designed to replace the legacy Python implementation.

## 1. Performance Comparison (Benchmark)
We compared the Rust implementation against the legacy Python/SQLcl exporter exporting the 2.5 GB `ALL_TYPES_TEST2` table.

| Metric | Rust Exporter | Python Legacy Exporter | Improvement |
| :--- | :--- | :--- | :--- |
| **Wall Time** | **~3 min 30 sec** | **> 6 min** (DNF at 99%) | **~1.7x Faster** |
| **Parallelism** | 1 Process | 20 Processes | **20x More Efficient** |
| **Throughput** | **~12.0 MB/s** | ~0.35 MB/s (per thread) | **~34x Faster per Core** |
| **Resources** | Minimal (Binary) | High (20 JVMs) | **Optimization** |

## 2. Key Features
*   **Safety**: Explicitly handles Oracle `TIMESTAMP WITH TIME ZONE`.
*   **Parity**: 
    *   **Delimiter**: Uses **Ctrl+P (`\x10`)** to avoid CSV collisions.
    *   **Filtering**: Supports `--query-where` for chunking.
*   **Format**:
    *   **Standard Quoting**: Uses `NonNumeric` quoting (strings quoted) for readability.
    *   **XML**: Strips newlines for cleaner export.
    *   **Architecture**:
    *   **Dynamic Concurrency**: Automatically limits to **80% of CPU Cores**.
    *   **Coordinator Mode**: Auto-discovers tables, chunks large tables (>1GB), and manages export queue.
    *   **Resume Capability**: Skips existing chunks/files (File-based resume).
    *   **Validation**: 
        *   **Stats**: Row Count, PK Hash, and **Column Sums** (Tier 1-3).
        *   **Artifacts**: Generates `validation.sql` (Detailed checks), `column_mapping.md`.
        *   **Safety**: Explicit `ROW_HASH` (SHA256) column option for auditing.
    *   **BigQuery Schema**: Generates `schema.json` and `bigquery.ddl`.
    *   **Output Structure**: Organized `data/` and `config/` folders.

## 3. Output Directory Structure
The exporter organizes artifacts per table:
```
export_data/
└── SCHEMA_NAME/
    └── TABLE_NAME/
        ├── config/
        │   ├── bigquery.ddl      # BQ Create Table
        │   ├── oracle.ddl        # Source Oracle DDL
        │   ├── schema.json       # BigQuery JSON Schema
        │   ├── metadata.json     # Full Metadata (pk_hash, aggregates, duration, types)
        │   ├── validation.sql    # BQ Validation Queries
        │   ├── export.sql        # SQLcl Fallback Export Script
        │   └── load_command.sh   # BQ Load Script (Dynamic Delimiter)
        └── data/
            ├── data.csv.gz       # Single file (if < 1GB)
            └── data_chunk_XXXX.csv.gz # Multiple chunks (if > 1GB)
```

## 4. Usage

### A. Coordinate Full Export (Recommended)
This mode automatically handles everything (chunking, parallelism, schema, validation).

1.  **Create Config** (`config.yaml`):
    ```yaml
    database:
      username: "TESTUSER"
      password: "testpassword"
      host: "localhost"
      port: 1521
      service: "FREEPDB1"
    
    export:
      output_dir: "./export_data"
      schema: "TESTUSER"
      # table: "MY_TABLE" # Optional: Limit to specific table
      cpu_percent: 50     # Optional: Use 50% of available cores (Default: 50, Max: 80)
      field_delimiter: "|" # Optional: Custom delimiter (Default: Ctrl+P / \x10)
    ```

2.  **Run**:
    ```bash
    docker run --rm --network host \
       -v $(pwd)/config.yaml:/app/config.yaml \
       -v $(pwd)/export_data:/app/export_data \
       oracle_rust_exporter --config /app/config.yaml
    ```

### B. Complete End-to-End Example (From Scratch)
Follow these steps to build and run a full export:

1.  **Build the Docker Image**:
    ```bash
    docker build -t oracle_rust_exporter .
    ```

2.  **Prepare Directories**:
    ```bash
    mkdir -p $(pwd)/export_data
    chmod 777 $(pwd)/export_data # Ensure Docker can write
    ```

3.  **Create Config File** (`config.yaml`):
    ```yaml
    database:
      username: "SYSTEM"
      password: "mypassword"
      host: "oracle-db-host"
      port: 1521
      service: "ORCL"
    
    export:
      output_dir: "/app/export_data" # Path INSIDE container
      schema: "HR"                   # Schema to export
      exclude_tables:                # Optional exclusions
        - "LOGS"
        - "TEMP_DATA"
      cpu_percent: 50                # Dynamic parallelism (Default 50%)
      field_delimiter: "|"           # Custom delimiter
    ```

4.  **Run the Exporter**:
    ```bash
    docker run --rm --network host \
      -v $(pwd)/config.yaml:/app/config.yaml \
      -v $(pwd)/export_data:/app/export_data \
      oracle_rust_exporter --config /app/config.yaml
    ```

5.  **Verify Output**:
    ```bash
    ls -R export_data/
    # Should see: HR/EMPLOYEES/data/data.csv.gz, HR/EMPLOYEES/config/schema.json, etc.
    ```

### C. Ad-hoc Single Table (Worker Mode)
Legacy-style single table export.

```bash
docker run --rm --network host \
  -v $(pwd)/output:/output \
  oracle_rust_exporter \
  --username TESTUSER --password testpassword \
  --host localhost --service FREEPDB1 \
  --table ALL_TYPES_TEST \
  --output /output/test.csv.gz \
  --query-where "ROWNUM <= 100"
```

## 5. Data Type Mapping & Transformations

Explicit documentation of how Oracle types are transformed for BigQuery CSV import.

| Oracle Type | Logic / Transformation | BigQuery Type |
| :--- | :--- | :--- |
| **NUMBER(P,S)** | If Scale > 0 -> `NUMERIC` (or `BIGNUMERIC` if >38 digits). <br> If Scale 0 -> `INT64` (or `NUMERIC` if >19 digits). | `NUMERIC`/`BIGNUMERIC`/`INT64` |
| **FLOAT / BINARY_FLOAT** | Standard Float Mapping. | `FLOAT64` |
| **BINARY_DOUBLE** | Standard Double Mapping. | `FLOAT64` |
| **CHAR / NCHAR** | Trailing spaces are **removed** (`rtrim`). | `STRING` |
| **VARCHAR2 / NVARCHAR2** | Standard String. | `STRING` |
| **DATE** | `YYYY-MM-DD HH:MM:SS.FFFFFF` (ISO8601-like). | `DATETIME` |
| **TIMESTAMP** | `YYYY-MM-DD HH:MM:SS.FFFFFF` | `DATETIME` |
| **TIMESTAMP(TZ)** | Converted to **UTC** (ISO8601). Output: `YYYY-MM-DDTHH:MM:SS.FFFFFFZ`. | `TIMESTAMP` |
| **INTERVAL** | Exported as ISO8601 Duration String (e.g. `P1DT1H`). | `STRING` |
| **RAW / BLOB** | **Base64 Encoded**. | `BYTES` |
| **XMLTYPE** | Stored as CLOB/BLOB. Line breaks (`\n`, `\r`) replaced with **SPACE** to prevent CSV breakage. | `STRING` |
| **ROWID** | `ROWIDTOCHAR` explicit conversion. | `STRING` |
| **CLOB** | Exported as text (Standard Quoted). | `STRING` |
| **ROW_HASH** | `SHA256` Hash (Hex String) if enabled. | `STRING` |

### Notes
1.  **XML Newlines**: Replaced with space. To preserve structure, consider extracting as Base64 (BLOB) or custom post-processing.
2.  **Fallback**: `export.sql` contains a SQLcl-compatible script to reproduce the export if Rust fails.
3.  **Delimiter**: `load_command.sh` automatically uses the configured `field_delimiter`.
