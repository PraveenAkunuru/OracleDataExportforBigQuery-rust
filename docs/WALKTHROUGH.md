# Advanced Usage & Defaults

This document explains exactly how the exporter handles your data, how it maps types to BigQuery, and how the output is structured.

## 1. Directory Structure

I use a standard layout for all exports so processes can be automated easily.

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
│   │       ├── data_chunk_0001.csv.gz
│   │       └── ...
│   └── ...
└── summary_report.json           # Top-level success/failure report
```

### Why this structure?
*   **Separation**: Configs and Data are separate. You can zip `config/` separately for auditing.
*   **Automation**: The `load_command.sh` is generated with the exact path and schema settings for that specific table.

## 2. BigQuery Data Type Mapping

I've hardcoded these mappings to ensure safety and compliance with BigQuery standards.

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

*Note: For XML, I replace `\n` and `\r` with spaces. If you need exact whitespace preservation, we typically export as Base64/BLOB.*

## 3. Configuration Reference (`config.yaml`)

All available options for the `export` section.

```yaml
export:
  # Required
  output_dir: "./exports"
  
  # Targeting (At least one required)
  schemas: ["HR", "SALES"]
  tables: ["ORDERS"]          # Optional: Filter specific tables
  
  # exclude_tables: ["LOGS"]  # Skip these
  
  # Performance / Behavior
  parallel: 8                 # Threads. High impact on RAM/Net.
  cpu_percent: 50             # Auto-throttle if host is busy.
  prefetch_rows: 5000         # Oracle fetch size. 5000 is usually sweet spot.
  
  # Parity / Format
  field_delimiter: "|"        # Default is \x10 (Ctrl+P) to be safe.
  enable_row_hash: true       # Calculate SHA256 per row?
  use_client_hash: true       # Use Rust (Client) vs Oracle (Server) for hashing.
  
  # Automation
  load_to_bq: false           # If true, tries to run 'bq load' immediately.
```

## 4. Special Features

### Row Hashing
If `enable_row_hash: true`, I append a column `ROW_HASH` to the CSV. This is a SHA256 hash of the row content. You can use this in BigQuery to deduplicate rows or verify that data hasn't changed since export.

### Chunking
If a table is larger than 1GB (I check `DBA_SEGMENTS`), I split it into multiple chunks using `DBMS_PARALLEL_EXECUTE` logic (ROWID ranges). This allows me to download parts of the table in parallel.
- **Small Tables**: 1 Chunk (Single thread).
- **Large Tables**: N Chunks (Up to `parallel` threads).
