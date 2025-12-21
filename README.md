# Oracle Data Exporter for BigQuery

A high-performance, production-ready tool specialized in exporting Oracle database tables into BigQuery-compliant formats. Built in Rust for maximum speed, reliability, and efficient resource utilization.

## 🚀 Key Features

*   **High-Speed Extraction**: Multi-threaded export with dynamic chunking to maximize throughput.
*   **BigQuery Native**: Generates BigQuery-compliant CSVs (Gzipped), JSON Schemas, and Load Commands.
*   **Automatic DDL Translation**: Converts Oracle DDL (Tables, Partitioning, Clustering) to BigQuery SQL.
*   **Data Integrity**: Optional Row Hashing (Client-side or Server-side) to ensure data fidelity.
*   **Resilience**: Smart isolation to prevent impacting the source database.
*   **Zero-Dependency Deployment**: Can be bundled as a standalone portable artifact for Linux environments.

## 📋 Prerequisites

1.  **Oracle Instant Client**: The application requires the Oracle Instant Client libraries (v19+ recommended).
    *   Ensure `LD_LIBRARY_PATH` includes the directory containing `libclntsh.so` and `libaio.so.1`.
2.  **Google Cloud SDK** (Optional): Required if you enable direct GCS upload or BigQuery loading.

## 🛠️ Installation & Build

### From Source
```bash
# Build the release binary
cargo build --release

# The binary will be available at target/release/oracle_rust_exporter
```

### Portable Bundle (Linux)
To create a self-contained bundle (including `lib`, `bin`, and configs) for RHEL/CentOS/Debian:
```bash
./build_bundle.sh
```

## 🏃 Usage

The exporter supports two modes: **Ad-hoc (Single Table)** and **Coordinator (Batch/Config-driven)**.

### 1. Basic Mode (CLI)
Quickly export a single table to a file.

```bash
export LD_LIBRARY_PATH=/opt/oracle/instantclient:$LD_LIBRARY_PATH

./oracle_rust_exporter \
  --host localhost \
  --port 1521 \
  --service ORCL \
  --username MY_USER \
  --password "secret" \
  --table MY_TABLE \
  --output ./my_export_data.csv.gz
```

### 2. Coordinator Mode (Recommended)
Orchestrates complex exports with chunking, parallelism, and full metadata generation.

```bash
./oracle_rust_exporter --config config.yaml
```

**Example `config.yaml`:**
```yaml
database:
  username: "TESTUSER"
  password: "password123"
  host: "10.0.0.5"
  port: 1521
  service: "ORCLPDB"

export:
  output_dir: "./exports"
  schemas: ["TESTUSER"]
  tables: ["ORDERS", "CUSTOMERS"] # Optional filter
  parallel: 8            # Concurrency level
  enable_row_hash: true  # Add validity hash column
  use_client_hash: true  # Use Rust for hashing (faster)
  load_to_bq: false      # Auto-load to BigQuery?

gcp: # Optional
  project_id: "my-gcp-project"
  location: "us"
  gcs_bucket: "my-staging-bucket"
```

## 📂 Output Structure

When running in Coordinator Mode, the tool creates a structured output directory:

```text
exports/
└── TESTUSER/
    └── MY_TABLE/
        ├── data/
        │   ├── data_chunk_0000.csv.gz
        │   ├── data_chunk_0001.csv.gz
        │   └── ...
        └── config/
            ├── bigquery_schema.json    # BQ Schema definition
            ├── bigquery.ddl            # CREATE TABLE ...
            ├── oracle_ddl.sql          # Original Source DDL
            ├── load_command.sh         # Script to Load to BQ
            └── summary_report.json     # Stats & Row Counts
```

## ⚙️ Advanced Configuration (Environment)
*   `RUST_LOG`: Control logging verbosity (e.g., `export RUST_LOG=info` or `debug`).
*   `LD_LIBRARY_PATH`: **CRITICAL**. Must point to your Oracle Instant Client directory.
