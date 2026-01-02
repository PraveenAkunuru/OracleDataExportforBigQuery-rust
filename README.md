# Oracle Data Exporter for BigQuery (Rust)

[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A high-performance, single-binary utility for migrating large-scale Oracle databases to BigQuery. Built with Rust for maximum performance, memory safety, and minimal deployment footprint.

## 🌟 Key Features

*   **⚡ Blazing Fast Extraction**: Multi-threaded streaming from Oracle direct to Gzip-compressed CSV.
*   **🧩 Intelligent Parallel Chunking**: Automatically splits large tables (>1GB) into parallel chunks using `ROWID` ranges for maximum throughput.
*   **🏗️ Hexagonal Architecture**: Clean separation of concerns between business logic and database infrastructure.
*   **💎 Virtual Column Support**: Automatically handles Oracle virtual columns via a **Physical Table / Logical View split** in BigQuery.
*   **🌍 Spatial Data Support**: Converts `SDO_GEOMETRY` to WKT/Geography automatically.
*   **🛡️ Idempotency & Resiliency**: Built-in resume capability—already exported chunks are skipped automatically.
*   **📊 Comprehensive Reporting**: Generates detailed JSON reports with MB/s, row counts, and error details.
*   **⚙️ Adaptive Resource Management**: Throttles parallelism based on Oracle server CPU load or user-defined limits.

---

## 🚀 Getting Started

### Prerequisites

*   **Oracle Instant Client**: The exporter requires Oracle shared libraries.
*   **Network**: Access to the Oracle listener (usually port 1521).

### Quick Start (Linux)

1.  **Run with Wrapper Script**:
    The provided `run.sh` automatically sets up `LD_LIBRARY_PATH`.
    ```bash
    ./run.sh --username HR --password hr --host db-server --service ORCLPDB --table HR.EMPLOYEES --output ./output
    ```

2.  **Using a Config File (Recommended)**:
    Create a `config.yaml`:
    ```yaml
    database:
      username: "SYSTEM"
      password: "*****"
      host: "localhost"
      port: 1521
      service: "FREEPDB1"
    export:
      output_dir: "./exports"
      schemas: ["HR", "SALES"]
      cpu_percent: 80  # Use 80% of available CPU cores
    ```
    Execute:
    ```bash
    ./run.sh --config config.yaml
    ```

---

## 🛠️ Advanced Usage

### 1. Table Filtering & Exclusion
Filter tables directly in your config:
```yaml
export:
  tables: ["EMPLOYEES", "DEPARTMENTS"] # Only these
  exclude_tables: ["TEMP_LOGS", "BACKUP_%"] # Exclude these (supports globs)
```

### 2. Row Hashing (Data Integrity)
Enable row-level hashing to detect duplicates or changes:
```yaml
export:
  enable_row_hash: true
  use_client_hash: true # Calculate hash in Rust (saves Oracle CPU)
```

### 3. Parallelism Tuning
| Argument | Description | Default |
|----------|-------------|---------|
| `--parallel` | Fixed number of threads | CPU count |
| `--cpu-percent` | Percentage of hosts CPUs to use | 100 |
| `--prefetch-rows` | Rows to buffer from Oracle | 5000 |

---

## 🔍 Understanding the Artifacts

For every table exported, the utility creates:
1.  **`data/`**: Compressed `*.csv.gz` files (chunks).
2.  **`config/schema.json`**: BigQuery-compatible schema definition.
3.  **`config/bigquery.ddl`**: SQL script to create the table and logical view.
4.  **`config/load_command.sh`**: A ready-to-use `bq load` bash script.
5.  **`config/metadata.json`**: Full technical details of the source table.

---

## 🏗️ Architecture

This project follows **Hexagonal Architecture**. 
-   **Domain**: Data types and mapping logic (`src/domain/`).
-   **Application**: The `ExportOrchestrator` (`src/application/`).
-   **Ports**: Trait interfaces for I/O (`src/ports/`).
-   **Infrastructure**: Concrete adapters for Oracle and Local Storage (`src/infrastructure/`).

For more details, see **[ARCHITECTURE.md](ARCHITECTURE.md)**.

---

## 🧪 Testing

We provide a comprehensive Docker-based integration suite.
```bash
./run_docker_test.sh
```
This spawns a real Oracle 23c instance, populates complex types, and runs the full exporter suite.

---

## 📜 License
Distributed under the MIT License. See `LICENSE` for more information.

---
**Maintained by**: Praveen Akunuru
**Target**: Oracle 11g, 12c, 19c, 21c, 23c -> Google BigQuery
