# Oracle Data Exporter for BigQuery (Rust)

[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

A high-performance, single-binary utility for migrating large-scale Oracle databases to BigQuery. Built with Rust for maximum performance, memory safety, and minimal deployment footprint.

## 🌟 Key Features

*   **⚡ Efficient and Fast Extraction**: Multi-threaded streaming from Oracle direct to Gzip-compressed CSV or Apache Parquet. Because we are streaming direct to the files, memory usage will be relatively low. 
*   **📦 Native Parquet Integration**: High-performance Oracle-to-Parquet export via native **Apache Arrow** builders.
*   **🏊 Connection Pooling**: Integrated `r2d2` pooling for stable, high-throughput parallel sessions.
*   **🧩 Intelligent Parallel Chunking**: Automatically splits large tables (>1GB) into parallel chunks using `ROWID` ranges.
*   **🏗️ Certified Hexagonal Architecture**: Clean separation of concerns ensuring codebase maintainability and testability.
*   **💎 Virtual Column Support**: Hybrid approach that materializes complex types (XML, Spatial, Interval) in **Physical Tables** while recreating native Oracle logic in **Logical Views**.
*   **🛡️ Resilient Data Discovery**: Multi-layered fallback for table size discovery and `DATA_DEFAULT` truncation protection.
*   **📊 Comprehensive Reporting**: Generates detailed JSON reports with MB/s, row counts, and data point validation.

## 📈 Performance (Oracle 19c/23c on Docker Container - 2 vCPUs / 4GB RAM)

*   **CSV (Gzip)**: **~12.4 MB/s** (~41,500 rows/s)
*   **Parquet**: **~11.0 MB/s** (~36,700 rows/s)

> **Note**: Performance is typically bottlenecked by the Docker Container's IO/Network limits in this environment. The exporter is capable of higher throughput on bare metal.

---

## 🚀 Getting Started

### Quick Start (Development)

1.  **Clone and Build**:
    ```bash
    cargo build --release
    ```

2.  **Run with Native Client**:
    Ensure `LD_LIBRARY_PATH` points to your Oracle Instant Client.
    ```bash
    ./target/release/oracle_rust_exporter --username HR --password hr --host db-server --service ORCLPDB --table HR.EMPLOYEES --output ./output
    ```

3.  **Using a Config File (Recommended)**:
    Create a `config.yaml` (see `config/config.example.yaml` for a template):
    ```yaml
    database:
      username: "SYSTEM"
      password: "password"
      host: "localhost"
      port: 1521
      service: "FREEPDB1"
    export:
      output_dir: "./exports"
      schemas: ["HR", "SALES"]
      parallel: 4
      format: "parquet"
    ```
    Execute:
    ```bash
    ./target/release/oracle_rust_exporter --config config/config.yaml
    ```

---

## 🛠️ Advanced Usage

### Format & Compression
| Argument | Description | Allowed Values |
|----------|-------------|----------------|
| `--format` | Output file format | `csv`, `parquet` |
| `--compression` | Parquet compression | `snappy` (default), `zstd`, `gzip`, `lz4`, `none` |
| `--parquet-batch-size` | Rows per Arrow batch | Default: 50,000 |

### Resource Management
| Argument | Description | Default |
|----------|-------------|---------|
| `--parallel` | Fixed number of threads | Automatic |
| `--cpu-percent` | % of host CPUs to use | 100 |
| `--prefetch-rows` | Rows to buffer from Oracle | 5000 |

---

## 📊 Data Type Mapping Reference

The exporter performs high-fidelity type conversion to ensure Oracle data is represented correctly in BigQuery.

| Oracle Type | BigQuery Type | Parquet/Arrow Type | Transformation / Logic |
|:---|:---|:---|:---|
| `NUMBER(P, 0)` (P ≤ 18) | `INT64` | `Int64` | Native integer mapping. |
| `NUMBER(P, 0)` (P > 18) | `BIGNUMERIC` | `Decimal128` / `Utf8` | Handled as precision decimal or fallback string. |
| `NUMBER(P, S)` | `BIGNUMERIC` | `Decimal128(P, S)` | Financial-grade decimal precision. |
| `FLOAT`, `BINARY_DOUBLE` | `FLOAT64` | `Float64` | IEEE 754 floating point. |
| `VARCHAR2`, `CLOB`, `LONG` | `STRING` | `Utf8` | UTF-8 encoded strings. |
| `DATE`, `TIMESTAMP` | `DATETIME` | `Timestamp(Micro, None)` | Local calendar time (no offset). |
| `TIMESTAMP WITH TZ` | `TIMESTAMP` | `Timestamp(Micro, UTC)` | Normalized to UTC. |
| **`XMLTYPE`** | `STRING` | `Utf8` | Serialized to XML; newlines stripped for BQ. |
| **`SDO_GEOMETRY`** | `GEOGRAPHY` (View) | `Utf8` | Converted to WKT via `SDO_UTIL`. |
| **`JSON`** (Native) | `JSON` | `Utf8` | Native BigQuery JSON support. |
| `BLOB`, `RAW`, `BFILE` | `BYTES` | `Binary` | Binary data retention. |
| `BOOLEAN` | `BOOL` | `Boolean` | Native flag support. |
| `INTERVAL` | `INTERVAL` | `Utf8` | Converted via robust SQL `EXTRACT` logic (e.g., `YEAR-MONTH 0 0:0:0`) for BigQuery compatibility. |

### 🕒 Interval Transformation Details
Oracle `INTERVAL` types are converted to strings using robust SQL logic to ensure BigQuery compatibility:

- **Year-Month**: `(YEAR)-(MONTH) 0 0:0:0`
  - Example: `1-2` -> `1-2 0 0:0:0`
  - Logic: `CASE WHEN ... THEN '-' ELSE '' END || ABS(YEAR) || '-' || ABS(MONTH) || ' 0 0:0:0'`
- **Day-Second**: `0-0 (DAY) (HOUR):(MIN):(SEC)`
  - Example: `3 12:30:45` -> `0-0 3 12:30:45`
  - Logic: `'0-0 ' || CASE WHEN ... THEN '-' ELSE '' END || ABS(DAY) || ' ' || ABS(HOUR) || ':' || ABS(MIN) || ':' || ABS(SEC)`

---

## 🔍 Understanding the Artifacts

For every table exported, the utility creates:
1.  **`data/`**: Compressed `*.csv.gz` or `*.parquet` files.
2.  **`config/schema.json`**: BigQuery-compatible schema definition.
3.  **`config/bigquery.ddl`**: SQL script for table and logical view creation.
4.  **`config/load_command.sh`**: A ready-to-use `bq load` script.
5.  **`config/metadata.json`**: Technical source metadata and validation stats.

---

## 🏗️ Architecture

This project follows **Hexagonal Architecture**. 
-   **Domain**: `src/domain/` (Entities and Mapping)
-   **Application**: `src/application/` (Orchestration)
-   **Ports**: `src/ports/` (Interfaces)
-   **Infrastructure**: `src/infrastructure/` (Oracle and Storage Adapters)

See **[ARCHITECTURE.md](ARCHITECTURE.md)** for technical depth.

---

## 🧪 Testing

Run the full 360-degree integration suite (requires Docker):
```bash
bash tests/scripts/run_all.sh
```

---

## ❓ Troubleshooting

For issues with connections, type mismatches, or crashes, see the **[Troubleshooting Guide](TROUBLESHOOTING.md)**.

---

## 📜 License
Apache License 2.0. See [LICENSE](LICENSE) for more information.

**Maintained by**: Praveen Akunuru
**Target**: Oracle 11g+ -> Google BigQuery
