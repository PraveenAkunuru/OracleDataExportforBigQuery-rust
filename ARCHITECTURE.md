# 🏗️ Architecture & Technical Reference

This document serves as the master reference for the **Oracle Data Exporter for BigQuery**. It covers the architectural patterns, data strategies, and operational constraints of the system.

---

## 🏛️ Hexagonal Architecture (Ports and Adapters)

The application is structured to strictly decouple business orchestration from external I/O. This ensures that the core logic is testable without a database and that adapters (e.g., swapping Local Storage for GCS) can be replaced with minimal impact.

### 1. Domain Layer (`src/domain/`)
*   **Role**: Internal state and pure logic.
*   **Key Files**:
    *   [`entities.rs`](src/domain/entities.rs): Definitions for `TableMetadata`, `ExportTask`, and `TaskResult`.
    *   [`mapping.rs`](src/domain/mapping.rs): Translation rules for converting Oracle types to Arrow/BigQuery types.
    *   [`errors.rs`](src/domain/errors.rs): Centralized error handling using `thiserror`.

### 2. Application Layer (`src/application/`)
*   **Role**: Orchestration and coordination.
*   **Key Files**:
    *   [`orchestrator.rs`](src/application/orchestrator.rs): The "Brain". Discovers tables, manages Rayon parallel pools, and coordinates tasks.

### 3. Ports (`src/ports/`)
*   **Role**: Dependency inversion contracts (Traits).
*   **Interfaces**:
    *   [`metadata_port.rs`](src/ports/metadata_port.rs): Contract for metadata discovery and schema reading (`MetadataPort`).
    *   [`extraction_port.rs`](src/ports/extraction_port.rs): Contract for row-level data extraction (`ExtractionPort`).
    *   [`storage_port.rs`](src/ports/storage_port.rs): Contract for writing sidecar artifacts (`StoragePort`).

### 4. Infrastructure Layer (`src/infrastructure/`)
*   **Role**: Implementation of Ports using specific technologies.
*   **Adapters**:
    *   [`metadata.rs`](src/infrastructure/oracle/metadata.rs): Queries Oracle dictionary views (`ALL_TAB_COLS`, etc.) with multi-layered fallback for size discovery.
    *   [`extractor.rs`](src/infrastructure/oracle/extractor.rs): Handles high-performance streaming to CSV or native Parquet.
    *   [`fs_adapter.rs`](src/infrastructure/storage/fs_adapter.rs): Generates DDL, Schema JSON, and Load scripts on the local filesystem.
    *   [`connection_manager.rs`](src/infrastructure/oracle/connection_manager.rs): Implements `r2d2` connection pooling for stable multi-threaded access.

---

## ⚡ Performance Strategies

### 1. Connection Pooling (R2D2)
To maximize throughput without overwhelming the Oracle listener, the exporter uses an internal connection pool implemented in [`connection_manager.rs`](src/infrastructure/oracle/connection_manager.rs):
-   **Stable Parallelism**: Threads pull connections from the pool as needed.
-   **Resource Safety**: Prevents "too many sessions" errors during large-scale parallel exports.

### 2. Parallel Chunking (DBMS_PARALLEL_EXECUTE)
For large tables, the exporter triggers a chunked export strategy coordinated by the [`orchestrator.rs`](src/application/orchestrator.rs):
-   Uses `DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_ROWID` via [`metadata.rs`](src/infrastructure/oracle/metadata.rs).
-   **Resilient Cleanup**: Chunks are cleaned up immediately after metadata fetch via an internal closure-wrap to ensure no stale tasks remain in Oracle.
-   **Size Fallback**: If `ALL_SEGMENTS` is inaccessible, it falls back to `USER_SEGMENTS` and then to table statistics.

### 3. Zero-Copy Native Parquet
Implemented in [`extractor.rs`](src/infrastructure/oracle/extractor.rs):
-   **Arrow Batching**: Oracle rows are streamed directly into Arrow arrays.
-   **No Intermediate Text**: Data maps directly from Oracle OCI buffers to binary Parquet blocks, maximizing MB/s.

---

## 📈 Benchmarks (Oracle 23c Docker Environment)

The following metrics represent performance verified in the integration test environment (Table: `ALL_TYPES_TEST2`, ~8M rows). 

> [!NOTE]
> **Throughput Metrics**: MB/s is calculated based on the **compressed output volume** written to disk. Because Gzip (CSV) is consistently more "dense" than Snappy (Parquet) for Oracle text data, it processes significantly more rows to generate the same output volume.

| Format | Throughput (MB/s) | Throughput (Rows/s) | CPU Usage |
| :--- | :--- | :--- | :--- |
| **CSV (Gzip)** | **18.8 MB/s** | **63,000** | Moderate (Gzip limit) |
| **Parquet** | **17.0 MB/s** | **26,000** | High (Arrow Serialization) |

---

## 🛡️ Resiliency Features

### 1. DATA_DEFAULT Protection
Oracle virtual columns are backed by `LONG` columns in the dictionary. The exporter (via [`metadata.rs`](src/infrastructure/oracle/metadata.rs)) monitors the length of these expressions and warns if they approach or exceed the 32KB truncation limit.

### 2. Contextual Error Reporting
Errors defined in [`errors.rs`](src/domain/errors.rs) are enriched with `table_name` and `chunk_id` context as they bubble up through the hexagonal layers.

---

## 🔍 Specialized Data Handling

### Virtual Columns
The exporter re-implements Oracle virtual columns as **BigQuery Views**, ensuring logic parity. Mapping rules are defined in [`mapping.rs`](src/domain/mapping.rs).

### Spatial Data (`SDO_GEOMETRY`)
Spatial data is extracted via `SDO_UTIL.TO_WKTGEOMETRY` and cast to BigQuery `GEOGRAPHY`.

---

## 🛠️ Maintainer Notes

*   **Adding a Type**: Update `map_oracle_to_arrow` in [`mapping.rs`](src/domain/mapping.rs).
*   **Integration Testing**: Run [`tests/scripts/run_all.sh`](tests/scripts/run_all.sh) to execute the full 360-degree verification suite.
