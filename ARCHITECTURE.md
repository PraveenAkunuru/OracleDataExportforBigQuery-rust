# 🏗️ Architecture & Technical Reference

This document serves as the master reference for the **Oracle Data Exporter for BigQuery**. It covers the architectural patterns, data strategies, and operational constraints of the system.

---

## 🏛️ Hexagonal Architecture (Ports and Adapters)

The application is structured to strictly decouple business orchestration from external I/O. This ensures that the core logic is testable without a database and that adapters (e.g., swapping Local Storage for GCS) can be replaced with minimal impact.

### 1. Domain Layer (`src/domain/`)
*   **Role**: Internal state and pure logic.
*   **Key Files**:
    *   [`export_models.rs`](src/domain/export_models.rs): Definitions for `TableMetadata`, `ExportTask`, and `TaskResult`.
    *   [`bq_type_mapper.rs`](src/domain/bq_type_mapper.rs): Translation rules for converting Oracle types to BigQuery types.
    *   [`error_definitions.rs`](src/domain/error_definitions.rs): Centralized error handling using `thiserror`.

### 2. Application Layer (`src/application/`)
*   **Role**: Orchestration and coordination.
*   **Key Files**:
    *   [`export_orchestrator.rs`](src/application/export_orchestrator.rs): The "Brain". Discovers tables, manages Rayon parallel pools, and executes tasks.

### 3. Ports (`src/ports/`)
*   **Role**: Dependency inversion contracts (Traits).
*   **Interfaces**:
    *   `SchemaReader`: Contract for metadata discovery.
    *   `DataStreamer`: Contract for row extraction.
    *   `ArtifactWriter`: Contract for writing sidecar files.

### 4. Infrastructure Layer (`src/infrastructure/`)
*   **Role**: Implementation of Ports using specific technologies.
*   **Adapters**:
    *   `OracleMetadataAdapter`: Queries `ALL_TAB_COLS`, `ALL_CONSTRAINTS`, etc.
    *   `OracleExtractionAdapter`: Handles `oracle-rust` connection pooling and streaming.
    *   `LocalArtifactAdapter`: Generates DDL/JSON on the local filesystem.

---

## ⚡ Performance Strategies

### 1. Parallel Chunking (DBMS_PARALLEL_EXECUTE)
For tables larger than **1GB**, the exporter triggers a chunked export strategy:
-   It uses `DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_ROWID` on the Oracle side.
-   Chunks are fetched as `(start_rowid, end_rowid)` pairs.
-   Each chunk is assigned to a worker in the Rayon thread pool.
-   Workers execute independent `SELECT ... WHERE ROWID BETWEEN ...` queries.

### 2. Zero-Copy Streaming
Data flow is designed to minimize memory overhead:
-   **Oracle Cursor** -> **Row Buffer** -> **CSV Formatter** -> **Gzip Encoder** -> **File Stream**.
-   The application never loads a full table (or even a full chunk) into RAM.

### 3. Adaptive Parallelism
If `parallel` is not specified, the exporter calculates concurrency based on:
-   `DB Server CPU Count` (discovered via `SchemaReader`).
-   `Local Host CPU Count`.
-   The lower of the two is used to avoid saturating either end.

---

## 💎 Specialized Data Handling

### Virtual Columns
Oracle virtual columns are not physical data. The exporter:
1.  Discovers the SQL expression via `ALL_TAB_COLS.DATA_DEFAULT`.
2.  Excludes the column from the `bq load` process (Physical Table).
3.  Creates a **BigQuery View** that re-implements the expression.

### Spatial Data (`SDO_GEOMETRY`)
Spatial data is extracted using `SDO_UTIL.TO_WKTGEOMETRY(col)`.
The generated BigQuery View casts the resulting string to the `GEOGRAPHY` type.

### Raw/Binary Data
`BLOB` and `RAW` types are automatically encoded as **Base64** during extraction.

---

## 🛡️ Operational Handbook

### Verification
The tool includes a `validate_table` method (in `SchemaReader`) that can verify:
-   Row counts (Source vs Target).
-   Primary Key Hashes.
-   Column Aggregates (Sum/Min/Max).

### Deployment (Zero-Install)
The recommended deployment is a "portable" directory containing:
-   `oracle_rust_exporter`: The statically linked Rust binary (except for Oracle client).
-   `lib/`: Minimal Oracle Instant Client shared libraries.
-   `run.sh`: Wrapper to set `LD_LIBRARY_PATH`.

---

## 🛠️ Maintainer Notes

*   **Adding a New Type**: Update `map_oracle_to_bq` in `bq_type_mapper.rs`.
*   **Adding GCS Support**: Create a `GcsArtifactAdapter` in `src/infrastructure/gcp/` implementing the `ArtifactWriter` port.
*   **Testing**: Always run `./run_docker_test.sh` before merging significant changes.
