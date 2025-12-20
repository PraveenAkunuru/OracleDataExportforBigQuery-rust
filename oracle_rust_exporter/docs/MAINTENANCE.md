# Restart Guide: Oracle Rust Exporter
**Date**: 2025-12-19
**Status**: Feature Complete (Rust Parity with Python)

## 1. Project Context (Memory Aid)
We are migrating an Oracle-to-BigQuery exporter from **Python** to **Rust**.
The Rust version (`oracle_rust_exporter`) is now **Feature Complete**, matching or exceeding the Python legacy version.

### Key Achievements (Last Session)
-   **Parity Achieved**:
    -   **Row Hash**: Rust now generates `ROW_HASH` (SHA256) in CSVs (matching Python).
    -   **Validation**: Generated `validation.sql` now includes **Column Sums** and **Row Counts**.
    -   **Documentation**: Automatically generates `column_mapping.md`.
    -   **Format**: Standardized CSV quoting (`NonNumeric`) and XML newline stripping.
-   **Performance**: Rust is ~34x faster per core.

## 2. How to Run (Reference)

### A. Rust Application (Recommended)
The Rust application is containerized. To run it:

1.  **Navigate to Directory**:
    ```bash
    cd oracle_rust_exporter
    ```
2.  **Build Image**:
    ```bash
    docker build -t oracle_rust_exporter .
    ```
3.  **Run (Coordinator Mode)**:
    ```bash
    # Ensure config_full.yaml and output directory exist
    mkdir -p export_data_full
    chmod 777 export_data_full
    
    docker run --rm --network host \
      -v $(pwd)/config_full.yaml:/app/config.yaml \
      -v $(pwd)/export_data_full:/app/export_data_full \
      oracle_rust_exporter --config /app/config.yaml
    ```
    *Note: `config_full.yaml` is currently configured with `enable_row_hash: true`.*

### B. Python Application (Legacy)
The legacy Python application requires the virtual environment.

1.  **Navigate to Root**:
    ```bash
    cd /usr/local/google/home/pakunuru/OracleDataExportForBigQuery
    ```
2.  **Activate Virtual Environment**:
    ```bash
    source .venv/bin/activate
    ```
3.  **Run**:
    ```bash
    python3 main.py --config config.yaml
    ```

## 3. Next Steps (To-Do)
1.  **Deployment**: Implement the "Portable Bundle" build script (targeting CentOS 7) as per `deployment_strategy.md`.
2.  **Code Archive**: Decide when to archive the Python code (`src/*.py`).
3.  **Cleanup**: Remove temporary export directories (`export_data_full`, etc.) when verified.

## 4. Troubleshooting
-   **Permissions**: Docker writes as `root` (or app user). Use `sudo` or Docker to clean up output directories:
    `docker run --rm -v $(pwd):/work alpine rm -rf /work/export_data_full`
-   **Database Access**: Both apps use `network host` or direct connection strings. Ensure VPN/Tunnel is active if connecting to remote DB.
