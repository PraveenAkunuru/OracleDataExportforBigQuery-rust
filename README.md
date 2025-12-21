# Oracle Data Exporter for BigQuery (Rust)

A high-performance, multi-threaded tool to export Oracle tables to BigQuery-ready compressed CSVs. It handles type mapping, parallel chunking, and validation automatically.

## 🚀 Getting Started

### Linux (RHEL/CentOS/Ubuntu)
1.  **Download** the release tarball to your server.
2.  **Extract** and enter the directory:
    ```bash
    tar -xzvf oracle_exporter_dist.tar.gz
    cd oracle_exporter_dist
    ```
3.  **Run** using the wrapper script (sets up library paths for you):
    ```bash
    ./run.sh --help
    ```

### Windows
1.  **Download** the release zip.
2.  **Extract** to a folder (e.g., `C:\oracle_exporter`).
3.  **Install** [Oracle Instant Client Basic Lite](https://www.oracle.com/database/technologies/instant-client/downloads.html) and add it to your `PATH`.
4.  **Run** via PowerShell:
    ```powershell
    .\oracle_rust_exporter.exe --help
    ```

---

## 🛠 Common Tasks

### 1. Run a Full Export
The best way to run production exports is using a configuration file.

1.  Create `config.yaml` (see [example](tests/configs/integration/config_perf_client_hash.yaml)):
    ```yaml
    database:
      username: "HR"
      password: "password"
      host: "192.168.1.10"
      service: "ORCL"
    export:
      output_dir: "./exports"
      schemas: ["HR", "SALES"]
    ```
2.  Run the exporter:
    ```bash
    ./run.sh --config config.yaml
    ```

### 2. Run a Single Table (Ad-Hoc)
You don't need a config file for quick exports. You can use CLI arguments.

**Command**:
```bash
./run.sh \
  --username SYSTEM --password manager \
  --host localhost --service ORCLPDB \
  --table HR.EMPLOYEES \
  --output ./exports/hr_employees
```
*Note: This output mode (`--output ./dir`) activates the Coordinator, enabling parallel chunking for large tables.*

### 3. Resuming a Failed Export
The exporter is **idempotent**. 
If a job fails (e.g., network disconnect), just **run the exact same command again**.

-   It automatically checks for existing `*.csv.gz` files.
-   **Skips** chunks that are already on disk.
-   **Retries** chunks that are missing.
-   **Retries** chunks that are missing.

### 4. Advanced Features Support
The exporter automatically handles complex Oracle scenarios:

*   **Virtual Columns**: Detected and exported via a **Physical Table / Logical View** split in BigQuery.
    *   Data is stored in `_PHYSICAL` table (excluding virtual columns).
    *   Logic is restored in the `VIEW` (e.g., `TOTAL_COST` as `QUANTITY * PRICE`).
*   **Spatial Data (`SDO_GEOMETRY`)**:
    *   Extracted as **WKT** (Well-Known Text).
    *   BigQuery View automatically converts it to `GEOGRAPHY` type.
*   **Adaptive Parallelism**:
    *   Automatically throttles concurrency based on **Oracle Server load** to prevent database saturation.
    *   Just remove `parallel` from your config to enable.
---

## 📚 Documentation
For deep dives, architecture explanations, and tuning guides, see:

-   **[ARCHITECTURE.md](ARCHITECTURE.md)**: The Master Reference.
    -   **Architecture Map**: Codebase structure.
    -   **Performance Tuning**: Handling slow networks & CPU.
    -   **Operational Handbook**: Verification & Maintenance.
    -   **Deployment Strategy**: How the "Zero-Install" build works.
