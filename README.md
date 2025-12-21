# Oracle Data Exporter for BigQuery (Rust)

I built this tool to solving a specific problem: moving massive Oracle tables to BigQuery was too slow and fragile with existing Python scripts. This is a complete rewrite in Rust, designed for raw speed, stability, and correctness.

## Why use this?

*   **Speed**: It’s multi-threaded and asynchronous. On my benchmarks, it handles ~40-60 MB/s per node (depending on network), which is about 30x faster than the legacy single-threaded Python exporter.
*   **Correctness**: It doesn't just dump data. It handles weird Oracle types (`TIMESTAMP WITH TIME ZONE`, `INTERVAL`, `XMLTYPE`) and maps them correctly to BigQuery standards.
*   **Safety**: It supports "Client-Side Hashing" (calculating SHA256 in Rust) so you can verify data integrity without burning CPU cycles on your production Oracle database.
*   **Zero-Dependency**: The binary stands alone. You don't need to install Python, pip, or hunt down dependencies on your servers. Just drop the binary and the Oracle Instant Client libraries.

## Prerequisites

You just need two things:
1.  **Oracle Instant Client**: The proprietary Oracle libraries (`libclntsh.so`).
2.  **Environment**: Linux (RHEL/CentOS/Debian) is the primary target.

## Building

If you want to build it yourself:

```bash
cargo build --release
```

To create a deployable bundle (zip file) with all the scripts and libs included:

```bash
./build_bundle.sh
```

## How to Run

There are two ways to use this.

### 1. The "Just Export One Table" Mode

Great for ad-hoc dumps or testing.

```bash
# Point to your Instant Client first!
export LD_LIBRARY_PATH=/opt/oracle/instantclient:$LD_LIBRARY_PATH

./oracle_rust_exporter \
  --host 10.0.0.5 \
  --service ORCL \
  --username SCOTT \
  --password tiger \
  --table EMP \
  --output ./emp.csv.gz
```

### 2. Coordinator Mode (Production)

This is what you should use for actual migrations. It reads a config file, discovers metadata, splits large tables into chunks (automatically!), and can even generate the BigQuery table schemas for you.

```bash
./oracle_rust_exporter --config config.yaml
```

**Minimal `config.yaml`:**

```yaml
database:
  username: "SCOTT"
  password: "tiger"
  host: "10.0.0.5"
  port: 1521
  service: "ORCL"

export:
  output_dir: "./data_dump"
  schemas: ["SCOTT"]       # Schemas to scan
  parallel: 8              # Threads to use
  use_client_hash: true    # Enable the fast Rust-side hashing
```

## What you get

After running the coordinator, you'll see a structure like this:

```
data_dump/
  SCOTT/
    EMP/
      data/
        data_chunk_0.csv.gz
        data_chunk_1.csv.gz
      config/
        bigquery_schema.json    # Ready for 'bq load'
        oracle_ddl.sql          # In case you need the source DDL
        load_command.sh         # A helper script to load this into BQ
```

## Tuning Tips

*   **`parallel`**: Start with 4 or 8. If your network is fast, you can push this higher, but watch your DB load.
*   **`use_client_hash`**: Always keep this `true` for large tables. It offloads the expensive SHA256 calculation to the exporter (Rust), saving your Oracle DB from CPU spikes.
