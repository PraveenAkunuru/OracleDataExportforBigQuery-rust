# Oracle Data Exporter for BigQuery (Rust)

A high-performance, production-ready tool to export Oracle database tables to BigQuery-compliant CSV files. This application is written in Rust for speed, reliability, and minimal resource usage, serving as a drop-in replacement for the legacy Python exporter.

## Key Features
-   **High Performance**: ~30x faster than legacy Python implementation.
-   **Parity**: Full feature parity (ROW_HASH, Schema Generation, Validation).
-   **Zero-Install**: Deploys as a self-contained portable bundle (no Docker required on backend).
-   **Configurable**: Dynamic parallelism, custom delimiters, and resource throttling.

## Documentation
-   **[Walkthrough & Usage](docs/WALKTHROUGH.md)**: Detailed configuration guide and data type mappings.
-   **[Deployment Strategy](docs/DEPLOYMENT.md)**: Instructions for building the portable production bundle.
-   **[Maintenance](docs/MAINTENANCE.md)**: Tips for restarting, troubleshooting, and verifying exports.

## Quick Start (Docker)
```bash
# Build
docker build -t oracle_rust_exporter .

# Run
docker run --rm --network host \
  -v $(pwd)/config.yaml:/app/config.yaml \
  -v $(pwd)/output:/app/output \
  oracle_rust_exporter --config /app/config.yaml
```

## Production Build
To create the portable Linux bundle for RHEL 7/8 environments:
```bash
./build_bundle.sh
```
*(Requires Oracle Instant Client ZIP, see [Deployment Strategy](docs/DEPLOYMENT.md))*

### Runtime Requirements
The application uses **Oracle Instant Client**. You must set `LD_LIBRARY_PATH` to the location of `libclntsh.so` (usually the `lib/` directory in the bundle).
```bash
export LD_LIBRARY_PATH=$(pwd)/lib:$LD_LIBRARY_PATH
```
