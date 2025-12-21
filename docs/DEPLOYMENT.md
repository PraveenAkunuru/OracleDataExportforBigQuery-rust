# Deployment Guide

I designed this application to be "Zero-Install" for the production environment. You do **NOT** need Docker, Root Access, or a Rust toolchain on the target Oracle server. 

You just need a standard Linux user and the folder we generate.

## The Strategy

1.  **Build Phase**: We use Docker (locally or CI/CD) to compile the binary on an older Linux kernel (CentOS 7). This ensures the binary works on everything from RHEL 7 to RHEL 9.
2.  **Runtime Phase**: We package the binary + `config.yaml` + a minimal set of Oracle Libraries into a `.tar.gz`.
3.  **Deployment**: SCP this tarball to the server, extract, and run.

## 1. Creating the Bundle

I included a script `build_bundle.sh` in the root. It handles the "Holy Build" (compiling on old glibc) and dependency gathering.

### Prerequisites for Build
-   Docker (running locally).
-   `instantclient-basiclite-linux.x64-19.x.zip` (You need to download this from Oracle and place it in the project root. I cannot redistribute it).

### Run the Build
```bash
./build_bundle.sh
```

This will produce: `oracle_exporter_dist.tar.gz`.

## 2. Deploying to Production

1.  **Transfer**: Copy `oracle_exporter_dist.tar.gz` to the target server.
2.  **Extract**:
    ```bash
    tar -xzvf oracle_exporter_dist.tar.gz
    cd oracle_exporter_dist
    ```
3.  **Configure**: Edit `config.yaml` with the production DB credentials.
4.  **Run**:
    ```bash
    ./run.sh
    ```

## 3. The `run.sh` Wrapper

The `run.sh` script is critical. It sets `LD_LIBRARY_PATH` to the local `./lib` folder so the application finds the Oracle libraries immediately. You don't need to mess with global system variables.

```bash
#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export LD_LIBRARY_PATH="$DIR/lib:$LD_LIBRARY_PATH"
exec "$DIR/oracle_rust_exporter" "$@"
```
