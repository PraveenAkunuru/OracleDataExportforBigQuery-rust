# 🏗️ Build Guide for Production

This guide explains how to build the `oracle_rust_exporter` for production environments, specifically for **Legacy Linux Servers** (e.g., Oracle Linux 7/8, RHEL 7/8) where you **cannot install tools like Docker or Rust**.

## ⚠️ The Golden Rule: "Build Local, Deploy Binary"

**You do NOT need to install Rust, GCC, or Docker on your Production Server.**

Rust binaries are self-contained. You should:
1.  **Build** the binary on a separate "Build Machine" (Unix/Mac/Windows with Docker **OR** a Linux VM).
2.  **Copy** the single executable file (`oracle_rust_exporter`) to your Production Server.
3.  **Run** it.

---

## Strategy A: The "Portable Bundle" (Recommended)

Use the existing helper script `scripts/build/build_bundle.sh`. This script is extremely robust:
1.  Uses **CentOS 7** (GLIBC 2.17) to ensure the binary runs on **ANY** Linux from the last 10 years (RHEL 7+, Oracle 7+, Ubuntu 14.04+).
2.  **Bundles** the Oracle Instant Client libraries (so you don't need to install them on the server).
3.  Creates a `.tar.gz` with a `run.sh` wrapper.

### Steps
1.  **Download Oracle Client ZIP**:
    Download `instantclient-basiclite-linux.x64-19.x.zip` and place it in `scripts/build/`.
2.  **Run Builder**:
    ```bash
    cd scripts/build
    ./build_bundle.sh
    ```
3.  **Deploy**:
    Copy `dist/oracle_exporter_bundle.tar.gz` to your server.
4.  **Run**:
    ```bash
    tar -xzf oracle_exporter_bundle.tar.gz
    cd dist
    ./run.sh --help
    ```

---

## Strategy B: Binary Only (If Oracle Client is already installed)

If your production server already has Oracle Database or Instant Client installed, you can just build the binary.

---

## Strategy B: The "Matching VM" (No Docker)

If you strictly cannot use Docker anywhere (even locally), you must find a **Development VM** that matches your production OS (e.g., Oracle Linux 8).

1.  **Install Build Tools** (On Dev VM only):
    ```bash
    # Oracle Linux / RHEL / CentOS
    sudo dnf groupinstall "Development Tools"
    sudo dnf install openssl-devel
    
    # Install Rust
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
    source $HOME/.cargo/env
    ```

2.  **Build Release**:
    ```bash
    cargo build --release
    ```

3.  **Deploy**:
    Copy `target/release/oracle_rust_exporter` to your production server.

---

## 🛠️ Runtime Requirements (Production)

The production server only needs:
1.  **The Binary**: `oracle_rust_exporter`
2.  **Oracle Instant Client**: The `LD_LIBRARY_PATH` must point to `libclntsh.so`.
    *   *Note: If the server runs Oracle Database, this is already installed.*

### Troubleshooting: "GLIBC version not found"
If you build on a **New** OS (e.g., Ubuntu 22.04) and try to run on an **Old** OS (e.g., Oracle Linux 7), you will see:
`version 'GLIBC_2.29' not found`

**Fix**: Use **Strategy A**. The Docker container uses an older GLIBC baseline, ensuring forward compatibility with your legacy servers.
