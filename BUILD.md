# 🏗️ Build Guide for Production

This guide explains how to build the `oracle_rust_exporter` for production environments, specifically for **Legacy Linux Servers** (e.g., Oracle Linux 7/8, RHEL 7/8) where you **cannot install tools like Docker or Rust**.

## ⚠️ The Golden Rule: "Build Local, Deploy Binary"

**You do NOT need to install Rust, GCC, or Docker on your Production Server.**

Rust binaries are self-contained. You should:
1.  **Build** the binary on a separate "Build Machine" (Unix/Mac/Windows with Docker **OR** a Linux VM).
2.  **Copy** the single executable file (`oracle_rust_exporter`) to your Production Server.
3.  **Run** it.

---

## Strategy A: The "Build Container" (Recommended)

If you have Docker on your **Laptop** or **CI/CD Server**, use this method. It guarantees compatibility by compiling against the exact OS libraries used in production (Oracle Linux 8).

### 1. Run the Build Script
On your machine (NOT production), run:
```bash
./scripts/build_in_docker.sh
```
*This script launches a temporary OracleLinux container, installs the Rust compiler *inside* it, builds the project, and extracts the binary to your local folder.*

### 2. Verified Output
You will see a file named `oracle_rust_exporter` in your project root.
Verify it is an executable:
```bash
file oracle_rust_exporter
# Output should say: ELF 64-bit LSB shared object, x86-64 ...
```

### 3. Deploy
Upload this single file to your production server:
```bash
scp oracle_rust_exporter user@prod-server:/app/bin/
```

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
