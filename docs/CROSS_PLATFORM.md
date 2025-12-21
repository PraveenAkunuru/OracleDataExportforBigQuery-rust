# Cross-Platform Build Guide

Oracle 19c runs on many operating systems. This guide explains how to build the `oracle_rust_exporter` for each supported target.

## Summary Matrix

| OS | Architecture | Status | Build Method |
| :--- | :--- | :--- | :--- |
| **Linux** | x86-64 | ✅ **Supported** | `./build_bundle.sh` (Docker) |
| **Linux** | ARM64 (aarch64) | ⚠️ **Available** | Docker (requires ARM config) |
| **Windows**| x86-64 | ✅ **Supported** | `.\build_windows.ps1` |
| **Solaris**| SPARC / x86 | 🚧 **Manual** | Compile on host (Tier 2 Rust) |
| **AIX** | POWER (ppc64) | 🚧 **Manual** | Compile on host (Tier 3 Rust) |
| **HP-UX** | Itanium | ❌ **Impossible** | No Rust compiler available |

---

## 1. Linux (Universal)

The default `./build_bundle.sh` targets **x86-64** (Intel/AMD) likely found in on-prem data centers.

### Building for ARM64 (e.g., Oracle Cloud Ampere)
If you are running on ARM64 servers, you need to rebuild the Docker image for `linux/arm64`.

1.  Download **Oracle Instant Client for Linux ARM64**.
2.  Run the build command manually:
    ```bash
    docker run --platform linux/arm64 --rm -v $(pwd):/src -w /src ubuntu:20.04 ...
    ```
    *Note: We recommend Ubuntu 20.04 for ARM builds as CentOS 7 ARM support is limited.*

---

## 2. Windows

We provide a PowerShell script `build_windows.ps1` to automate this.

### Requirements
*   Windows 10/11 or Server 2016+.
*   [Rust for Windows](https://rustup.rs/) (install `x86_64-pc-windows-msvc`).
*   Oracle Instant Client for Windows x64 (Basic Lite).

### Steps
1.  Open PowerShell.
2.  Run: `.\build_windows.ps1`
3.  The output `oracle_exporter_win.zip` will appear in `dist\`.

---

## 3. Solaris & AIX (Legacy Unix)

We cannot provide an automated script for these because the Rust compiler and Linker rely on system-specific headers found only on the running OS.

### Protocol for Solaris/AIX
1.  **Install Rust**:
    *   **Solaris**: Use the packages provided by OpenCSW or build from source (Tier 2).
    *   **AIX**: Use [IBM Open XL C/C++](https://www.ibm.com/open-source/) tools which include Rust.
2.  **Install Oracle Client**:
    *   Download the Instant Client ZIP for Solaris/AIX.
    *   Set `LD_LIBRARY_PATH` (Linux/Solaris) or `LIBPATH` (AIX).
3.  **Compile**:
    ```bash
    cargo build --release
    ```
4.  **Verify**:
    If you see `DPI-1047`, your `LIBPATH` is incorrect. On AIX, library handling is strict.

---

## 4. HP-UX

**Status**: ⛔ **Not Supported**
Rust does not officially support HP-UX (IA64). To support HP-UX, you would need to rewrite the application in C or Java 8.
