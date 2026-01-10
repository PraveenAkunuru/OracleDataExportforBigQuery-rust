# 🛠️ Development Guide

This guide is for developers contributing to the **Oracle Data Exporter for BigQuery**. It covers building from source, running tests, and contribution guidelines.

## 🏗️ Building from Source

### Prerequisites
*   **Rust**: Stable toolchain (`rustup install stable`).
*   **Oracle Instant Client**: Required/linked at runtime.
*   **GCC/Clang**: For linking.

### Build Commands

```bash
# Debug Build
cargo build

# Release Build (Optimized)
cargo build --release
```

### Build Strategies (Legacy Systems)

If you need to build for an older Linux system (e.g., Oracle Linux 7) and cannot use Docker there:

1.  **Portable Bundle (Recommended)**:
    Use `scripts/build/build_bundle.sh`. This runs a Docker container (CentOS 7) to build a GLIBC-compatible binary and bundles it with Instant Client.
    ```bash
    cd scripts/build
    ./build_bundle.sh
    ```

2.  **Manual VM Build**:
    Set up a VM with the *same OS* as production. Install Rust and `openssl-devel`, then run `cargo build --release`.

---

## 🧪 Testing Guide

We have a master script that runs the entire suite:

```bash
# Runs Unit, Negative, and Integration (Docker) tests
bash tests/scripts/run_all.sh
```

### 1. Prerequisites
*   **Docker**: For `oracle23-free` container (Integration tests).
*   **Environment Variables**: Create a `.env` file in the root.
    ```bash
    DB_USER=TESTUSER
    DB_PASS=your_password
    DB_SYSTEM_USER=SYSTEM
    DB_SYSTEM_PASS=your_system_password
    ```

### 2. Individual Tests

*   **Unit Tests**: `cargo test`
*   **Integration Tests**: `bash tests/scripts/run_integration_tests.sh`
*   **Negative Tests**: `bash tests/scripts/run_negative_tests.sh`

### 3. Troubleshooting Tests
*   **Connection Refused**: Ensure `docker ps` shows `oracle23-free` running on port 1521.
*   **Login Denied**: Check `.env` matches your Docker container's password.

---

## 🤝 Contributing

We welcome contributions!

### Community Guidelines
This project follows [Google's Open Source Community Guidelines](https://opensource.google/conduct/).

### Code Reviews
All submissions require review via GitHub pull requests.

### CLA
Contributions must be accompanied by a Contributor License Agreement (CLA).
Visit <https://cla.developers.google.com/> to sign.

### Style Guide
*   Run `cargo fmt` before committing.
*   Ensure `cargo clippy` passes.
