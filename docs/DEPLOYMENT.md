# Deployment Strategy: Production-Safe Approach

## 🚨 KEY CLARIFICATION: No Docker Needed on Prod
**The customer DOES NOT need to install Docker.**
**The customer DOES NOT need `root` access.**

We (the developers) use Docker **ONLY** to build the application artifact.
The deliverable we give to the customer is a simple **Folder** (or `.zip` file) containing a Linux Executable.
They run it like any other script: `./run.sh`.

---

## 1. Target Environment Analysis
Based on your description ("4 years old, Oracle 19c, Java 8"):
*   **Operating System**: Likely **RHEL 8** or **RHEL 7.9**.
*   **Requirement**: Build on **CentOS 7** (glibc 2.17) to ensure usage on older Linux kernels.
*   **Oracle Client**: Bundle **version 19.x** to match the server.

## 2. The "Zero-Install" Portable Bundle (Recommended)
**Solution**: A **Self-Contained User-Space Bundle** (.tar.gz) containing the binary and minimal Oracle libraries.

### 2.1 Required Downloads (Links)

You will need to download these resources to build the bundle:

1.  **Oracle Instant Client (Basic Lite) - Linux x86-64**
    *   **Download Page**: [Oracle Instant Client Downloads for Linux x86-64](https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html)
    *   **File to Look For**: `instantclient-basiclite-linux.x64-19.25.0.0.0dbru.zip` (Version may vary slightly, get the latest 19.x).
    *   *Note*: Requires a free Oracle Account to download.

2.  **Build Environment (Docker Image)**
    *   **Image**: `centos:7`
    *   **Link**: [CentOS Official Docker Image](https://hub.docker.com/_/centos)
    *   *Command*: `docker pull centos:7`

### 3. Package Structure
Create a directory structure:
```text
oracle_exporter_dist/
├── oracle_rust_exporter     # The executable (built on CentOS 7)
├── config.yaml              # Template config
├── run.sh                   # Wrapper script
└── lib/                     # Extracted from instantclient-basiclite ZIP
    ├── libclntsh.so.19.1
    ├── libclntshcore.so.19.1
    ├── libnnz19.so
    └── ...
```

### 4. Build Strategy (The "Holy Build")
Compile the Rust binary using the CentOS 7 image to ensure it works on your Customer's RHEL 7/8 servers.

**Build Command (Example):**
```bash
# Verify your local source is in $(pwd)
docker run --rm -v $(pwd):/src -w /src centos:7 /bin/bash -c "
    yum install -y gcc make openssl-devel && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && \
    source \$HOME/.cargo/env && \
    cargo build --release"
```
*Note: You may need to install minimal Oracle headers in the build container if `rust-oracle` requires them during build time (it usually does for linking).*

### 5. The Wrapper Script (`run.sh`)
This script tells the application to use the bundled libraries in `./lib` instead of looking for them on the system.

```bash
#!/bin/bash
# Get the directory where this script resides
APP_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Tell the linker to look for libraries in ./lib FIRST (User-Space)
export LD_LIBRARY_PATH="$APP_ROOT/lib:$LD_LIBRARY_PATH"

# Run the executable
exec "$APP_ROOT/oracle_rust_exporter" "$@"
```
