#!/bin/bash
set -euo pipefail

# Configuration
DIST_DIR="dist"
ARTIFACT_NAME="oracle_exporter_bundle.tar.gz"
IMAGE="centos:7"

echo "=== Oracle Rust Exporter: Portable Bundle Builder ==="
echo "Target OS: RHEL 7 / CentOS 7 (glibc 2.17 compatibility)"

# 1. Check for Oracle Instant Client Zip
ZIP_FILE=$(ls instantclient-basiclite-linux.x64-*.zip 2>/dev/null | head -n 1)

if [ -z "$ZIP_FILE" ]; then
    echo "❌ Error: Oracle Instant Client ZIP not found!"
    echo "Please download 'instantclient-basiclite-linux.x64-19.x.zip' from Oracle website"
    echo "and place it in this directory."
    echo "Link: https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html"
    exit 1
fi

echo "✅ Found Oracle Client: $ZIP_FILE"

# 2. Cleanup & Prep
rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR/lib"

# 3. Extract Libraries
echo "-> Extracting Oracle Libraries..."
unzip -q -j "$ZIP_FILE" "*/libclntsh.so*" "*/libclntshcore.so*" "*/libnnz*.so*" "*/libociei.so*" -d "$DIST_DIR/lib/"
# Create symlinks if needed (some apps look for .so without version, though ODPI usually handles it)
# Specifically libclntsh.so -> libclntsh.so.19.1
cd "$DIST_DIR/lib"
ln -sf libclntsh.so.* libclntsh.so
cd ../..

# 4. Build Binary (Docker/CentOS 7)
echo "-> Compiling Rust Binary in $IMAGE container..."

# We use a heredoc to run commands INSIDE the container
docker run --rm \
    -v "$(pwd)":/src \
    -w /src \
    -e CARGO_HOME=/src/.cargo_cache \
    $IMAGE /bin/bash -c "
        set -e
        # Install Build Deps
        yum -y install gcc make openssl-devel
        
        # Install Rust
        if [ ! -f \$HOME/.cargo/bin/cargo ]; then
            curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
        fi
        source \$HOME/.cargo/env
        
        # Build Release
        echo 'Building release binary...'
        cargo build --release --target-dir /src/target_centos7
"

# 5. Assemble Bundle
echo "-> Assembling Bundle..."
cp target_centos7/release/oracle_rust_exporter "$DIST_DIR/"
cp config_full.yaml "$DIST_DIR/config_template.yaml"

# Create run.sh wrapper
cat > "$DIST_DIR/run.sh" << 'EOF'
#!/bin/bash
# Wrapper to run oracle_rust_exporter with bundled libraries
APP_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Set LD_LIBRARY_PATH to include our bundled lib/ dir
export LD_LIBRARY_PATH="$APP_ROOT/lib:${LD_LIBRARY_PATH:-}"

# Run the binary
exec "$APP_ROOT/oracle_rust_exporter" "$@"
EOF

chmod +x "$DIST_DIR/run.sh"

# 6. Tarball
echo "-> Creating Tarball: $ARTIFACT_NAME"
tar -czf "$ARTIFACT_NAME" -C "$DIST_DIR" .

echo "=== SUCCESS ==="
echo "Bundle created: $ARTIFACT_NAME"
echo "Deploy this file to the customer server."
