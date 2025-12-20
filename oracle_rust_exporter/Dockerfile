# Stage 1: Builder
# We use Oracle Linux 8 to easily get the Instant Client, then install Rust
FROM oraclelinux:8-slim as builder

# Install build dependencies and Instant Client
RUN microdnf install -y oracle-instantclient-release-el8 && \
    microdnf clean all && \
    microdnf makecache
RUN microdnf install -y gcc openssl-devel oracle-instantclient-basic oracle-instantclient-devel tar gzip && \
    microdnf clean all

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app
COPY . .

# Build the binary
RUN cargo build --release

# Stage 2: Runtime
FROM oraclelinux:8-slim

# Install Instant Client (Runtime only)
# Install Instant Client (Runtime only)
RUN microdnf install -y oracle-instantclient-release-el8 && \
    microdnf clean all && \
    microdnf makecache
RUN microdnf install -y oracle-instantclient-basic && \
    microdnf clean all

WORKDIR /app
COPY --from=builder /app/target/release/oracle_rust_exporter .

# Default entrypoint
ENTRYPOINT ["./oracle_rust_exporter"]
