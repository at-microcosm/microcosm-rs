# Multi-stage build for constellation.
#
# The build context must be the workspace ROOT (microcosm-rs/)
FROM rust:1.96-bookworm AS builder

# rocksdb (bundled) builds a C++ library and runs bindgen (needs libclang);
# tungstenite's native-tls feature needs OpenSSL headers.
RUN apt-get update && apt-get install -y --no-install-recommends \
        clang \
        libclang-dev \
        build-essential \
        libssl-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

# Cache the cargo registry and target dir across builds. Because the target dir
# lives in a cache mount (not the layer), copy the finished binary out in the
# same RUN so it survives into the image.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release -p constellation \
    && cp target/release/main /usr/local/bin/constellation

FROM debian:12.14-slim AS runtime

# ca-certificates for TLS to jetstream/tap; libssl3 for native-tls at runtime.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/constellation /usr/local/bin/constellation

RUN mkdir -p /data
VOLUME ["/data"]

ENTRYPOINT ["constellation"]
CMD ["--help"]
