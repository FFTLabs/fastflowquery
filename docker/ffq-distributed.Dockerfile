FROM rust:1.84-bookworm AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY third_party ./third_party
COPY rust-toolchain.toml rustfmt.toml ./

RUN cargo build --release -p ffq-distributed --features grpc --bin ffq-coordinator --bin ffq-worker

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/ffq-coordinator /usr/local/bin/ffq-coordinator
COPY --from=builder /app/target/release/ffq-worker /usr/local/bin/ffq-worker
