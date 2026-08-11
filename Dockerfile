# syntax=docker/dockerfile:1.7

# ---------- Builder ----------
FROM rust:1-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        clang \
        cmake \
        libclang-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY constants.toml ./
COPY build.rs ./
COPY src ./src
# `build.rs` derives the version from git, so the build needs `.git`. This is the
# last COPY before the build: it changes every commit and invalidates the layer,
# but the dependency compile is preserved by the cache mounts below.
COPY .git ./.git

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target \
    cargo build --release --locked && \
    cp target/release/celestia-adapter-evaluator /usr/local/bin/celestia-adapter-evaluator

# ---------- Runtime ----------
FROM gcr.io/distroless/cc-debian12:nonroot

COPY --from=builder /usr/local/bin/celestia-adapter-evaluator /usr/local/bin/celestia-adapter-evaluator

ENTRYPOINT ["/usr/local/bin/celestia-adapter-evaluator"]
