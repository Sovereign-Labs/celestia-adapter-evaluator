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
# Standard checkouts derive the version from .git. Linked worktree builds pass
# their exact revision with --build-arg EVALUATOR_BUILD_VERSION=<revision>.
COPY .git ./.git
ARG EVALUATOR_BUILD_VERSION

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target \
    cargo build --release --locked && \
    cp target/release/celestia-adapter-evaluator /usr/local/bin/celestia-adapter-evaluator

# ---------- Runtime ----------
FROM gcr.io/distroless/cc-debian12:nonroot

COPY --from=builder /usr/local/bin/celestia-adapter-evaluator /usr/local/bin/celestia-adapter-evaluator

ENTRYPOINT ["/usr/local/bin/celestia-adapter-evaluator"]
