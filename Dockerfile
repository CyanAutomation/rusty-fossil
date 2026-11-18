FROM rust:1-bookworm AS build
WORKDIR /workspace
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
RUN cargo build --locked --release -p rusty-fossil

FROM debian:bookworm-slim AS runtime-directory
RUN mkdir -p /var/lib/rusty-fossil \
    && chown 65532:65532 /var/lib/rusty-fossil

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=runtime-directory --chown=65532:65532 \
    /var/lib/rusty-fossil /var/lib/rusty-fossil
COPY --from=build /workspace/target/release/rusty-fossil /usr/local/bin/rusty-fossil
ENV RUSTY_FOSSIL_STATE_DIR=/var/lib/rusty-fossil
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/rusty-fossil"]
