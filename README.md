# Rusty-Fossil

Rusty-Fossil is a clean-room Rust controller foundation for repository coding work. **Soyuz is the only run-control plane**: it authenticates submissions, assigns canonical run IDs, records run state, and publishes Queue messages. Soyuz is not the LLM gateway and does not run the coding agent.

This Phase 0/1 prototype provides the architecture and contract baseline, a typed Soyuz worker client, a migration-managed SQLite store, and local health, readiness, and Prometheus metrics endpoints. It deliberately does not consume production Queue messages, create Docker workers, or execute coding tasks. Readiness stays false until a later phase supplies the protected intake and execution lifecycle.

There is no Rusty-Fossil task-submission endpoint or local execution command. A local run record can represent only a run that Soyuz already owns.

## Workspace

- `crates/fossil-core` — canonical identifiers, contract-v1 Queue envelope, external statuses, and the local lifecycle rules.
- `crates/fossil-soyuz` — typed worker requests and reads, bounded transport, error classification, and start-authorization proofs.
- `crates/fossil-storage` — SQLite migrations, transactional lifecycle updates, claim fencing, recovery metadata, and a bounded callback outbox.
- `crates/rusty-fossil` — controller binary and read-only diagnostics API.
- `docs/` — contract findings, architecture, compatibility inventory, decisions, and roadmap.

## Requirements

- Rust stable. The declared minimum is Rust 1.89; `rust-toolchain.toml` selects stable and `Cargo.lock` pins resolved dependencies.
- Docker and Docker Compose only for building or running the packaged controller. The Phase 1 service does not need a Docker socket or daemon.

There are no JavaScript or TypeScript sources in this phase.

## Build and run

```sh
cargo build --workspace --release --locked
cargo run -p rusty-fossil
```

The service binds to `127.0.0.1:8080` by default and creates `./state/fossil.sqlite3`. It can start without Soyuz credentials for local diagnostics; `/ready` reports the missing configuration and Phase 1 limitations with HTTP 503.

```sh
curl -i http://127.0.0.1:8080/health
curl -i http://127.0.0.1:8080/ready
curl http://127.0.0.1:8080/metrics
```

Only these read-only HTTP routes exist:

- `GET /health` — process liveness.
- `GET /ready` — readiness for execution work; false in Phase 1.
- `GET /metrics` — controller and diagnostic request counters.

There are no `POST /runs`, `POST /execute`, retry, claim, or start routes.

## Configuration

Use mounted files for `SOYUZ_WORKER_API_TOKEN` and `CLOUDFLARE_QUEUE_TOKEN`; environment values are supported for local tests. The worker token and Queue read/write token are separate. Client submission credentials are not accepted or required.

Start from `.env.example` and see [the configuration notes](docs/CONTRACTS.md#configuration-boundary). Soyuz and Queue values are optional only for running diagnostics: without all required values Rusty-Fossil cannot become execution-ready. Even with complete configuration, production intake and execution remain unavailable in this phase.

## Tests and checks

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --release --locked
```

The deterministic Queue and worker protocol fixtures do not use production credentials. Mock HTTP integration tests use an ephemeral localhost port. CI also checks Linux AMD64 and ARM64 builds and runs `cargo audit` against the lockfile.

## Local container

Create two secret files before using Compose:

```sh
mkdir -p secrets
: > secrets/soyuz_worker_api_token
: > secrets/cloudflare_queue_token
docker compose up --build
```

The Compose port is published on host loopback and the state directory is persistent. It has no production Queue consumer configured or enabled. Readiness stays false while Phase 1 intake and execution are absent.

## Design and compatibility

- [Architecture and trust boundary](docs/ARCHITECTURE.md)
- [Soyuz and Cloudflare contracts](docs/CONTRACTS.md)
- [Kaseki compatibility inventory](docs/COMPATIBILITY.md)
- [Architecture decisions](docs/DECISIONS/)
- [Implementation plan](docs/IMPLEMENTATION-PLAN.md)
