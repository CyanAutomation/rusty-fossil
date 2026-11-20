# Rusty-Fossil

Rusty-Fossil is the host-side controller foundation for disposable repository workers. **Soyuz is the only run-control authority**: it accepts submissions, assigns canonical run IDs, owns lifecycle and cancellation state, and publishes Cloudflare Queue messages. Rusty-Fossil has no submission endpoint or local-run fallback.

Phase 2A adds a Cloudflare Queue HTTP pull adapter, strict contract-v1 request validation, a durable authorization coordinator, restart-safe execution descriptors, fenced claim/start proofs, a single local handoff slot, and a retryable callback outbox. The transport and coordinator are implemented for mock-driven use, but the production service does not start Queue polling. Docker execution and coding-task execution are not implemented. `/ready` therefore remains false.

## Workspace

- `crates/fossil-core` — canonical IDs, normalized Soyuz request schema, Queue parsing, and local lifecycle rules.
- `crates/fossil-queue` — Cloudflare HTTP pull, ack/retry, response bounds, backoff, shutdown handling, and transport counters.
- `crates/fossil-soyuz` — worker API client, claim/start/canonical proofs, callback requests, and bounded error classification.
- `crates/fossil-storage` — SQLite migrations, private local state, durable descriptors and intents, one-slot reservation, recovery, and callback outbox.
- `crates/rusty-fossil` — controller configuration, authorization coordinator, read-only diagnostics, and callback replay logic.
- `docs/` — contracts, architecture, compatibility evidence, decisions, and follow-on plan.

## Authorization path

```mermaid
sequenceDiagram
    participant Q as Cloudflare Queue
    participant C as Rusty-Fossil coordinator
    participant DB as Local SQLite
    participant S as Soyuz
    Q->>C: pull one leased message
    C->>C: decode base64 and validate full contract-v1 request
    C->>DB: persist observed run, descriptor and intake slot
    C->>S: GET canonical run
    C->>DB: persist claim intent and stable claim callback ID
    C->>S: POST claim with worker ID and fence
    S-->>C: accepted claim, or reconcile canonical owner/fence
    C->>DB: persist claim proof and stable started callback ID
    C->>S: POST started
    S-->>C: start accepted
    C->>DB: persist start proof
    C->>S: final canonical authorization check
    C->>DB: confirm durable proof against the reserved slot
    C->>Q: ack lease only after durable authorized handoff
    Note over C,Q: No worker is launched in Phase 2A.
```

The canonical run UUID is the deduplication key. Queue message and lease IDs are transport identities only. The database retains the complete normalized request needed for restart recovery; SQLite files and their directory are restricted to the service account. Terminal callback intents are stored with local terminal transitions in one transaction.

## Build and local diagnostics

Requirements: Rust stable (minimum 1.89). Docker/Compose can build and run the controller image, but the controller has no Docker socket access.

```sh
cargo build --workspace --release --locked
cargo run -p rusty-fossil
```

The service binds to `127.0.0.1:8080` by default and creates `./state/fossil.sqlite3`.

- `GET /health` reports process liveness.
- `GET /ready` reports configuration, local state, Queue probe, recovery backlog, handoff readiness, and missing execution capabilities. It returns 503 because Queue intake is disabled and Docker execution is absent.
- `GET /metrics` reports read-only diagnostics, callback backlog, and local slot availability.

There are no `POST /runs`, `POST /execute`, retry, claim, or start routes. Queue intake must not be enabled in Compose or in a production deployment as part of Phase 2A.

## Configuration

Use mounted secret files for `SOYUZ_WORKER_API_TOKEN` and `CLOUDFLARE_QUEUE_TOKEN`; direct values are supported for local mock tests. The Soyuz worker token and Cloudflare Queue read/write token are separate. Configure `SOYUZ_API_ORIGIN`, `SOYUZ_WORKER_ID`, `CLOUDFLARE_ACCOUNT_ID`, and `CLOUDFLARE_QUEUE_ID` as needed. Worker IDs remain restricted to 1–64 ASCII letters, digits, `-`, or `_`.

See [Soyuz and Queue contract notes](docs/CONTRACTS.md#configuration-boundary) and `.env.example`. Configuration validity alone does not enable intake or mark coding execution ready.

## Checks

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --release --locked
```

Protocol and coordinator tests use local HTTP mocks and no production credentials, public network, paid inference, Docker daemon, or GitHub publication credentials. CI additionally checks Linux AMD64 and ARM64 builds and audits dependencies.

## Design documents

- [Architecture and recovery boundaries](docs/ARCHITECTURE.md)
- [Soyuz and Cloudflare contracts](docs/CONTRACTS.md)
- [Compatibility inventory](docs/COMPATIBILITY.md)
- [Architecture decisions](docs/DECISIONS/)
- [Implementation plan and Phase 2A-S / 2B prerequisites](docs/IMPLEMENTATION-PLAN.md)
