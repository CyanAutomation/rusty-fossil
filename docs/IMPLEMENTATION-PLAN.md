# Implementation plan

Soyuz remains the sole run-control authority through every phase. Rusty-Fossil never creates an independent run or treats an advisory result as authorization.

## Phase 0 — Contract and architecture baseline

Completed: Soyuz contract and requirement IDs, Kaseki operation index, OpenAPI, compatibility inventory and trust boundary are recorded.

## Phase 1 — Controller foundation

Completed: Rust workspace, redacted typed config, read-only Axum API, Prometheus diagnostics, migration-managed SQLite, contract-v1 core types and fixtures, typed Soyuz worker client, CI, and non-root controller packaging.

## Phase 2A — Soyuz Queue intake and durable authorization

Implemented in the repository: Cloudflare Queues HTTP pull/ack/retry adapter, complete supported Soyuz contract-v1 request validation and durable descriptor retention, claim/start intents, canonical final gate, one SQLite slot, duplicate/uncertain response recovery, typed callbacks and persisted retry state, bounded outbox, and readiness/diagnostics. Production Queue polling and callback replay remain unwired in the service binary. No Docker worker or coding process is started by the controller.

## Phase 2A.1 — Recovery and Queue ownership hardening

Merged in the repository. `RecoveryManager` reconciles persisted nonterminal runs independently of Queue delivery, reuses stable callback identities, recognizes cross-host ownership, and stops on ambiguous active work. Intake checks both the single-slot/recovery state and outbox headroom. Queue ack/retry failures and permanent polling suspension are counted and logged. No production Queue resource is enabled by this implementation.

## Phase 2A-S — Senku advisory decision foundation

Merged in the repository. The `fossil-senku` crate supports all five Kaseki-owned profile IDs with typed inputs, versioned envelopes, authenticated bounded transport, strict response validation, evidence minimization/redaction, and per-profile client metrics. `task_admission_v1` has a mockable coordinator seam. Senku output never authorizes a claim/start/retry or overrides deterministic checks. The client is optional and is not instantiated by the production binary. Provision its credential only through an authorized process; never reuse the Soyuz token.

## Phase 2B — Disposable worker execution

Merged in the repository as a synthetic-only capability:

- `fossil-supervisor` provides the deterministic mock, Bollard Docker Engine backend and `fossil-supervisord` binary. The controller does not receive Docker socket access.
- A private Unix socket accepts authenticated, bounded `launch`, `cancel` and `reconcile` messages containing only a canonical run ID. The supervisor reloads the descriptor, slot, worker, claim fence and start proof from SQLite.
- The supervisor revalidates Soyuz before launch and immediately before a new start. Launch intent, stable container name, Docker IDs, lifecycle state, terminal result and callback identity are persisted around external side effects.
- Recovery inspects exact container name, ID, labels, image and effective isolation profile. Ambiguous create/start state is not replayed and becomes manual review. The slot remains reserved until terminal callback persistence and cleanup completion.
- The fixed synthetic worker supports success, controlled failure, timeout, cancellation, bounded output and unexpected termination. It has no shell, prompt interpretation, repository, credentials or network capability.
- Cancellation and timeout persist stop intent, stop the exact container, record a terminal result and outbox entry, then clean up.
- The Docker profile enforces non-root UID, read-only root, dropped capabilities, no-new-privileges, no network, no host mounts/devices/ports, resource/runtime/log limits, Docker's default seccomp profile and no OOM-killer disablement.
- `AuthorizationCoordinator::with_supervisor_client` is an explicit library opt-in. Production `main` still does not construct the Queue poller, callback replay loop, Senku client or supervisor client.

Multi-host SQLite coordination, automatic cross-host takeover, production Queue polling and real coding workloads are out of scope. See [Phase 2B operations](PHASE-2B-OPERATIONS.md) and ADRs 0008–0010.

## Phase 2B.1 — Docker execution reliability and integration hardening

This implementation strengthens the synthetic-only path without activating production Queue consumption or coding execution:

- SQLite transactionally reserves the 1,000th outbox row for a mandatory terminal callback. Optional events and heartbeats are suppressed at 999; duplicate callback IDs remain idempotent, and no pending or blocked callback is deleted. See ADR 0011.
- `DockerSupervisor` observes heartbeat insertion outcomes, keeps the worker running when capacity suppresses a heartbeat, and exposes low-cardinality callback, reconciliation, duplicate-launch, cleanup and backlog counters. The supervisor process logs a structured snapshot; the counters are not connected to controller `/metrics` while production wiring remains disabled.
- The ignored Docker process integration runs the actual coordinator and `fossil-supervisord` binaries with mock Soyuz and disposable SQLite state. It validates the Queue fixture, durable handoff, duplicate delivery, one container ID across ungraceful supervisor restart, cancellation, callback replay with the stable event ID, cleanup and slot release. A separate case validates SIGTERM as an `execution_uncertain` terminal result.
- Real Docker lifecycle tests pass on a native ARM64 Docker Engine. A `test-support`-only marker barrier injects process termination at launch-intent, create-request, container identity, start-request, Docker start, running monitor, Docker exit, terminal persistence and cleanup boundaries. The process tests cover no create retry after an uncertain create request, same-container reattachment after successful create/start side effects, and terminal/cleanup recovery. The suite does not yet simulate every lost API response while the supervisor remains alive or every malformed Docker identity/error response; do not claim a complete Docker Engine fault matrix.
- CI builds `rusty-fossil` and `fossil-supervisord` for Linux AMD64 and ARM64, builds the synthetic worker OCI image for AMD64 and ARM64, and retains Rust 1.89 MSRV, format, Clippy, workspace tests, release build, dependency audit and Docker integration jobs.
- No production Queue, Pi worker, real coding workload or publication capability is enabled.

The operational and hardware validation prerequisites are in [Phase 2B operations](PHASE-2B-OPERATIONS.md). AMD64/ARM64 build success is not a Raspberry Pi hardware test.

## Phase 2C-A — Pi coding worker foundation (in progress)

The current branch adds a Pi 1.6.0 package and integrity-locked dependency graph, a bounded read-only prompt-file reader at a fixed container path, a transcript-free JSONL event classifier, wrapper tests, and a CI build for AMD64 and ARM64 that requests SBOM/provenance attestations. The wrapper rejects writable or symlink payloads and caps prompt input at 32 KiB; the supervisor does not yet stage or mount this payload. Schema v7 persists a versioned `PiCodingV1` launch identity, canonical publish-none request policy, opaque workspace identity and a per-run repository/ref/commit tuple validated against that run's request. The `fossil-workspace` library implements an exact operator-owned pilot allowlist, restricted Git HTTPS, commit-pinned checkout, bounded tree validation and safe cleanup; automated unit tests cover its policy and security bounds, but it has not fetched a repository or been wired into the supervisor. Bounded Docker stdout parsing feeds Pi-specific durable terminal classes and the stable callback outbox. This branch adds a `fossil-gateway` broker prototype and dormant Compose profile. The broker verifies run-fenced tokens, fixes the configured Cloudflare AI Gateway endpoint/model, bounds requests and responses, limits per-run request count/bytes, disables redirects, and persists content-free counters. It does not yet issue worker tokens, provide a cost-based spend ceiling, receive Pi traffic, or demonstrate host firewall enforcement. The supervisor remains synthetic-only and rejects Pi before container creation; no task descriptor, workspace, gateway configuration or live result callback reaches a coding worker.

Complete this phase by adding a separately trusted Pi image/resource profile, integrating each per-run authorized repository commit into the trusted launch path, staging prompts durably, issuing fenced gateway capabilities, enforcing worker egress at a verified host/network boundary, adding cost-based spend controls, persisting and recovering structured results, and proving cancellation/timeout/restart behavior on a native ARM64 host. The supervisor must reject the current Pi identity before Docker create until those runtime controls are implemented. Preserve Soyuz proof-gating, the existing supervisor boundary, and the single-host execution slot. Keep separate coding-job timeout, cancellation grace, Soyuz outage grace, supervisor shutdown grace and container recovery deadline settings; do not raise the synthetic timeout globally. Resolve drain-mode and maintenance behavior before multi-hour jobs are enabled. The policy proposal is [ADR 0012](DECISIONS/0012-phase-2c-long-running-execution-policy.md). No production Queue, real coding workload, or publication capability is enabled.

The generalized read-only ARM64 host review is recorded in [host compatibility](ARM64-HOST-COMPATIBILITY.md) and [host validation](ARM64-HOST-VALIDATION.md). It gives representative Raspberry Pi hardware characteristics while omitting host, account, and access identifiers. The preflight did not establish a dedicated service identity, effective SSH policy, or deployed Compose boundary. No worker is running. Follow the operational gates in [Phase 2C-A operations](PHASE-2C-A-OPERATIONS.md) before attempting a real run.

The broker boundary and current gates are captured in [ADR 0014](DECISIONS/0014-pi-model-broker-boundary.md). The broker profile requires an operator-provisioned dedicated service identity and private state directory; neither was created or verified during this work.

## Phase 3 — Verification and publication

Add deterministic validation, quality gates, bounded artifacts, diff/result collection and GitHub publication. Treat model output, files, logs and validation output as untrusted. Keep publication credentials and repository content out of bounded Soyuz operational metadata.

## Phase 4 — Operational conformance

Run controlled Queue and Soyuz integration tests, confirm idempotent callback behavior and DLQ handling, measure Linux ARM64/Pi use, test outage and restoration procedures, and compare Rust behavior with the Kaseki inventory. Changes to Soyuz, Senku or Kaseki require separate changes and review.

## Rollout prerequisites

1. Keep production Queue polling, callback replay, recovery supervision and Senku inference disabled until transport, recovery, DLQ, credential and worker controls are separately reviewed.
2. Verify Queue producer content type and payload encoding with a non-production Queue; define DLQ retention and controlled redrive.
3. Provision Senku credentials only through the currently authorized process, or review a separate permission change for a dedicated Rusty-Fossil identity.
4. Require the Docker-enabled lifecycle and process integration tests plus AMD64/ARM64 builds before merging Phase 2B.1.
5. Validate the actual Raspberry Pi model, OS, Docker Engine, cgroup controls, seccomp, memory headroom, storage and shutdown/recovery behavior before any Pi rollout.
6. Keep real coding workloads, GitHub publication and multi-host takeover disabled until their separate phases and reviews are complete.
