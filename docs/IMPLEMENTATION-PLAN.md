# Implementation plan

Soyuz remains the sole run-control authority through every phase. Rusty-Fossil never creates an independent run or treats an advisory result as authorization.

## Phase 0 — Contract and architecture baseline

Completed: current Soyuz contract and requirement IDs recorded; Kaseki operation index and OpenAPI inspected; compatibility inventory and trust boundary documented.

## Phase 1 — Controller foundation

Completed: Rust workspace, redacted typed config, read-only Axum API, Prometheus diagnostics, migration-managed SQLite, contract-v1 core types and fixtures, typed Soyuz worker client, CI, and non-root controller packaging.

## Phase 2A — Soyuz Queue intake and durable authorization

Implemented in this change:

- Cloudflare Queues HTTP pull/ack/retry adapter with a separate token, bounded response handling, one-message polling and shutdown control.
- Complete supported Soyuz contract-v1 request validation and durable descriptor retention.
- Durable claim/start intents, private-field Soyuz proofs, canonical final execution gate and a single SQLite slot.
- Duplicate/uncertain-response recovery and ack only after a durable authorized handoff.
- Typed terminal/event callback payloads, persisted attempts/deadlines, bounded outbox, success-only delivery marking and blocked stale/permanent outcomes.
- Readiness and diagnostics that distinguish configuration, state, transport probe, recovery, handoff and missing execution runtime.
- Local mock integration coverage. Production Queue polling and callback replay remain unwired in the service binary. No Docker worker or coding process is started.

The adapter remains unconstructed in `main`. Before enabling it operationally, verify actual Soyuz producer content type and Queue message body encoding in a controlled non-production Queue and configure the documented finite retry/DLQ policy. Queue transport and coordinator metrics are available from their components; transport status remains unprobed by the read-only diagnostics API.

## Phase 2A.1 — Recovery and Queue ownership hardening

Implemented in this worktree; merge and runtime rollout remain pending. `RecoveryManager` reconciles persisted nonterminal runs independently of Queue delivery, reuses stable callback identities, recognizes cross-host ownership, and stops on ambiguous active work. Intake checks both the single-slot/recovery state and outbox headroom. The callback outbox retains its 1,000-item cap and pauses new intake at 999 pending/blocked items to protect one mandatory terminal callback. Queue ack/retry failures and permanent polling suspension are counted and logged. No production Queue resource was changed.

## Phase 2A-S — Senku advisory decision foundation

Implemented in this worktree; merge and runtime rollout remain pending. The new `fossil-senku` crate supports all five Kaseki-owned profile IDs with typed inputs, versioned envelopes, authenticated bounded transport, strict consumer validation, evidence minimization/redaction, and per-profile client metrics. `task_admission_v1` has a mockable coordinator seam. Senku output never authorizes a claim/start/retry or overrides deterministic checks. The client is optional and is not instantiated by the production binary. Senku's current permission matrix grants these profiles to the Kaseki identity; provision a separate credential under that authorization or review a dedicated Rusty-Fossil identity with a separate Senku change. Never reuse the Soyuz token.

## Phase 2B — Disposable worker execution

Implemented in this branch as synthetic-only execution:

- `fossil-supervisor` provides the deterministic mock plus a Bollard Docker Engine backend and the `fossil-supervisord` binary. The controller does not receive Docker socket access.
- A private Unix socket accepts authenticated, bounded `launch`, `cancel`, and `reconcile` messages containing only a canonical run ID. The supervisor selects the scenario from its own runtime configuration and reloads the descriptor, local slot, worker, claim fence and start proof from SQLite.
- The supervisor revalidates Soyuz before launch and immediately before a new start. Launch intent, stable container name, Docker IDs, lifecycle state, terminal result and callback identity are persisted in schema v4 before/after the corresponding external side effect.
- Recovery inspects exact container name, ID, labels, image and effective isolation profile. Lost create responses are never retried; missing or conflicting containers become manual review. The single execution slot remains occupied until terminal callback persistence and cleanup completion.
- The fixed synthetic worker supports success, controlled failure, timeout, cancellation, bounded output and unexpected termination. It has no shell, prompt interpretation, repository, credentials or network capability.
- Cancellation and timeout persist stop intent, attempt graceful Docker stop, force-stop after a bounded grace period, record terminal state and callback outbox entry, then clean up.
- The Docker profile enforces non-root UID, read-only root, dropped capabilities, no-new-privileges, no network, no host mounts/devices/ports, memory/CPU/PID/tmpfs/runtime/log limits, Docker's default seccomp profile and no OOM-killer disablement.
- `AuthorizationCoordinator::with_supervisor_client` is an explicit library opt-in. Production `main` still does not construct the Queue poller, callback replay loop, Senku client or supervisor client.
- Default workspace tests remain Docker-free. A separate Docker-enabled CI job builds the synthetic image and runs the ignored Engine integration. This development host denies access to its Docker socket, so the real Engine test cannot be run locally.

This branch does not enable production Queue polling or real coding workloads. Multi-host SQLite coordination and automatic cross-host takeover remain out of scope. See [Phase 2B operations](PHASE-2B-OPERATIONS.md) and ADRs 0008–0010 for recovery assumptions and operator actions.

## Phase 2C — Coding-agent runtime

Introduce Pi CLI only behind the worker boundary. Implement repository preparation, allowlisted environment and secrets, gateway configuration, and isolation controls. Test disposable repositories and non-production credentials.

## Phase 3 — Verification and publication

Add deterministic validation, quality gates, bounded artifacts, diff/result collection and GitHub publication. Treat model output, files, logs and validation output as untrusted. Keep publication credentials and repository content out of bounded Soyuz operational metadata.

## Phase 4 — Operational conformance

Run controlled Queue and Soyuz integration tests, confirm idempotent callback behavior and DLQ handling, measure Linux ARM64/Pi use, test outage and restoration procedures, and compare Rust behavior with the Kaseki inventory. Changes to Soyuz, Senku or Kaseki require separate changes and review.

## Rollout prerequisites

1. Review this Phase 2A implementation and CI.
2. Verify producer content type and base64 payload behavior with a non-production queue.
3. Define DLQ retention/replay and blocked-callback operator runbooks.
4. Merge Phase 2A.1 and 2A-S only after CI passes on Linux AMD64 and ARM64 and the sandbox-limited mock transport tests run in a socket-enabled environment.
5. Provision Senku credentials with the currently authorized profile scope; a dedicated Rusty-Fossil identity requires a separately reviewed Senku permission change.
6. Require the Docker-enabled CI lifecycle test plus AMD64/ARM64 workspace builds before merging Phase 2B. Test on real Raspberry Pi hardware before any Pi rollout.
7. Keep production Queue polling, recovery supervision, callback replay, and Senku inference disabled until transport, recovery, DLQ, credential, and worker controls are separately reviewed.

The acceptance status in this worktree is implementation-present and awaiting review/merge. Do not call either phase merged or production-ready before that review.
