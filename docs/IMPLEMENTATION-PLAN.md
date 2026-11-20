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

Before enabling the adapter operationally, verify actual Soyuz producer content type and Queue message body encoding in a controlled non-production Queue. Establish and document the Cloudflare dead-letter queue policy and operator procedures for blocked callback rows. Queue transport metrics and coordinator metrics are available from their components; the diagnostic HTTP API currently exposes local outbox and slot gauges, while transport status remains unprobed.

## Phase 2A-S — Senku advisory decision foundation

Create a separate `fossil-senku` crate and typed client for Kaseki-owned profiles: `task_admission_v1`, `kaseki_context_routing_v1`, `validation_recovery_v1`, `kaseki_goal_check_v1`, and `kaseki_run_evaluation_v1`. Add fixtures, response validation, bounded timeouts, stage-specific degradation behavior and mock tests. Results remain advisory. Deterministic security validation and the Soyuz claim/start protocol cannot be overridden. Do not add JEV or call OpenRouter directly; Senku availability is not required to receive a valid Soyuz authorization.

## Phase 2B — Disposable worker execution

Add a separate, narrowly scoped worker supervisor and non-root worker image. The supervisor must re-load the durable descriptor and require exact persisted start proof plus a fresh canonical gate before launch. Enforce one slot, CPU/memory/PID/disk/network/time limits, bounded outputs, cancellation, cleanup, graceful shutdown and explicit crash recovery. Do not auto-launch after ambiguous restart state. Keep Docker socket access out of the controller container where an external supervisor boundary is feasible. Start with a mock executor and prove no launch without the exact run, worker and claim fence.

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
4. Land 2A-S only after documenting that advisory output cannot authorize work.
5. Land 2B only after proving the proof-gated mock supervisor and restart/cancellation boundaries.
6. Keep production Queue polling disabled until transport, recovery, and worker controls are separately reviewed.
