# Implementation plan

This roadmap preserves Soyuz as the sole authority for admitting and authorizing runs. Each phase should land as a focused change with mock-driven tests and no production deployment by default.

## Phase 0 — Contract and architecture baseline

Completed in this foundation: current Soyuz contract and requirement IDs recorded; Kaseki API operation index and linked OpenAPI document inspected; behavioral compatibility inventory written; trust boundary and architecture decisions documented.

## Phase 1 — Controller foundation

Implemented here: Rust workspace, typed config and redacted secrets, read-only Axum API, Prometheus metrics, local SQLite migration/state store, contract-v1 envelope and Cloudflare pull/ack models, typed worker client, durable start authorization proof, callback outbox model, fixtures, CI, and controller packaging. No production Queue pull or claim and no coding execution.

## Phase 2A — Soyuz intake and authorization

Implement the Cloudflare HTTP pull adapter with a separate Queue token and configured Soyuz account/Queue identifiers. Before production use, exercise it against a local mock and a controlled non-production Queue. Validate each message, read canonical Soyuz state, persist claim intent, claim with a stable per-attempt fence, reconcile uncertain responses after restart, post `/started`, persist authorization, and acknowledge only after durable acceptance. Duplicate, stale, cancelled, unsupported-version, timeout, and callback replay cases must be covered. Do not launch real Docker workers in this phase.

## Phase 2B — Disposable worker execution

Implement Docker Engine integration behind the proof-gated launch boundary and enforce one active slot per host. Add cancellation, timeout, graceful shutdown, resource limits, cleanup, worker health, and controlled mock-executor integration before using a coding runtime. Keep Docker privileges out of the controller container where an external supervisor boundary is feasible.

## Phase 2C — Coding-agent runtime

Introduce the Pi CLI as a worker backend. Implement repository preparation, gateway configuration, allowlisted environment and secrets, and isolation controls. Test with disposable repositories and non-production credentials.

## Phase 3 — Verification and publication

Add deterministic validation, quality gates, bounded artifacts, diff/result collection, and GitHub publication. Treat model output, repository files, logs, and validation output as untrusted. Keep publication credentials and repository content out of Soyuz's bounded event metadata.

## Phase 4 — Full Soyuz and Senku integration

Complete production callback delivery and outbox recovery, artifact upload limits, cancellation reconciliation, advisory Senku decisions, retry and recovery rules, operational metrics, and provider-specific behavior. Preserve Soyuz as run-control authority; it remains separate from the LLM gateway.

## Phase 5 — Conformance and migration

Run Rusty-Fossil against the language-independent fixtures, compare observed behavior with the Kaseki behavioral inventory, benchmark Linux ARM64 and Raspberry Pi resource use, and plan a controlled rollout on spare hardware. Any change to the Soyuz contract or other repositories must be separately reviewed.

## Recommended next PR

The next focused PR should implement Phase 2A's Queue HTTP adapter and canonical authorization flow against mocks only. It should use the existing SQLite intent/fence/proof interfaces and exercise uncertain pull, claim, started, acknowledgement, restart, duplicate delivery, and cancellation recovery. Keep the production consumer disabled until the controlled mock path and safety checks are complete; do not add a standalone execution mode or feature flag that bypasses Soyuz.
