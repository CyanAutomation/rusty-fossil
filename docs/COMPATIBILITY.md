# Compatibility inventory

## Scope

Rusty-Fossil is a clean-room Rust controller, not a source-level port of Kaseki-Agent. Kaseki test suites were inspected as behavioral evidence: `src/integrations/soyuz/SoyuzAdapter.test.ts`, `src/job-persistence-manager.test.ts`, `src/job-scheduler.test.ts`, and `src/kaseki-api-routes.test.ts`. Their TypeScript internals are not run against Rust. Contract-v1 behavior is represented by Rust types, independent JSON fixtures, SQLite tests and local HTTP mocks.

The [Kaseki operation index](https://kaseki-tunnel.scheimann.xyz/docs) links to an OpenAPI 3.1 document and advertises client submission, run status/retry/cancellation, worker diagnostics, logs, artifacts, webhooks, gateway diagnostics, and callback operations. Rusty-Fossil does not proxy or implement those Kaseki routes. In particular, this service has no `POST /api/v1/runs`, `POST /api/v1/runs/local`, or independent execution route.

## Ownership and behavior inventory

| Behavior or capability | Owner | Rusty-Fossil treatment |
| --- | --- | --- |
| Authenticate client submission, allocate canonical run/display IDs, persist and publish the run, own lifecycle and cancellation intent. | Soyuz | Never implemented here. Rusty-Fossil has no client submission token or submission endpoint. |
| Queue contract-v1 message, normalized request, correlation/request IDs. | Soyuz emits; controller validates. | Full currently supported normalized request is typed and preserved in SQLite. Unknown fields or modes fail closed. |
| At-least-once Queue pull, lease ack/retry and dead-letter policy. | Cloudflare Queue and controller adapter. | HTTP pull/ack/retry client is implemented and mock-tested. Coordinator limits pulls to one message. Failed ack/retry is counted and safely logged; permanent errors suspend polling. Production polling and live DLQ policy remain disabled. The documented rollout policy requires a finite retry limit and DLQ. |
| Read canonical state, claim with worker identity and fence, authorize `/started`. | Soyuz is authority; Rusty-Fossil coordinates. | Implemented coordinator and proof-gated store path with mocks. Claim/start IDs persist before network calls; uncertain outcomes reconcile. No executor follows the handoff. |
| Request recovery after controller restart. | Rusty-Fossil local journal; Soyuz canonical state. | `RecoveryManager` can reconcile pending records independently of Queue delivery and reuses persisted callback IDs. Full validated descriptor, claims, proofs, slot and callback outbox persist in SQLite. Corrupt state fails closed. Ambiguous active execution is not relaunched. The production service does not activate the recovery loop. |
| Worker events, heartbeat, completion, failure and cancellation callbacks. | Rusty-Fossil reports; Soyuz records. | Typed bounded requests and durable exact-payload outbox replay are implemented; tests use mock Soyuz. The production binary does not start a callback delivery loop. |
| One active local handoff and recovery-aware Queue intake. | Rusty-Fossil host. | Unique SQLite slot, recovery-aware intake capacity, and 999-of-1,000 outbox high-water mark with one mandatory callback reservation are implemented. Callback replay does not require the execution slot. The intake/recovery loops are not constructed by the production service. |
| Readiness and diagnostics. | Rusty-Fossil. | Only `/health`, `/ready`, and `/metrics`. Readiness remains false because production intake is disabled and no coding runtime exists. |
| Docker isolation, repository checkout, shell/coding agent, validation and publication. | Future Phase 2B/2C/3 worker. | Deferred. No Docker socket, repository checkout, command execution, Pi CLI, Git operation or publication capability was added. |
| Senku decisions. | Senku; Rusty-Fossil prepares evidence and consumes advice. | `fossil-senku` supports all five Kaseki-owned profiles, bounded/redacted inputs, authenticated one-shot transport and strict response validation. `task_admission_v1` has a mockable coordinator seam; its output cannot authorize work. The production binary does not instantiate the Senku client. No JEV dependency or direct OpenRouter call. |
| Standalone/local submission, automatic fallback, Kaseki API/dashboard, webhook testing, gateway diagnostics. | Legacy Kaseki interface. | Explicitly excluded. Soyuz outage never enables local fallback. |

## Evidence and coverage map

Requirement IDs refer to current Soyuz `docs/TEST-REQUIREMENTS.md`. Tests are local and deterministic. They do not demonstrate production Queue, live Soyuz, Docker, or external service behavior.

| Requirement | Rust evidence | Coverage |
| --- | --- | --- |
| `QUEUE-MESSAGE-01`, `RUN-VALIDATE-01`, `RUN-REPO-HTTPS-01` | `fossil-core/tests/contracts.rs`, queue fixtures. | Contract version, full request preservation, unsupported fields, malformed URL, bounds and base64 fixtures. |
| Cloudflare pull/ack contract | `fossil-queue` unit tests with wiremock. | One-message pull, base64 JSON body, lease-specific ack/retry, response classification, transport failure and counters. |
| Phase 2A.1 recovery and intake safety | `rusty-fossil` coordinator tests and `fossil-storage/tests/store.rs`. | Temporary Soyuz outage recovered without Queue redelivery; canonical other-worker ownership releases local slot without callback; outbox high-water reserves terminal callback capacity. Mock-server tests compile but require local socket binding and cannot execute in the current sandbox. |
| Phase 2A-S profile contracts | `fossil-senku/tests/contracts.rs`. | Five profile IDs, versioned request body, profile-specific validation, result mirror, dynamic answer checks, probability/confidence validation, evidence redaction/bounds and config validation. Authenticated HTTP mock test requires local socket binding; no production Senku or provider is called. |
| `AUTH-REST-01`, `WORKER-READ-01` | `fossil-soyuz/tests/client.rs`. | Worker bearer token, canonical reads, status/cancellation, bounded response and error classes. |
| `WORKER-CLAIM-01`, `WORKER-CLAIM-ID-01`, `WORKER-CLAIM-FENCE-01` | Soyuz client and storage tests. | Claim wire request, durable stable callback ID, proof-only claim authorization, and mismatch rejection. No real multi-host race. |
| `WORKER-START-01`, `WORKER-START-02`, `WORKER-CLAIM-RECOVERY-01` | Coordinator and storage mock tests. | Claim before start, stable start ID, success and canonical reconciliation, fresh exact-fence gate, cancellation rejection, durable slot and duplicate redelivery. |
| `CALLBACK-IDEMPOTENCY-01`, `WORKER-EVENT-01`, `WORKER-FAIL-01`, `WORKER-CANCEL-01` | Storage and coordinator callback replay tests; Soyuz client callback tests. | Full payload persistence, exact ID/fence replay, retry deadline, success marking, and stale-claim blocking. Live duplicate semantics are an integration dependency. |
| `HEALTH-DB-01` | `rusty-fossil/tests/service.rs`. | Read-only health/readiness/metrics output and local state health. No production Queue probe is performed. |
| Worker ID policy | `fossil-core/tests/contracts.rs`. | Empty, 65-character, Unicode, period and colon values rejected; 64-character valid identifier accepted. |

## Preserved behaviors and explicit limits

- Soyuz's canonical UUID is the only deduplication and run-identity key. Display IDs and Queue leases never authorize work.
- The normalized `publishMode` is preserved; omitted values resolve to `pr`. Explicit non-`pr` modes are test-only exceptions under the shared publication policy. This controller does not publish to GitHub.
- Queue data alone cannot authorize a claim/start; canonical state and private-field proofs are required.
- A failed or uncertain Soyuz request does not enable direct local execution.
- Unsupported normalized run settings fail closed rather than being discarded.
- The Phase 2A coordinator prepares a durable authorized handoff, but no process or Docker worker is launched.
- Production polling, recovery supervision, callback replay and Senku inference are not started from the service binary. This keeps this implementation from consuming production messages or invoking paid inference.
- Multi-host local SQLite coordination, claim renewal, real DLQ behavior, live Queue producer content type, Docker restart, and execution cleanup remain unverified or deferred.

## Follow-on evidence

Phase 2B needs a proof-gated supervisor mock, isolation/resource-limit tests, cancellation and cleanup tests, and explicit crash-boundary tests before any real worker image is enabled. Before runtime rollout, separately review the Kaseki-owned Senku credential and live Queue/DLQ configuration.
