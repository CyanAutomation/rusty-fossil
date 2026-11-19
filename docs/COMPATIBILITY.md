# Compatibility inventory

## Scope

Rusty-Fossil is a new Rust implementation, not a source-level port. The Kaseki test suites were inspected as behavioural evidence only: `src/integrations/soyuz/SoyuzAdapter.test.ts`, `src/job-persistence-manager.test.ts`, `src/job-scheduler.test.ts`, and `src/kaseki-api-routes.test.ts`. Their Jest/TypeScript mocks and scheduler internals are not run against Rust. Contract-v1 behavior is recast as Rust tests and language-independent JSON fixtures.

The published [Kaseki operation index](https://kaseki-tunnel.scheimann.xyz/docs) links to an OpenAPI 3.1 document. It currently advertises 33 operations across 32 paths. Several are relevant as legacy capabilities but intentionally absent from Rusty-Fossil, including `POST /api/v1/runs`, `POST /api/v1/runs/local`, run retry/cancel routes, worker diagnostics, shell/log reads, artifact downloads, webhook testing, and gateway probes.

## Inventory

| Behavior or capability | Owner | Rusty-Fossil treatment |
| --- | --- | --- |
| Authenticate client submission, allocate canonical run ID/display ID, persist run, publish Queue message, own canonical lifecycle and cancellation intent. | Soyuz | Never implemented here. The Rust service has no client submission token or submission route. |
| Queue `contractVersion: "1"`, canonical UUIDv7 `runId`, normalized request, correlation and request identifiers. | Soyuz emits; controller validates. | Contract model and fixtures validate the envelope. Unknown versions, invalid IDs, oversized payloads, bad timestamps, missing prompt, unsafe repository schemes, and invalid refs fail closed. |
| At-least-once Queue delivery, duplicate run handling, explicit lease acknowledgement/retry. | Cloudflare Queue and future controller. | Core pull/ack models and recorded base64 JSON fixture exist. No live HTTP pull, lease, acknowledgement, or retry is performed in Phase 1. |
| Read canonical worker run state before start and while active. | Controller reads; Soyuz owns state. | Typed worker read and cancellation projection exist with mock tests. No periodic production polling runs. |
| Durable claim intent and one-host claim fencing. | Controller and Soyuz. | SQLite persists claim callback ID before authorization; duplicate same IDs are idempotent and a different fence is rejected. Soyuz API client models claim; no claim is sent from this prototype. |
| Only start after successful Soyuz `/started`; reconcile an uncertain response against canonical state. | Controller and Soyuz. | Client returns an unforgeable start proof after a successful callback or matching canonical read. Storage requires that proof to persist the start authorization. No worker launch exists. |
| Heartbeat, operational events, completion, failure, and cancellation callbacks. | Controller reports; Soyuz records. | Typed bounded request models and mock tests exist. A typed durable outbox is present, but no production callbacks are delivered. |
| Host readiness, repository destination safety, task admission, execution timeout and one active worker slot. | Future Rust controller. | Only configuration validation and readiness foundations exist. No repository access, shell, scheduler, or task admission runs. |
| Docker creation, worker isolation, Pi CLI, validation, Git operations, publication, artifacts. | Future disposable worker. | Deferred. No Docker socket access, Docker privilege, repository checkout, coding agent, or live execution. |
| Direct/local run submission, standalone retry, automatic fallback, Kaseki REST API, dashboard, webhook, gateway diagnostics. | Legacy Kaseki API/CLI. | Explicitly excluded. The Soyuz/Kaseki guide describes optional Soyuz routing and an explicitly selected local route; the attached Rusty-Fossil requirement supersedes that policy. Soyuz remains mandatory here and outage never enables local execution. |

## Behavioral evidence retained

The original Soyuz adapter tests demonstrate observable recovery cases that should guide Phase 2A: uncertain Queue acknowledgements must redeliver without a second local run; an accepted claim with a lost response must be reconciled after restart; claim must precede `/started`; a start that cannot be authorized must not execute; two hosts or controller instances cannot both claim one run; stale callback claims are fenced; and a pre-start cancellation must not open the execution gate. The persistence tests also cover restart recovery, terminal callback outbox durability, stale claim intent, and malformed state handling.

The Rust suite covers only Phase 1 portions of those behaviors. It does not claim Kaseki runtime parity.

## Contract requirement mapping

| Soyuz requirement IDs | Rust evidence | Coverage status |
| --- | --- | --- |
| `QUEUE-MESSAGE-01` | `fossil-core/tests/contracts.rs`, recorded v1/v2 and Cloudflare pull fixtures. | Envelope validation and unsupported versions tested; live pull deferred. |
| `RUN-REPO-HTTPS-01` | `malformed_or_non_https_queue_messages_are_not_validated_work`. | HTTPS boundary tested. |
| `AUTH-REST-01`, `WORKER-READ-01` | `fossil-soyuz/tests/client.rs`. | Worker token, typed read, status and cancellation fields mock tested. |
| `WORKER-CLAIM-01`, `WORKER-CLAIM-ID-01`, `WORKER-CLAIM-FENCE-01` | `fossil-soyuz/tests/client.rs`, `fossil-storage/tests/store.rs`. | Request contract and local immutable fence tested; real concurrent Soyuz claim race deferred. |
| `WORKER-START-01`, `WORKER-START-02`, `WORKER-CLAIM-RECOVERY-01` | Start timeout/proof tests plus storage authorization gate/restart tests. | Successful callback and same-claim reconciliation tested with mocks; live lease expiry and Queue republish deferred. |
| `WORKER-EVENT-01`, `CALLBACK-IDEMPOTENCY-01` | Stable event retry test and SQLite callback outbox test. | Same ID/payload replay tested locally; Soyuz's live duplicate behavior remains an integration test. |
| `WORKER-COMPLETE-01`, `WORKER-FAIL-01`, `WORKER-CANCEL-01` | Typed completion/failure/cancel request models. | Wire models exist; terminal responses and execution cleanup are deferred. |
| `KASEKI-CANCEL-POLL-01`, `CANCEL-QUEUED-01`, `CANCEL-RUNNING-01` | Worker read test validates cancellation state and local lifecycle cancellation transition. | Model/state behavior only; no active worker to cancel. |
| `HEALTH-DB-01` | Service API tests and SQLite store open/reopen tests. | Local liveness, readiness, metrics and durable store tested; Soyuz production health is not probed. |

The canonical ID references are kept in Rust test comments and the test names. Full execution-path, network outage, live Queue, real Soyuz, Docker, artifact and cancellation cleanup tests are deferred until their corresponding phase exists. They must not be marked passing before then.

## Remaining evidence and assumptions

- Cloudflare documents HTTP pull JSON messages as base64-encoded. The fixture uses that documented form and assumes Soyuz continues publishing with JSON content type. Phase 2A must verify the actual producer content type against a non-production Queue before enabling pull.
- Contract v1 makes `claimCallbackId` optional for compatibility with older hosts. Rusty-Fossil's start proof requires it so a Rust worker cannot launch without per-attempt fencing.
- Soyuz's current Kaseki guide describes its optional integration and standalone fallback. That is a Kaseki deployment choice, not a Rusty-Fossil requirement; the new Rust service follows the stricter Soyuz-only rule in the implementation brief.
- The published Kaseki page is an operation index, not behavior evidence for every endpoint. The OpenAPI schema was confirmed to exist and was enumerated, but unrelated Kaseki operations remain out of scope.
