# Soyuz and Cloudflare contracts

## Sources

The implementation follows Soyuz's contract-v1 schemas and worker API. The working references used for this phase are [Soyuz transport contract](https://github.com/CyanAutomation/soyuz/blob/main/docs/CONTRACT.md), [Kaseki integration policy](https://github.com/CyanAutomation/soyuz/blob/main/docs/KASEKI-INTEGRATION.md), [Soyuz test requirements](https://github.com/CyanAutomation/soyuz/blob/main/docs/TEST-REQUIREMENTS.md), and the current source schemas in [`run-request.ts`](https://github.com/CyanAutomation/soyuz/blob/main/src/contracts/run-request.ts) and [`worker-callbacks.ts`](https://github.com/CyanAutomation/soyuz/blob/main/src/contracts/worker-callbacks.ts). The tests use independent JSON fixtures under `crates/fossil-core/tests/fixtures/`.

Cloudflare behavior is based on the official [Queues pull-consumer guide](https://developers.cloudflare.com/queues/configuration/pull-consumers/) and [Queues API](https://developers.cloudflare.com/api/resources/queues/). The API adapter posts to `/client/v4/accounts/{account_id}/queues/{queue_id}/messages/pull` and `/messages/ack`. A dedicated token needs Queue read and write access. Cloudflare Queue delivery is at least once.

## Identity and delivery

| Value | Meaning | Rust representation |
| --- | --- | --- |
| Soyuz `runId` | Canonical UUIDv7 run identity and deduplication key. | `RunId` |
| Soyuz `displayId` | Human-facing run label. | `DisplayId` |
| Queue `id` | Ephemeral message identity. | `QueueMessageId` |
| Queue `lease_id` | One pull lease used only for ack or retry. | `QueueLeaseId` |
| Soyuz `workerId` | Stable host identity. | `WorkerId` |
| Claim `callbackId` | Per-attempt claim fence. | `ClaimCallbackId` |
| Callback/event ID | Stable operation idempotency key across retries. | `EventId` |

Worker IDs intentionally remain 1–64 ASCII letters, digits, hyphen or underscore. Periods, colons, whitespace, Unicode and empty values fail validation. See [ADR 0004](DECISIONS/0004-worker-id-local-restriction.md).

Cloudflare pull requests use `batch_size: 1` so the host does not build an in-memory queue, plus a configured visibility timeout. The pull parser accepts no more than one result message. A JSON-content Queue message's body is base64 decoded before contract parsing. Acknowledgement and retry requests refer only to `lease_id`; one lease operation is accepted only when Cloudflare confirms `ackCount == 1` or `retryCount == 1`. The adapter uses bounded response reads and does not log credentials or raw bodies.

## Queue contract v1

The envelope contains `contractVersion: "1"`, canonical UUIDv7 `runId`, RFC 3339 `createdAt`, UUID correlation/request IDs, and a normalized `request`. Unknown contract versions, duplicate fields, unknown fields, malformed base64, or a body above 96 KiB fail closed.

The request model preserves the supported Soyuz run options:

| Option | Validation |
| --- | --- |
| `repoUrl` | HTTPS absolute URL, host required, no embedded credentials, at most 2,048 UTF-16 code units. |
| `ref` | Defaults to `main`; 1–512 UTF-16 code units. |
| `projectName` | Optional, 1–200 code units. |
| `taskPrompt` | Optional only when `startupCheck: true`; otherwise required, 10–32,000 code units. |
| `changedFilesAllowlist` and `allowlist.include` | At most 500 entries, each 1–512 code units. |
| `maxDiffBytes` | 1–100,000,000. |
| `validationCommands`, nested validation commands, auto-lint commands | At most 50 commands, each 1–2,048 code units. |
| `goalSetting`, `scoutingConfig`, `goalCheck`, `runEvaluation` | Preserve enabled/model/timeout options; models 1–200 code units and timeouts 60–10,800 seconds. `goalCheck.maxRetries` is 0–5. |
| `taskMode` | `patch` or `inspect`; defaults to `patch`. |
| `publishMode` | `auto`, `none`, `branch`, or `pr`; defaults to `pr`. |
| `startupCheckMode` | `boot` or `baseline-validation`. |
| `tracing`, `idempotencyKey`, `timeoutSeconds` | Preserve UUID tracing IDs and UUID idempotency key; timeout 60–10,800 seconds. |

Fields are typed and `deny_unknown_fields` is applied recursively. In particular, unsupported execution/security controls such as a future webhook configuration are rejected, not silently dropped. The normalized descriptor persisted to SQLite preserves all listed fields and the envelope's run, time and correlation identities. It does not persist Cloudflare transport IDs.

### Default publication mode policy

Every normal run defaults to `publishMode: "pr"` in Soyuz. Rusty-Fossil mirrors that default when parsing a legacy or incomplete normalized queue request and otherwise preserves Soyuz's explicit value; it does not choose a mode or publish to GitHub itself. `none`, `branch`, and `auto` are rare, explicit exceptions for narrowly scoped tests, not routine run settings. See Kirisame's [cross-project publish mode policy](https://github.com/CyanAutomation/kirisame/blob/0ffdd11cdea1e63f7c7218f21969dfdb177a31c8/docs/PUBLISH-MODE-POLICY.md).

## Soyuz worker API

Worker requests use `Authorization: Bearer <SOYUZ_WORKER_API_TOKEN>`, separate from client submission and Queue credentials. Success uses `{ "data": ..., "requestId": "..." }`; errors use `{ "error": { "code", "message", "requestId" } }`. Bodies are bounded, and error formatting never exposes response bodies.

| Operation | Phase 2A behavior |
| --- | --- |
| `GET /v1/worker/runs/:runId` | Read canonical status, cancellation, worker, current claim fence and health. Required before authorization and at the final handoff gate. |
| `POST /claim` | Compare-and-set claim with stable callback ID, worker ID and optional lease (30–300 seconds). A successful response yields a private-field proof. Uncertain responses reconcile with canonical run ownership/fence. |
| `POST /started` | Sent only after durable claim proof and stable start callback ID. Successful response or exact canonical `running` state can yield start evidence. Cancellation cannot reconcile as authorization. |
| `POST /events` | Bounded operational callback with stable event ID, worker and current claim fence. Event types use contract spellings such as `stage.changed`, `progress.updated`, and `worker.heartbeat`. |
| `POST /completed`, `/failed`, `/cancelled` | Full bounded request models, including mandatory failure class/message, completion metadata and optional outcome summary. Durable outbox replay sends the exact stored logical payload and ID. |

The client does not create a new callback ID on retry. Only retryable network, timeout, rate-limit and service-unavailable outcomes are retried automatically by outbox replay. A stale claim, conflict, authentication rejection, missing run, invalid request or protocol mismatch becomes blocked for operator review. A callback is marked delivered only after Soyuz returns success.

## Persistence, retry and cancellation

- Claim intent and ID commit before `/claim`; the accepted/reconciled proof is required before `/started`.
- Start callback ID commits before `/started`; accepted/reconciled proof and the complete request descriptor are required before reserving the local slot.
- The final gate requires canonical `running`, matching run/worker/claim, and no `cancelRequestedAt`.
- Queue ack follows durable handoff reservation. A failed ack redelivers, and the durable run record prevents another authorization/handoff attempt.
- Queue and local recovery are at-least-once. Rusty-Fossil does not claim exactly-once execution across independent systems.
- Callback attempt count and a capped exponential retry deadline with stable per-event jitter are persisted before network I/O. Pending plus blocked outbox entries are bounded at 1,000. SQLite reserves the final row transactionally: optional event and heartbeat callbacks return `SuppressedCapacity` at 999 rows, while a terminal result for the run owning the reserved execution slot can insert into row 1,000. At the hard limit, or without the matching slot, terminal persistence fails closed and retains the execution fence. Duplicate event IDs remain idempotent. No blocked row is deleted to recover capacity. See [ADR 0011](DECISIONS/0011-terminal-callback-capacity-reservation.md).
- Terminal local state, terminal callback intent and slot release can be committed together. A blocked callback remains visible in recovery diagnostics and prevents descriptor cleanup.
- A queued cancellation is already terminal in Soyuz and is skipped. A claimed run cancelled before launch cannot cross the execution gate. During future execution, cancellation must stop and clean up the worker before `/cancelled` is acknowledged.

## Configuration boundary

`SOYUZ_API_ORIGIN`, `SOYUZ_WORKER_ID`, and either `SOYUZ_WORKER_API_TOKEN_FILE` or `SOYUZ_WORKER_API_TOKEN` configure the Soyuz worker client. `CLOUDFLARE_ACCOUNT_ID`, `CLOUDFLARE_QUEUE_ID`, either `CLOUDFLARE_QUEUE_TOKEN_FILE` or `CLOUDFLARE_QUEUE_TOKEN`, and `CLOUDFLARE_QUEUE_VISIBILITY_TIMEOUT_MS` configure Queue access. The default visibility timeout is 60 seconds and accepted range is 1 second to 12 hours. `RUSTY_FOSSIL_POLL_INTERVAL_SECONDS` and `RUSTY_FOSSIL_REQUEST_TIMEOUT_SECONDS` configure transport timing. Secret files are preferred for deployment; Queue and Soyuz credentials are separate and redacted from debug output.

## Readiness and operational limitations

The diagnostics API is read-only. `/ready` reports state-store health, configuration validity, Queue transport health (unprobed unless an explicit probe is supplied), recovery backlog, handoff readiness, optional Docker-supervisor availability, synthetic execution availability, production coding readiness, and runtime limitations. `dockerSupervisorAvailable: null` means this process has not probed the private supervisor IPC endpoint; it does not claim the daemon is healthy. Readiness checks do not pull Queue messages, create claims or invoke inference. Phase 2B adds a separate synthetic-only Docker supervisor, but production `main` does not construct the coordinator or poller, so complete configuration does not enable production intake. Readiness remains false for missing production runtime wiring and real coding execution. `/metrics` exports bounded gauges for supervisor probe state, synthetic availability and production coding readiness.

Queue, coordinator and outbox counters are low-cardinality. They do not label runs, message bodies, prompts, repository names, callback text, secrets, or leases. `fossil-supervisord` emits structured fixed-cardinality counters for heartbeat suppression/errors, terminal persistence retries/failures, restart reconciliation/reattachment, ambiguous state, duplicate launch prevention, cleanup retries and recovery backlog. `SupervisorIpcClient` exposes request/failure counts from its component snapshot. These process-level counters are not exported through controller `/metrics` while the production binary keeps the coordinator and IPC client unwired. The service exposes only `/health`, `/ready` and `/metrics`; there is no submission or task-starting endpoint.

## Senku boundary for Phase 2A-S

The `fossil-senku` crate implements the Senku REST protocol for the five Kaseki-owned profiles below. It sends one request per evaluation to `POST /v1/profiles/{profileId}/evaluate`, with `{ "schemaVersion": "1", "input": { ... } }`, and disables redirects. Requests have a 128 KiB total cap; response streams are stopped at 64 KiB. No automatic retry, provider failover, JEV client, or direct OpenRouter call is used.

| Profile ID | Typed input | Contract-specific validation |
| --- | --- | --- |
| `task_admission_v1` | `TaskAdmissionInput` | HTTPS repository URL and ref, bounded task instructions/options, profile answer IDs and labels. |
| `kaseki_context_routing_v1` | `ContextRoutingInput` | Supported routing mode, bounded sanitized evidence, returned routing identities constrained to submitted context. No raw source, shell command, or tool output is intentionally supplied. |
| `validation_recovery_v1` | `ValidationRecoveryInput` | Bounded command metadata and at most 8 KiB redacted output; redaction occurs before taking the trailing excerpt. Missing advice never authorizes a retry. |
| `kaseki_goal_check_v1` | `GoalCheckInput` | Unique submitted criterion IDs; returned answers must exactly match this request's criteria. |
| `kaseki_run_evaluation_v1` | `RunEvaluationInput` | Bounded run-state evidence and profile-specific score/answer validation. |

The client validates schema version, request ID, completed status, exact profile ID and version, provider identity, exact answer identities and labels/scales, probability keys and normalization, nullable confidence, and the canonical `result` mirror. Unknown, missing, malformed, or oversized responses fail as typed errors. A null confidence remains null. Dynamic context/goal answers are matched to the request rather than accepted by naming pattern alone.

Evidence is recursively bounded to depth 8 and 64 list entries. Profile evidence caps are 32 KiB for task admission/context, 8 KiB for validation recovery, 48 KiB for goal check, and 96 KiB for run evaluation, with a 128 KiB serialized envelope cap. Sensitive-key values, bearer/JWT/provider/GitHub tokens, AWS credentials, URL passwords, and private-key blocks are redacted. Validation output is fully redacted before taking its tail. Raw prompts, evidence, provider responses, tokens, and rationale must not appear in logs or metrics.

`task_admission_v1` is available behind an optional coordinator `DecisionService` seam. Its output is discarded as an authorization input; deterministic contract checks and Soyuz's canonical claim/start/fence protocol remain mandatory. Other profile calls are typed client capabilities only. Missing or failed advice is a degraded advisory result: context is retained, validation failure remains a failure, goal-check uncertainty is not success, and run-evaluation uncertainty does not fabricate a score. Senku availability is reported separately and does not make Soyuz readiness fail. The service binary does not instantiate the client or perform paid inference.

Senku's current permission matrix allows these profiles to the Kaseki machine identity, not a Soyuz identity. Rusty-Fossil needs a separately provisioned authorized Senku bearer token; never reuse `SOYUZ_WORKER_API_TOKEN`. A dedicated Rusty-Fossil machine identity requires a separately reviewed Senku permission change and is not implemented here. Configure `SENKU_API_ORIGIN` and either `SENKU_API_TOKEN_FILE` or `SENKU_API_TOKEN`. Production origin must be HTTPS. HTTP is accepted only for loopback in development/tests; origins cannot contain credentials, paths, queries, or fragments. The Compose overlay in `docker-compose.senku.yml` demonstrates a mounted secret but does not enable inference in `main`.

The Senku client exposes per-profile attempts, successes, invalid responses, transport failures, degraded counts, and bounded-duration totals through `SenkuClient::metrics()`. These snapshots are not connected to the diagnostic HTTP endpoint until the client is intentionally wired into a non-production runtime.

### Controlled Senku contract-test procedure

Default Rust tests use synthetic fixtures and never call a live Senku or provider. To compare against a Senku checkout, run its local worker with a mocked decision provider and a synthetic Kaseki-authorized test credential; point only a local Rust integration test at its loopback origin. Exercise each profile's request fixture, response fixture, error envelope, and size boundary. Do not point the test harness at the deployment origin, inject production task evidence, add the cross-repository harness to required offline CI, or enable this client in `main` as part of the comparison.

## Queue acknowledgement, retry and dead-letter operations

| Situation | Adapter behavior | Operator interpretation |
| --- | --- | --- |
| Durable authorization/handoff is committed | Ack the exact lease and require `ackCount == 1`. | An ack timeout is uncertain; redelivery is deduplicated by Soyuz run ID and SQLite, never by lease ID. |
| Coordinator requests retry | Retry the exact lease with bounded exponential delay and jitter (capped at 375 seconds); require `retryCount == 1`. | A failed retry request is logged/counted; lease expiry can redeliver, so do not resubmit a run. |
| Pull/ack/retry receives a recoverable network, timeout, rate-limit, or service-unavailable error | Bounded backoff; polling loop continues. | Check Queue transport metrics and Cloudflare status. |
| Permanent authorization/configuration/protocol error | Polling suspends and increments a suspension counter; structured logs record the safe class. | Fix token scopes/configuration or contract mismatch, then restart the adapter under controlled rollout. |
| Invalid or poison message | No Soyuz claim; bounded retry lets configured Cloudflare policy move it to DLQ. | Inspect sanitized failure category and source run; do not replay unchanged poison input. |

Production Cloudflare Queue configuration is deliberately not modified here. Before enabling polling, set a finite retry limit (recommended initial maximum: 5 deliveries), configure a dedicated DLQ, and retain source and DLQ messages for at least 7 days or the incident-response window, whichever is longer. Alert on DLQ arrivals and failed ack/retry operations. Investigate poison messages against the canonical Soyuz run and contract version. Replay only after correcting the cause, using the original canonical `runId` and a controlled Queue redrive; never create a replacement run to bypass a stuck message. DLQ retention and actual redrive behavior must be verified in a non-production account first.

For a blocked callback, inspect the redacted failure class, pending age and stable callback/event ID, then verify current Soyuz state and worker fence. Do not delete a blocked row to clear capacity. Reconcile or resolve it through an operator-approved store procedure that preserves the callback's canonical identity; otherwise keep intake backpressured.

## Phase 2B Docker supervisor

The separate `fossil-supervisord` accepts an authenticated run ID over a private Unix socket. It reloads the descriptor, worker identity, claim fence, start authorization and reserved slot from the shared SQLite journal, then rereads canonical Soyuz state before creating a container and immediately before starting it. Queue acknowledgements and IPC requests are not launch authorization. The supervisor owns the Docker socket; the controller image does not receive it.

Only the fixed synthetic worker image is accepted. The container runs as UID 65532 with a read-only root filesystem, all Linux capabilities dropped, `no-new-privileges`, no network, no host namespaces, no devices or mounts, capped tmpfs and JSON logs, bounded CPU/memory/PID/runtime/output, and no restart policy. Startup refuses a non-Linux Docker Engine or a daemon that does not report memory, CPU quota, PID and seccomp support. Each created container is inspected against the expected image and full effective profile before start and during reconciliation.

Launch intent and deterministic name are committed before Docker create. The create and start request states fence uncertain responses; recovery inspects only that same name and labels and never retries an ambiguous create. Confirmed terminal state, typed result, stable callback ID and callback payload are persisted through the existing outbox before cleanup. The slot is released only after exact-container cleanup is confirmed. The Docker suite covers actual Engine lifecycle; ignored process integrations exercise coordinator authorization, duplicate delivery, create/start crash boundaries, terminal/cleanup recovery, callback replay and SIGTERM. They do not claim distributed exactly-once execution, and lost API responses with a continuing supervisor or the full malformed-identity matrix remain follow-up tests. See the [Phase 2B operations guide](PHASE-2B-OPERATIONS.md), [ADR 0011](DECISIONS/0011-terminal-callback-capacity-reservation.md), [ADR 0012](DECISIONS/0012-phase-2c-long-running-execution-policy.md), and ADRs 0007–0010 for setup, failure handling and limitations.

## Phase 2C-A Pi worker foundation (not a supervisor workload)

The Pi 1.6.0 wrapper accepts a bounded UTF-8 prompt over stdin, passes it to a fixed Pi JSON-mode command without shell interpolation or prompt arguments, and restricts its child environment. Its Rust event classifier accepts bounded JSONL and returns only completion class and event/tool counts. The package lock includes dependency integrity values, and CI builds a separate non-root AMD64/ARM64 image with SBOM/provenance attestations. The current supervisor does not admit or launch this image; no task descriptor, workspace, gateway credentials, event summary or coding result reaches it. Follow [Phase 2C-A operations](PHASE-2C-A-OPERATIONS.md) for implementation and validation status.
