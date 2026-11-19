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
- Callback attempt count and a capped exponential retry deadline with stable per-event jitter are persisted before network I/O. Pending plus blocked outbox entries are bounded at 1,000; full capacity fails enqueue and applies backpressure.
- Terminal local state, terminal callback intent and slot release can be committed together. A blocked callback remains visible in recovery diagnostics and prevents descriptor cleanup.
- A queued cancellation is already terminal in Soyuz and is skipped. A claimed run cancelled before launch cannot cross the execution gate. During future execution, cancellation must stop and clean up the worker before `/cancelled` is acknowledged.

## Configuration boundary

`SOYUZ_API_ORIGIN`, `SOYUZ_WORKER_ID`, and either `SOYUZ_WORKER_API_TOKEN_FILE` or `SOYUZ_WORKER_API_TOKEN` configure the Soyuz worker client. `CLOUDFLARE_ACCOUNT_ID`, `CLOUDFLARE_QUEUE_ID`, either `CLOUDFLARE_QUEUE_TOKEN_FILE` or `CLOUDFLARE_QUEUE_TOKEN`, and `CLOUDFLARE_QUEUE_VISIBILITY_TIMEOUT_MS` configure Queue access. The default visibility timeout is 60 seconds and accepted range is 1 second to 12 hours. `RUSTY_FOSSIL_POLL_INTERVAL_SECONDS` and `RUSTY_FOSSIL_REQUEST_TIMEOUT_SECONDS` configure transport timing. Secret files are preferred for deployment; Queue and Soyuz credentials are separate and redacted from debug output.

## Readiness and operational limitations

The diagnostics API is read-only. `/ready` reports state-store health, configuration validity, Queue transport health (unprobed unless an explicit probe is supplied), recovery backlog, handoff readiness, and runtime limitations. Readiness checks do not pull Queue messages, create claims or invoke inference. The production binary does not construct the coordinator or poller, so complete configuration does not enable production intake. Execution readiness remains false because no Docker runtime exists.

Queue, coordinator and outbox counters are low-cardinality. They do not label runs, message bodies, prompts, repository names, callback text, secrets, or leases. The service exposes only `/health`, `/ready` and `/metrics`; there is no submission or task-starting endpoint.

## Senku boundary for Phase 2A-S

Senku is a later advisory decision service. A follow-on `fossil-senku` crate may support the Kaseki-owned profiles `task_admission_v1`, `kaseki_context_routing_v1`, `validation_recovery_v1`, `kaseki_goal_check_v1`, and `kaseki_run_evaluation_v1`. Advisory results cannot replace deterministic validation or Soyuz authorization. Do not add JEV or direct OpenRouter calls here; Senku availability will not be required to accept a valid Soyuz authorization.

## Phase 2B Docker prerequisites

The future supervisor must accept only a locally persisted descriptor plus an exact durable start/gate proof. It must use one slot, non-root workers, strict CPU/memory/PID/disk/network/time limits, bounded output, cancellation cleanup and explicit restart policy. Keep Docker socket access out of the controller image and do not auto-launch after ambiguous restart state.
