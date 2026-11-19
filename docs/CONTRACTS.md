# Soyuz and Queue contracts

## Sources and discovery

Soyuz's current versioned cross-service contract is `contractVersion: "1"`. The current primary references are [Soyuz transport contract](https://github.com/CyanAutomation/soyuz/blob/main/docs/CONTRACT.md), [Kaseki integration policy](https://github.com/CyanAutomation/soyuz/blob/main/docs/KASEKI-INTEGRATION.md), [Soyuz test requirements](https://github.com/CyanAutomation/soyuz/blob/main/docs/TEST-REQUIREMENTS.md), and [Soyuz architecture](https://github.com/CyanAutomation/soyuz/blob/main/docs/ARCHITECTURE.md). These are not copied into Rust source; the contract tests use independent JSON fixtures.

The referenced published Kaseki page at [kaseki-tunnel.scheimann.xyz/docs](https://kaseki-tunnel.scheimann.xyz/docs) was read on 2026-11-18. It is a human-readable operation index with 33 operations and an explicit link to [OpenAPI 3.1 JSON](https://kaseki-tunnel.scheimann.xyz/api/v1/openapi.json). The linked document was fetched and reports 32 paths. It lists health/readiness, metrics, usage, capabilities, preflight, validation, run submission and status, local run submission, retry, cancellation, events, logs, artifacts, scorecards, webhooks, gateway diagnostics, and Soyuz callback diagnostics/retry. Rusty-Fossil does not implement or proxy those Kaseki API paths. In particular, its runtime does not implement the `POST /api/v1/runs` or `POST /api/v1/runs/local` operations.

## Canonical identities

| Value | Meaning | Rust representation |
| --- | --- | --- |
| Soyuz `runId` | Canonical UUIDv7 run identity and durable deduplication key. | `RunId` |
| Soyuz `displayId` | Human-facing label such as `soyuz-213`. | `DisplayId` |
| Cloudflare message `id` | Ephemeral Queue message identifier. | `QueueMessageId` |
| Cloudflare `lease_id` | Lease used only to acknowledge or retry one pulled message. | `QueueLeaseId` |
| Soyuz `workerId` | Stable, unique identity for one controller host. | `WorkerId` |
| Claim `callbackId` / echoed `claimCallbackId` | Per-attempt fencing identity used to reject callbacks from stale claims. | `ClaimCallbackId` |
| Worker callback/event ID | Stable idempotency identity for that operation across retries. | `EventId` |

These types remain distinct even when their wire representation is a string or UUID. A display ID, queue lease, or callback ID never replaces the canonical run UUID.

## Queue envelope and delivery

Soyuz publishes the validated normalized request in a JSON envelope containing:

- `contractVersion`, exactly the string `"1"`;
- `runId`, canonical UUIDv7;
- `createdAt`, RFC 3339 timestamp;
- UUID `correlationId` and `requestId`;
- `request`, including `repoUrl`, `ref`, and a non-empty `taskPrompt` unless `startupCheck` is true.

The host must reject unknown versions before work can be considered. The parser caps an envelope at 96 KiB, requires HTTPS `repoUrl` without embedded credentials, validates the ref, and does not store the prompt. The request supports additional Soyuz fields; Phase 1 retains only fields needed for the initial validation and ignores unneeded request extensions rather than inventing local semantics.

Cloudflare Queues delivers at least once. Deduplicate by `runId`; do not use the ephemeral message ID or `lease_id` as a run key. Soyuz has no `/next-job` endpoint. Its contract calls for a direct Cloudflare HTTP pull consumer in a future phase.

Cloudflare's current HTTP pull API uses `POST /accounts/{account_id}/queues/{queue_id}/messages/pull` with `batch_size` and `visibility_timeout_ms`. The current documented limits are batch size 1–100 and visibility timeout up to 12 hours. Pull returns message IDs and `lease_id`; `POST .../messages/ack` accepts `acks` and `retries`, each keyed by `lease_id`. Queue credentials need both Queue read and write permissions. For JSON-content messages, the HTTP pull response body is base64 encoded; the recorded fixture covers that form. See the official [Cloudflare pull-consumer guide](https://developers.cloudflare.com/queues/configuration/pull-consumers/) and [Queues API](https://developers.cloudflare.com/api/resources/queues/).

Phase 1 defines the pull, message, and acknowledgement data models and tests fixtures only. It does not make a pull request, acknowledge, retry, or lease production messages.

## Worker API

Every worker request uses `Authorization: Bearer <WORKER_API_TOKEN>`, separate from Soyuz's client submission token and Cloudflare Queue token. Success responses use `{ "data": ..., "requestId": "..." }`. Errors use `{ "error": { "code", "message", "requestId" } }`. The Rust client never includes remote messages or raw bodies in error display.

| Operation | Current contract behavior | Phase 1 status |
| --- | --- | --- |
| `GET /v1/worker/runs/:id` | Read canonical `runId`, `displayId`, exact status, cancellation, claim, heartbeat and health fields before start and during execution. | Typed request/response and cancellation-read model; mock tested. |
| `POST /v1/worker/runs/:id/claim` | Compare-and-set `queued` to `claimed`; body includes v1, callback ID, worker ID, optional lease 30–300 seconds (default 120). The callback ID is also the claim fencing token. | Typed client; no production call. |
| `POST /v1/worker/runs/:id/started` | Authorizes the start for the owned live claim. A worker must wait for success before launch. | Typed client returns a private-field authorization proof only on successful response. |
| `POST /v1/worker/runs/:id/events` | Bounded operational event, at most 12 KiB; event ID and claim ID remain stable across retries. | Typed model/client, bounded and mock tested. |
| Heartbeat | Sent as a `heartbeat` operational event; refreshes liveness, does not change lifecycle status. | Typed helper through the events endpoint. |
| `POST /v1/worker/runs/:id/completed` | Reports bounded terminal completion fields. | Typed callback request; no live execution or callback delivery. |
| `POST /v1/worker/runs/:id/failed` | Reports a bounded failure class/message, including a valid pre-start failure on an owned live claim. | Typed callback request; no live execution or callback delivery. |
| `POST /v1/worker/runs/:id/cancelled` | Confirms cancellation after local cleanup. | Typed callback request; no live execution or callback delivery. |

The response reader caps bodies at 96 KiB. The client classifies timeout, network, authentication, authorization, missing run, stale claim, conflict, rate-limit, and service-unavailable outcomes. It does not automatically retry a non-idempotent operation. Callers supply persisted callback/event IDs so an explicit retry reuses the same identity.

## External statuses and cancellation

Rusty-Fossil accepts only current contract-v1 statuses: `admitting`, `queued`, `claimed`, `running`, `cancel_requested`, `completed`, `failed`, `cancelled`, and `admission_failed`. Unknown values fail deserialization. These statuses are separate from the local controller lifecycle.

Queued cancellation becomes terminal in Soyuz, so a later delivery must be skipped. A claimed-but-not-started run may be cancelled before Docker starts. A running cancellation becomes `cancel_requested`; the worker observes canonical state, stops its process, performs cleanup, then reports `/cancelled`. If completion wins, completion remains terminal. A stale or mismatched claim cannot authorize launch or confirm state for a different attempt.

## Idempotency, fencing, and outages

- Queue redelivery is expected; local uniqueness is by canonical `runId`.
- The local claim intent and callback ID are committed before any future claim request. An uncertain network response is reconciled against Soyuz; it does not create a second claim ID or run.
- The local start authorization is committed only from a `StartedAuthorization` returned by a successful `/started` response or a canonical read confirming matching `runId`, worker ID, claim ID, and active status.
- Replacing the persisted claim fence is rejected. The initial Phase 1 store does not implement claim renewal or a later attempt; a future phase must add explicit supersession history before any new claim can be authorized.
- Callback outbox IDs and payloads are stable across retries. Reusing an ID with different payload is rejected. Outbox records contain typed, bounded operational fields, not prompts, credentials, repository contents, or raw logs.
- Soyuz unavailability prevents new execution. The current Rust process has no fallback to direct local execution and no independent retry queue.

## Out of scope

The current Soyuz contract also defines artifact upload (25,000,000 bytes per file and at most 100 artifacts), run history, client submission, D1 retention, dashboards, and other public routes. Rusty-Fossil Phase 1 does not upload artifacts, submit runs, read the client API, use a `/next-job` route, connect to production Queues, or modify Soyuz or Kaseki.
