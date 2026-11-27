# Contract overview

This document describes the compatibility rules for versioned data contracts used by this workspace.

## Versioning

- Validate input against the schema for its declared version.
- Preserve documented identifiers and optional fields when normalizing a message.
- Reject unknown contract versions and unsupported fields rather than silently reinterpreting them.
- Add a new version when a change would make existing input or output incompatible.

The Senku v1 response envelope is an additive-reader exception: the client validates every required top-level field, ignores unknown top-level fields, and accepts any positive `attemptCount`. Profile answer IDs, answer object fields, answer values, and the versioned request input remain strict. This lets Senku add response metadata without weakening validation of decision content.

Rusty-Fossil pins Senku's generated profile contract artifact at `crates/fossil-senku/contracts/senku-profiles.v1.json`. The root `.senku-contract.sha256` file records the exact artifact digest and source Senku revision; CI verifies the checked-in artifact against that digest. Update the artifact and pin together as a reviewed contract change.

## Fixtures and tests

Checked-in fixtures provide representative contract examples. Add tests for required fields, supported variants, normalization, and compatibility whenever a schema changes.

Environment-specific configuration and release procedures are maintained separately from this contract reference.

## Model broker request and cost-reporting contract

The standalone `fossil-gateway` accepts the configured model and supported OpenAI-compatible chat fields. It strips `max_tokens` and `max_completion_tokens` so client defaults cannot impose an output-token budget; it does not cap `n` or impose a per-run monetary, inference-count, or request-rate budget. The provider/model may still enforce its own technical limits. Fixed route/model authorization and transport protections remain: request/response body safety bounds, authenticated run fencing, TLS, redirect refusal, and field/content validation.

The SQLite ledger stores run fences, inference request IDs, Cloudflare log IDs, and any reported estimated cost. It has no usage ceiling and does not reject inference based on count, rate, token quantity, or cost. `GET /v1/cost` returns `estimatedCostUsd`, `currency`, `source`, `complete`, `requestCount`, `pricedRequestCount`, and `pendingRequestCount`. Cost is telemetry for Soyuz's cost-per-PR accounting. It is not a guaranteed invoice total and does not control request admission. Cloudflare cost data may be unavailable while a log is pending.

The intended Soyuz transport is an idempotent `progress.updated` operational event with `stage: "inference"`, `step: "cost_report"`, and a compact `payload` containing `kind: "ai_gateway_cost"`, `estimatedCostUsd`, `currency`, `source`, `requestCount`, `pricedRequestCount`, `pendingRequestCount`, and `complete`. Soyuz's existing progress event accepts arbitrary bounded payload metadata. `AuthorizationCoordinator::enqueue_gateway_cost_report` validates and durably enqueues that event with a stable per-run event ID. The supervisor does not yet fetch `/v1/cost` or invoke the method; runtime wiring remains open. Legacy budget state is discarded when the new telemetry ledger opens and is never consulted for request admission.
