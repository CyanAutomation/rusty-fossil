---
name: soyuz-run-lifecycle
description: Use when changing Queue intake, canonical run reads, claim or start authorization, recovery, callbacks, or lifecycle state in Rusty-Fossil.
---

# Soyuz run lifecycle changes

Use this skill for changes that cross the Queue, coordinator, Soyuz client, SQLite journal, recovery manager, or callback outbox. Soyuz remains the only authority for run identity, lifecycle state, claims, and start authorization.

## Workflow

1. Read the relevant parts of the [architecture](../../../docs/ARCHITECTURE.md), [contracts](../../../docs/CONTRACTS.md), and [compatibility inventory](../../../docs/COMPATIBILITY.md). Trace the existing path across crate boundaries before changing it.
2. Keep canonical run IDs, display IDs, Queue message IDs, Queue lease IDs, worker IDs, claim fences, and callback IDs distinct. Treat Queue input as untrusted and validate the complete supported contract.
3. Define the durable state transition before making network calls. Persist stable callback IDs and operation intent first. Build claim and start authorization only from `fossil-soyuz` proof types, and confirm the proof matches the persisted run, worker, claim fence, descriptor, and reserved slot.
4. For an uncertain response, reread canonical Soyuz state and reconcile using the same persisted ID. Do not mint replacement IDs, retry non-idempotent operations speculatively, create a local run authority, or fall back to local execution.
5. Add or update contract and lifecycle tests before implementation. Use recorded JSON fixtures, mock HTTP services, and temporary SQLite state; cover the relevant uncertain-response, redelivery, restart, cancellation, ownership-conflict, and outbox cases. Tests must not require production credentials, public network access, Soyuz infrastructure, or Docker. Include applicable Soyuz requirement IDs in test comments and the compatibility inventory.
6. Update the architecture, protocol notes, compatibility inventory, and implementation roadmap when behavior or external contracts change. Clearly distinguish implemented library behavior from behavior wired into the production binary.
7. Run the applicable checks from the [contributing guide](../../../CONTRIBUTING.md) and report any environment limitation without implying the check passed.

## Boundaries

- Advisory Senku output cannot authorize a claim, start, retry, or execution handoff.
- Keep diagnostics read-only. Do not log prompts, raw requests, credentials, callback bodies, or Queue messages.
- Preserve durable callback and event IDs across retries; never mark delivery before a successful Soyuz response.
- Do not describe a feature as production-active unless the service actually constructs and runs it.
