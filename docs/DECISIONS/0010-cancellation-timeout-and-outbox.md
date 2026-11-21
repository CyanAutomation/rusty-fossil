# ADR 0010: Persist stop intent and callback before Docker cleanup

- Status: Accepted
- Date: 2026-11-21

## Context

Soyuz cancellation can race with a fast worker exit. Timeouts must be independent of log activity, and temporary Soyuz or Docker outages cannot be treated as evidence that a workload stopped. Terminal callback delivery already uses the controller's durable SQLite outbox.

## Decision

The supervisor polls actual Docker inspection state and canonical Soyuz state. It records a `stop_requested` state and the first stop reason before sending Docker's graceful stop signal. It waits at most the configured grace interval, then sends a force-kill and confirms the exact container is exited before classifying a supervisor-requested cancellation or timeout.

If Docker inspection shows the container already exited before a stop intent is persisted, the observed exit result wins. If it was running and the durable stop intent precedes its exit, cancellation and timeout receive their corresponding terminal classification; a stop caused by prolonged Soyuz unavailability is classified as execution uncertainty. The runtime deadline is computed from Docker's persisted `StartedAt` timestamp and the configured fixed runtime limit. It is independent of output and heartbeat success and survives a supervisor restart. Because Docker exposes a wall-clock timestamp rather than a restart-stable monotonic clock, correct timeout accounting assumes the host clock is synchronized; a backwards or forwards clock adjustment can delay or advance timeout handling.

While Soyuz is unavailable, the supervisor does not start new work. For an already-running workload it keeps inspecting Docker, allows a bounded 30-second Soyuz outage grace, then persists `execution_uncertain`, stops the exact container and leaves callback delivery to the shared outbox. Heartbeats are bounded, use the worker and claim fence, and are only operational signals; they do not prove process liveness.

Terminal result, stable callback ID and typed callback payload are committed through the existing outbox in one SQLite transaction. The Docker row advances to cleanup-pending; the execution slot is released only after exact-container cleanup completes. The controller remains responsible for delivering the outbox entry to Soyuz.

## Consequences

- Callback delivery is at-least-once and reuses the persisted event ID.
- The execution slot remains occupied during unresolved stop or cleanup work.
- If exact container state is unavailable or identity mismatches, the supervisor records manual review instead of guessing a terminal result.
- The service `main` does not start callback replay or Queue polling; operators must not interpret the Phase 2B binary as production-ready intake.
