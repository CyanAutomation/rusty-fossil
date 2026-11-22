# ADR 0011: Reserve transactional callback capacity for terminal results

- Status: Accepted
- Date: 2026-11-22

## Context

The callback outbox has a hard maximum of 1,000 pending or blocked entries. Intake pauses at 999 so an admitted execution retains one place for its terminal result. That intake-only check is insufficient: a running supervisor can emit optional heartbeat and operational events after intake, consuming the last row and preventing the terminal callback from being persisted. Dropping a terminal result would leave the run and execution slot unresolved.

## Decision

Classify completion, failure and cancellation callbacks as mandatory; classify heartbeat and operational event callbacks as optional. Keep the existing 1,000-row cap and enforce the classification inside SQLite's immediate transaction for every callback insertion path.

When there are 999 outstanding rows, optional callbacks return `SuppressedCapacity` and do not create a row. The result is counted and logged at the supervisor boundary; suppression does not stop a worker. An authorized Docker launch that still owns the reserved execution slot may insert its stable terminal callback as row 1,000. If the run does not own that slot, the run/event identity mismatches, or the hard cap is already reached, terminal persistence fails closed and the execution fence remains held. Re-enqueueing an existing event ID is idempotent. No pending or blocked callback is deleted or mutated to create capacity.

Heartbeat coalescing is not used. Suppression is simpler and does not mutate an event whose delivery outcome may be uncertain.

## Consequences

- The active admitted run retains one terminal callback row despite any number of optional callback attempts, subject to SQLite availability and the database not already violating its cap.
- At-most-once launch remains a single-host guarantee under the durable SQLite assumptions; this does not provide distributed exactly-once execution.
- Supervisor counters expose optional enqueue, suppression, insertion errors, and terminal persistence retries/failures. The production controller does not export supervisor counters until that runtime is explicitly wired.
- Callback replay preserves the persisted event ID and payload. Operators must resolve blocked entries through the approved process and must not delete rows to bypass backpressure.

## Validation

Storage tests cover 998/999-row pressure, blocked rows, restart with a nearly full outbox, a concurrent optional and terminal insertion, and repeated terminal event IDs. The real Docker lifecycle and process tests cover terminal persistence and callback replay; the default storage suite remains Docker-free.
