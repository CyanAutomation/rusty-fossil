# ADR 0005: Reconcile persisted runs independently and reserve callback capacity

- Status: accepted
- Date: 2026-11-21

## Context

Queue redelivery cannot be the only trigger for reconciling an unresolved local run: a single occupied execution slot can prevent the next pull that would deliver the duplicate. Separately, accepting a new run near the callback outbox limit can leave no room for a mandatory terminal result.

## Decision

Use `RecoveryManager` to enumerate durable unresolved runs and reconcile them through the existing Soyuz coordinator path, using the persisted descriptor and callback identities. Recovery can run at startup and periodically without a Queue lease. Keep explicit outcomes for awaiting Soyuz, recovered, owned elsewhere, cancellation pending, terminal reconciliation, ambiguous execution, and manual review.

Never infer that a process stopped from a restart or expired claim. If work may be active, preserve local ownership records and require review; never launch a replacement automatically. When Soyuz confirms another worker owns a run and local execution is not active, terminalize only the local intake record, release its slot, emit no false terminal callback, and acknowledge the duplicate.

Keep the callback outbox at 1,000 entries and stop new intake at 999 pending/blocked entries. Existing run reservations remain idempotently recoverable. Callback delivery continues independently of slot availability; blocked callbacks are retained for review.

## Consequences

Recovery decisions and attempts are durable and observable. The operational supervisor remains opt-in; the service binary does not start it or Queue polling in this phase. The 999-entry intake watermark protects one terminal callback but does not clear blocked records or change Soyuz ownership.
