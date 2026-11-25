# Soyuz worker API simulator

Rusty-Fossil's coordinator integration tests use a test-only HTTP server in
`crates/rusty-fossil/tests/support/soyuz_simulator.rs`. The tests run the real
`SoyuzClient`, `AuthorizationCoordinator`, and SQLite store against it. This
adds stateful protocol coverage beyond the route-by-route WireMock tests in
`crates/fossil-soyuz/tests/client.rs`.

The simulator follows the worker contract and behavior inspected in Soyuz at
[`e36b47890066742b366fa1d1c2aab47486220660`](https://github.com/CyanAutomation/soyuz/tree/e36b47890066742b366fa1d1c2aab47486220660), especially
[`docs/CONTRACT.md`](https://github.com/CyanAutomation/soyuz/blob/e36b47890066742b366fa1d1c2aab47486220660/docs/CONTRACT.md),
[`src/api/workers.ts`](https://github.com/CyanAutomation/soyuz/blob/e36b47890066742b366fa1d1c2aab47486220660/src/api/workers.ts),
[`src/db/runs.ts`](https://github.com/CyanAutomation/soyuz/blob/e36b47890066742b366fa1d1c2aab47486220660/src/db/runs.ts),
and [`tests/api/worker-api.test.ts`](https://github.com/CyanAutomation/soyuz/blob/e36b47890066742b366fa1d1c2aab47486220660/tests/api/worker-api.test.ts).

## Covered behavior

- Worker bearer authentication and contract-v1 success/error envelopes.
- Canonical run reads and the worker callback routes Rusty-Fossil uses.
- Queued-to-claimed compare-and-set with default 120-second lease, lease bounds,
  same-worker claim replay, competing-worker rejection, and expired-claim
  reconciliation.
- Start authorization only for the current worker and claim fence, including
  lease expiry and stale-attempt rejection.
- Running and cancellation-requested state, terminal cancellation, and bounded
  worker events.
- Pre-execution failure callbacks from an owned live claim.
- Callback/event idempotency for a matching identity and payload; conflicts
  when an ID is reused with changed data.
- A committed operation followed by a 503 response, using the real coordinator
  recovery path to reconcile uncertain `/started` results.
- Cancellation between accepted `/started` and the coordinator's final
  canonical execution gate.

The implementation uses a mutex for one simulator process's compare-and-set
operations and a controllable clock for lease tests. Tests also run two real
`SoyuzClient` instances concurrently against one simulated run.

## Boundaries

The simulator does not implement run submission, Cloudflare Queue publication
or leases, Soyuz's D1 schema/transaction engine, scheduled-job timing, retention,
artifacts, publication, or all service authentication modes. Its expired-claim
control method models the resulting requeue for a test; it does not implement
the scheduled reconciler or its Queue delivery. It is an independent model of
the worker API, so a passing Rust test is not evidence that the deployed Soyuz
implementation conforms. Soyuz's own requirement tests remain the source for
database race and route implementation behavior; the simulator makes Rusty-
Fossil's client/coordinator tests more realistic and repeatable.

Run the simulator-backed integration suite with:

```sh
cargo test -p rusty-fossil --test soyuz_simulator
```
