# ADR 0012: Keep long-running coding execution policy separate from synthetic limits

- Status: Proposed for Phase 2C-A; no runtime behavior changes in Phase 2B.1
- Date: 2026-11-22

## Context

The synthetic worker currently defaults to a 15-minute runtime and accepts a maximum of 30 minutes. A coding task can need hours. Raising the shared runtime limit would also lengthen synthetic tests and increase the period in which a worker occupies the single execution slot. Supervisor SIGTERM currently persists an `execution_uncertain` stop intent, stops active work and cleans up. Treating planned supervisor maintenance like a transparent restart would silently change established Phase 2B semantics.

## Proposed policy for Phase 2C-A

Keep independently configured deadlines with supervisor-owned bounds:

| Policy | Phase 2B synthetic | Phase 2C-A proposal |
| --- | --- | --- |
| Workload execution timeout | 15-minute default; 30-minute maximum | Separate coding-job timeout. Start the non-production pilot with a 2-hour default and 4-hour hard maximum, then revise only from measured task duration and operator review. Queue input cannot raise the bound. |
| Cancellation grace | Current 5-second default, 30-second maximum | Separate coding cancellation grace, initially up to 30 seconds, followed by force-stop and confirmed exit. |
| Soyuz outage grace | Current 30-second default, 120-second maximum | Separate coding outage grace, initially 60 seconds; then stop and report `execution_uncertain` unless a reviewed recovery policy says otherwise. |
| Supervisor shutdown grace | Current bounded drain/stop behavior | Separate process shutdown deadline (initially 60 seconds). Shutdown policy remains explicit and cannot silently change cancellation semantics. |
| Container recovery deadline | Current create-response grace and periodic recovery | Separate durable reconciliation deadline. Expiry without proven identity or outcome becomes manual review and keeps the slot fenced. |

All values remain supervisor configuration, not task-controlled runtime options. A task's effective deadline is the minimum of its authorized profile limit and the supervisor's hard bound. Persist the selected policy and absolute deadline in the launch journal so restart does not reset the clock. Deadline calculations continue to rely on synchronized wall-clock time until a restart-stable host clock source is designed.

## Proposed maintenance and shutdown behavior

Phase 2C-A should add a drain state before allowing long-running jobs: reject new launch requests, keep the current supervisor alive to monitor active jobs, and let operators choose between waiting for completion and explicit cancellation. Planned supervisor replacement may permit a running container to continue only after a successor process has acquired the single-host supervisor lock, read the same SQLite journal, and successfully reattached to the exact container. The predecessor must not release ownership until that validation succeeds. Host shutdown, Docker loss or failed successor reattachment must retain the current fail-closed stop/uncertain behavior.

An operator-requested cancellation remains a distinct durable action. A hard-stop policy is mandatory for host shutdown, expired job deadline, unrecoverable Docker state or a configured maximum drain period. No process should claim that a task completed while terminal state or callback delivery remains unresolved.

## Consequences and prerequisites

- Phase 2B synthetic defaults and SIGTERM behavior remain unchanged.
- Multi-hour continuation requires a native Pi test of recovery, clock behavior, memory, storage wear, Docker cgroups, outbound network policy and controlled credentials.
- Phase 2C-A must define a local maintenance control and single-supervisor ownership mechanism before implementing continuation across process replacement.
- The timeout values above are a proposal for a non-production pilot, not implemented configuration or production guarantees.
