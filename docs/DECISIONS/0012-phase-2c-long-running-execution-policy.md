# ADR 0012: Keep long-running coding execution policy separate from synthetic limits

- Status: Coding policy proposed; schema v7 persists the synthetic launch profile/deadline and a non-executable Pi workload identity
- Date: 2026-11-22

## Context

The synthetic worker currently defaults to a 15-minute runtime and accepts a maximum of 30 minutes. A coding task can need hours. Raising the shared runtime limit would also lengthen synthetic tests and increase the period in which a worker occupies the single execution slot. Supervisor SIGTERM currently persists an `execution_uncertain` stop intent, stops active work and cleans up. Treating planned supervisor maintenance like a transparent restart would silently change established Phase 2B semantics.

## Proposed policy for Phase 2C-A

Keep independently configured deadlines with supervisor-owned bounds:

| Policy | Phase 2B synthetic | Phase 2C-A proposal |
| --- | --- | --- |
| Workload execution timeout | 15-minute default; 30-minute maximum | Separate coding-job timeout. Start the non-production pilot with a 2-hour default and a 4-hour local supervisor ceiling, then revise only from measured task duration and operator review. The current `SoyuzRunRequest` validator caps `timeout_seconds` at 10,800 seconds (3 hours), so the effective deadline must be the minimum of the persisted Soyuz request, the selected coding profile, and the local ceiling. Do not expand the Soyuz contract in this phase. |
| Cancellation grace | Current 5-second default, 30-second maximum | Separate coding cancellation grace, initially up to 30 seconds, followed by force-stop and confirmed exit. |
| Soyuz outage grace | Current 30-second default, 120-second maximum | Separate coding outage grace, initially 60 seconds; then stop and report `execution_uncertain` unless a reviewed recovery policy says otherwise. |
| Supervisor shutdown grace | Current bounded drain/stop behavior | Separate process shutdown deadline (initially 60 seconds). Shutdown policy remains explicit and cannot silently change cancellation semantics. |
| Container recovery deadline | Current create-response grace and periodic recovery | Separate durable reconciliation deadline. Expiry without proven identity or outcome becomes manual review and keeps the slot fenced. |

The profile, cancellation, outage, shutdown and recovery bounds remain trusted supervisor configuration, not arbitrary task options. The current canonical request timeout is validated by Soyuz and preserved in the durable descriptor; the effective coding deadline must be the minimum of that validated timeout, the trusted coding profile and the local supervisor ceiling. Schema v5 now persists the synthetic workload kind/version, image identity, profile version, effective resource/runtime policy, and absolute synthetic deadline. The Pi-specific values and request-derived coding deadline remain unimplemented. Deadline calculations continue to rely on synchronized wall-clock time until a restart-stable host clock source is designed.

## Host service identity

The pilot deployment expects the controller and supervisor containers to use the operator-configured service UID/GID on a controlled, local-only trusted LAN. Do not create a separate host account as part of this phase. The root Compose definition mounts `/run/docker.sock` and adds its numeric group only to the supervisor; the controller has no socket mount and uses an internal Compose network. Sharing a host UID/GID provides process confinement but is not account separation, and Docker access remains host-level authority. Before enabling the Compose stack, verify the effective Docker API reachability is confined to the trusted LAN and unavailable through Cloudflare Tunnel routes, from the Pi worker network, and from untrusted networks. The tunnel must expose only explicitly configured application services; it does not proxy host-originated requests by itself. The Compose boundary has not been deployed or verified on a target host.

## Proposed maintenance and shutdown behavior

Phase 2C-A should add a conservative drain state before allowing long-running jobs: reject new launches, preserve durable worker state, and stop active work by an explicit bounded deadline. Do not implement a seamless supervisor handoff unless exclusive supervisor ownership and exact-container reattachment can be proven. Until then, planned shutdown must not create a second supervisor or speculate about prompt redelivery. Host shutdown, Docker loss or ambiguous identity must retain the current fail-closed stop/uncertain behavior.

An operator-requested cancellation remains a distinct durable action. A hard-stop policy is mandatory for host shutdown, expired job deadline, unrecoverable Docker state or a configured maximum drain period. No process should claim that a task completed while terminal state or callback delivery remains unresolved.

## Consequences and prerequisites

- Phase 2B synthetic defaults and SIGTERM behavior remain unchanged.
- The 2026-11-27 Phase 2C-A.3 work has not implemented these coding-specific deadline, cancellation, outage, shutdown, or maintenance controls. The Pi workload remains rejected by the supervisor; see the [integration map](../PHASE-2C-A.3-INTEGRATION-MAP.md).
- Multi-hour continuation requires a native Pi test of recovery, clock behavior, memory, storage wear, Docker cgroups, outbound network policy and controlled credentials.
- Phase 2C-A must define a local maintenance control and single-supervisor ownership mechanism before implementing continuation across process replacement.
- The timeout values above are a proposal for a non-production pilot, not implemented configuration or production guarantees.
