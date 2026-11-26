# ADR 0007: Keep Docker authority in a narrowly scoped supervisor

- Status: Accepted
- Date: 2026-11-21

## Context

Access to the Docker Engine socket is effectively host administrative authority. The Rusty-Fossil controller handles Queue parsing, canonical Soyuz authorization, recovery, callbacks, and diagnostics. Giving that process unrestricted Docker access would combine control-plane parsing with host-level container control and would not meet the Phase 2B trust boundary.

## Decision

Docker Engine access belongs to a separate, narrowly scoped supervisor process. It must not expose a general Docker API proxy or accept caller-selected images, mounts, flags, or commands. Its request must identify a persisted execution handoff; the supervisor must reload the descriptor, verify the exact worker and claim/start identities, confirm the reserved local slot, and perform a fresh canonical Soyuz check immediately before container start. The controller must not mount or otherwise receive the Docker socket.

The first workload is the fixed synthetic worker only. It uses a small scenario enum supplied by controlled test/runtime configuration. Queue task prompts remain inert metadata. A mock supervisor can share the proof-gated lifecycle contract, but a process or IPC boundary must revalidate authorization because Rust private fields do not protect serialized messages.

## Consequences

- The supervisor must have explicit lifecycle ownership, a restricted local IPC endpoint, and a dedicated security review.
- Container launch intent and identity must be durable before Docker side effects; daemon uncertainty must stop new launches until exact-identity reconciliation completes.
- The controller continues to own Queue intake, the SQLite journal, recovery scheduling, and the existing Soyuz callback outbox.
- This ADR does not authorize enabling production Queue polling or real coding workloads.
- Phase 2B implements the separate `fossil-supervisord` process, authenticated local IPC, a Bollard Docker backend, durable launch states, the fixed synthetic worker, and restart reconciliation. Production `main` still does not construct the coordinator or enable Queue intake; a supervisor deployment alone cannot claim or start work. Docker socket access remains host-level authority and must be granted only to the dedicated supervisor identity.
- Agentlab's owner expects both containers to run as the existing `pi` UID/GID. The root Compose definition mounts the Docker socket only into the supervisor container and assigns the Docker group only there. This is a host-specific process-sandbox exception under a shared Unix identity, not the distinct-account boundary recommended above. The `pi` account remains rootful-Docker capable, and the Compose definition is not installed or verified.
