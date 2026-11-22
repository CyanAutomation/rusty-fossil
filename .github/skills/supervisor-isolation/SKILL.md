---
name: supervisor-isolation
description: Use when changing fossil-supervisor, fossil-supervisord, Docker isolation, supervisor IPC, container lifecycle recovery, or related operations.
---

# Supervisor and Docker isolation changes

Use this skill for work that touches the isolated execution boundary. The controller must not gain Docker Engine access; the supervisor independently validates durable local authorization and fresh canonical Soyuz state.

## Workflow

1. Read the [architecture](../../../docs/ARCHITECTURE.md), [Phase 2B operations guide](../../../docs/PHASE-2B-OPERATIONS.md), and the relevant decisions, especially ADRs 0007–0010. Confirm the current phase and production wiring before assuming a runtime is active.
2. Keep IPC narrow. Requests identify a canonical run and must not supply a prompt, image, command, mounts, or arbitrary Docker settings. The supervisor reloads the descriptor, proof, claim fence, and reserved slot from SQLite and performs a fresh Soyuz authorization check.
3. Preserve the fixed, digest-pinned worker image and bounded isolation profile. Keep the Docker socket with the dedicated supervisor only; do not add Docker privileges or mounts to the controller.
4. Persist launch intent and deterministic container identity before Docker side effects. When create or start results are uncertain, inspect the exact recorded name, ID, labels, image, and profile. Never issue a second create speculatively; keep ambiguous execution fenced for manual review.
5. Persist terminal state and callback intent before cleanup. Release the execution slot only after the exact managed container is confirmed removed or absent and cleanup is recorded. Never clean up by broad name prefix or use `docker system prune`.
6. Add or update mock-based tests for the changed state transition. Use the real Docker Engine integration only in its trusted Docker-enabled CI or an explicitly suitable development environment; it is intentionally ignored in ordinary workspace test runs.
7. Update the operations guide and relevant ADRs when operator steps, safety bounds, or recovery behavior change. Report exactly which checks ran and whether Docker access was available.

## Operational limits

- Follow the [operations guide](../../../docs/PHASE-2B-OPERATIONS.md) for host prerequisites, secret-file permissions, shutdown, and incident response.
- Do not edit SQLite directly to authorize a launch or free a blocked slot.
- Do not delete durable state, remove an unrelated container, or weaken a security profile to recover from an ambiguous state.
- The current worker is synthetic-only. Do not infer repository checkout, Pi CLI, Git, or publication capability from Phase 2B support.
