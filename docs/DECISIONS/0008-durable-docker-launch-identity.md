# ADR 0008: Persist Docker launch identity before side effects

- Status: Accepted
- Date: 2026-11-21

## Context

Docker create and start requests can succeed even when the client loses the response. A retry using a new name could launch a second workload for the same Soyuz run. The controller also must preserve its one-host, one-container execution slot across process and daemon restarts.

## Decision

SQLite schema v7 stores one `container_launches` row per canonical run. The row contains the run, worker, claim fence, start callback, versioned workload kind, fixed synthetic scenario where applicable, immutable image identity, profile version, effective resource/runtime policy, deterministic name, optional Docker ID, state, timestamps, absolute runtime deadline, terminal callback ID and bounded diagnostic result. Pi identities can additionally store the canonical repository URL/ref, resolved commit and opaque per-run workspace identity. These values are not included in Docker labels. The supervisor still accepts only the synthetic profile and fails closed for Pi records. Migrations preserve existing synthetic rows and leave new repository/workspace values unset rather than guessing.

The supervisor commits `launch_intent_persisted` before Docker access and uses a transactional transition to `create_requested` so only one cooperating process sends the initial create. A lost create response is reconciled by inspecting that exact name and checking all immutable labels, configured image, Docker ID when present, and effective security profile. The create call is never repeated after `create_requested` is durable.

The supervisor similarly commits `start_requested` before start. If the response is lost, it inspects the same container. A confirmed running container is recovered from the persisted start proof after exact identity checks; a still-created container needs a fresh canonical Soyuz gate before start. A missing, conflicting, or unverifiable container becomes `manual_review_required`. The supervisor never creates a replacement for ambiguous history.

The execution slot stays reserved through terminal result persistence and `cleanup_pending`. Only confirmed removal or confirmed absence after a terminal result advances to `cleanup_completed` and releases the slot.

## Consequences and assumptions

- At-most-once launch is the goal under one durable SQLite database shared by processes on one host and a Docker daemon that preserves its container state.
- This is not a distributed exactly-once guarantee. SQLite loss/corruption, external deletion, daemon data-root loss, or copying the DB to another host requires operator review.
- The unique container name and labels are reconciliation evidence, not authorization. Authorization still requires the durable Soyuz proof and a fresh canonical read.
- Ambiguous rows intentionally hold the slot until an operator resolves them.
