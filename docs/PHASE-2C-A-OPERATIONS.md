# Phase 2C-A Pi worker operations

## Status

The branch contains a Pi 1.6.0 worker wrapper, a locked runtime dependency graph, a fixed multi-architecture Node base digest, and a Rust JSONL event classifier. CI now tests the wrapper and builds the AMD64/ARM64 worker image with SBOM and provenance attestations.

This is a foundation, not an operational coding runtime. The supervisor still accepts only its synthetic workload. It does not mount a repository, provide Pi configuration, contact a gateway, persist the Pi event summary, or select this image. Do not launch the image as an execution fallback.

## Local checks

Run the focused checks from the repository root:

```sh
node --test worker/pi/worker.test.mjs
cargo test -p fossil-supervisor --lib pi_events --locked
```

The multi-platform image build is:

```sh
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --file worker/pi/Dockerfile.pi-worker \
  --output type=oci,dest=/tmp/pi-worker.oci \
  .
```

The build uses the digest-pinned Node 26 Bookworm slim image and Pi 1.6.0 lockfile. It runs `pi --version` during image construction. Dependency lifecycle scripts are disabled. CI also emits SBOM and provenance attestations. Do not treat a successful cross-architecture build as a Raspberry Pi runtime test.

## Enablement gates

Before connecting the image to the supervisor, implement and review all of these:

1. Versioned workload identity in SQLite with a backward-compatible migration. The synthetic path must keep its existing resource policy and recovery behavior.
2. An explicit Pi image digest allowlist and a separately bounded Pi resource profile. Inspect image, labels, workload version, workspace identity, and the effective security profile before start and during recovery.
3. Host-side preparation of the exact Soyuz-authorized descriptor. Restrict the pilot to an approved HTTPS repository, resolve the ref to a commit, disable submodules and Git LFS, reject unsafe redirects and local/private destinations, and bind only a per-run workspace.
4. A real egress enforcement point that permits only the approved Git origin and one model gateway. Docker bridge mode alone is not an allowlist. Block metadata and private network destinations and preserve TLS checks.
5. Brokered, least-privilege, non-production gateway authentication. Do not pass reusable provider, GitHub, Soyuz, Cloudflare or Senku credentials into a shell-capable worker.
6. Bounded JSONL capture integrated into durable outcomes and the existing callback outbox. Missing, truncated, malformed, aborted, provider-error, timed-out or ambiguous output must not become success.
7. Cancellation, timeout, restart reconciliation, slot retention, cleanup, and callback replay tests for the Pi workload type.
8. A secured Agentlab access path and a reviewed host resource budget. Record memory under the actual Pi run before setting production-like limits.

Production Queue polling, Senku inference and publication remain out of scope. There is no push token or publication path in this worker.

## Current Agentlab disposition

The read-only host inventory is in [Agentlab compatibility](AGENTLAB-COMPATIBILITY.md), and the validation record is in [Agentlab validation](AGENTLAB-VALIDATION.md). No Rusty-Fossil image or container was created on Agentlab. The terminal reported that the default `pi` SSH password had not been changed, and that account is in the rootful Docker group. Do not deploy until the host owner secures access and reviews a dedicated execution identity. A permitted test repository and gateway-only route are also not configured.
