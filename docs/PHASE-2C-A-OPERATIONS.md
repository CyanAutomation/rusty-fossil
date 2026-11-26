# Phase 2C-A Pi worker operations

## Status

The branch contains a Pi 1.6.0 worker wrapper, a locked runtime dependency graph, a fixed multi-architecture Node base digest, and a Rust JSONL event classifier. The supervisor terminal finalizer has a bounded Docker stdout parser path, and SQLite/outbox can persist Pi-specific result classes with a stable callback identity. CI tests the wrapper and builds the AMD64/ARM64 worker image with SBOM and provenance attestations. SQLite schema v7 persists the `PiCodingV1` workload/version, immutable image/profile identity, effective policy/deadline and opaque workspace identity. `fossil-workspace` now provides an exact pilot repository allowlist, bounded branch/ref and tree validation, hostname-restricted HTTPS Git transport, commit verification and private per-run checkout/cleanup. It has not been used for a remote fetch and is not wired into the supervisor. The record is prepared only for a patch task with `publishMode: none`; Pi image creation remains rejected by the supervisor, so the parser/result path is not reachable from a live Pi container.

This is still not an operational coding runtime. The supervisor accepts only its synthetic workload. It does not call the repository library, mount a repository, provide Pi configuration, contact a gateway, persist Pi event counters, or select this image. The durable Pi record is not permission to launch; do not launch the image as an execution fallback.

## Local checks

Run the focused checks from the repository root:

```sh
node --test worker/pi/worker.test.mjs
cargo test -p fossil-supervisor --lib pi_events --locked
cargo test -p fossil-workspace --locked
```

The default workspace-preparation tests are Docker-free and do not use network sockets. The optional live CONNECT-proxy test is ignored by default because some test environments prohibit loopback binding; run it explicitly only in a network-enabled environment with loopback access.

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

1. Extend the schema v7 Pi launch identity into an executable supervisor profile. The synthetic path must keep its existing resource policy and recovery behavior.
2. An explicit Pi image digest allowlist and a separately bounded Pi resource profile. Inspect image, labels, workload version, workspace identity, and the effective security profile before start and during recovery.
3. Host-side preparation of the exact Soyuz-authorized descriptor. Restrict the pilot to an approved HTTPS repository, resolve the ref to a commit, disable submodules and Git LFS, reject unsafe redirects and local/private destinations, and bind only a per-run workspace.
4. A real egress enforcement point that permits only the approved Git origin and one model gateway. Docker bridge mode alone is not an allowlist. Block metadata and private network destinations and preserve TLS checks.
5. Brokered, least-privilege, non-production gateway authentication. Do not pass reusable provider, GitHub, Soyuz, Cloudflare or Senku credentials into a shell-capable worker.
6. Bounded JSONL capture integrated into durable outcomes and the existing callback outbox. Missing, truncated, malformed, aborted, provider-error, timed-out or ambiguous output must not become success.
7. Cancellation, timeout, restart reconciliation, slot retention, cleanup, and callback replay tests for the Pi workload type.
8. A secured ARM64 host access path and a reviewed resource budget. Record memory under the actual Pi run before setting production-like limits.

Production Queue polling, Senku inference and publication remain out of scope. There is no push token or publication path in this worker.

## Current ARM64 host disposition

The 2026-11-26 anonymized read-only host review is recorded in [ARM64 host compatibility](ARM64-HOST-COMPATIBILITY.md), with evidence in [ARM64 host validation](ARM64-HOST-VALIDATION.md). The operator reports replacing a prior default credential; no credential was requested or accessed. The reviewed account had rootful Docker-group authority, and SSH authentication policy could not be established from the available `sshd -T` output. Other containers were running and left untouched. No Rusty-Fossil image or container was created. The repository-preparation library has not performed a checkout or task execution. Cloudflare metadata was reviewed read-only; no credential values were read and no inference request was sent. Non-production broker/limits remain unverified, and a gateway-only worker route is not configured.
