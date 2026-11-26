# Phase 2C-A Pi worker operations

## Status

The branch contains a Pi 1.6.0 worker wrapper, a locked runtime dependency graph, a fixed multi-architecture Node base digest, and a Rust JSONL event classifier. The worker now reads a fixed-path payload file at `/run/fossil/task-prompt.txt`, with a 32 KiB limit and checks for read-only regular-file input; the supervisor does not yet stage or mount it. This prepares an immutable-payload handoff contract but does not provide durable prompt delivery. The supervisor terminal finalizer has a bounded Docker stdout parser path, and SQLite/outbox can persist Pi-specific result classes with a stable callback identity. CI tests the wrapper and builds the AMD64/ARM64 worker image with SBOM and provenance attestations. SQLite schema v7 persists the `PiCodingV1` workload/version, immutable image/profile identity, effective policy/deadline and opaque workspace identity. `fossil-workspace` now provides an exact pilot repository allowlist, bounded branch/ref and tree validation, hostname-restricted HTTPS Git transport, commit verification and private per-run checkout/cleanup. It has not been used for a remote fetch and is not wired into the supervisor. The record is prepared only for a patch task with `publishMode: none`; Pi image creation remains rejected by the supervisor, so the parser/result path is not reachable from a live Pi container.

This is still not an operational coding runtime. The supervisor accepts only its synthetic workload. It does not call the repository library, mount a repository, provide Pi configuration, contact a gateway, persist Pi event counters, or select this image. The durable Pi record is not permission to launch; do not launch the image as an execution fallback.

## Running the host services as `pi`

The host owner expects Rusty-Fossil to run as the existing `pi` account. Do not provision a separate host account for this deployment. Example units are in [`deploy/systemd`](../deploy/systemd/): both services run with `User=pi` and `Group=pi`; `fossil-supervisord` receives Docker socket access through the `docker` group, while the controller unit masks `/run/docker.sock` with `InaccessiblePaths` and has no Docker socket setting.

The `pi` account itself remains in the rootful Docker group. systemd's `SupplementaryGroups=` extends the account's supplementary groups, so the controller unit does not rely on group removal; its boundary is the service mount namespace and the absence of another Docker API endpoint. This process sandbox is not a separate Unix security principal. An interactive shell as `pi` and the trusted supervisor retain Docker authority. The host operator must verify that Docker does not listen on a TCP socket and that the installed controller unit cannot see or connect to the Unix socket.

The example units are not installed or enabled. Before installation, place the binaries and root-owned `EnvironmentFile`s at the paths named in the units, make required secret files readable only by `pi` (for example `root:pi` mode `0640`), and keep the SQLite state directory private to `pi`. Run `systemd-analyze verify` on the units, inspect the effective unit properties, and verify the controller's masked socket from inside its service sandbox. Do not start either service for coding workloads until the Pi launch path, repository preparation, broker, egress policy, and recovery tests are complete.

When durable prompt delivery is integrated, stage the canonical descriptor prompt as an atomically created per-run file under the private workspace root before any Docker create/start request. The file must be bounded, complete, read-only, readable by container UID `65532` (normally mode `0444` inside a parent directory private to `pi`), and mounted read-only at `/run/fossil/task-prompt.txt`. Persist the payload identity before Docker side effects, never put the prompt in environment, labels or command arguments, and never rewrite or replay it into a second container after ambiguous launch state. The wrapper's file reader alone does not implement these lifecycle guarantees.

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

The 2026-11-26 read-only host review is recorded in [Agentlab compatibility](AGENTLAB-COMPATIBILITY.md) and [Agentlab validation](AGENTLAB-VALIDATION.md), with the ARM64 inventory in [host compatibility](ARM64-HOST-COMPATIBILITY.md). The password change is owner-reported. SSH authentication policy could not be established from the `pi` account. Three healthy containers were observed; all were left untouched. No Rusty-Fossil image or container was created. The `pi` account has rootful Docker access; the new systemd examples are not installed or verified. The repository-preparation library has not performed a checkout or task execution. Cloudflare read-only metadata showed the existing default AI Gateway has request logging enabled and no spend limit; it was not used for inference. A dedicated non-production gateway, broker and worker egress policy remain unverified.
