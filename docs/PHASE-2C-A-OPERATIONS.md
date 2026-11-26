# Phase 2C-A Pi worker operations

## Status

The branch contains a Pi 1.6.0 worker wrapper, a locked runtime dependency graph, a fixed multi-architecture Node base digest, and a Rust JSONL event classifier. The worker reads a fixed-path prompt file and has a fixed OpenAI-compatible provider definition for `http://fossil-gateway:8787/v1`. It requires a bounded read-only run token at `/run/fossil/gateway-token` and explicit model pricing; it writes transient Pi configuration under `/tmp/pi-agent`. The `fossil-gateway` crate now implements a run-fenced HMAC token verifier, bounded OpenAI-compatible request policy, a fixed Cloudflare universal compatibility endpoint, content-free per-run request counters, a maximum of 10 requests per run per minute, TLS/redirect controls, timeouts, and bounded upstream responses. It requires a private state directory and database, and reads secret files without following symlinks. `docker-compose.pi.yml` adds a dormant `pi-pilot` broker profile on an internal worker network and the supervisor egress network. The supervisor does not mint tokens, attach workers to that network, or stage prompts; no worker can use this broker through the supervisor path. The broker enforces request count, rate, request-byte, output-token, response-byte, and time limits, but not a dollar spend ceiling. No live model request was made. The supervisor terminal finalizer has a bounded Docker stdout parser path, and SQLite/outbox can persist Pi-specific result classes with a stable callback identity. CI tests the wrapper and builds the AMD64/ARM64 worker image with SBOM and provenance attestations. SQLite schema v7 persists the `PiCodingV1` workload/version, immutable image/profile identity, effective policy/deadline and opaque workspace identity. `fossil-workspace` provides an exact pilot repository allowlist, bounded branch/ref and tree validation, hostname-restricted HTTPS Git transport, commit verification and private per-run checkout/cleanup. It has not been used for a remote fetch and is not wired into the supervisor. The supervisor still rejects Pi image creation, so the parser/result path is not reachable from a live Pi container.

This is still not an operational coding runtime. The supervisor accepts only its synthetic workload. It does not call the repository library, mount a repository, provide Pi configuration, contact a gateway, persist Pi event counters, or select this image. The durable Pi record is not permission to launch; do not launch the image as an execution fallback.

## Running the Docker Compose stack as `pi`

The root [`docker-compose.yml`](../docker-compose.yml) maps both containers to operator-configured numeric UID/GID values; the supervisor alone gets the Docker socket and its required group. The controller has no Docker socket mount, Docker group, or supervisor secret mount, and runs on an internal Compose network. Diagnostics are published on host loopback only. Compose-managed restart policies let Docker Engine bring the containers back after a daemon restart.

Both services may share a host UID and SQLite directory. The container split hides the Docker socket mount and credentials from the controller container, but it does not create a separate Unix security principal. Verify that the Docker daemon has no reachable TCP API. The controller network is marked internal, but that configuration is not a substitute for checking the host's daemon listeners and firewall.

For each target host, configure `.env` from [`.env.example`](../.env.example) with operator-approved service UID/GID values and the Docker socket's numeric group. Recheck those values and confirm `DOCKER_SOCKET_PATH` matches the intended socket before deployment. Create the state/runtime directories with the dedicated service identity and mode 0700. Put the authorized Soyuz worker token and a generated 32–256 byte visible-ASCII IPC token in `secrets/`, owned by that identity with mode 0400. Keep these files out of source control.

Set `FOSSIL_SUPERVISOR_IMAGE` to the fixed trusted GHCR synthetic-worker repository with a published SHA-256 digest and pull that exact image before startup; the supervisor never pulls it. The current `fossil-supervisord` only accepts synthetic scenarios. Validate and start the containers with:

```sh
docker compose config --quiet
# Replace the placeholder with the digest configured in .env.
docker pull ghcr.io/cyanautomation/rusty-fossil-synthetic-worker@sha256:REPLACE_WITH_64_HEX_DIGEST
docker compose build
docker compose up -d
docker compose ps
docker compose logs -f rusty-fossil fossil-supervisord
```

For normal stop/restart recovery, use `docker compose stop -t 90 fossil-supervisord` and `docker compose up -d fossil-supervisord`. `docker compose down` removes this project's containers and network but preserves the bind-mounted state and secret files. If upgrading from the previous Compose definition, back up the existing named `rusty-fossil-state` volume before switching to the bind-mounted state path; inspect and verify the copied SQLite database before retiring that volume.

## Update and rollback

Use a unique local image tag for each controller/supervisor build and keep the previous tag. Before an upgrade, stop both containers and back up the entire state directory. To roll back the binaries, set `RUSTY_FOSSIL_IMAGE` in `.env` to the previous tag and run `docker compose up -d --no-build`. Preserve the current state and secrets. SQLite migrations are forward-only; only restore an older state backup when no run is active and the prior binaries and database backup are a matching pair. Never use `docker compose down -v` or Docker-wide prune commands for rollback.

The supervisor's default bridge allows outbound connectivity and does not provide destination filtering. The disposable synthetic workers currently receive `network_mode: none`. Do not use Compose networking as the future Pi worker's egress control. Do not start coding workloads until the Pi launch path, repository preparation, broker, enforced egress policy, and recovery tests are complete.

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

The `docker-compose.pi.yml` overlay is configuration scaffolding for a future broker integration. Do not start `fossil-gateway-broker` from the `pi-pilot` profile as a usable worker service: the supervisor has no token issuer or revocation path, it does not attach workers to `pi_worker_internal`, and no cost ceiling or tested host egress policy is in place. A later deployment also needs an operator-provisioned dedicated gateway UID/GID, its private mode-0700 state directory, and mode-0400 credential/signing-key files owned by that identity. Compose-profile rendering tests validate the declared mounts and network membership; they do not validate live Docker routing or host firewall behavior.

Before connecting the image to the supervisor, implement and review all of these:

1. Extend the schema v7 Pi launch identity into an executable supervisor profile. The synthetic path must keep its existing resource policy and recovery behavior.
2. An explicit Pi image digest allowlist and a separately bounded Pi resource profile. Inspect image, labels, workload version, workspace identity, and the effective security profile before start and during recovery.
3. Host-side preparation of the exact Soyuz-authorized descriptor. Restrict the pilot to an approved HTTPS repository, resolve the ref to a commit, disable submodules and Git LFS, reject unsafe redirects and local/private destinations, and bind only a per-run workspace.
4. A real egress enforcement point that permits only the approved Git origin and one model gateway. Docker bridge mode alone is not an allowlist. Block metadata and private network destinations and preserve TLS checks.
5. Complete the broker integration: supervisor token issuance/revocation, worker network attachment, cost-based spend controls, and verified host egress policy remain open. The prototype authenticates run-scoped tokens, enforces request/count/byte/output-token/time/response bounds and sends only to the configured Cloudflare AI Gateway endpoint. Keep the Cloudflare gateway credential in the broker; the worker token is visible to Pi's shell tools and must be scoped to one run. Do not pass reusable provider, GitHub, Soyuz, Cloudflare or Senku credentials into a shell-capable worker.
6. Bounded JSONL capture integrated into durable outcomes and the existing callback outbox. Missing, truncated, malformed, aborted, provider-error, timed-out or ambiguous output must not become success.
7. Cancellation, timeout, restart reconciliation, slot retention, cleanup, and callback replay tests for the Pi workload type.
8. A secured ARM64 host access path and a reviewed resource budget. Record memory under the actual Pi run before setting production-like limits.

Production Queue polling, Senku inference and publication remain out of scope. There is no push token or publication path in this worker.

## Current ARM64 host disposition

The 2026-11-26 read-only ARM64 preflight is recorded in [host compatibility](ARM64-HOST-COMPATIBILITY.md) and [host validation](ARM64-HOST-VALIDATION.md). Host and account identifiers, exact capacity readings, and provider-account settings are omitted from these generalized records. No Rusty-Fossil image or container was created. The service identity, effective SSH policy, Compose socket boundary, approved non-production gateway, spend controls, and worker egress policy remain unverified. The repository-preparation library has not performed a checkout or task execution.
