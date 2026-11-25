# Phase 2B supervisor operations

Phase 2B is a synthetic-only execution capability. It does not consume Queue messages by itself, and the production `rusty-fossil` binary does not construct the coordinator or supervisor IPC client. An explicitly configured non-production runtime may attach `SupervisorIpcClient`; no local command can create a Soyuz run or launch a worker without the durable handoff and canonical Soyuz checks.

## Host and Docker requirements

- Linux AMD64 or ARM64 with Docker Engine 20.10 or newer (Engine API v1.41 minimum), cgroup memory, CPU quota and PID controls enabled, and the Docker default seccomp profile enabled. `fossil-supervisord` negotiates the Engine API and refuses daemons that do not report these required controls.
- The host clock must remain synchronized. Runtime deadlines use Docker's persisted `StartedAt` wall-clock value so a supervisor restart does not reset the deadline.
- A trusted Docker Engine installation and the fixed synthetic worker image published as `ghcr.io/cyanautomation/rusty-fossil-synthetic-worker@sha256:<digest>`. The supervisor never pulls an image; provision the digest-pinned image before starting it. The included Dockerfile builds `linux/amd64` and `linux/arm64` images.
- Rusty-Fossil and the supervisor must share the same private SQLite file (`fossil.sqlite3`) and run under the same Unix identity. The database and parent directories contain task metadata and must remain mode 0700/0600.
- A private Unix socket parent directory (0700), socket (0600), Soyuz token file (0400), and IPC token file (0400). Secret files must be owned by the supervisor service account so that mode 0400 still permits it to read them. The IPC token must contain 32–256 visible ASCII bytes. Do not put token values in environment variables, command arguments or logs.

The Docker Engine socket grants substantial host authority. Give access only to the dedicated supervisor account. Membership in the rootful `docker` group is effectively root-equivalent; protect the host and that account accordingly. A rootless Docker socket reduces daemon privilege but still grants the supervisor control of that user's containers and files; use a dedicated account and verify its cgroup limits. Do not mount the Docker socket into the controller container.

The callback outbox is capped at 1,000 pending or blocked rows. Intake already stops at 999. SQLite also enforces the reserved slot during callback insertion: heartbeat and other optional event callbacks at 999 return a capacity-suppressed result, which increments a supervisor counter and does not stop the workload. An admitted active run can persist its mandatory terminal callback as row 1,000. The local recovery journal has an absolute 90-day expiry from canonical Soyuz queue creation; callback payloads and execution descriptors are removed at expiry and cannot be recreated by delayed Queue replay. Until then, callback rows are not deleted to recover capacity. Restore callback delivery or resolve blocked entries through the approved operator process.

The image runs as UID/GID 65532 with a read-only root filesystem, no network, no capabilities, no-new-privileges, default seccomp, no host namespaces, ports, devices, bind mounts or volumes, one bounded `/tmp` tmpfs, capped JSON logs, and no automatic restart. Default limits are 500 millicores, 256 MiB memory with no swap, 32 PIDs, 16 MiB tmpfs, 15 minutes, 5 seconds graceful stop, 30 seconds Soyuz-outage grace and 64 KiB captured output. `SupervisorLimits` validates any overrides within the supported hard bounds; a Docker host without required cgroup controls is rejected before the socket is served. The maximum normal runtime remains far below the 90-day persistence deadline.

Docker references: [Engine API](https://docs.docker.com/reference/api/engine/), [SDK guidance](https://docs.docker.com/reference/api/engine/sdk/), [Engine security](https://docs.docker.com/engine/security/), [container run controls](https://docs.docker.com/engine/containers/run/) and [rootless mode](https://docs.docker.com/engine/security/rootless/).

## Configure and start

Copy [`fossil-supervisord.env.example`](../fossil-supervisord.env.example) to a root-managed configuration directory and replace the placeholder image digest, worker ID, Soyuz origin and secret-file paths. Obtain the worker token through the existing authorized Soyuz process; do not reuse the Cloudflare or Senku token. Set the image digest only after the image is built and inspected for the host architecture.

Create the private state and runtime directories for the dedicated service account, then install the environment file so only that account and root can read it. Set up `FOSSIL_SUPERVISOR_STATE_DB` to the exact `fossil.sqlite3` file used by the controller. Start the `fossil-supervisord` executable under a service manager with restart-on-failure, SIGTERM forwarding, a finite stop timeout above the container grace period, and no Docker socket access for the controller service. `fossil-supervisord` checks configuration, the API version and host resource controls before creating its private socket.

When stopping or upgrading the supervisor, send SIGTERM through the service manager. It stops accepting IPC, persists an `execution_uncertain` stop intent for any in-flight launch, waits for the exact container to stop and clean up, and exits after the active work drains. If Docker or the state store is unavailable and it cannot finish within 60 seconds, it exits with an error while retaining the durable stop intent; restart the service after the dependency is restored. A service restart may resume only a persisted, identifiable launch; ambiguous create/start state remains fenced for review. Do not delete the SQLite file or Docker state as a shutdown shortcut.

The included env file is a configuration example, not a complete service-manager policy. Provision runtime directories, file ownership, credentials and socket permissions through the host's existing secret and service-management system.

## Run the Docker integration suite

The ordinary workspace tests use mocks and do not need Docker. To run the real Engine suite on a trusted Linux host with Docker access, build an image for the host architecture and build the test-support supervisor executable:

```sh
docker build --platform linux/arm64 -f Dockerfile.synthetic-worker \
  -t rusty-fossil-synthetic-worker:test .
cargo build -p fossil-supervisor --features test-support --bin fossil-supervisord --locked
FOSSIL_SUPERVISOR_TEST_IMAGE="$(docker image inspect --format '{{.Id}}' rusty-fossil-synthetic-worker:test)" \
  cargo test -p fossil-supervisor --features test-support --test docker_engine --locked -- --ignored
FOSSIL_SUPERVISOR_TEST_IMAGE="$(docker image inspect --format '{{.Id}}' rusty-fossil-synthetic-worker:test)" \
FOSSIL_SUPERVISORD_BIN="$PWD/target/debug/fossil-supervisord" \
  cargo test -p rusty-fossil --test synthetic_execution_engine --locked -- --ignored
```

For AMD64, use `--platform linux/amd64`; for ARM64, use `--platform linux/arm64`. Prefer a native matching Engine for lifecycle execution. The CI workflow builds an OCI image for both architectures and runs integration tests on its Linux Docker runner. The Linux target matrix cross-builds both `rusty-fossil` and `fossil-supervisord` for AMD64 and ARM64. Those checks are not a substitute for Raspberry Pi hardware validation.

The ignored real-Engine suite uses ephemeral SQLite databases, mock Soyuz, an immutable local Docker image ID, fresh UUIDv7 run IDs and labels scoped to each run. It covers success, nonzero exit, timeout, cancellation while running, bounded output truncation and abrupt worker termination. The actual coordinator/`fossil-supervisord` process tests exercise the Queue fixture, claim/start gate, durable handoff, duplicate delivery, crash before launch intent, create/start boundaries, running-container restart, post-exit terminal persistence, post-removal cleanup, stable callback replay, cancellation and slot release. A separate case verifies SIGTERM persists `execution_uncertain` and cleans up. All five process tests and six Docker lifecycle tests have passed locally against a native ARM64 Docker Engine. Tests use isolated SQLite and mock Soyuz only. Cleanup checks the exact run, managed, worker, claim, start and workload labels and full container ID before removing that exact ID; it never prunes or touches unrelated workloads.

The `test-support` supervisor build has marker barriers for deterministic process termination before/after launch intent, create request, Docker create, identity persistence, start request, Docker start, running monitor, Docker exit, terminal persistence and Docker removal. The actual-process suite verifies fail-closed handling when `create_requested` is durable but Docker has no matching container, and reattachment/finalization at later boundaries. It does not simulate every lost API response while the process remains alive or all malformed Docker identity/error responses; do not interpret it as a complete Docker daemon fault matrix.

## Recovery and operator response

- **Supervisor or controller restart:** The supervisor's five-second recovery loop reads nonterminal launch rows and inspects only the recorded deterministic name. It verifies the stored container ID, image, labels and security profile before reattaching. A running worker is monitored with its Docker `StartedAt`; an exited worker is finalized from Docker state and bounded logs.
- **Docker daemon interruption:** Keep the launch row and execution slot intact. The supervisor fails closed for new launches. After the daemon returns, restart or allow the recovery loop to inspect the exact container. If Docker cannot prove the identity or terminal state, the row becomes `manual_review_required`; do not start a replacement.
- **Lost create/start response:** `create_requested` and `start_requested` remain durable. Reconciliation searches the same name and labels. The supervisor never sends a second create after the durable create fence.
- **Cancellation:** A fresh canonical Soyuz read must match run, worker and claim fence and request cancellation. The supervisor persists `stop_requested(cancelled)`; the monitor sends a graceful stop, force-stops after the configured grace and confirms exit before persisting `/cancelled`. If Docker already recorded an exit before the stop intent, that observed outcome wins.
- **Timeout:** Deadline checking uses `StartedAt` and a fixed configured limit, independent of output and heartbeat success. Timeout persists a stop intent, stops the exact container and writes a failure callback. Host clock adjustment can change when a wall-clock deadline fires.
- **Soyuz outage:** New create and start actions require a fresh canonical read. A running worker continues to be inspected during a bounded 30-second outage grace, then is stopped and recorded as `execution_uncertain`. The existing callback outbox retains the stable event ID for controller-owned delivery.
- **Callback or cleanup pending:** Do not clear the outbox row, launch row, or execution slot manually. Restore Soyuz/Docker connectivity and allow normal replay/reconciliation. A pending terminal callback does not make the container safe to delete before its result is durably recorded.
- **Ambiguous execution:** Check the durable row, exact Docker name/ID, labels, creation/start timestamps, daemon events/logs and current Soyuz fence. Keep the slot fenced until evidence resolves the state. Do not edit SQLite directly to authorize another launch.

For read-only inventory, list only matching labeled containers:

```sh
docker ps -a --filter label=io.rusty-fossil.managed=true \
  --format '{{.ID}} {{.Names}} {{.Status}} {{.Label "io.rusty-fossil.run-id"}}'
```

Before manual cleanup, match the run ID, full stored container ID, all launch labels and a terminal or manually resolved SQLite state. Prefer the supervisor's idempotent cleanup path. Do not run `docker system prune`, remove by broad name prefix, or force-remove a running container to clear a blocked slot.

Docker retains capped logs only while the disposable container exists; the supervisor reads at most the configured output bound and sends no raw stdout/stderr to Soyuz. SQLite retains the immutable request descriptor and callback metadata under the existing store retention policy. Restrict and encrypt the state volume when task prompts require confidentiality. No artifacts or repository contents are produced in this phase. Supervisor metrics are emitted as a fixed-cardinality structured log snapshot every 60 seconds; they are not exposed by controller `/metrics` while production execution wiring remains disabled. No run IDs or task data are metric labels.

## Limitations before Phase 2C

The synthetic scenarios prove lifecycle plumbing only. Production Queue polling, callback replay, and supervisor-client construction remain disabled in `main`; the readiness endpoint does not claim production coding readiness. A separate Pi CLI image and invocation foundation exists, but it is not connected to the supervisor and cannot execute an authorized task. There is no repository checkout, Git operation in a workload, live Senku call, GitHub publication, multi-host SQLite coordination, automatic cross-host takeover, or arbitrary Docker API.

AMD64/ARM64 image builds are not Raspberry Pi hardware validation. Before any Pi rollout, verify the exact Pi model and OS, Docker Engine and cgroup configuration, seccomp, memory headroom under controller load, storage wear/capacity, image architecture and shutdown recovery on that hardware. Phase 2C-A should add the Pi coding-agent runtime only behind this same proof-gated supervisor, first using a disposable repository and non-production credentials. Keep the synthetic runtime at 15 minutes (current maximum 30 minutes); give coding jobs a separately bounded timeout, with separate cancellation grace, Soyuz outage grace, supervisor shutdown grace and container recovery deadline. Do not change Phase 2B shutdown semantics implicitly. Before enabling multi-hour jobs, decide whether maintenance uses drain mode, refuses new launches, allows controlled worker continuation, or invokes operator cancellation/hard stop. See [ADR 0012](DECISIONS/0012-phase-2c-long-running-execution-policy.md).
