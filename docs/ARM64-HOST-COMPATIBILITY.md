# ARM64 host compatibility reference

**Updated:** 2026-11-26
**Scope:** Representative Raspberry Pi host profile; host identity and access details are omitted
**Disposition:** Pi coding-worker deployment remains gated

## Representative platform

The following characteristics describe a representative Raspberry Pi 4-class test host. They are useful for planning the ARM64 worker, but do not certify any particular deployment or establish safe sustained workload limits.

| Property | Representative observation |
| --- | --- |
| Hardware | Raspberry Pi 4 Model B class; ARM64 (`aarch64`), four Cortex-A72 cores |
| Operating system | Debian 13 class Linux |
| Container runtime | Docker Engine 29.x; rootful daemon; cgroup v2; `overlayfs`; built-in seccomp support reported |
| Memory | Approximately 4 GiB |
| Storage | Approximately 60 GiB SD-backed root storage |
| Existing workloads | Other host workloads may be present; reserve capacity before a pilot |
| Clock | Synchronized during the inventory |

Exact kernel, daemon patch, capacity readings, host/account identifiers, access method, network details, and provider-account configuration are intentionally excluded. These values are indicative and must be rechecked on the target host before deployment.

## Security and readiness

A read-only remote session was established for the inventory, but its authentication method and effective SSH policy were not established. No credentials were requested or accessed. Host-specific account membership and Docker socket permissions are omitted from this repository report.

The observed Docker daemon is rootful. Treat Docker access as host-level authority. A dedicated supervisor identity and restricted socket boundary must be provisioned and verified by an operator before running a shell-capable coding worker. Do not infer that a container-level mount split limits the authority of an interactive account or of the trusted supervisor.

No repository checkout or model inference was performed. The repository-preparation library is not wired into the supervisor. The broker prototype is not integrated, has no dollar-spend ceiling, and has not been tested against a live provider or host egress policy. No Pi workload, cancellation, restart, callback, cleanup, or under-load resource test was run.

## Deployment gate

Keep Pi execution disabled until the supervisor integration, repository and workspace isolation, broker token lifecycle, cost controls, enforced egress, cancellation/recovery tests, and operator-reviewed host boundary are complete. The representative hardware profile and ARM64 builds are not native task validation. No host configuration or unrelated workload was changed during the read-only inventory.
