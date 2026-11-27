# ARM64 host compatibility reference

**Updated:** 2026-11-27
**Scope:** Representative Raspberry Pi host profile and generalized deployment boundary; host identity and access details are omitted
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

The observed Docker daemon is rootful. Treat Docker access as host-level authority. The intended deployment assumes the host is on a controlled, local-only trusted LAN; that assumption must be confirmed through effective routing and firewall reachability, not inferred from a wildcard socket bind. The supervisor receives the Docker Unix socket; the controller and Pi worker do not. Any Docker TCP API must remain within the trusted LAN and unavailable from the worker network and Cloudflare Tunnel routes.

The intended Cloudflare pattern uses a `cloudflared` connector that initiates an outbound tunnel and exposes only explicitly configured host services. A read-only reference deployment showed the connector running as a container without a published host port; its application service used a separate Docker bridge. The actual ingress route configuration and effective reachability were not verified. Cloudflare Tunnel does not proxy server-initiated API requests by itself; those use the host's default route unless separately routed through Cloudflare Gateway/WARP. Do not expose Docker Engine, SSH, supervisor IPC, or administrative services through Tunnel. See [Cloudflare Tunnel fundamentals](https://developers.cloudflare.com/tunnel/concepts/) and [private network routing](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/private-net/).

No repository checkout or model inference was performed. The repository-preparation library is not wired into the supervisor. The broker prototype is not integrated or tested against a live provider or host egress policy. Its request, rate, byte, output-token and time bounds are technical usage controls; per-run monetary spending ceilings are explicitly out of scope. Zero data retention is not a project requirement and will not be enabled. No Pi workload, cancellation, restart, callback, cleanup, or under-load resource test was run.

## Deployment gate

Keep Pi execution disabled until the supervisor integration, repository and workspace isolation, broker token lifecycle, enforced worker egress, cancellation/recovery tests, and operator-reviewed trusted-LAN/Tunnel boundary are complete. The representative hardware profile and ARM64 builds are not native task validation. No host configuration or unrelated workload was changed during the read-only inventory.
