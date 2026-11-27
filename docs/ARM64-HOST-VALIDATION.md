# ARM64 host validation record

**Latest inventory:** 2026-11-27
**Phase:** 2C-A
**Outcome:** Read-only Raspberry Pi host preflight completed; no coding workload run

| Check | Result |
| --- | --- |
| Remote access | An authorized remote session was used for read-only checks. Authentication method and effective SSH policy were not established; host and account identifiers are omitted. |
| Platform | A Raspberry Pi 4-class ARM64 host with Debian 13-class Linux was observed. Exact host, kernel, and patch identifiers are omitted. |
| Docker | Rootful Docker Engine 29.x, cgroup v2, `overlayfs`, and built-in seccomp support were reported. These properties need rechecking on any deployment target. |
| Capacity | The host class has approximately 4 GiB RAM and SD-backed storage. Exact readings and safe under-load limits were not recorded here. |
| Docker authority | Docker access is host-level authority. A candidate non-loopback Docker Engine TCP listener was observed. The operator confirmed that listener is expected. No connection or reachability test was made; its effective transport and authentication controls remain unverified. |
| Cloudflare Tunnel | A `cloudflared` connector may expose only explicitly configured host services through outbound-initiated tunnels. The exact routes were not verified. Do not expose Docker Engine, SSH, supervisor IPC, or administration endpoints. Host-originated requests use the default route unless separately routed through Cloudflare Gateway/WARP. |
| Worker network | Host LAN trust does not extend to the Pi worker. Enforce and test worker-to-broker-only egress independently of the host's trusted-LAN and Tunnel routes. |
| Other workloads | No unrelated workload was changed. Reserve memory, CPU, and storage for existing services before any pilot. |
| Repository preparation | No remote fetch or checkout was performed. |
| Model access | No inference request was sent. The broker now records observed estimated cost without a spending, request-count/rate, or inference-token budget; the coordinator has a durable event-enqueue seam, but no runtime caller currently delivers it to Soyuz. An approved data-handling route remains unverified. |
| Pi lifecycle | No worker image or coding task was deployed; cancellation, restart, callback, cleanup, and resource-under-load checks were not run. |
| Host changes | None. The inventory was read-only. |

## Release gate

Native validation remains outstanding. Complete the software controls and automated lifecycle/security coverage first. Before a pilot, an operator must verify host identity, SSH policy, the trusted-LAN boundary, Cloudflare Tunnel service routes, Docker socket/API reachability, worker egress enforcement, resource headroom, and protection of unrelated workloads. Do not treat ARM64 CI builds or this generalized preflight as evidence of a successful Pi coding task.
