# ARM64 host validation record

**Latest inventory:** 2026-11-26
**Phase:** 2C-A
**Outcome:** Read-only Agentlab preflight completed; no Pi workload run

| Check | Result |
| --- | --- |
| Host identity and SSH | Termius connected to the Agentlab host as `pi`; authentication method is not established. Effective SSH policy could not be read as `pi`, and the standard `authorized_keys` file was absent. |
| Owner-reported password change | Acknowledged as remediated per the host owner. No credential value was accessed. |
| Platform | Raspberry Pi 4 Model B Rev 1.5, Debian 13, ARM64. |
| Docker profile support | Rootful Engine 29.9.2, cgroup v2, `overlayfs`, and built-in seccomp support reported. |
| Docker authority | `pi` belongs to the rootful `docker` group; the socket is `root:docker` mode `0660`. The owner expects Rusty-Fossil to run as `pi`. Systemd examples now mask the socket from the controller process and grant it to the supervisor only; they have not been installed or verified on the host. |
| Existing services | Three healthy containers were observed; an earlier inventory recorded two. All were left untouched. |
| Capacity | 3.20 GiB memory available, 2.00 GiB swap free, and 33.3 GiB root-filesystem space available at the check. No workload peak measurements were taken. |
| Clock | Synchronized at the check. |
| Repository preparation | No remote fetch or checkout was performed. |
| Gateway and egress | Read-only Cloudflare metadata found the existing default gateway has request logging enabled and no spend limit. It was not used. No broker, non-production inference, or enforced egress route was tested. |
| Pi task lifecycle | No worker image was deployed and no coding task, cancellation, restart, callback, or cleanup test was run. |
| Host changes | None. |

The controller/supervisor unit split is a proposed configuration, not verified host state. Do not start a coding worker until the software runtime, gateway isolation, and unit restrictions have passed their automated checks and an operator verifies the installed service sandbox. See [Agentlab compatibility](AGENTLAB-COMPATIBILITY.md) and the [Phase 2C-A operations guide](PHASE-2C-A-OPERATIONS.md).
