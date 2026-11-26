# Agentlab validation record

**Inventory date:** 2026-11-22

**Review update:** 2026-11-26
**Phase:** 2C-A
**Outcome:** Native coding-worker smoke test not run; preflight blocked deployment

## Evidence

| Check | Result |
| --- | --- |
| Confirm selected Agentlab host and inspect hardware | Raspberry Pi 4 Model B Rev 1.5, ARM64, four cores |
| OS, kernel, memory, swap and storage | Debian 13, Pi kernel 6.18.50, 3.7 GiB RAM, 2 GiB zram, SD-card-backed ext4; see [compatibility report](AGENTLAB-COMPATIBILITY.md) |
| Docker, cgroups and seccomp | Docker 29.9.2/API 1.56, rootful, cgroup v2 and required controllers reported, seccomp available |
| Existing workloads | Two unrelated containers observed and left untouched |
| Password remediation | Host owner reports the previous default `pi` password has been changed to a stronger password; no password was requested or accessed. The remediation is not independently verified from the host. |
| Host access from this environment | Agentlab is not accessible from this cloud environment. No host connection or recheck was performed. |
| SSH and Docker authority review | Current SSH authentication policy and Docker group/socket access were not rechecked. The historical 2026-11-22 inventory found `pi` in the rootful Docker group. No dedicated supervisor identity or restricted socket access was verified. |
| User-designated pilot repository | `https://github.com/CyanAutomation/somatic`, ref `main`, observed at `0d02873b71256892b4ec8026a3b0e2a5ab6aaa91`; selected from GitHub metadata on 2026-11-26. No checkout or task execution was performed. |
| Cloudflare AI Gateway inventory | Read-only Cloudflare inspection found a gateway named `default`. Its production status, provider/model, access policy, spending controls and broker suitability were not established; no credentials were read and no inference request was sent. |
| Approved non-production model route | Not identified/configured; outbound HTTPS and proxy policy not verified |
| Pi image build | CI job added; not run in this worktree. Local Docker Engine socket is unavailable to this development environment. |
| Pi task, memory measurement, cancellation and restart recovery | Not run; no Pi supervisor workload type or secure egress path exists in this branch |
| Cleanup | No Rusty-Fossil Docker resources were created, so none required cleanup |

## Release gate

The native proof required by the task is outstanding. Agentlab is inaccessible from this cloud environment, so no host connection or recheck was performed. The owner-reported password remediation does not verify current SSH or Docker access policy. The user designated `CyanAutomation/somatic` as the pilot repository; its `main` ref was observed at the commit above, but no checkout was performed. Native validation requires an environment with authorized Agentlab reachability, a reviewed dedicated execution identity and restricted Docker socket access, and a non-production gateway broker with enforced destination policy. The supervisor still rejects Pi launch identities before Docker create. Complete the repository, gateway, result and lifecycle integrations before starting any Pi container. Measure host and container memory during the bounded run and verify the full run identity, image digest, workspace identity, egress policy, result persistence, callback, and cleanup evidence.

An ARM64 build or the current hardware inventory is not a successful Pi coding-task validation.
