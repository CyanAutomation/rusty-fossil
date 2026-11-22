# Agentlab validation record

**Date:** 2026-11-22
**Phase:** 2C-A
**Outcome:** Native coding-worker smoke test not run; preflight blocked deployment

## Evidence

| Check | Result |
| --- | --- |
| Confirm selected Agentlab host and inspect hardware | Raspberry Pi 4 Model B Rev 1.5, ARM64, four cores |
| OS, kernel, memory, swap and storage | Debian 13, Pi kernel 6.18.50, 3.7 GiB RAM, 2 GiB zram, SD-card-backed ext4; see [compatibility report](AGENTLAB-COMPATIBILITY.md) |
| Docker, cgroups and seccomp | Docker 29.9.2/API 1.56, rootful, cgroup v2 and required controllers reported, seccomp available |
| Existing workloads | Two unrelated containers observed and left untouched |
| SSH and Docker authority review | Deployment blocked: terminal warned the default `pi` SSH password was unchanged; `pi` is in the rootful Docker group |
| Approved repository and model route | Not supplied/configured; outbound HTTPS and proxy policy not verified |
| Pi image build | CI job added; not run in this worktree. Local Docker Engine socket is unavailable to this development environment. |
| Pi task, memory measurement, cancellation and restart recovery | Not run; no Pi supervisor workload type or secure egress path exists in this branch |
| Cleanup | No Rusty-Fossil Docker resources were created, so none required cleanup |

## Release gate

The native proof required by the task is outstanding. Resume only after the host owner secures SSH and reviews a dedicated execution identity, a non-production gateway broker and destination policy are in place, and a specific approved disposable repository is selected. Then complete the durable supervisor workload integration and its lifecycle tests before starting any Pi container. Measure host and container memory during the bounded run and verify the full run identity, image digest, workspace identity, egress policy, result persistence, callback, and cleanup evidence.

An ARM64 build or the current hardware inventory is not a successful Pi coding-task validation.
