# Agentlab compatibility report

**Inventory date:** 2026-11-22
**Access method:** Termius interactive SSH; read-only inventory only
**Result:** Raspberry Pi ARM64 Docker host confirmed; workload deployment gate not met

## Host inventory

| Property | Observed result |
| --- | --- |
| Host identity | Agentlab, as selected in the existing Termius connection |
| Hardware | Raspberry Pi 4 Model B Rev 1.5, Cortex-A72 |
| Architecture | `aarch64` / ARM64, four CPU cores |
| OS and kernel | Debian GNU/Linux 13 (trixie), kernel `6.18.50+rpt-rpi-v8` |
| Memory | About 3.7 GiB total; about 421 MiB used and 3.3 GiB available at inventory time |
| Swap | 2 GiB zram, unused at inventory time |
| Storage | 59 GiB ext4 root filesystem on the SD-card block device; about 23 GiB used and 34 GiB available |
| Docker | Engine 29.9.2, API 1.56, Linux ARM64, rootful daemon |
| Cgroups | cgroup v2 with systemd driver; CPU, cpuset, IO, memory and PID controllers reported |
| Seccomp | Docker reports built-in seccomp support |
| Runtime/storage driver | `runc`, `overlayfs` |
| Docker socket | Unix socket owned by `root:docker`, mode `0660`; interactive `pi` account belongs to `docker` group |
| Clock | NTP synchronized; host timezone Europe/London |
| Network | Docker registry hostname resolved through DNS (IPv6). Outbound HTTPS, proxy requirements and gateway reachability were not tested. |
| Existing workloads | Two unrelated containers were running. A point-in-time sample showed roughly 102 MiB / 8.6% CPU / 6 PIDs for one and 61 MiB / idle CPU / 11 PIDs for the other. Neither was touched. |

Storage is an SD card, so sustained write volume and endurance should be considered when sizing logs, SQLite and temporary workspaces. The inventory did not check card health or benchmark IO.

## Security and feasibility

The SSH terminal displayed the warning that the default password for the `pi` account had not been changed. That same account is in the `docker` group on a rootful daemon, which gives it host-root-equivalent control over containers. This is a deployment blocker for an untrusted coding workload. No password, SSH, firewall, Docker, or account settings were changed.

The RAM snapshot suggests capacity may be available for a controller and one bounded worker, but it does not establish a safe Pi workload limit. Pi, repository checkout, Node, model activity and the existing containers were not measured together. The Phase 2B synthetic worker's 256 MiB limit must not be reused for Pi without measurement.

No gateway endpoint, broker, approved disposable repository, or non-production gateway credential was available for validation. DNS resolution alone does not establish safe egress. Do not attach unrestricted outbound networking to the worker.

## Changes made on the host

None. Only inventory commands were run. Existing containers, images, volumes, networks, files, Docker configuration, accounts, and firewall settings were left unchanged.
