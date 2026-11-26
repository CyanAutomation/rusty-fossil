# ARM64 host compatibility report

**Latest check:** 2026-11-26
**Access method:** Existing authorized SSH session; read-only inventory
**Result:** ARM64 Docker host confirmed; coding-worker deployment gate remains closed
**Privacy:** Host name, account names, precise resource measurements, network identifiers, and account-specific provider metadata are intentionally omitted.

## Host inventory

| Property | Observation |
| --- | --- |
| Host identity | Anonymized ARM64 Linux host |
| Hardware | ARM64 Linux host; model and form factor omitted |
| OS and kernel | Linux; exact distribution and kernel version withheld |
| Capacity | Memory and storage were sampled; safe coding-worker headroom has not been established under load |
| Docker | Rootful ARM64 daemon; cgroup v2; `overlayfs`; seccomp support reported |
| Docker socket | Available to an interactive operator account through its Docker group membership |
| SSH authentication policy | Effective password/key policy could not be established from the available `sshd -T` output |
| Other workloads | Unrelated containers were running and left untouched; exact count and measurements omitted |
| Clock | Synchronized at inventory time |

The inventory does not establish a safe runtime resource limit. No Pi model task, repository checkout, Node process, gateway request, or memory/IO stress run was performed. Storage health and sustained write endurance were not measured.

## Security and feasibility

The host owner reported that a previous default credential on the interactive account had been replaced with a stronger credential. No password or credential value was requested or accessed. The remediation is owner-reported.

The Docker daemon is rootful, and the interactive operator account has Docker-group authority. That provides host-root-equivalent control. A dedicated supervisor service identity and restricted Docker socket access were not verified. Current SSH authentication policy also remains unknown. These are deployment blockers for a shell-capable coding workload.

Cloudflare metadata was inspected read-only. No credential values or account-specific identifiers were retained, no inference request was sent, and a non-production broker, effective request/spending limits, and enforced destination policy were not established. No worker gateway route is configured.

## Repository and workspace

The repository-preparation library has a single explicitly configured public pilot repository. It resolves branch references through a hostname-restricted HTTPS CONNECT proxy, verifies the fetched commit, rejects unsafe tree entries, and bounds checkout size. No remote fetch or checkout was performed, and the library is not wired into the supervisor. A dedicated unprivileged repository-preparation identity remains required for integration.

## Changes made on the host

None. Only read-only inventory commands were run. No accounts, SSH configuration, firewall rules, Docker settings, images, containers, volumes, networks, files, or provider credentials were changed or accessed.
