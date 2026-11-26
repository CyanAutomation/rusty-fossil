# ARM64 host validation record

**Latest inventory:** 2026-11-26
**Phase:** 2C-A
**Outcome:** Read-only host and Cloudflare checks completed; no coding-worker test run
**Privacy:** Host name, account names, precise resource measurements, network identifiers, and provider metadata identifiers are omitted.

## Evidence

| Check | Result |
| --- | --- |
| Authorized connection | An SSH session to the designated ARM64 test host was established and used for read-only checks. Host and account identifiers are withheld. |
| Hardware and platform | ARM64 Linux with Docker was confirmed; exact hardware model, OS release, and kernel are omitted. |
| Capacity | Memory, swap, and storage were checked. No workload measurements were taken, so usable limits remain unestablished. |
| Docker isolation | Rootful daemon, cgroup v2, `overlayfs`, and seccomp support were observed. |
| Docker authority | The interactive account has Docker-group access to the rootful daemon. A restricted socket and dedicated supervisor identity were not verified. |
| SSH authentication | `sshd -T` did not return the effective policy values needed to establish whether password authentication is enabled or keys are required. |
| Credential remediation | The host owner reports replacing a prior default credential. No password or credential was requested or accessed; the report is owner-provided. |
| Existing workloads | Unrelated containers were running and left untouched; exact count and resource details are omitted. |
| Clock | Synchronized at recheck. |
| Repository preparation | The library has an exact operator-configured pilot URL. No Git network request or checkout was made. |
| Model-access controls | Cloudflare metadata was inspected read-only. No credential values or account identifiers were retained, and no inference request was sent. A non-production broker, effective limits, and enforced egress remain unverified. |
| Local repository-preparation tests | Tests cover URL/ref policy, canonical request binding, commit parsing, destination filtering, tree bounds, and safe cleanup. They do not test a remote checkout or native task execution. |
| Pi image and task | No image was deployed; no Pi invocation, gateway inference, coding task, cancellation, or restart test was run. |
| Host changes | No Rusty-Fossil resources or host configuration were changed. |

## Release gate

Native validation remains outstanding. Rootful Docker-group access would give a coding runtime host-level Docker authority. SSH authentication policy and service-account/socket separation remain unverified. The software also lacks supervisor integration for repository preparation, an enabled Pi Docker profile, brokered model access, and enforced worker egress.

Complete the software controls and automated lifecycle/security coverage before an operator-reviewed host configuration change. Do not treat this inventory or an ARM64 build as successful native task validation.
