# Agentlab compatibility snapshot

**Updated:** 2026-11-27
**Scope:** Read-only native ARM64 preflight
**Disposition:** Pi execution remains disabled

Agentlab is a Raspberry Pi 4-class ARM64 host. A read-only preflight observed four CPUs, approximately 3.7 GiB total RAM with 3.2 GiB available, 2 GiB swap, and approximately 34 GiB free on the root filesystem. Docker Engine 29.2.9 and Compose 5.6.1 were available. Docker reported cgroup v2 and the built-in seccomp profile. These are point-in-time observations, not resource limits or proof that a Pi worker fits safely under load.

Three containers were running at the time of the preflight. No container with the `rusty-fossil` Compose project label was present. No image was pulled or built, no container or network was created, and no unrelated workload was changed.

The host has a rootful Docker daemon and a Docker Unix socket. A read-only listener check found a candidate Docker Engine TCP listener bound beyond loopback. The operator confirmed that listener is expected. We did not connect to it or independently verify transport, authentication or effective reachability. No daemon, firewall, account or network settings were changed.

See [Agentlab validation](AGENTLAB-VALIDATION.md) for the separate Cloudflare and canary disposition. Exact host/account identifiers, private addresses, credentials, and unrelated container names are intentionally omitted.
