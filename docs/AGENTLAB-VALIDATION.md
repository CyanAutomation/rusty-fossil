# Agentlab validation

The 2026-11-26 preflight was read-only. It verified the intended Agentlab host, a working Termius SSH session as `pi`, ARM64 hardware, Docker Engine 29.9.2, Docker Compose v5.6.1, cgroup v2, seccomp support, synchronized time, and current memory/storage headroom. It also verified `pi` UID/GID 1000:1000 and Docker GID 985, found three running containers (the earlier inventory reported two), and confirmed no dedicated Rusty-Fossil account. SSH authentication policy could not be established from the `pi` account.

The host owner reports that the previous default password was changed. No password was requested or accessed. No account, daemon setting, network rule, image, container, volume, or file was changed.

No Rust binaries or Pi image were deployed. No repository checkout, gateway inference request, coding task, cancellation, restart, callback, resource observation under load, or cleanup test was performed. The existing Cloudflare default gateway metadata was inspected read-only; request logging is enabled and spend limits are disabled, so it was not used. The software still rejects Pi launches, and Agentlab execution remains gated on the remaining software work and operator verification of the proposed [Docker Compose deployment](../docker-compose.yml).
