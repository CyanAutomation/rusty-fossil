# Agentlab validation

The 2026-11-26 preflight was read-only. It verified the intended Agentlab host, a working Termius SSH session as `pi`, ARM64 hardware, Docker 29.9.2, cgroup v2, seccomp support, synchronized time, and current memory/storage headroom. It also found three running containers (the earlier inventory reported two), rootful Docker-group access for `pi`, no dedicated Rusty-Fossil account, and an SSH policy that could not be established from the `pi` account.

The host owner reports that the previous default password was changed. No password was requested or accessed. No account, unit, daemon setting, network rule, image, container, volume, or file was changed.

No Rust binaries or Pi image were deployed. No repository checkout, gateway inference request, coding task, cancellation, restart, callback, resource observation under load, or cleanup test was performed. The existing Cloudflare default gateway metadata was inspected read-only; request logging is enabled and spend limits are disabled, so it was not used. The software still rejects Pi launches, and Agentlab execution remains blocked pending the software work and operator verification of the proposed [systemd deployment profile](../deploy/systemd/).
