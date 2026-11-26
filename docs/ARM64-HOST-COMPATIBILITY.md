# ARM64 host compatibility report

**Latest check:** 2026-11-26
**Host alias:** Agentlab
**Access:** Existing Termius SSH connection; read-only checks
**Disposition:** Pi coding-worker deployment remains gated

## Verified host state

| Property | Observation |
| --- | --- |
| Hardware and architecture | Raspberry Pi 4 Model B Rev 1.5; ARM64 (`aarch64`) |
| Operating system | Debian 13; Linux `6.18.50+rpt-rpi-v8` |
| Docker | Engine 29.9.2; rootful; cgroup v2; `overlayfs`; built-in seccomp profile reported |
| Docker socket | `/run/docker.sock`, mode `0660`, owned by `root:docker` |
| Interactive account | `pi`; member of the `docker` group and able to use the rootful daemon |
| Dedicated Rusty-Fossil account | Absent; the owner expects Rusty-Fossil to run as `pi` |
| Memory | 3.71 GiB total; 3.20 GiB available at the check |
| Swap | 2.00 GiB total and free at the check |
| Root filesystem | 58.0 GiB total; 33.3 GiB available at the check (filesystem block counts) |
| Running containers | Three healthy containers observed; the earlier inventory recorded two |
| Clock | `timedatectl` reported synchronized |

The container count differs from the earlier inventory. The running services were not changed. These point-in-time measurements do not establish safe sustained memory, CPU, storage-write, or thermal limits for a Pi coding task.

## SSH and credential status

An SSH session through the saved Termius connection succeeded as `pi`. This confirms that the configured connection works; it does not establish whether this session used a password, key, certificate, or another method. The standard `/home/pi/.ssh/authorized_keys` file was absent. Reading the effective `sshd -T` configuration failed because the `pi` account could not read `/etc/ssh/sshd_config.d/50-cloud-init.conf`; password authentication and other key sources therefore remain unverified.

The host owner reports that the previous default password was changed. This is recorded as owner-reported remediation. No password or credential was requested, read, printed, or stored.

## Rusty-Fossil service identity

Rusty-Fossil is expected to run as `pi`; these files do not provision another account or change host group membership. The proposed systemd units in [`deploy/systemd`](../deploy/systemd/) run both processes as `pi`. The supervisor receives the `docker` group and uses the Docker socket. The controller unit masks `/run/docker.sock` with `InaccessiblePaths`; the controller must not receive a Docker socket bind, a Docker-specific environment variable, or another route to the Engine.

The interactive `pi` account still has rootful Docker authority. The systemd unit split constrains the controller process; it does not reduce the authority of an interactive `pi` shell or the trusted supervisor. Before installing the units, an operator must verify the effective unit sandbox and confirm the Docker daemon does not expose a TCP API. No unit was installed or started during this inventory.

## Repository and model access

`fossil-workspace` has a fixed pilot repository policy and bounded Git transport, but it has not fetched or checked out the repository and is not wired into the supervisor. A read-only Cloudflare account metadata check found one existing default AI Gateway with authentication enabled, request logging enabled, a 50-request/60-second rate limit, and spend limits disabled. It is not designated for non-production worker use and was not used for inference. No credential values were read, and no account configuration was changed. No broker, non-production inference credential, or enforced worker egress policy was verified.

## Host changes

None. Only read-only inventory commands were run. No accounts, SSH settings, firewall rules, Docker settings, images, containers, volumes, networks, files, or credentials were changed.
