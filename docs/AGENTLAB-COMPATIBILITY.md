# Agentlab compatibility

The current Agentlab inventory and deployment disposition are recorded in [ARM64 host compatibility](ARM64-HOST-COMPATIBILITY.md). The host owner expects Rusty-Fossil to run as `pi`; the repository now includes a systemd example that grants Docker access to `fossil-supervisord` and masks the socket from the controller process.

The unit files have not been installed or exercised on Agentlab. The `pi` account remains a member of the rootful Docker group, so an interactive shell as `pi` retains host-root-equivalent Docker authority; the service split is process confinement under a shared Unix identity, not account separation. SSH authentication policy is unverified. A read-only Cloudflare check found the existing default AI Gateway logs requests and has no spend limit; it was not used for inference. No model broker or worker egress policy has been validated. The coding-worker deployment gate remains closed.
