import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { chmodSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");

test("Compose keeps Docker Engine access inside fossil-supervisord", () => {
  const temporaryDirectory = mkdtempSync(join(tmpdir(), "rusty-fossil-compose-"));
  const soyuzToken = join(temporaryDirectory, "soyuz-token");
  const ipcToken = join(temporaryDirectory, "ipc-token");
  const senkuToken = join(temporaryDirectory, "senku-token");
  const envFile = join(temporaryDirectory, "compose.env");

  try {
    writeFileSync(soyuzToken, "synthetic-soyuz-token\n", { mode: 0o400 });
    writeFileSync(ipcToken, "synthetic-ipc-token-01234567890123456789\n", { mode: 0o400 });
    writeFileSync(senkuToken, "synthetic-senku-token\n", { mode: 0o400 });
    chmodSync(soyuzToken, 0o400);
    chmodSync(ipcToken, 0o400);
    chmodSync(senkuToken, 0o400);
    writeFileSync(
      envFile,
      [
        "RUSTY_FOSSIL_UID=1001",
        "RUSTY_FOSSIL_GID=1001",
        "DOCKER_GID=985",
        `RUSTY_FOSSIL_HOST_STATE_DIR=${join(temporaryDirectory, "state")}`,
        `RUSTY_FOSSIL_RUNTIME_DIR=${join(temporaryDirectory, "runtime")}`,
        "RUSTY_FOSSIL_HOST_PORT=18080",
        "RUSTY_FOSSIL_IMAGE=rusty-fossil:compose-test",
        "DOCKER_SOCKET_PATH=/var/run/docker.sock",
        "SOYUZ_API_ORIGIN=https://soyuz.example.test",
        "SOYUZ_WORKER_ID=agentlab-test",
        `SOYUZ_WORKER_API_TOKEN_HOST_FILE=${soyuzToken}`,
        `FOSSIL_SUPERVISOR_IPC_TOKEN_HOST_FILE=${ipcToken}`,
        `SENKU_API_TOKEN_HOST_FILE=${senkuToken}`,
        "SENKU_API_ORIGIN=https://senku.example.test",
        `FOSSIL_SUPERVISOR_IMAGE=ghcr.io/cyanautomation/rusty-fossil-synthetic-worker@sha256:${"a".repeat(64)}`,
      ].join("\n") + "\n",
    );

    const rendered = execFileSync(
      "docker",
      [
        "compose",
        "--project-directory",
        repositoryRoot,
        "--env-file",
        envFile,
        "-f",
        join(repositoryRoot, "docker-compose.yml"),
        "config",
        "--format",
        "json",
      ],
      { cwd: repositoryRoot, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] },
    );
    const config = JSON.parse(rendered);
    const controller = config.services["rusty-fossil"];
    const supervisor = config.services["fossil-supervisord"];
    const hostStateDirectory = join(temporaryDirectory, "state");

    assert.ok(controller, "controller service is configured");
    assert.ok(supervisor, "supervisor service is configured");
    assert.equal(supervisor.depends_on["rusty-fossil"].condition, "service_started");
    assert.equal(controller.user, "1001:1001");
    assert.equal(supervisor.user, "1001:1001");
    assert.deepEqual(supervisor.group_add, ["985"]);
    assert.equal(controller.group_add, undefined);
    assert.ok(
      controller.volumes.some(
        (volume) => volume.source === hostStateDirectory && volume.target === "/var/lib/rusty-fossil",
      ),
      "controller must use the durable shared state directory",
    );
    assert.ok(
      supervisor.volumes.some(
        (volume) => volume.source === hostStateDirectory && volume.target === "/var/lib/rusty-fossil",
      ),
      "supervisor must use the same durable state directory",
    );
    assert.equal(
      controller.volumes.some((volume) => volume.source === "/var/run/docker.sock"),
      false,
      "controller must not mount the Docker socket",
    );
    assert.equal(
      supervisor.volumes.filter((volume) => volume.source === "/var/run/docker.sock").length,
      1,
      "supervisor must receive exactly one Docker socket mount",
    );
    assert.equal(
      Boolean(controller.secrets?.some((secret) => secret.source === "soyuz_worker_api_token")),
      false,
      "controller must not receive the supervisor's Soyuz token",
    );
    assert.ok(supervisor.secrets.some((secret) => secret.source === "soyuz_worker_api_token"));
    assert.ok(supervisor.secrets.some((secret) => secret.source === "fossil_supervisor_ipc_token"));
    assert.ok(Object.hasOwn(controller.networks, "controller_internal"));
    assert.equal(config.networks.controller_internal.internal, true);
    assert.ok(Object.hasOwn(supervisor.networks, "supervisor_egress"));
    assert.notEqual(config.networks.supervisor_egress.internal, true);
    assert.equal(controller.ports[0].host_ip, "127.0.0.1");
    assert.equal(supervisor.ports, undefined);
    assert.equal(controller.read_only, true);
    assert.equal(supervisor.read_only, true);
    assert.equal(controller.restart, "unless-stopped");
    assert.equal(supervisor.restart, "unless-stopped");

    const senkuOverlayRendered = execFileSync(
      "docker",
      [
        "compose",
        "--project-directory",
        repositoryRoot,
        "--env-file",
        envFile,
        "-f",
        join(repositoryRoot, "docker-compose.yml"),
        "-f",
        join(repositoryRoot, "docker-compose.senku.yml"),
        "config",
        "--format",
        "json",
      ],
      { cwd: repositoryRoot, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] },
    );
    const overlay = JSON.parse(senkuOverlayRendered);
    const advisoryController = overlay.services["rusty-fossil"];
    assert.ok(Object.hasOwn(advisoryController.networks, "controller_internal"));
    assert.ok(Object.hasOwn(advisoryController.networks, "senku_egress"));
    assert.ok(advisoryController.secrets.some((secret) => secret.source === "senku_api_token"));
    assert.notEqual(overlay.networks.senku_egress.internal, true);
  } finally {
    rmSync(temporaryDirectory, { recursive: true, force: true });
  }
});
