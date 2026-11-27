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
  const gatewayToken = join(temporaryDirectory, "gateway-token");
  const gatewaySigningKey = join(temporaryDirectory, "gateway-signing-key");
  const gatewayTlsCertificate = join(temporaryDirectory, "gateway-tls-certificate");
  const gatewayTlsKey = join(temporaryDirectory, "gateway-tls-key");
  const envFile = join(temporaryDirectory, "compose.env");
  const dockerSocket = join(temporaryDirectory, "docker-engine.sock");

  try {
    writeFileSync(soyuzToken, "synthetic-soyuz-token\n", { mode: 0o400 });
    writeFileSync(ipcToken, "synthetic-ipc-token-01234567890123456789\n", { mode: 0o400 });
    writeFileSync(senkuToken, "synthetic-senku-token\n", { mode: 0o400 });
    writeFileSync(gatewayToken, "synthetic-gateway-token-abcdefghijklmnopqrstuvwxyz\n", {
      mode: 0o400,
    });
    writeFileSync(gatewaySigningKey, "synthetic-gateway-signing-key-abcdefghijklmnopqrstuvwxyz\n", {
      mode: 0o400,
    });
    writeFileSync(gatewayTlsCertificate, "synthetic-tls-certificate\n", { mode: 0o400 });
    writeFileSync(gatewayTlsKey, "synthetic-tls-private-key\n", { mode: 0o400 });
    for (const path of [
      soyuzToken,
      ipcToken,
      senkuToken,
      gatewayToken,
      gatewaySigningKey,
      gatewayTlsCertificate,
      gatewayTlsKey,
    ]) {
      chmodSync(path, 0o400);
    }
    writeFileSync(
      envFile,
      [
        // Synthetic IDs are test fixtures; they are not host account values.
        "RUSTY_FOSSIL_UID=64001",
        "RUSTY_FOSSIL_GID=64001",
        "FOSSIL_GATEWAY_UID=64002",
        "FOSSIL_GATEWAY_GID=64002",
        "DOCKER_GID=64003",
        `RUSTY_FOSSIL_HOST_STATE_DIR=${join(temporaryDirectory, "state")}`,
        `FOSSIL_GATEWAY_HOST_STATE_DIR=${join(temporaryDirectory, "gateway-state")}`,
        `RUSTY_FOSSIL_RUNTIME_DIR=${join(temporaryDirectory, "runtime")}`,
        "RUSTY_FOSSIL_HOST_PORT=0",
        "RUSTY_FOSSIL_IMAGE=rusty-fossil:compose-test",
        `DOCKER_SOCKET_PATH=${dockerSocket}`,
        "SOYUZ_API_ORIGIN=https://soyuz.example.test",
        "SOYUZ_WORKER_ID=compose-test-worker",
        `SOYUZ_WORKER_API_TOKEN_HOST_FILE=${soyuzToken}`,
        `FOSSIL_SUPERVISOR_IPC_TOKEN_HOST_FILE=${ipcToken}`,
        `FOSSIL_GATEWAY_AUTH_TOKEN_HOST_FILE=${gatewayToken}`,
        `FOSSIL_GATEWAY_SIGNING_KEY_HOST_FILE=${gatewaySigningKey}`,
        `FOSSIL_GATEWAY_TLS_CERT_HOST_FILE=${gatewayTlsCertificate}`,
        `FOSSIL_GATEWAY_TLS_KEY_HOST_FILE=${gatewayTlsKey}`,
        "FOSSIL_GATEWAY_ENDPOINT=https://gateway.example.invalid/v1",
        "FOSSIL_GATEWAY_MODEL=compose-test-model",
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
    assert.equal(controller.user, "64001:64001");
    assert.equal(supervisor.user, "64001:64001");
    assert.deepEqual(supervisor.group_add, ["64003"]);
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
      controller.volumes.some((volume) => volume.source === dockerSocket),
      false,
      "controller must not mount the Docker socket",
    );
    assert.equal(
      supervisor.volumes.filter((volume) => volume.source === dockerSocket).length,
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
    assert.equal(controller.ports[0].published, "0");
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

    const piOverlayRendered = execFileSync(
      "docker",
      [
        "compose",
        "--profile",
        "pi-pilot",
        "--project-directory",
        repositoryRoot,
        "--env-file",
        envFile,
        "-f",
        join(repositoryRoot, "docker-compose.yml"),
        "-f",
        join(repositoryRoot, "docker-compose.pi.yml"),
        "config",
        "--format",
        "json",
      ],
      { cwd: repositoryRoot, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] },
    );
    const piOverlay = JSON.parse(piOverlayRendered);
    const broker = piOverlay.services["fossil-gateway-broker"];
    assert.deepEqual(broker.profiles, ["pi-pilot"]);
    assert.equal(broker.user, "64002:64002", "broker uses a separate synthetic test identity");
    assert.equal(broker.environment.FOSSIL_GATEWAY_MAX_OUTPUT_TOKENS, undefined);
    assert.equal(broker.environment.FOSSIL_GATEWAY_MAX_COST_MICRO_USD, undefined);
    assert.equal(
      broker.environment.FOSSIL_GATEWAY_TLS_CERT_FILE,
      "/run/secrets/fossil_gateway_tls_certificate",
    );
    assert.equal(
      broker.environment.FOSSIL_GATEWAY_TLS_KEY_FILE,
      "/run/secrets/fossil_gateway_tls_private_key",
    );
    assert.equal(broker.ports, undefined, "broker must not publish a host port");
    assert.equal(
      broker.volumes.some((volume) => volume.source === dockerSocket),
      false,
      "broker must not receive Docker Engine access",
    );
    assert.deepEqual(
      broker.secrets.map((secret) => secret.source).sort(),
      [
        "fossil_gateway_auth_token",
        "fossil_gateway_signing_key",
        "fossil_gateway_tls_certificate",
        "fossil_gateway_tls_private_key",
      ],
      "broker receives only its dedicated upstream, signing, and TLS credentials",
    );
    assert.ok(Object.hasOwn(broker.networks, "pi_worker_internal"));
    assert.ok(Object.hasOwn(broker.networks, "supervisor_egress"));
    assert.equal(piOverlay.networks.pi_worker_internal.internal, true);
    assert.equal(piOverlay.networks.supervisor_egress.internal, undefined);
    assert.equal(broker.read_only, true);
    assert.deepEqual(broker.cap_drop, ["ALL"]);
    assert.ok(broker.security_opt.includes("no-new-privileges:true"));
  } finally {
    rmSync(temporaryDirectory, { recursive: true, force: true });
  }
});
