import { constants } from "node:fs";
import { mkdir, open } from "node:fs/promises";
import { spawn } from "node:child_process";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

export const MAX_TASK_PROMPT_BYTES = 32 * 1024;
export const TASK_PROMPT_PATH = "/run/fossil/task-prompt.txt";
export const MAX_GATEWAY_TOKEN_BYTES = 512;
export const GATEWAY_TOKEN_PATH = "/run/fossil/gateway-token";
export const GATEWAY_CA_CERT_PATH = "/run/fossil/gateway-ca.pem";
export const PI_AGENT_DIRECTORY = "/tmp/pi-agent";
export const PI_GATEWAY_PROVIDER = "fossil-gateway";
export const PI_GATEWAY_BASE_URL = "https://fossil-gateway:8787/v1";
const PI_EXECUTABLE = "/opt/pi/node_modules/.bin/pi";
const MODEL_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,199}$/;
const GATEWAY_TOKEN_PATTERN = /^[A-Za-z0-9._-]{32,512}$/;

export function parseTaskPrompt(bytes) {
  if (!Buffer.isBuffer(bytes) || bytes.length === 0 || bytes.length > MAX_TASK_PROMPT_BYTES) {
    throw new Error("invalid task prompt");
  }

  let prompt;
  try {
    prompt = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch {
    throw new Error("invalid task prompt");
  }

  if (!prompt.trim()) {
    throw new Error("invalid task prompt");
  }
  return prompt;
}

export function buildPiInvocation(model) {
  if (typeof model !== "string" || !MODEL_ID_PATTERN.test(model)) {
    throw new Error("invalid Pi model");
  }

  return {
    file: PI_EXECUTABLE,
    args: [
      "--mode",
      "json",
      "--no-session",
      "--no-mcp",
      "--no-extensions",
      "--no-approve",
      "--no-context-files",
      "--tools",
      "read,bash,edit,write",
      "--provider",
      PI_GATEWAY_PROVIDER,
      "--model",
      model,
    ],
    shell: false,
  };
}

export async function readTaskPromptFile(filePath = TASK_PROMPT_PATH) {
  let file;
  try {
    file = await open(filePath, constants.O_RDONLY | constants.O_NOFOLLOW);
    const metadata = await file.stat();
    if (
      !metadata.isFile() ||
      metadata.size > MAX_TASK_PROMPT_BYTES ||
      (metadata.mode & 0o222) !== 0 ||
      (metadata.mode & 0o444) === 0
    ) {
      throw new Error("invalid task prompt");
    }

    const chunks = [];
    const buffer = Buffer.alloc(4096);
    let size = 0;
    while (true) {
      const { bytesRead } = await file.read(
        buffer,
        0,
        Math.min(buffer.length, MAX_TASK_PROMPT_BYTES + 1 - size),
        null,
      );
      if (bytesRead === 0) break;
      size += bytesRead;
      if (size > MAX_TASK_PROMPT_BYTES) {
        throw new Error("invalid task prompt");
      }
      chunks.push(Buffer.from(buffer.subarray(0, bytesRead)));
    }
    return parseTaskPrompt(Buffer.concat(chunks, size));
  } finally {
    await file?.close();
  }
}

export function validateGatewayToken(token) {
  if (
    typeof token !== "string" ||
    Buffer.byteLength(token, "utf8") > MAX_GATEWAY_TOKEN_BYTES ||
    !GATEWAY_TOKEN_PATTERN.test(token)
  ) {
    throw new Error("invalid gateway token");
  }
  return token;
}

export async function readGatewayTokenFile(filePath = GATEWAY_TOKEN_PATH) {
  let file;
  try {
    file = await open(filePath, constants.O_RDONLY | constants.O_NOFOLLOW);
    const metadata = await file.stat();
    if (
      !metadata.isFile() ||
      metadata.size < 32 ||
      metadata.size > MAX_GATEWAY_TOKEN_BYTES ||
      (metadata.mode & 0o222) !== 0 ||
      (metadata.mode & 0o444) === 0
    ) {
      throw new Error("invalid gateway token");
    }

    const chunks = [];
    const buffer = Buffer.alloc(256);
    let size = 0;
    while (true) {
      const { bytesRead } = await file.read(
        buffer,
        0,
        Math.min(buffer.length, MAX_GATEWAY_TOKEN_BYTES + 1 - size),
        null,
      );
      if (bytesRead === 0) break;
      size += bytesRead;
      if (size > MAX_GATEWAY_TOKEN_BYTES) {
        throw new Error("invalid gateway token");
      }
      chunks.push(Buffer.from(buffer.subarray(0, bytesRead)));
    }
    let token;
    try {
      token = new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(chunks, size));
    } catch {
      throw new Error("invalid gateway token");
    }
    return validateGatewayToken(token);
  } finally {
    await file?.close();
  }
}

export function buildPiModelsConfig(model, gatewayToken, cost) {
  if (typeof model !== "string" || !MODEL_ID_PATTERN.test(model)) {
    throw new Error("invalid Pi model");
  }
  validateGatewayToken(gatewayToken);
  if (
    !cost ||
    ![cost.input, cost.output, cost.cacheRead, cost.cacheWrite].every(
      (value) => typeof value === "number" && Number.isFinite(value) && value >= 0,
    )
  ) {
    throw new Error("invalid model pricing");
  }
  return {
    providers: {
      [PI_GATEWAY_PROVIDER]: {
        baseUrl: PI_GATEWAY_BASE_URL,
        apiKey: gatewayToken,
        authHeader: true,
        api: "openai-completions",
        models: [
          {
            id: model,
            name: model,
            reasoning: false,
            input: ["text"],
            cost,
            contextWindow: 32768,
            maxTokens: 4096,
          },
        ],
      },
    },
  };
}

function readModelCostFromEnvironment(environment = process.env) {
  const read = (name) => {
    const value = environment[name];
    if (typeof value !== "string" || !/^(?:0|[1-9]\d*)(?:\.\d{1,8})?$/.test(value)) {
      throw new Error("invalid model pricing");
    }
    return Number(value);
  };
  return {
    input: read("FOSSIL_PI_COST_INPUT_USD_PER_MILLION"),
    output: read("FOSSIL_PI_COST_OUTPUT_USD_PER_MILLION"),
    cacheRead: read("FOSSIL_PI_COST_CACHE_READ_USD_PER_MILLION"),
    cacheWrite: read("FOSSIL_PI_COST_CACHE_WRITE_USD_PER_MILLION"),
  };
}

async function writePiModelsConfig(model, gatewayToken, cost) {
  await mkdir(PI_AGENT_DIRECTORY, { recursive: true, mode: 0o700 });
  const configPath = join(PI_AGENT_DIRECTORY, "models.json");
  let file;
  try {
    file = await open(
      configPath,
      constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW,
      0o600,
    );
    await file.writeFile(JSON.stringify(buildPiModelsConfig(model, gatewayToken, cost)), "utf8");
  } finally {
    await file?.close();
  }
}

async function runPi(prompt, model) {
  const gatewayToken = await readGatewayTokenFile();
  const cost = readModelCostFromEnvironment();
  await writePiModelsConfig(model, gatewayToken, cost);
  const invocation = buildPiInvocation(model);
  const child = spawn(invocation.file, invocation.args, {
    cwd: "/workspace",
    env: {
      CI: "true",
      HOME: "/tmp/pi-home",
      NODE_EXTRA_CA_CERTS: GATEWAY_CA_CERT_PATH,
      PATH: "/opt/pi/node_modules/.bin:/usr/local/bin:/usr/bin:/bin",
      PI_CODING_AGENT_DIR: PI_AGENT_DIRECTORY,
      PI_SKIP_VERSION_CHECK: "1",
      PI_TELEMETRY: "0",
      TMPDIR: "/tmp",
      FOSSIL_PI_MODEL: model,
    },
    shell: false,
    // Pi's JSONL stays on container stdout, which the supervisor reads via bounded Docker logs.
    stdio: ["pipe", "inherit", "inherit"],
  });

  let settled = false;
  const removeSignalHandlers = [];
  for (const signal of ["SIGTERM", "SIGINT", "SIGHUP"]) {
    const relaySignal = () => child.kill(signal);
    process.on(signal, relaySignal);
    removeSignalHandlers.push(() => process.off(signal, relaySignal));
  }

  child.stdin.end(prompt, "utf8");

  return new Promise((resolve, reject) => {
    child.once("error", (error) => {
      if (!settled) {
        settled = true;
        removeSignalHandlers.forEach((remove) => remove());
        reject(new Error(`Pi process failed to start: ${error.code ?? "unknown"}`));
      }
    });
    child.once("exit", (code, signal) => {
      if (settled) return;
      settled = true;
      removeSignalHandlers.forEach((remove) => remove());
      resolve(signal ? 128 : (code ?? 1));
    });
  });
}

async function main() {
  let prompt;
  try {
    prompt = await readTaskPromptFile();
  } catch {
    process.stderr.write("invalid task prompt\n");
    return 2;
  }

  const model = process.env.FOSSIL_PI_MODEL;
  try {
    return await runPi(prompt, model);
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.message : "Pi process failed"}\n`);
    return 1;
  }
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  main().then((exitCode) => {
    process.exitCode = exitCode;
  });
}
