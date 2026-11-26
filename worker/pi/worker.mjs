import { constants } from "node:fs";
import { open } from "node:fs/promises";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

export const MAX_TASK_PROMPT_BYTES = 32 * 1024;
export const TASK_PROMPT_PATH = "/run/fossil/task-prompt.txt";
const PI_EXECUTABLE = "/opt/pi/node_modules/.bin/pi";
const MODEL_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}$/;

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

function runPi(prompt, model) {
  const invocation = buildPiInvocation(model);
  const child = spawn(invocation.file, invocation.args, {
    cwd: "/workspace",
    env: {
      CI: "true",
      HOME: "/tmp/pi-home",
      PATH: "/opt/pi/node_modules/.bin:/usr/local/bin:/usr/bin:/bin",
      PI_CODING_AGENT_DIR: "/tmp/pi-agent",
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
