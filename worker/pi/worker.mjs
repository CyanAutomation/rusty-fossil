import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

export const MAX_TASK_PROMPT_BYTES = 32 * 1024;
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

async function readPromptFromStdin() {
  const chunks = [];
  let size = 0;
  for await (const chunk of process.stdin) {
    size += chunk.length;
    if (size > MAX_TASK_PROMPT_BYTES) {
      throw new Error("invalid task prompt");
    }
    chunks.push(chunk);
  }
  return parseTaskPrompt(Buffer.concat(chunks, size));
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
    prompt = await readPromptFromStdin();
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
