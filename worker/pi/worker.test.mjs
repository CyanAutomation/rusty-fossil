import assert from "node:assert/strict";
import { chmod, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  MAX_TASK_PROMPT_BYTES,
  TASK_PROMPT_PATH,
  buildPiInvocation,
  parseTaskPrompt,
  readTaskPromptFile,
} from "./worker.mjs";

const workerDirectory = new URL(".", import.meta.url);

test("accepts a bounded UTF-8 task prompt without normalizing it", () => {
  const prompt = "Fix the test. Preserve this line.\nAnd this line: π";
  assert.equal(parseTaskPrompt(Buffer.from(prompt, "utf8")), prompt);
});

test("rejects empty, oversized, and invalid UTF-8 prompts", () => {
  assert.throws(() => parseTaskPrompt(Buffer.alloc(0)), /invalid task prompt/);
  assert.throws(
    () => parseTaskPrompt(Buffer.alloc(MAX_TASK_PROMPT_BYTES + 1, 0x61)),
    /invalid task prompt/,
  );
  assert.throws(() => parseTaskPrompt(Buffer.from([0xff, 0xfe])), /invalid task prompt/);
});

test("reads a bounded prompt from an immutable payload file", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "rusty-fossil-pi-prompt-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const promptPath = join(directory, "prompt.txt");
  const prompt = "Fix one test. Keep this exact line.\nπ";
  await writeFile(promptPath, Buffer.from(prompt, "utf8"), { mode: 0o400 });

  assert.equal(TASK_PROMPT_PATH, "/run/fossil/task-prompt.txt");
  assert.equal(await readTaskPromptFile(promptPath), prompt);
});

test("rejects oversized, empty, invalid UTF-8, and symlink payload files", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "rusty-fossil-pi-prompt-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const promptPath = join(directory, "prompt.txt");

  await writeFile(promptPath, Buffer.alloc(MAX_TASK_PROMPT_BYTES + 1, 0x61));
  await chmod(promptPath, 0o444);
  await assert.rejects(readTaskPromptFile(promptPath), /invalid task prompt/);
  await chmod(promptPath, 0o600);
  await writeFile(promptPath, Buffer.alloc(0));
  await chmod(promptPath, 0o444);
  await assert.rejects(readTaskPromptFile(promptPath), /invalid task prompt/);
  await chmod(promptPath, 0o600);
  await writeFile(promptPath, Buffer.from([0xff, 0xfe]));
  await chmod(promptPath, 0o444);
  await assert.rejects(readTaskPromptFile(promptPath), /invalid task prompt/);

  const targetPath = join(directory, "target.txt");
  await writeFile(targetPath, "do not follow links", { mode: 0o444 });
  await rm(promptPath);
  await symlink(targetPath, promptPath);
  await assert.rejects(readTaskPromptFile(promptPath));
});

test("builds a fixed Pi JSON-mode invocation without a shell or prompt argument", () => {
  const invocation = buildPiInvocation("gateway/disposable-test-model");
  assert.equal(invocation.file, "/opt/pi/node_modules/.bin/pi");
  assert.deepEqual(invocation.args, [
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
    "gateway/disposable-test-model",
  ]);
  assert.equal(invocation.shell, false);
  assert.ok(!invocation.args.join(" ").includes("task prompt"));
});

test("rejects empty and malformed model identifiers", () => {
  assert.throws(() => buildPiInvocation(""), /invalid Pi model/);
  assert.throws(() => buildPiInvocation("model with spaces"), /invalid Pi model/);
  assert.throws(() => buildPiInvocation("--help"), /invalid Pi model/);
});

test("pins the package and its complete dependency graph to Pi 1.6.0", async () => {
  const manifest = JSON.parse(await readFile(new URL("package.json", workerDirectory), "utf8"));
  const lock = JSON.parse(await readFile(new URL("package-lock.json", workerDirectory), "utf8"));

  assert.equal(manifest.dependencies["@earendil-works/pi-coding-agent"], "1.6.0");
  assert.equal(manifest.engines.node, ">=22.19.0");
  assert.equal(lock.lockfileVersion, 3);
  assert.equal(lock.name, manifest.name);
  assert.equal(lock.version, manifest.version);
  assert.equal(lock.packages[""]["dependencies"]["@earendil-works/pi-coding-agent"], "1.6.0");
  assert.ok(
    Object.values(lock.packages)
      .filter((pkg) => pkg.resolved?.startsWith("https://registry.npmjs.org/"))
      .every((pkg) => /^sha512-/.test(pkg.integrity)),
    "every registry dependency must have an integrity hash",
  );
  assert.equal(lock.packages["node_modules/@earendil-works/pi-coding-agent"].version, "1.6.0");
  assert.match(
    lock.packages["node_modules/@earendil-works/pi-coding-agent"].integrity,
    /^sha512-/,
  );
});

test("pins a multiarch Node image and runs the worker without root", async () => {
  const dockerfile = await readFile(new URL("Dockerfile.pi-worker", workerDirectory), "utf8");

  assert.match(dockerfile, /^FROM node:26-bookworm-slim@sha256:[a-f0-9]{64}/m);
  assert.match(dockerfile, /^FROM node:26-bookworm-slim@sha256:[a-f0-9]{64} AS dependencies$/m);
  assert.match(dockerfile, /^FROM node:26-bookworm-slim@sha256:[a-f0-9]{64} AS worker$/m);
  assert.match(dockerfile, /^USER 65532:65532$/m);
  assert.match(dockerfile, /npm ci --omit=dev/);
  assert.match(dockerfile, /ca-certificates git/);
  assert.match(dockerfile, /mkdir -p [^\n]*\/run\/fossil/);
  assert.match(dockerfile, /touch \/run\/fossil\/task-prompt\.txt/);
  assert.doesNotMatch(dockerfile, /\bnode:latest\b/);
});
