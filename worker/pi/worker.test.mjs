import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { MAX_TASK_PROMPT_BYTES, buildPiInvocation, parseTaskPrompt } from "./worker.mjs";

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
  assert.doesNotMatch(dockerfile, /\bnode:latest\b/);
});
