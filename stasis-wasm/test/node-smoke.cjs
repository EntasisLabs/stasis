"use strict";

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const pkgNode = path.join(__dirname, "..", "pkg-node", "stasis_wasm.js");

function loadClient() {
  assert.ok(
    fs.existsSync(pkgNode),
    `missing ${pkgNode}; run \`npm run build\` in stasis-wasm first`,
  );
  return require(pkgNode);
}

function parseJson(raw) {
  return JSON.parse(raw);
}

test("stasis-wasm version matches package.json", () => {
  const { version } = loadClient();
  assert.equal(version(), require("../package.json").version);
});

test("mock create: register/invoke/ping", async () => {
  const { StasisWasmClient } = loadClient();
  const client = await StasisWasmClient.create();
  await client.register_agent("planner", "Planner", "Break work into steps");

  const completion = await client.invoke_agent("planner", "Plan a kickoff");
  assert.equal(completion, "stasis-wasm mock completion");

  const jobId = await client.enqueue_ping(7);
  assert.equal(typeof jobId, "string");
  const processed = await client.process_once("default", "browser-worker");
  assert.equal(processed, jobId);
  assert.equal(await client.job_state(jobId), "succeeded");
});

test("mock capabilities advertise grapheme-wasm and cors note", async () => {
  const { StasisWasmClient } = loadClient();
  const client = await StasisWasmClient.create();
  const caps = parseJson(client.capabilities());
  assert.equal(caps.llm, "mock");
  assert.equal(caps.tools, true);
  assert.equal(caps.grapheme, true);
  assert.equal(caps.graphemeEngine, "grapheme-wasm-0.7.1");
  assert.ok(Array.isArray(caps.graphemeStdlib));
  assert.ok(caps.graphemeStdlib.includes("core"));
  assert.equal(caps.memory, "locus-in-memory");
  assert.equal(caps.identity, "in-memory");
  assert.equal(caps.replay, "job-store-attempts-and-lineage");
  assert.match(caps.corsNote, /baseUrl/);
});

test("OpenAI create path constructs without a network call", async () => {
  const { StasisWasmClient } = loadClient();
  const client = await StasisWasmClient.create({
    apiKey: "sk-test-not-a-real-key",
    model: "gpt-4o-mini",
    baseUrl: "https://example.invalid/v1",
  });
  const caps = parseJson(client.capabilities());
  assert.equal(caps.llm, "openai-http");
  assert.equal(caps.tools, true);
  assert.equal(caps.grapheme, true);
});

test("memory store then recall across turns (no LLM)", async () => {
  const { StasisWasmClient } = loadClient();
  const client = await StasisWasmClient.create();
  const stored = parseJson(
    await client.store_memory("session-a", "the kickoff is Tuesday at 10am"),
  );
  assert.equal(typeof stored.node_id, "string");
  assert.ok(stored.node_id.length > 0);

  const recalled = parseJson(
    await client.recall_memory("session-a", "kickoff"),
  );
  assert.ok(recalled.retrieved >= 1);
  const blob = JSON.stringify(recalled.snippets);
  assert.match(blob, /Tuesday/);
});

test("identity upsert is visible on later identity_context", async () => {
  const { StasisWasmClient } = loadClient();
  const client = await StasisWasmClient.create();
  await client.upsert_identity("user:demo", "Demo User");
  const context = parseJson(await client.identity_context("user:demo"));
  assert.equal(context.user_present, true);
  assert.equal(context.persona_present, true);
  assert.equal(context.user_id, "user:demo");
  assert.equal(context.persona_name, "Demo User");
});

test("tool-loop echo records invocations in kernel job history", async () => {
  const { StasisWasmClient } = loadClient();
  const client = await StasisWasmClient.create();
  const jobId = await client.enqueue_tool_loop(
    "tool-echo-1",
    "please echo this payload",
    "echo",
    JSON.stringify({ hello: "world" }),
    "session-tools",
  );
  const drained = parseJson(
    await client.process_available("default", "browser-worker"),
  );
  assert.ok(drained.processed.includes(jobId));
  assert.equal(await client.job_state(jobId), "succeeded");

  const record = parseJson(await client.job_record(jobId));
  assert.equal(record.job_type, "workflow.stasis.tool_loop");

  const history = parseJson(await client.job_history(jobId));
  assert.ok(history.attempts.length >= 1);
  const last = history.attempts[history.attempts.length - 1];
  assert.equal(last.outcome, "succeeded");
  const diagnostics =
    typeof last.diagnostics === "string"
      ? JSON.parse(last.diagnostics)
      : last.diagnostics;
  assert.equal(diagnostics.provider, "stasis-tool-loop");
  assert.ok(
    diagnostics.invoked_tools?.includes("echo") ||
      diagnostics.tool_name === "echo",
  );
  const output = JSON.stringify(diagnostics.tool_output ?? diagnostics.tool_invocations);
  assert.match(output, /echo/);
});

test("grapheme-wasm echo job runs in-guest (not a JS fake)", async () => {
  const { StasisWasmClient } = loadClient();
  const client = await StasisWasmClient.create();
  const jobId = await client.enqueue_grapheme_echo("grapheme-echo-1", "hello from grapheme");
  await client.process_available("default", "browser-worker");
  assert.equal(await client.job_state(jobId), "succeeded");

  const history = parseJson(await client.job_history(jobId));
  const last = history.attempts[history.attempts.length - 1];
  assert.equal(last.outcome, "succeeded");
  const diagnostics =
    typeof last.diagnostics === "string"
      ? JSON.parse(last.diagnostics)
      : last.diagnostics;
  assert.equal(diagnostics.status, "success");
  const blob = JSON.stringify(diagnostics);
  assert.match(blob, /hello from grapheme|grapheme/i);
});

test("inline grapheme source job compiles via grapheme-wasm", async () => {
  const { StasisWasmClient } = loadClient();
  const client = await StasisWasmClient.create();
  const source = `import core from "grapheme/core"

query Ping {
  core.echo(message: "ok") {
    state { current }
  }
}
`;
  const jobId = await client.enqueue_grapheme("grapheme-inline-1", source);
  await client.process_available("default", "browser-worker");
  assert.equal(await client.job_state(jobId), "succeeded");
});

test("resume_from_history does not re-call the LLM on succeeded jobs", async () => {
  const { StasisWasmClient } = loadClient();
  const client = await StasisWasmClient.create();
  const jobId = await client.enqueue_tool_loop(
    "replay-1",
    "echo once",
    "echo",
    JSON.stringify({ n: 1 }),
    "session-replay",
  );
  await client.process_available("default", "browser-worker");
  assert.equal(await client.job_state(jobId), "succeeded");

  const resumed = parseJson(await client.resume_from_history(jobId));
  assert.equal(resumed.replayed_from_history, true);
  assert.equal(resumed.llm_called, false);
  assert.equal(resumed.state, "succeeded");
  assert.ok(resumed.attempt_count >= 1);
  assert.ok(resumed.diagnostics);
});
