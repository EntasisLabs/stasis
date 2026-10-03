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
  assert.equal(caps.hostToolBootstrap, true);
  assert.equal(caps.hostToolResultEnvelope, "stasis.tool-result.v1");
  assert.equal(caps.hostToolCallbackTimeoutMs, 30000);
  assert.equal(caps.hostToolCallbackTimeoutMaxMs, 600000);
  assert.equal(caps.hostToolAbortSignal, true);
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

  const reflex = parseJson(
    await client.decide_memory_reflex(
      "session-a",
      "do you remember what we discussed about the kickoff",
    ),
  );
  assert.equal(reflex.schema_version, "locus-sdk.memory.v4");
  assert.equal(reflex.kind, "dispatch");
  assert.equal(reflex.action, "recall");
  assert.equal(reflex.topic, "locus.memory.recall");
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

test("host tools accept async callbacks and return structured results", async () => {
  const { StasisWasmClient } = loadClient();
  const client = await StasisWasmClient.create();
  let received;
  client.register_tool(
    "catalog_lookup",
    "Look up a catalog item",
    {
      type: "object",
      properties: { id: { type: "string" } },
      required: ["id"],
      additionalProperties: false,
    },
    async (input) => {
      received = input;
      await Promise.resolve();
      return { id: input.id, available: true };
    },
  );

  const result = await client.invoke_tool("catalog_lookup", { id: "item-1" });
  assert.deepEqual(received, { id: "item-1" });
  assert.deepEqual(result, {
    ok: true,
    result: { id: "item-1", available: true },
  });
});

test("host tool registration rejects duplicates and invalid definitions", async () => {
  const { StasisWasmClient } = loadClient();
  const client = await StasisWasmClient.create();
  const callback = async () => ({ ok: true });
  client.register_tool("custom", "A custom tool", { type: "object" }, callback);

  assert.throws(
    () => client.register_tool("custom", "Again", { type: "object" }, callback),
    /already registered/,
  );
  assert.throws(
    () => client.register_tool("echo", "Replace echo", { type: "object" }, callback),
    /already registered/,
  );
  assert.throws(
    () => client.register_tool("", "Missing name", { type: "object" }, callback),
    /name must be non-empty/,
  );
  assert.throws(
    () => client.register_tool("bad_schema", "Bad schema", "object", callback),
    /schema must be a JSON object/,
  );
});

test("malformed host tool arguments are structured and skip the callback", async () => {
  const { StasisWasmClient } = loadClient();
  const client = await StasisWasmClient.create();
  let calls = 0;
  client.register_tool(
    "typed_tool",
    "Requires a count",
    {
      type: "object",
      properties: { count: { type: "integer" } },
      required: ["count"],
      additionalProperties: false,
    },
    async () => {
      calls += 1;
      return true;
    },
  );

  const result = await client.invoke_tool("typed_tool", { count: "many" });
  assert.equal(result.ok, false);
  assert.equal(result.error.code, "invalid_arguments");
  assert.match(result.error.message, /expected type 'integer'/);
  assert.equal(calls, 0);
});

test("unknown host tools reject with the registry error", async () => {
  const { StasisWasmClient } = loadClient();
  const client = await StasisWasmClient.create();
  await assert.rejects(
    client.invoke_tool("missing_tool", {}),
    /tool not registered: missing_tool/,
  );
});

test("callback throws and rejections become structured tool errors", async () => {
  const { StasisWasmClient } = loadClient();
  const client = await StasisWasmClient.create();
  client.register_tool(
    "throws",
    "Throws synchronously",
    { type: "object" },
    () => {
      throw new Error("sync boom");
    },
  );
  client.register_tool(
    "rejects",
    "Rejects asynchronously",
    { type: "object" },
    async () => {
      throw new Error("async boom");
    },
  );

  const thrown = await client.invoke_tool("throws", {});
  assert.equal(thrown.ok, false);
  assert.equal(thrown.error.code, "callback_failed");
  assert.match(thrown.error.message, /sync boom/);

  const rejected = await client.invoke_tool("rejects", {});
  assert.equal(rejected.ok, false);
  assert.equal(rejected.error.code, "callback_failed");
  assert.match(rejected.error.message, /async boom/);
});

test("non-JSON callback results become structured tool errors", async () => {
  const { StasisWasmClient } = loadClient();
  const client = await StasisWasmClient.create();
  client.register_tool(
    "undefined_result",
    "Returns undefined",
    { type: "object" },
    async () => undefined,
  );
  client.register_tool(
    "cyclic_result",
    "Returns a cycle",
    { type: "object" },
    async () => {
      const value = {};
      value.self = value;
      return value;
    },
  );

  const undefinedResult = await client.invoke_tool("undefined_result", {});
  assert.equal(undefinedResult.ok, false);
  assert.equal(undefinedResult.error.code, "invalid_callback_result");

  const cyclicResult = await client.invoke_tool("cyclic_result", {});
  assert.equal(cyclicResult.ok, false);
  assert.equal(cyclicResult.error.code, "invalid_callback_result");
});

test("host callback deadlines abort cooperative work and return structured errors", async () => {
  const { StasisWasmClient } = loadClient();
  const client = await StasisWasmClient.create();
  let context;
  let abortObserved = false;
  client.register_tool_with_options(
    "never_settles",
    "Exercise callback deadline handling",
    { type: "object", additionalProperties: false },
    (_input, callbackContext) => {
      context = callbackContext;
      callbackContext.signal.addEventListener("abort", () => {
        abortObserved = true;
      });
      return new Promise(() => {});
    },
    { timeoutMs: 20 },
  );

  const result = await client.invoke_tool("never_settles", {});
  assert.equal(result.ok, false);
  assert.equal(result.error.tool, "never_settles");
  assert.equal(result.error.code, "callback_timeout");
  assert.match(result.error.message, /20ms/);
  assert.equal(context.tool, "never_settles");
  assert.equal(context.timeoutMs, 20);
  assert.equal(context.signal.aborted, true);
  assert.equal(abortObserved, true);
});

test("tool-loop jobs recover from callbacks that never settle", async () => {
  const { StasisWasmClient } = loadClient();
  const client = await StasisWasmClient.create();
  client.register_tool_with_options(
    "stalled_host_tool",
    "Never settles",
    { type: "object", additionalProperties: false },
    () => new Promise(() => {}),
    { timeoutMs: 20 },
  );

  const jobId = await client.enqueue_tool_loop(
    "stalled-host-tool-job",
    "call the selected tool",
    "stalled_host_tool",
    JSON.stringify({}),
    "host-tools",
  );
  await client.process_available("default", "browser-worker");
  assert.equal(await client.job_state(jobId), "succeeded");

  const history = parseJson(await client.job_history(jobId));
  const diagnostics = JSON.stringify(history.attempts.at(-1).diagnostics);
  assert.match(diagnostics, /callback_timeout/);
  assert.match(diagnostics, /stalled_host_tool/);
});

test("host callback timeout options reject unbounded or excessive deadlines", async () => {
  const { StasisWasmClient } = loadClient();
  const client = await StasisWasmClient.create();
  const register = (timeoutMs) =>
    client.register_tool_with_options(
      `timeout_${timeoutMs}`,
      "Invalid timeout",
      { type: "object" },
      async () => true,
      { timeoutMs },
    );

  assert.throws(() => register(0), /between 1 and 600000/);
  assert.throws(() => register(600001), /between 1 and 600000/);
  assert.throws(() => register(-1), /invalid tool options/);
});

test("model tool calls dispatch through the registered host callback", async () => {
  const { StasisWasmClient } = loadClient();
  const client = await StasisWasmClient.create();
  let calls = 0;
  client.register_tool(
    "host_status",
    "Read host status",
    { type: "object", additionalProperties: false },
    async () => {
      calls += 1;
      return { status: "ready" };
    },
  );

  const jobId = await client.enqueue_tool_loop(
    "host-tool-loop-1",
    "check status",
    "host_status",
    JSON.stringify({}),
    "host-tools",
  );
  await client.process_available("default", "browser-worker");
  assert.equal(await client.job_state(jobId), "succeeded");
  assert.equal(calls, 1);

  const history = parseJson(await client.job_history(jobId));
  const diagnostics = history.attempts.at(-1).diagnostics;
  const blob = JSON.stringify(diagnostics);
  assert.match(blob, /host_status/);
  assert.match(blob, /ready/);
  assert.match(blob, /"ok":true/);
});

test("tool-loop returns malformed model arguments to the model as tool output", async () => {
  const { StasisWasmClient } = loadClient();
  const client = await StasisWasmClient.create();
  let calls = 0;
  client.register_tool(
    "requires_query",
    "Requires a query",
    {
      type: "object",
      properties: { query: { type: "string" } },
      required: ["query"],
      additionalProperties: false,
    },
    async () => {
      calls += 1;
      return { unexpected: true };
    },
  );

  const jobId = await client.enqueue_tool_loop(
    "invalid-host-tool-arguments",
    "call the selected tool",
    "requires_query",
    JSON.stringify({}),
    "host-tools",
  );
  await client.process_available("default", "browser-worker");
  assert.equal(await client.job_state(jobId), "succeeded");
  assert.equal(calls, 0);

  const history = parseJson(await client.job_history(jobId));
  const blob = JSON.stringify(history.attempts.at(-1).diagnostics);
  assert.match(blob, /invalid_arguments/);
  assert.match(blob, /requires_query/);
});

test("ergonomic SDK prompt hides queue plumbing and returns full result", async () => {
  const { createStasis, Stasis, tool } = require("../sdk/node.cjs");
  let calls = 0;
  const stasis = await createStasis({
    instructions: "Be concise.",
    tools: [
      tool({
        name: "sdk_status",
        description: "Read SDK status",
        parameters: { type: "object", additionalProperties: false },
        async execute() {
          calls += 1;
          return { status: "ready" };
        },
      }),
    ],
  });

  assert.ok(stasis instanceof Stasis);
  const result = await stasis.session("sdk-session").prompt("check status", {
    operationId: "sdk-prompt-1",
  });

  assert.equal(result.status, "done");
  assert.equal(result.text, "stasis-wasm mock completion");
  assert.equal(result.diagnostics.output_text, "stasis-wasm mock completion");
  assert.equal(calls, 1);
});

test("ergonomic SDK supports idempotent submit and wait", async () => {
  const { createStasis } = require("../sdk/node.cjs");
  const stasis = await createStasis();

  const first = await stasis.submit("echo once", { operationId: "sdk-submit-1" });
  const duplicate = await stasis.submit("echo twice", { operationId: "sdk-submit-1" });
  assert.equal(first.accepted, true);
  assert.equal(duplicate.accepted, false);

  const result = await stasis.wait(first.operationId);
  assert.equal(result.status, "done");
});

test("ergonomic SDK installs extensions and renders prompt sections", async () => {
  const { createStasis, extension } = require("../sdk/node.cjs");
  const stasis = await createStasis({
    extensions: [
      extension({
        name: "editor",
        sections: [
          { key: "preamble", tag: false, render: () => "You are an editor." },
          { key: "tone", render: ({ session }) => `Session: ${session}` },
        ],
      }),
    ],
  });

  const receipt = await stasis.submit("edit this", {
    operationId: "sdk-extension-1",
    session: "copy-desk",
  });
  const record = parseJson(await stasis.raw.job_record(receipt.operationId));
  const payload = JSON.parse(record.payload_ref);
  assert.equal(
    payload.system_prompt,
    "You are an editor.\n\n<tone>\nSession: copy-desk\n</tone>",
  );
});

test("CommonJS and ESM SDK cores stay behaviorally identical", () => {
  const fs = require("node:fs");
  const path = require("node:path");
  const sdkDir = path.join(__dirname, "..", "sdk");
  let expected = fs.readFileSync(path.join(sdkDir, "core.mjs"), "utf8");
  expected = expected
    .replace("export function memoryLifecycleStore", "function memoryLifecycleStore")
    .replace("export function webStorageLifecycleStore", "function webStorageLifecycleStore")
    .replace("export function tool", "function tool")
    .replace("export function extension", "function extension")
    .replace("export class StasisSession", "class StasisSession")
    .replace("export class Stasis", "class Stasis")
    .replace("export function createStasisWith", "function createStasisWith");
  expected += "\nmodule.exports = { Stasis, StasisSession, createStasisWith, extension, memoryLifecycleStore, tool, webStorageLifecycleStore };\n";
  assert.equal(fs.readFileSync(path.join(sdkDir, "core.cjs"), "utf8"), expected);
});


test("SDK streams durable lifecycle events and reconnects from a cursor", async () => {
  const { createStasis } = require("../sdk/node.cjs");
  const stasis = await createStasis();
  const events = [];
  for await (const event of stasis.stream("stream this", { operationId: "sdk-stream-1" })) {
    events.push(event);
  }
  assert.deepEqual(events.map(({ type }) => type), ["accepted", "state", "completed"]);
  assert.equal(events.at(-1).result.status, "done");

  const reconnected = [];
  for await (const event of stasis.events("sdk-stream-1", { after: events[0].id })) {
    reconnected.push(event);
  }
  assert.deepEqual(reconnected.map(({ id }) => id), events.slice(1).map(({ id }) => id));
});

test("completed lifecycle snapshots survive runtime replacement", async () => {
  const { createStasis, memoryLifecycleStore } = require("../sdk/node.cjs");
  const lifecycleStore = memoryLifecycleStore();
  const first = await createStasis({ lifecycleStore });
  const original = await first.prompt("persist this", { operationId: "sdk-persist-1" });

  const replacement = await createStasis({ lifecycleStore });
  const restored = await replacement.wait("sdk-persist-1");
  assert.deepEqual(restored, original);
});

test("web storage adapter stores versioned JSON snapshots", async () => {
  const { createStasis, webStorageLifecycleStore } = require("../sdk/node.cjs");
  const values = new Map();
  const storage = {
    getItem: (key) => values.get(key) ?? null,
    setItem: (key, value) => values.set(key, value),
    removeItem: (key) => values.delete(key),
  };
  const stasis = await createStasis({ lifecycleStore: webStorageLifecycleStore(storage) });
  await stasis.prompt("persist locally", { operationId: "sdk-web-store-1" });
  const snapshot = JSON.parse(values.get("stasis:lifecycle:sdk-web-store-1"));
  assert.equal(snapshot.version, 1);
  assert.equal(snapshot.result.status, "done");
  assert.equal(snapshot.events.at(-1).type, "completed");
});

test("SDK cancellation is a persisted terminal lifecycle state", async () => {
  const { createStasis } = require("../sdk/node.cjs");
  const stasis = await createStasis();
  const receipt = await stasis.submit("cancel me", { operationId: "sdk-cancel-1" });
  assert.equal(await stasis.cancel(receipt.operationId), true);
  assert.equal(await stasis.raw.job_state(receipt.operationId), "canceled");
  const result = await stasis.wait(receipt.operationId);
  assert.equal(result.status, "cancelled");
});

test("tools are replay-unsafe by default and validate replay policy", () => {
  const { tool } = require("../sdk/node.cjs");
  const unsafe = tool({ name: "write", description: "write", parameters: {}, execute() {} });
  assert.equal(unsafe.replay, "unsafe");
  const safe = tool({ name: "read", description: "read", parameters: {}, replay: "safe", execute() {} });
  assert.equal(safe.replay, "safe");
  assert.throws(
    () => tool({ name: "bad", description: "bad", parameters: {}, replay: "sometimes", execute() {} }),
    /tool.replay/,
  );
});

test("resume refuses dead-letter replay for unsafe tools", async () => {
  const { Stasis, memoryLifecycleStore, tool } = require("../sdk/core.cjs");
  const lifecycleStore = memoryLifecycleStore();
  await lifecycleStore.save({
    version: 1,
    operationId: "unsafe-replay-1",
    session: "default",
    tool: "charge_card",
    status: "dead_letter",
    events: [],
  });
  let replayed = false;
  const client = {
    register_tool() {},
    async job_state() { return "dead_letter"; },
    async replay_dead_letter() { replayed = true; return true; },
  };
  const stasis = new Stasis(client, {
    lifecycleStore,
    tools: [tool({
      name: "charge_card",
      description: "Charge a card",
      parameters: {},
      execute() { return true; },
    })],
  });
  await assert.rejects(stasis.resume("unsafe-replay-1"), /replay-unsafe tool charge_card/);
  assert.equal(replayed, false);
});

test("aborting a wait cancels the kernel job by default", async () => {
  const { createStasis } = require("../sdk/node.cjs");
  const stasis = await createStasis();
  const receipt = await stasis.submit("abort me", { operationId: "sdk-abort-1" });
  const controller = new AbortController();
  controller.abort();
  await assert.rejects(
    stasis.wait(receipt.operationId, { signal: controller.signal }),
    (error) => error?.name === "AbortError",
  );
  assert.equal(await stasis.raw.job_state(receipt.operationId), "canceled");
});
