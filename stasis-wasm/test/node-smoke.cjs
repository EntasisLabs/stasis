"use strict";

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const pkgNode = path.join(__dirname, "..", "pkg-node", "stasis_wasm.js");

test("stasis-wasm npm package register/invoke/ping on Node", async () => {
  assert.ok(
    fs.existsSync(pkgNode),
    `missing ${pkgNode}; run \`npm run build\` in stasis-wasm first`,
  );

  const { version, StasisWasmClient } = require(pkgNode);
  const crateVersion = version();
  assert.equal(typeof crateVersion, "string");
  assert.equal(crateVersion, require("../package.json").version);

  const client = await StasisWasmClient.create();
  await client.register_agent("planner", "Planner", "Break work into steps");

  const completion = await client.invoke_agent("planner", "Plan a kickoff");
  assert.equal(completion, "stasis-wasm mock completion");

  const jobId = await client.enqueue_ping(7);
  assert.equal(typeof jobId, "string");
  assert.ok(jobId.length > 0);

  const processed = await client.process_once("default", "browser-worker");
  assert.equal(processed, jobId);

  const state = await client.job_state(jobId);
  assert.equal(state, "succeeded");
});
