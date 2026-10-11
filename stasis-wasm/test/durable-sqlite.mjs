import { createRequire } from "node:module";
import assert from "node:assert/strict";
import test from "node:test";
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";
import path from "node:path";
import fs from "node:fs";
import { createNodeSqliteStorage, createSqlBridge } from "../../examples/cloudflare-durable-object/src/sql-bridge.js";

const require = createRequire(import.meta.url);
const here = path.dirname(fileURLToPath(import.meta.url));
const pkgNode = path.join(here, "..", "pkg-node", "stasis_wasm.js");

function loadWasm() {
  assert.ok(fs.existsSync(pkgNode), `missing ${pkgNode}; run npm run build in stasis-wasm first`);
  return require(pkgNode);
}

function openKernel() {
  const { install_durable_sql_bridge, StasisDurableSqlite } = loadWasm();
  const db = new DatabaseSync(":memory:");
  const bridge = createSqlBridge(createNodeSqliteStorage(db));
  install_durable_sql_bridge(bridge.exec, bridge.transaction);
  return { db, kernel: () => new StasisDurableSqlite() };
}

test("durable sqlite: enqueue, dedupe, process, and read back after eviction", () => {
  const { kernel: open } = openKernel();
  let kernel = open();
  const accepted = JSON.parse(kernel.accept_job("order-17", "ship-it"));
  assert.equal(accepted.disposition, "accepted");

  const duplicate = JSON.parse(kernel.accept_job("order-17", "ship-it-again"));
  assert.equal(duplicate.disposition, "duplicate");
  assert.equal(duplicate.jobId, accepted.jobId);

  const processed = JSON.parse(kernel.process_once("default", "worker-a"));
  assert.equal(processed.processed, true);
  assert.equal(processed.jobId, accepted.jobId);
  assert.equal(processed.state, "succeeded");
  assert.equal(JSON.parse(kernel.process_once("default", "worker-a")).processed, false);

  kernel = open();
  const job = JSON.parse(kernel.get_job(accepted.jobId));
  assert.equal(job.state, "succeeded");
  assert.equal(job.payload, "ship-it");
  assert.equal(job.idempotencyKey, "order-17");
  assert.equal(job.attempts, 1);
  assert.equal(job.leaseOwner, null);

  const again = JSON.parse(kernel.accept_job("order-17", "ship-it"));
  assert.equal(again.disposition, "duplicate");
  assert.equal(again.jobId, accepted.jobId);
});

test("durable sqlite: grapheme workflow state survives eviction", () => {
  const { kernel: open } = openKernel();
  let kernel = open();
  const payload =
    'grapheme:json:{"source":"op main()","workflow_id":"wf-1","state_current":{"step":2,"cursor":"abc"}}';
  JSON.parse(kernel.accept_job("wf-1", payload));
  const processed = JSON.parse(kernel.process_once("default", "worker-a"));
  assert.equal(processed.processed, true);

  kernel = open();
  const state = JSON.parse(kernel.load_workflow_state("wf-1"));
  assert.equal(state.step, 2);
  assert.equal(state.cursor, "abc");
});
