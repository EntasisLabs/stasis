import { createRequire } from 'node:module';
import { DatabaseSync } from 'node:sqlite';
import assert from 'node:assert/strict';
import test from 'node:test';
import { createSqlBridge, createNodeSqliteStorage, createDurableObjectStorage } from '../../examples/cloudflare-durable-object/src/sql-bridge.js';
const wasm = createRequire(import.meta.url)('../pkg-node/stasis_wasm.js');
function install(storage) { const b=createSqlBridge(storage); wasm.install_durable_sql_bridge(b.exec,b.transaction); return new wasm.StasisDurableSqlite(); }
function count(db, sql) { return db.prepare(sql).get().n; }
function jobCount(db) { return count(db, 'SELECT COUNT(*) AS n FROM runtime_job'); }
function attemptCount(db) { return count(db, 'SELECT COUNT(*) AS n FROM runtime_attempt'); }
function receiptCount(db) { return count(db, 'SELECT COUNT(*) AS n FROM runtime_receipt'); }

test('DO-shaped synchronous host reconstruction preserves IDs and completed attempt/receipt', () => {
  const db=new DatabaseSync(':memory:'); const native=createNodeSqliteStorage(db); let transactions=0;
  const ctx={storage:{sql:{exec:(sql,...params)=>native.exec(sql,params)},transactionSync(fn){ transactions++; return native.transaction(fn); }}};
  let kernel=install(createDurableObjectStorage(ctx));
  const first=JSON.parse(kernel.accept_job('first','{}'));
  assert.equal(JSON.parse(kernel.process_once('default','worker')).processed,true);
  kernel.free();
  kernel=install(createDurableObjectStorage(ctx));
  const second=JSON.parse(kernel.accept_job('second','{}'));
  assert.notEqual(first.jobId,second.jobId);
  assert.equal(JSON.parse(kernel.get_job(first.jobId)).state,'succeeded');
  assert.equal(JSON.parse(kernel.accept_job('first','{}')).disposition,'duplicate');
  assert.equal(attemptCount(db),1);
  assert.equal(receiptCount(db),1);
  assert.ok(transactions>1);
  kernel.free(); db.close();
});

test('bridge replacement cannot redirect an existing kernel into another object', () => {
  const a=new DatabaseSync(':memory:'); const old=install(createNodeSqliteStorage(a));
  const accepted=JSON.parse(old.accept_job('object-a','{}'));
  const b=new DatabaseSync(':memory:'); const current=install(createNodeSqliteStorage(b));
  current.accept_job('object-b','{}');
  assert.throws(()=>old.get_job(accepted.jobId));
  assert.throws(()=>old.accept_job('misrouted','{}'));
  assert.equal(jobCount(a),1);
  assert.equal(jobCount(b),1);
  old.free(); current.free(); a.close(); b.close();
});

test('host commit failure rolls back final publication instead of returning success', () => {
  const db=new DatabaseSync(':memory:'); const native=createNodeSqliteStorage(db); let fail=false;
  const storage={exec(sql,params){ const result=native.exec(sql,params); const blob=JSON.stringify(params); if(sql.includes('INSERT INTO runtime_receipt') || (sql.includes('UPDATE runtime_job') && blob.includes('Succeeded'))) fail=true; return result; },transaction(fn){ return native.transaction(()=>{fn();if(fail){fail=false;throw new Error('injected host commit failure');}}); }};
  const kernel=install(storage); const accepted=JSON.parse(kernel.accept_job('commit-failure','{}'));
  assert.throws(()=>kernel.process_once('default','worker'),/injected host commit failure/);
  assert.equal(JSON.parse(kernel.get_job(accepted.jobId)).state,'running');
  assert.equal(attemptCount(db),0);
  assert.equal(receiptCount(db),0);
  kernel.free(); db.close();
});

test('host that omits transaction callback is explicitly unsupported', () => {
  const db=new DatabaseSync(':memory:'); const native=createNodeSqliteStorage(db);
  assert.throws(()=>install({exec:native.exec,transaction(){}}),/transaction callback/);
  db.close();
});

test('oversize row payload fails closed without storing a job', () => {
  const db=new DatabaseSync(':memory:'); const kernel=install(createNodeSqliteStorage(db));
  assert.throws(()=>kernel.accept_job('oversize',JSON.stringify({blob:'x'.repeat(1100*1024)})),/1 MiB/);
  assert.equal(jobCount(db),0);
  kernel.free(); db.close();
});
