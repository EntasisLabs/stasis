import init, {
  install_durable_sql_bridge,
  StasisDurableSqlite,
} from "../../../stasis-wasm/pkg/stasis_wasm.js";
import { createDurableObjectStorage, createSqlBridge } from "./sql-bridge.js";

await init();

function json(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

async function readJson(request) {
  const text = await request.text();
  if (!text) return {};
  return JSON.parse(text);
}

export class StasisDurableObject extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    const bridge = createSqlBridge(createDurableObjectStorage(ctx));
    install_durable_sql_bridge(bridge.exec, bridge.transaction);
    this.kernel = new StasisDurableSqlite();
  }

  evict() {
    this.kernel = new StasisDurableSqlite();
    return { evicted: true };
  }

  async fetch(request) {
    const url = new URL(request.url);
    try {
      if (request.method === "GET" && url.pathname === "/") {
        return json({
          service: "stasis-durable-sqlite",
          storage: "durable-object-sqlite",
          orchestration: "StoreRuntime",
          sqlite: [
            "jobs",
            "job-leases",
            "heartbeats",
            "attempts",
            "inbound-idempotency",
          ],
          besideEngine: ["grapheme-workflow-state"],
          inMemory: ["recurring", "outbox", "waits", "continuations"],
        });
      }
      if (request.method === "POST" && url.pathname === "/jobs") {
        const body = await readJson(request);
        const key = body.idempotencyKey;
        if (!key || typeof body.payload !== "string") {
          return json({ error: "idempotencyKey and payload are required" }, 400);
        }
        const raw = this.kernel.accept_job(String(key), body.payload);
        return json(JSON.parse(raw));
      }
      if (request.method === "POST" && url.pathname === "/process") {
        const body = await readJson(request);
        const raw = this.kernel.process_once(
          body.queue || "default",
          body.workerId || "worker",
        );
        return json(JSON.parse(raw));
      }
      if (request.method === "POST" && url.pathname === "/evict") {
        return json(this.evict());
      }
      if (request.method === "GET" && url.pathname.startsWith("/jobs/")) {
        const id = decodeURIComponent(url.pathname.slice("/jobs/".length));
        return json(JSON.parse(this.kernel.get_job(id)));
      }
      if (request.method === "POST" && url.pathname === "/workflows") {
        const body = await readJson(request);
        if (!body.workflowId || typeof body.source !== "string" || body.state === undefined) {
          return json({ error: "workflowId, source, and state are required" }, 400);
        }
        this.kernel.save_workflow_state(
          String(body.workflowId),
          body.source,
          JSON.stringify(body.state),
        );
        return json({ workflowId: body.workflowId, saved: true });
      }
      if (request.method === "GET" && url.pathname.startsWith("/workflows/")) {
        const id = decodeURIComponent(url.pathname.slice("/workflows/".length));
        const state = this.kernel.load_workflow_state(id);
        return json({ workflowId: id, state: JSON.parse(state) });
      }
      return json({ error: "not found" }, 404);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return json({ error: message }, 500);
    }
  }
}

export default {
  async fetch(request, env) {
    const id = env.STASIS.idFromName("stasis");
    const stub = env.STASIS.get(id);
    return stub.fetch(request);
  },
};
