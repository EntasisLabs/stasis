import init, { StasisWasmClient } from "stasis-wasm";

await init();
const client = await StasisWasmClient.create({
  // Keep provider credentials behind a same-origin proxy in production.
  apiKey: "short-lived-host-token",
  model: "gpt-4o-mini",
  baseUrl: "/v1",
});

client.register_tool(
  "lookup_status",
  "Read the current status for a resource",
  {
    type: "object",
    properties: { resourceId: { type: "string" } },
    required: ["resourceId"],
    additionalProperties: false,
  },
  async ({ resourceId }, { signal }) => {
    const response = await fetch(`/api/resources/${encodeURIComponent(resourceId)}`, { signal });
    if (!response.ok) throw new Error(`status lookup failed: ${response.status}`);
    return response.json(); // must be JSON-serializable
  },
);

const jobId = await client.enqueue_tool_loop(
  "status-request-1",
  "What is the status of resource alpha?",
  "lookup_status",
  JSON.stringify({ resourceId: "alpha" }),
  "browser-session",
);
await client.process_available("default", "browser-worker");
console.log(JSON.parse(await client.job_history(jobId)));
