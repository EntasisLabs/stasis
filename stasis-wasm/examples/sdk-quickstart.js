import { createStasis, tool, webStorageLifecycleStore } from "stasis-wasm/sdk";

const lookupStatus = tool({
  name: "lookup_status",
  description: "Read the current status for a resource",
  replay: "safe",
  parameters: {
    type: "object",
    properties: { resourceId: { type: "string" } },
    required: ["resourceId"],
    additionalProperties: false,
  },
  async execute({ resourceId }, { signal }) {
    const response = await fetch(`/api/resources/${encodeURIComponent(resourceId)}`, { signal });
    if (!response.ok) throw new Error(`status lookup failed: ${response.status}`);
    return response.json();
  },
});

const stasis = await createStasis({
  llm: { apiKey: "short-lived-host-token", baseUrl: "/v1" },
  instructions: "Answer clearly and cite tool results.",
  tools: [lookupStatus],
  lifecycleStore: webStorageLifecycleStore(localStorage),
});

for await (const event of stasis.session("customer-42").stream(
  "What is the status of resource alpha?",
  { operationId: "request-42", toolInput: { resourceId: "alpha" } },
)) {
  console.log(event.type, event.result?.text ?? "");
}
