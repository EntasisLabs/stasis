# stasis-wasm

Optional `wasm-bindgen` bindings for the Stasis in-memory kernel (ADR-0009).

This crate wraps `stasis-rs --no-default-features` plus `llm-openai-http` and `grapheme`. It does **not** compile the dashboard, `stasisd`, or SurrealKV. Grapheme runs in-guest via published [`grapheme-wasm` 0.7.1](https://crates.io/crates/grapheme-wasm) (stdlib: `core` / `json` / `csv` / `yaml` / `html`).

The npm package name is `stasis-wasm` (version `0.14.0`). The Rust crate of the same name stays unpublished (`publish = false` on crates.io).

After publish:

```bash
npm install stasis-wasm
```

From this checkout (after `npm run build`): `npm install ./stasis-wasm`. Owner publish steps: [RELEASE.md](../RELEASE.md#npm-stasis-wasm).

## Golden path SDK

`stasis-wasm/sdk` is the high-level API for application code. It initializes WASM, installs
object-based tools and prompt sections, owns queue pumping, and returns parsed results. The raw
`StasisWasmClient` remains available as `stasis.raw` for memory, replay, Grapheme, and operator
controls.

```js
import { createStasis, tool } from "stasis-wasm/sdk";

const lookupStatus = tool({
  name: "lookup_status",
  description: "Read a resource status",
  parameters: {
    type: "object",
    properties: { resourceId: { type: "string" } },
    required: ["resourceId"],
    additionalProperties: false,
  },
  async execute({ resourceId }, { signal }) {
    const response = await fetch(`/api/resources/${resourceId}`, { signal });
    return response.json();
  },
});

const stasis = await createStasis({
  llm: { apiKey: userSuppliedKey, baseUrl: "/v1" },
  instructions: "Answer clearly and cite tool results.",
  tools: [lookupStatus],
});

const { text } = await stasis.session("customer-42").prompt(
  "What is the status of alpha?",
  { operationId: "request-42", toolInput: { resourceId: "alpha" } },
);
```

### Schema-native tools

TypeBox schemas can be passed directly. Their static type flows into `execute()` with no generic or
cast, while the same object is advertised to the model as JSON Schema:

```ts
import { Type } from "@sinclair/typebox";
import { tool } from "stasis-wasm/sdk";

const Parameters = Type.Object({ resourceId: Type.String() });
const lookup = tool({
  name: "lookup_status",
  description: "Read a resource status",
  parameters: Parameters,
  execute({ resourceId }) { // resourceId: string
    return { resourceId, status: "ready" };
  },
});
```

Use `typeboxSchema()` when the callback should also run through a TypeBox parser such as
`Value.Parse`. Zod and Valibot adapters convert their schemas to the JSON Schema needed by the
model and parse again immediately before `execute()`:

```ts
import { Value } from "@sinclair/typebox/value";
import { z } from "zod";
import * as v from "valibot";
import { toJsonSchema } from "@valibot/to-json-schema";
import { typeboxSchema, valibotSchema, zodSchema } from "stasis-wasm/sdk";

const strictTypeBox = typeboxSchema(Parameters, {
  parse: input => Value.Parse(Parameters, input),
});

const ZodParameters = z.object({ resourceId: z.string() });
const zodParameters = zodSchema(ZodParameters, {
  toJSONSchema: z.toJSONSchema,
});

const ValibotParameters = v.object({ resourceId: v.string() });
const valibotParameters = valibotSchema(ValibotParameters, {
  toJsonSchema,
  parse: input => v.parse(ValibotParameters, input),
});
```

The adapters intentionally do not bundle a validator: applications choose their TypeBox, Zod, or
Valibot version. `schema(jsonSchema, parse)` supports any other validator. Parser failures use the
existing structured callback-error path, so the tool loop can recover instead of crashing. Stasis
first checks the advertised JSON Schema subset, then runs the adapter parser; adapters are for
stricter checks and transformations, not for accepting values that contradict the advertised schema.

For long work, `submit()` returns an idempotent operation receipt and `wait()` joins it later:

```js
const receipt = await stasis.submit("Summarize the report", {
  operationId: "report-summary-42",
});
const result = await stasis.wait(receipt.operationId);
```

Extensions group tools with lazily rendered system-prompt sections:

```js
import { extension } from "stasis-wasm/sdk";

const editor = extension({
  name: "editor",
  tools: [lookupStatus],
  sections: [{ key: "preamble", tag: false, render: () => "You are an editor." }],
});
const stasis = await createStasis({ extensions: [editor] });
```

The high-level SDK also exposes reconnectable lifecycle events and pluggable snapshots:

```js
import { createStasis, webStorageLifecycleStore } from "stasis-wasm/sdk";

const stasis = await createStasis({
  lifecycleStore: webStorageLifecycleStore(localStorage),
});

let cursor = Number(localStorage.getItem("report-cursor") ?? 0);
for await (const event of stasis.stream("Summarize the report", {
  operationId: "report-42",
  after: cursor,
})) {
  cursor = event.id;
  localStorage.setItem("report-cursor", String(cursor));
  if (event.type === "completed") console.log(event.result.text);
}
```

`events(operationId, { after })` reconnects to an existing operation without submitting it again.
Every event and terminal result is written through `lifecycleStore`; provide any adapter implementing
`load(operationId)` and `save(snapshot)` for SQLite, IndexedDB, Durable Objects, or an application
database. `webStorageLifecycleStore()` is the browser-ready adapter and `memoryLifecycleStore()` is
the default. Completed snapshots can be read after replacing the WASM client.

`cancel(operationId)` now transitions the kernel job itself to `canceled`; aborting `wait()`,
`prompt()`, or a stream does the same unless `cancelOnAbort: false` is explicit. Tools declare
`replay: "safe" | "unsafe"` (unsafe by default), and `resume()` refuses to re-execute a dead-lettered
unsafe tool unless the caller explicitly passes `allowUnsafeReplay: true`.

Honest durability boundary: the default WASM kernel's queue, active LLM request, and in-guest Locus
memory are still in-memory. The lifecycle adapter durably preserves event cursors and completed
results across reloads, but cannot resurrect an unfinished job in a brand-new WASM runtime. Use a
persistent Stasis runtime backend for true in-flight crash recovery. Cancellation prevents a canceled
job from committing success, but JavaScript cannot preempt synchronous callback code; use an
`AbortSignal`-aware callback or a Web Worker for that work. See
[`examples/sdk-quickstart.js`](examples/sdk-quickstart.js).

## What a browser marketing demo can do

| Surface | API | Notes |
| --- | --- | --- |
| Mock LLM (CI / no key) | `StasisWasmClient.create()` | Deterministic completion; tool-loop still invokes registered tools |
| Real OpenAI-spec LLM | `create({ apiKey, model?, baseUrl? })` | `OpenAiHttpGateway` + tool-loop chat client (`fetch`) |
| Tools | `register_tool` + `enqueue_tool_loop`; built-in `echo` / `grapheme_run` | Host callbacks may be async; invocations are in job attempt diagnostics |
| Grapheme | `enqueue_grapheme` / `enqueue_grapheme_echo` | In-process `grapheme-wasm`; no JS fake tools |
| Memory + identity | `store_memory` / `recall_memory` / `upsert_identity` | In-session Locus + in-memory identity store |
| Replay | `job_history` / `resume_from_history` | Kernel job-store attempts + lineage; succeeded jobs do **not** re-call the LLM |
| Process loop | `process_once` / `process_available` | Drain the in-memory queue |

Call `capabilities()` for the honest guest profile, including Grapheme gaps.

## Host-defined tool bootstrap

Register tools after `create()` and before processing jobs. Registration is local to that
`StasisWasmClient`; freeing the client releases its JavaScript callbacks. Names must be unique
(including built-ins), descriptions must be non-empty, and input schemas must be JSON objects.
The schema is advertised to the model and checked again before callback dispatch.

```js
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
    if (!response.ok) throw new Error(`lookup failed: ${response.status}`);
    return response.json();
  },
);
```

A model-selected call and `invoke_tool(name, arguments)` use the same registry and validation.
Successful callback values are delivered to the loop as
`{ "ok": true, "result": <json> }`. Malformed arguments, callback throws/rejections, and
non-JSON callback values are delivered as
`{ "ok": false, "error": { "tool", "code", "message" } }`, allowing the model to recover.
An unknown tool remains a registry/job error because no tool invocation exists to report.
Callbacks are bounded by a 30-second deadline by default. At the deadline Stasis aborts the
`AbortSignal` passed as the callback's second argument and returns a structured
`callback_timeout` result, so a promise that never settles cannot hold the job forever. Use
`register_tool_with_options(..., { timeoutMs })` for a tool-specific deadline (1ms through 10
minutes), and pass `signal` to `fetch` or other abort-aware APIs so underlying work is cancelled,
not merely ignored. Deadlines depend on the JavaScript event loop, so CPU-heavy synchronous work
belongs in a Web Worker rather than directly inside a callback. Callbacks must return
JSON-serializable values; `undefined`, functions,
symbols, and cyclic objects are invalid. Runtime-side validation currently enforces the schema subset used by Stasis
(`type`, `properties`, `required`, property `enum`, and boolean `additionalProperties`); the full
schema is still advertised to the model. See
[`examples/browser-tool-bootstrap.js`](examples/browser-tool-bootstrap.js) for a minimal browser
flow.

## CORS / `baseUrl`

Browsers cannot call `https://api.openai.com` directly unless the OpenAI CORS policy allows your origin (it typically does not). Point `baseUrl` at a **same-origin** proxy that forwards to an OpenAI-compatible `/v1`:

```js
const client = await StasisWasmClient.create({
  apiKey: userSuppliedKey, // never hardcode
  model: "gpt-4o-mini",
  baseUrl: "/v1", // same-origin proxy → api.openai.com/v1 or Groq / OpenRouter / Ollama
});
```

`baseUrl` is appended with `/chat/completions` unless it already ends with that path. Hosts inject the key; this package never reads process env in the browser.

## Node

```js
const { version, StasisWasmClient } = require("stasis-wasm");

async function main() {
  console.log(version());
  const client = await StasisWasmClient.create(); // mock LLM
  console.log(JSON.parse(client.capabilities()));

  await client.register_agent("planner", "Planner", "Break work into steps");
  const completion = await client.invoke_agent("planner", "Plan a kickoff");

  await client.store_memory("demo-session", "kickoff is Tuesday");
  const recalled = JSON.parse(await client.recall_memory("demo-session", "kickoff"));

  const graphemeId = await client.enqueue_grapheme_echo("g1", "hello from grapheme");
  const toolId = await client.enqueue_tool_loop(
    "t1",
    "echo this",
    "echo",
    JSON.stringify({ hello: "world" }),
    "demo-session",
  );
  await client.process_available("default", "browser-worker");

  const replay = JSON.parse(await client.resume_from_history(toolId));
  // replay.llm_called === false — stored diagnostics, no second LLM round-trip

  console.log(completion, recalled, graphemeId, replay);
}

main();
```

OpenAI construction (no network until you `invoke_agent` / process a tool-loop job):

```js
const live = await StasisWasmClient.create({
  apiKey: process.env.OPENAI_API_KEY,
  model: "gpt-4o-mini",
  baseUrl: "https://api.openai.com/v1", // Node has no browser CORS; browsers still need a proxy
});
```

## Bundler (Vite / webpack)

```js
import init, { version, StasisWasmClient } from "stasis-wasm";

await init();
const client = await StasisWasmClient.create({
  apiKey: window.prompt("OpenAI API key"),
  baseUrl: "/v1",
});
```

## Grapheme gaps (not faked in JS)

- `grapheme:file:` payloads (no filesystem on `wasm32-unknown-unknown`)
- Host-only ops (`http`, `sql`, `pdf`, `image`, …) fail in-guest
- `execution_timeout` is not preemptively enforced (no `spawn_blocking` / worker threads)
- `max_steps` / `max_call_depth` use `grapheme-wasm` `RuntimeOptions` defaults

The workspace patches `grapheme-runtime` 0.7.1 to use `web-time::Instant` on wasm32 (`std::time::Instant::now` panics on `wasm32-unknown-unknown`). Drop `vendor/grapheme-runtime` when upstream ships a wasm-safe clock.

## Build

Requires `wasm32-unknown-unknown` and `wasm-bindgen-cli` matching `Cargo.lock`.

```bash
cd stasis-wasm
npm run build   # pkg/ (bundler) + pkg-node/ (Node)
npm test
npm run pack:dry
# from repo root, after npm login:
# ./scripts/publish-npm.sh          # dry-run
# CONFIRM_PUBLISH=yes ./scripts/publish-npm.sh --execute
```
