# Embed Stasis in a Browser Host

## Document Metadata

- Document Type: Cookbook Recipe
- Audience: Engineer
- Stability: Evolving
- Last Verified: 2026-10-03
- Verified Against:
  - Cargo.toml
  - tests/wasm_kernel_smoke.rs
  - src/infrastructure/llm/openai_http_gateway.rs
  - src/infrastructure/memory/locus_node_store_factory.rs
  - src/infrastructure/runtime/grapheme_wasm_workflow_engine.rs
  - stasis-wasm/src/lib.rs
  - stasis-wasm/package.json
  - docs/adr/ADR-0009-wasm-target-profile.md

## Outcome

Embed the Stasis **kernel** in a browser or wasm-bindgen host: in-memory jobs, injected LLM, and Locus memory — without the dashboard, `stasisd`, or SurrealKV.

This is **Story B** (Stasis *is* Wasm). Grapheme Stage B (Stasis *hosts* Wasm artifacts) remains native (`grapheme-host`). The WASM guest runs workflows through published `grapheme-wasm` 0.7.1.

## Recipe

### 1. Depend on the slim profile

```toml
stasis-rs = { version = "0.13", default-features = false }
# optional:
# features = ["llm-openai-http"]   # fetch → OpenAI-spec /v1/chat/completions
# features = ["grapheme"]          # handlers + grapheme-wasm 0.7.1 in-guest engine
# features = ["http-wasm"]         # webhook / cluster forwarder
# features = ["surreal-ws"]        # remote wss:// job runtime
# features = ["locus-persist"]     # indxdb:// or wss:// Locus stores
```

```bash
rustup target add wasm32-unknown-unknown
cargo check -p stasis-rs --target wasm32-unknown-unknown --no-default-features
```

Do not enable `native` on wasm32 — the crate `compile_error`s.

### 2. In-memory runtime + mock or HTTP LLM

```rust
use stasis::sdk_prelude::{
    InMemoryAgentRepository, InvokeAgentRequest, RegisterAgentRequest, RuntimeBackend,
    RuntimeSdk, StasisSdk,
};
use stasis::application::runtime::stasis_runtime_builder::StasisRuntimeBuilder;
use stasis::infrastructure::llm::mock_gateway::MockLlmGateway;

async fn boot() -> stasis::domain::errors::Result<()> {
    let sdk = StasisSdk::new(
        InMemoryAgentRepository::default(),
        MockLlmGateway::new("browser mock"),
    );
    sdk.register_agent(RegisterAgentRequest {
        id: "planner".into(),
        name: "Planner".into(),
        system_prompt: "Break work into steps".into(),
    })
    .await?;

    let _runtime = RuntimeSdk::from_builder(
        StasisRuntimeBuilder::new(RuntimeBackend::InMemory).with_locus_memory(),
    )
    .await?;
    Ok(())
}
```

For a real model, enable `llm-openai-http` and pass the key from the host (browsers have no process env). CORS usually requires a same-origin `/v1` proxy.

```rust
use stasis::sdk_prelude_ext::OpenAiHttpGateway;

let llm = OpenAiHttpGateway::new(api_key_from_host, "gpt-4o-mini")
    .with_base_url("/v1"); // same-origin proxy
```

### 3. Optional JS/TS package (`stasis-wasm`)

The bindings crate is also an npm package named `stasis-wasm`. From the repo:

```bash
cd stasis-wasm
npm run build   # needs wasm32-unknown-unknown + wasm-bindgen-cli
npm test
npm run pack:dry
```

After it is on the registry: `npm install stasis-wasm`. Bundler hosts (Vite / webpack) import the ESM `pkg/` build:

```js
import init, { version, StasisWasmClient } from "stasis-wasm";

await init();
// Mock path (CI / no key):
const client = await StasisWasmClient.create();
// Real OpenAI-spec path — browsers need a same-origin /v1 CORS proxy:
// const client = await StasisWasmClient.create({
//   apiKey: userSuppliedKey,
//   model: "gpt-4o-mini",
//   baseUrl: "/v1",
// });
console.log(JSON.parse(client.capabilities()));
await client.register_agent("planner", "Planner", "Break work into steps");
const text = await client.invoke_agent("planner", "Plan a kickoff");
await client.store_memory("demo", "kickoff is Tuesday");
await client.enqueue_grapheme_echo("g1", "hello from grapheme");
await client.enqueue_tool_loop("t1", "echo this", "echo", JSON.stringify({ ok: true }), "demo");
await client.process_available("default", "browser-worker");
const replay = JSON.parse(await client.resume_from_history("t1"));
// replay.llm_called === false
```

Node loads the CommonJS `pkg-node/` build (`require("stasis-wasm")`) — `init()` is not required.

#### Prefer the high-level SDK for application code

The `stasis-wasm/sdk` entry point removes WASM initialization, positional queue arguments, manual
JSON conversion, and worker draining from the normal path:

```js
import { createStasis, tool } from "stasis-wasm/sdk";

const stasis = await createStasis({
  llm: { apiKey: userSuppliedKey, baseUrl: "/v1" },
  instructions: "Be concise.",
  tools: [tool({
    name: "lookup_status",
    description: "Read a resource status",
    parameters: { type: "object", additionalProperties: false },
    execute: async (_input, { signal }) => fetch("/api/status", { signal }).then(r => r.json()),
  })],
});

const { text } = await stasis.session("customer-42").prompt("Check status", {
  operationId: "request-42",
});
```

TypeBox schemas can be passed directly to `parameters`; the SDK infers the callback input from the
schema's static type. Wrap one with `typeboxSchema(schema, { parse })` to run `Value.Parse` before
execution. `zodSchema(schema, { toJSONSchema: z.toJSONSchema })` and
`valibotSchema(schema, { toJsonSchema, parse })` provide the same inference and
runtime parsing without pinning validator dependencies inside Stasis. `schema(jsonSchema, parse)` is
the generic adapter. Parser failures become structured callback errors visible to the model loop.

Use `submit()` plus `wait()` to separate acceptance from completion. `stream()` returns an async
iterable of durable `accepted`, state, and terminal events; `events(operationId, { after })` reconnects
from the last consumed numeric cursor without resubmitting work. Configure
`webStorageLifecycleStore(localStorage)` or provide a `LifecycleStore` with `load()` and `save()` to
persist snapshots in IndexedDB, SQLite, a Durable Object, or an application database. Completed
results survive replacement of the in-memory WASM client.

`cancel()` transitions the kernel job to `canceled`, and an aborted wait does so by default. Mark
tools `replay: "safe"` only when executing them again cannot duplicate side effects; unsafe is the
default and `resume()` enforces it for dead letters. Use `stasis.raw` for lower-level memory,
Grapheme, and operator APIs.

The lifecycle store preserves event cursors and terminal results, not the in-flight WASM queue or
active LLM request. A new in-memory guest cannot resurrect unfinished work; true process-crash
recovery still requires a persistent runtime backend. Cancellation fences the job result but cannot
preempt synchronous JavaScript callback code.

#### Register browser-host tools

The WASM client supports domain-independent tools backed by JavaScript callbacks:

```js
client.register_tool(
  "lookup_status",
  "Read a resource status",
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

Register tools after `StasisWasmClient.create()` and before processing tool-loop jobs. The
runtime advertises the supplied name, description, and schema to the model, validates arguments,
and awaits the callback. Results use the `stasis.tool-result.v1` envelope: `{ ok: true, result }`
or `{ ok: false, error: { tool, code, message } }`. Duplicate names are rejected. Unknown names
remain registry/job errors. Callback throws, rejected promises, `undefined`, and other
non-JSON/cyclic results become structured errors visible to the agent loop. `invoke_tool()` is
available for direct dispatch through the same path. Calls have a 30-second default deadline;
the callback receives `{ tool, timeoutMs, signal }` as its second argument and `signal` is aborted
when the deadline expires. A timed-out call returns `callback_timeout`, so an unsettled promise
cannot block a job indefinitely. Use
`register_tool_with_options(name, description, schema, callback, { timeoutMs })` for a deadline
between 1ms and 10 minutes, and pass the signal to abort-aware APIs such as `fetch`. Deadline
timers require the JavaScript event loop; move CPU-heavy synchronous work to a Web Worker.
Callback registrations live only as long as their client instance.

`stasis-wasm` is workspace-optional and unpublished on crates.io (`publish = false`). The npm package name is `stasis-wasm@0.13.0` (public, unscoped). Point a local app at a checkout with `npm install ../path/to/stasis-wasm` after `npm run build`. Owner publish: [RELEASE.md](https://github.com/EntasisLabs/stasis/blob/main/RELEASE.md#npm-stasis-wasm).

See [stasis-wasm/README.md](https://github.com/EntasisLabs/stasis/blob/main/stasis-wasm/README.md) for Grapheme gaps (`grapheme:file:`, host-only `http`/`sql`, no preemptive timeout).

### 4. Memory persistence stays in Locus

Default WASM memory is `LocusMemoryStore::in_memory()`. For IndexedDB or remote Surreal, enable `locus-persist` and call `LocusNodeStoreFactory::from_surreal_endpoint`:

| Endpoint | Meaning |
| --- | --- |
| `indxdb://stasis` | Browser IndexedDB via `locus-surreal-adapter` |
| `mem://` | Embedded Surreal mem engine |
| `wss://…` | Remote Surreal WebSocket |

Do not reimplement STTP in the host. `locus-wasm` remains the browser persistence implementation; Stasis only wires adapters.

Job durability on wasm uses `--features surreal-ws` and `RuntimeSdk::surreal_ws("wss://…", ns, db)` — never `surrealkv://`.

### 5. What not to compile

- `stasisd`, Axum dashboard, `surreal-native` / SurrealKV
- `llm-genai` (native TLS `genai` client)
- Grapheme **host** (`grapheme-host` / `grapheme-full` / `grapheme-sdk/host`) — the guest uses `grapheme` + `grapheme-wasm` instead
- OTEL gRPC exporters, `rfkafka_wasi` (placeholder only)

## Verify

```bash
cargo test -p stasis-rs --test wasm_kernel_smoke
CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \
WASM_BINDGEN_TEST_ONLY_NODE=1 \
cargo test -p stasis-rs --target wasm32-unknown-unknown --no-default-features --test wasm_kernel_smoke

cargo check -p stasis-rs --target wasm32-unknown-unknown --no-default-features --features llm-openai-http,grapheme
cd stasis-wasm && npm run build && npm test
```
