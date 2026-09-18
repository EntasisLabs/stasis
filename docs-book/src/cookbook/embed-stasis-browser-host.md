# Embed Stasis in a Browser Host

## Document Metadata

- Document Type: Cookbook Recipe
- Audience: Engineer
- Stability: Evolving
- Last Verified: 2026-09-18
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
stasis-rs = { version = "0.11", default-features = false }
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

`stasis-wasm` is workspace-optional and unpublished on crates.io (`publish = false`). The npm package name is `stasis-wasm@0.12.0` (public, unscoped). Point a local app at a checkout with `npm install ../path/to/stasis-wasm` after `npm run build`. Owner publish: [RELEASE.md](https://github.com/EntasisLabs/stasis/blob/main/RELEASE.md#npm-stasis-wasm).

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
