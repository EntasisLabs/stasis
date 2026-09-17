# stasis-wasm

Optional `wasm-bindgen` bindings for the Stasis in-memory kernel (ADR-0009 phase W5).

This crate wraps `stasis-rs --no-default-features`. It does **not** compile the dashboard, `stasisd`, SurrealKV, or the Grapheme host.

```js
import init, { version, StasisWasmClient } from "./stasis_wasm.js";

await init();
console.log(version());

const client = await StasisWasmClient.create();
await client.register_agent("planner", "Planner", "Break work into steps");
const completion = await client.invoke_agent("planner", "Plan a kickoff");
const jobId = await client.enqueue_ping(7);
const processed = await client.process_once("default", "browser-worker");
```

Browser hosts inject LLM keys themselves (`OpenAiHttpGateway` lives on `stasis-rs` feature `llm-openai-http`, not in this bindings crate). Durable memory stays in Locus (`locus-wasm` / `locus-surreal-adapter`); this client uses in-memory Locus via the kernel.
