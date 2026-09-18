# stasis-wasm

Optional `wasm-bindgen` bindings for the Stasis in-memory kernel (ADR-0009 phase W5).

This crate wraps `stasis-rs --no-default-features`. It does **not** compile the dashboard, `stasisd`, SurrealKV, or the Grapheme host.

The npm package name is `stasis-wasm` (version `0.11.0`). The Rust crate of the same name stays unpublished (`publish = false` on crates.io).

After the first publish:

```bash
npm install stasis-wasm
```

From this checkout (after `npm run build`): `npm install ./stasis-wasm`. Owner publish steps: [RELEASE.md](../RELEASE.md#npm-stasis-wasm).

## Node

```js
const { version, StasisWasmClient } = require("stasis-wasm");

async function main() {
  console.log(version());
  const client = await StasisWasmClient.create();
  await client.register_agent("planner", "Planner", "Break work into steps");
  const completion = await client.invoke_agent("planner", "Plan a kickoff");
  const jobId = await client.enqueue_ping(7);
  await client.process_once("default", "browser-worker");
  console.log(completion, await client.job_state(jobId));
}

main();
```

## Bundler (Vite / webpack)

```js
import init, { version, StasisWasmClient } from "stasis-wasm";

await init();
const client = await StasisWasmClient.create();
```

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

Browser hosts inject LLM keys themselves (`OpenAiHttpGateway` lives on `stasis-rs` feature `llm-openai-http`, not in this bindings crate). Durable memory stays in Locus (`locus-wasm` / `locus-surreal-adapter`); this client uses in-memory Locus via the kernel.
