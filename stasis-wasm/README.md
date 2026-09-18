# stasis-wasm

Optional `wasm-bindgen` bindings for the Stasis in-memory kernel (ADR-0009).

This crate wraps `stasis-rs --no-default-features` plus `llm-openai-http` and `grapheme`. It does **not** compile the dashboard, `stasisd`, or SurrealKV. Grapheme runs in-guest via published [`grapheme-wasm` 0.7.1](https://crates.io/crates/grapheme-wasm) (stdlib: `core` / `json` / `csv` / `yaml` / `html`).

The npm package name is `stasis-wasm` (version `0.12.0`). The Rust crate of the same name stays unpublished (`publish = false` on crates.io).

After publish:

```bash
npm install stasis-wasm
```

From this checkout (after `npm run build`): `npm install ./stasis-wasm`. Owner publish steps: [RELEASE.md](../RELEASE.md#npm-stasis-wasm).

## What a browser marketing demo can do

| Surface | API | Notes |
| --- | --- | --- |
| Mock LLM (CI / no key) | `StasisWasmClient.create()` | Deterministic completion; tool-loop still invokes registered tools |
| Real OpenAI-spec LLM | `create({ apiKey, model?, baseUrl? })` | `OpenAiHttpGateway` + tool-loop chat client (`fetch`) |
| Tools | `enqueue_tool_loop` + built-in `echo` / `grapheme_run` | Invocations are in job attempt diagnostics |
| Grapheme | `enqueue_grapheme` / `enqueue_grapheme_echo` | In-process `grapheme-wasm`; no JS fake tools |
| Memory + identity | `store_memory` / `recall_memory` / `upsert_identity` | In-session Locus + in-memory identity store |
| Replay | `job_history` / `resume_from_history` | Kernel job-store attempts + lineage; succeeded jobs do **not** re-call the LLM |
| Process loop | `process_once` / `process_available` | Drain the in-memory queue |

Call `capabilities()` for the honest guest profile, including Grapheme gaps.

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
