# Epic: Stasis WASM Target Profile

## Document Metadata

- Document Type: Architecture Standard
- Audience: Engineer, Architect, Platform Owner
- Stability: Evolving
- Last Verified: 2026-08-24
- Verified Against:
  - Cargo.toml
  - src/lib.rs
  - src/application/composition/runtime_composition.rs
  - src/infrastructure/runtime/mod.rs
  - src/infrastructure/memory/locus_node_store_factory.rs
  - src/dashboard/mod.rs
  - src/bin/stasis_dashboard.rs
  - stasisd/Cargo.toml
  - docs/adr/ADR-0009-wasm-target-profile.md
  - docs/design/locus-integration-rfc-plan.md

Status: **Active Epic — Phase W0 (kickoff freeze)**  
Date: 2026-08-24  
Owner: Stasis Core  
ADR: [ADR-0009-wasm-target-profile.md](../adr/ADR-0009-wasm-target-profile.md)

Depends on:

- [ADR-0001](../adr/README.md) Durable Job Runtime
- [ADR-0002](../adr/README.md) Event-Driven Orchestration
- [ADR-0007](../adr/ADR-0007-agent-platform-runtime-contracts.md) Agent Platform Runtime Contracts
- [ADR-0009](../adr/ADR-0009-wasm-target-profile.md) WASM Target Profile
- Locus 0.5.0 / locus-sdk 0.3.0 WASM compilation support
- Grapheme 0.7.1 host-lite / Stage B Wasm artifacts

## 1. Epic goal

Make the Stasis **runtime kernel** (`stasis-rs`) compile and run in WASM environments, using the same ports and in-memory runtime contracts as native hosts.

```text
Browser / edge / WASI host
        │
        ▼
  stasis-rs (wasm profile)
    domain · ports · in-memory runtime · StasisSdk / RuntimeSdk
        │
        ▼
  Injected adapters
    mock/fetch LLM · Locus in-memory / indxdb / remote WS · HTTP outbox
```

**Done looks like:**

1. `cargo check -p stasis-rs --target wasm32-unknown-unknown --no-default-features` is green in CI.
2. In-memory `StasisSdk` + `RuntimeSdk` can register an agent, enqueue a typed job, and complete it under a WASM test harness.
3. Native defaults are unchanged: dashboard, `stasisd`, SurrealKV, genai, Grapheme host, Kafka/RabbitMQ still work with default features.
4. Official docs describe the WASM profile without mixing it up with Grapheme Stage B.

## 2. Two WASM stories (do not collapse)

| Story | What it means | Status at 0.9.3 |
| --- | --- | --- |
| **A. Stasis hosts Wasm** | Grapheme Stage B / Wasix artifacts run *inside* a Stasis worker | Native host path exists (`grapheme-sdk` `host`; Stage B opt-in upstream). Not a WASM *guest*. |
| **B. Stasis *is* Wasm** | `stasis-rs` compiles to `wasm32-unknown-unknown` (and later WASI) | **Not started.** Locus/Grapheme prep only. |

This epic is **story B**. Story A stays on the Grapheme handler track and must not block story B.

A later optional slice can compile the Grapheme *host* into the Stasis WASM guest. That is W4+, not W1.

## 3. Current state snapshot (0.9.3)

### Already landed (prep, not Stasis-on-WASM)

| Work | What it unlocked | What it did *not* do |
| --- | --- | --- |
| Grapheme 0.7.0 / 0.7.1 (`#5`, `#6`) | Lean `host` profile; Stage B AOT artifacts; `dashboard-lsp` / `grapheme-full` opt-in | Stasis still links Grapheme host unconditionally via `grapheme-sdk` `features = ["host"]` |
| Locus 0.5.0 / locus-sdk 0.3.0 (`#7`) | Locus core/SDK compile on `wasm32-unknown-unknown`; `http-providers` / `testing` / `surreal-runtime` splits | Stasis still uses default `locus-sdk` features and only `.with_locus_memory()` (in-memory). No `locus-wasm` / indxdb wiring |
| `transport-kafka-wasm` | Feature-gated stub publisher | Errors at runtime: not bound to `rfkafka_wasi` |

### Stasis kernel is still native-only

Evidence:

1. **No target cfgs.** Repo-wide search finds no `target_arch = "wasm32"` / `wasm-bindgen` / `wasm32-wasip1` in Rust sources.
2. **Unconditional native graph** in root `Cargo.toml`:
   - `axum`, `askama`, `askama_axum`, `rust-embed` (dashboard always in `lib.rs`)
   - `tokio` features `rt-multi-thread` (unsupported on `wasm32-unknown-unknown`)
   - `surrealdb` features `kv-surrealkv` + `rustls` (filesystem / native TLS)
   - `reqwest` `rustls-tls`, `genai`
   - `grapheme-sdk` / `grapheme-compiler` host
   - `dotenvy` + `std::fs` secrets (`application/config`)
3. **`default = []` is misleading.** Features only gate kafka / rabbitmq / otel / dashboard-embedded / LSP. The heavy native crates are not optional, so `--no-default-features` still pulls the full native graph.
4. **Process hosts are in-tree and native.** `src/bin/stasis_dashboard.rs` binds `tokio::net::TcpListener`. `stasisd` depends on `notify`, `signal`, and `rt-multi-thread`.
5. **Runtime backends.** `RuntimeBackend::{InMemory, SurrealMem, SurrealWs, SurrealKv}` are always compiled. `SurrealKv` is a filesystem path. Factory always imports `surrealdb::engine::any::Any`.
6. **Memory adapter.** `LocusMemoryStore::in_memory()` already uses WASM-capable Locus types. The blocker is the crate graph around it, not the adapter API.
7. **No WASM CI.** `.github/` has only a PR template. Locus has `wasm.yml`; Stasis does not.
8. **`build.rs`** always runs Tailwind / asserts `dashboard_assets/static/dashboard.css`. Harmless if the prebuilt CSS exists, but it is a dashboard-host concern.

### What can be portable with cfg work (no semantic redesign)

- `domain`, most `ports`, in-memory runtime + stores
- `StasisSdk` with `InMemoryAgentRepository` + `MockLlmGateway` / injected `AiChatClient`
- Locus in-memory memory ports (already used)
- Typed jobs, durable waits, in-memory leases (cooperative single-thread runtime)
- JSON agent codec, in-memory ingress / turn-wait
- `tokio::sync` (`mpsc`, `watch`) used by streaming and job context

### What stays native (explicitly out of the WASM profile)

| Surface | Why |
| --- | --- |
| `stasisd` | `notify`, filesystem watch, systemd, multi-thread runtime |
| `stasis_dashboard` / Axum dashboard | TCP listen, Askama, rust-embed |
| `RuntimeBackend::SurrealKv` | filesystem engine |
| File secrets / dotenv bootstrap | `std::fs` + `dotenvy` |
| `TcpSocketTransportPublisher` | `tokio::net` |
| `transport-kafka` / `transport-rabbitmq` | native clients |
| `otel` | `opentelemetry-otlp` gRPC / tokio runtime |
| Kafka WASM stub | placeholder only; do not bind in W1 |
| Grapheme host engine | `tokio::task` + host stdlib; story A |

## 4. North-star principles

1. **Kernel ≠ process host.** Same split as ADR-0008 (`stasis` vs `stasisd`).
2. **Native defaults do not regress.** Crates.io consumers keep today’s behavior with default features.
3. **Ports over vendors.** WASM hosts inject LLM, memory, and transport. No `window.fetch` types in `domain` / `application`.
4. **In-memory first, durable second.** Compile + cooperative runtime before Surreal WS / indxdb.
5. **Do not invent a second orchestrator** for the browser. Same job/lease/outbox contracts; thinner adapters.
6. **Story A ≠ story B.** Grapheme Stage B work does not count as “Stasis targets WASM.”

## 5. Target feature graph (W1 contract)

`Cargo.toml` today: `default = []` with always-on native deps. W1 makes that honest.

Proposed shape (names can tighten in the W1 PR; the *split* is frozen):

```toml
[features]
default = ["native"]
native = [
  "dashboard",
  "surreal-native",
  "llm-genai",
  "grapheme",
  "env-fs",
]
dashboard = []          # axum / askama / rust-embed
surreal-native = []     # kv-surrealkv + rustls + factory SurrealKv
llm-genai = []          # genai + reqwest rustls
grapheme = []           # grapheme-sdk host + handlers
env-fs = []             # dotenvy + file secrets
# existing: otel, transport-kafka, transport-rabbitmq, transport-kafka-wasm,
#           dashboard-embedded, dashboard-lsp, grapheme-full
```

Target-specific Tokio (required regardless of feature names):

```toml
[target.'cfg(not(target_arch = "wasm32"))'.dependencies]
tokio = { version = "1", features = ["macros", "rt-multi-thread", "sync", "time"] }

[target.'cfg(target_arch = "wasm32")'.dependencies]
tokio = { version = "1", features = ["macros", "rt", "sync", "time"] }
```

WASM check command (frozen):

```bash
rustup target add wasm32-unknown-unknown
cargo check -p stasis-rs --target wasm32-unknown-unknown --no-default-features
```

Native check must stay:

```bash
cargo check --workspace
```

`--no-default-features` on native becomes a slim kernel (no dashboard/genai/grapheme/surreal-kv). Document that as intentional. Today `--no-default-features` is a no-op; this is the first time it means something.

## 6. Phased epic board

### Phase W0 — Kickoff freeze (this document)

**Goal:** Lock the two WASM stories, in/out list, and feature split so W1 does not invent policy.

| Item | Decision |
| --- | --- |
| Target 1 | `wasm32-unknown-unknown` (browser / wasm-bindgen hosts) |
| Target 2 (later) | `wasm32-wasip1` / WASIX — not required for W1–W3 |
| Default features | `native` bundle; WASM uses `--no-default-features` |
| First runtime | `RuntimeBackend::InMemory` only |
| LLM on WASM | Injected `AiChatClient` / `MockLlmGateway`; no `genai` |
| Memory on WASM | Existing Locus in-memory adapters; indxdb/WS in W3–W4 |
| Grapheme | Feature-gated off the WASM guest until a dedicated slice |
| Bindings crate | No `stasis-wasm` until W5 |
| CI | `wasm.yml` lands with the first green `cargo check` (W1) |
| ADR | 0009 Proposed; flip to Accepted when W1 merges |

**Exit:** ADR + this board merged; no runtime code change required.

### Phase W1 — Feature graph + compile

**Goal:** Slim kernel `cargo check`s for `wasm32-unknown-unknown`.

Actions:

1. Make dashboard / Surreal filesystem / genai / Grapheme / env-fs optional; `default = ["native"]`.
2. `#[cfg]` dashboard module, `stasis_dashboard` binary, SurrealKV factory arm, Grapheme handlers, file-secret bootstrap, TCP publisher.
3. Target-specific Tokio features.
4. WASM `locus-sdk` / `locus-core-rs` with `--no-default-features` (no `http-providers` / `genai-provider`).
5. Skip or cfg `build.rs` Tailwind when targeting WASM.
6. Add `.github/workflows/wasm.yml` mirroring Locus (`cargo check` only).

Acceptance:

- Native `cargo check --workspace` + existing default-feature tests still pass.
- `cargo check -p stasis-rs --target wasm32-unknown-unknown --no-default-features` succeeds.
- `--no-default-features` documented in README / environment-configuration.

**First cooking slice:** optionalize `dashboard` + Tokio target features + cfg `src/dashboard` and the dashboard binary. That removes Axum/Askama/TCP listen from the WASM graph without touching job semantics.

### Phase W2 — In-memory runtime on WASM

**Goal:** The kernel *runs*, not just type-checks.

Actions:

1. WASM-friendly clock / id generator if `SystemClock` or RNG needs `js`/`getrandom`.
2. Smoke: `StasisSdk` register + invoke with `MockLlmGateway`.
3. Smoke: `RuntimeSdk` enqueue + process one in-memory job (prompt or typed no-op).
4. Test harness: `wasm-bindgen-test` (browser) and/or wasmtime for headless CI. Prefer one harness in W2; do not require both.

Acceptance:

- Automated WASM test proves register/invoke and one job completion.
- Job diagnostics/lineage fields unchanged vs native in-memory.

### Phase W3 — Network adapters (opt-in)

**Goal:** Talk to the outside world without native TLS/filesystem.

Actions:

1. Feature `http-wasm`: webhook / cluster forwarder via `reqwest` WASM (`fetch`) or a small `gloo-net` adapter behind the existing HTTP ports.
2. Optional `surreal-ws` on WASM: remote `wss://` only (same rule as `locus-surreal-adapter` wasm profile).
3. Keep `SurrealKv` / `surreal-native` off this target.

Acceptance:

- Feature-gated `cargo check` for `http-wasm` and `surreal-ws` on `wasm32-unknown-unknown`.
- Native HTTP webhook + SurrealWS paths unchanged.

### Phase W4 — Memory plane on WASM

**Goal:** Use the Locus 0.5 work for real, not just a version bump.

Actions:

1. Default WASM memory = existing `LocusMemoryStore::in_memory()`.
2. Optional IndexedDB / remote WS through `locus-surreal-adapter` / `locus-wasm` — **adapter wiring in Stasis**, not a fork of Locus APIs.
3. Identity-memory Surreal adapter stays behind `surreal-ws` / native features.

Acceptance:

- Memory recall/store job paths run on in-memory Locus under the WASM harness.
- Docs state that `locus-wasm` crates remain the browser persistence implementation; Stasis does not reimplement STTP.

### Phase W5 — Browser bindings (optional)

**Goal:** A `stasis-wasm` cdylib only after W1–W2 are green.

Actions:

1. `wasm-bindgen` surface analogous to `locus-wasm`: `version()`, init, in-memory client, register/invoke, enqueue/process.
2. Package output for Medousa / other web hosts.
3. Still no dashboard-in-WASM.

Acceptance:

- JS/TS can init an in-memory runtime and complete a mock agent turn.
- Bindings crate is workspace-optional (`publish` decision separate).

### Phase W6 — Docs + consumer migration

**Goal:** Official lane describes the profile; Medousa can adopt.

Actions:

1. README + docs-book environment page: feature table, targets, non-goals.
2. Cookbook: “embed Stasis in a browser host” (in-memory + injected LLM).
3. Flip ADR-0009 to Accepted if not already flipped at W1.
4. Note Story A (Grapheme Stage B) vs Story B in grapheme-workflow-handlers.

Acceptance:

- Official docs do not claim dashboard/`stasisd`/SurrealKV work on WASM.
- Medousa (or a fixture host) depends on the slim profile without forking Stasis.

## 7. Out of scope for the epic

1. Porting `stasisd` or the command-center process to WASM.
2. Binding `rfkafka_wasi` (keep the placeholder until a transport-specific PR).
3. Compiling OTEL gRPC exporters to WASM.
4. Multi-threaded Tokio / `spawn_blocking` semantics in the browser.
5. Changing ADR-0001 job/lease contracts to “fit” WASM.

## 8. Testing strategy

| Phase | Gate |
| --- | --- |
| W1 | Native workspace check + `cargo check --target wasm32-unknown-unknown --no-default-features` |
| W2 | WASM harness smoke (sdk + one job) |
| W3 | Feature-matrix checks (`http-wasm`, `surreal-ws`) |
| W4 | Memory job parity on in-memory Locus (WASM) |
| W5 | Bindings package build + JS unit test |
| All | Native default-feature tests remain the compatibility backstop |

Do not run the full native `cargo test --workspace` on WASM. Most `#[tokio::test]` suites assume a multi-thread runtime and filesystem.

## 9. Risks

| Risk | Mitigation |
| --- | --- |
| Optional-dep explosion breaks native CI | Land W1 in a dedicated PR; keep `default = ["native"]`; run full native tests before WASM claims |
| Grapheme host dragged in transitively | Keep `grapheme` off default-less WASM; do not enable `grapheme-full` / Stage B on the guest |
| `surrealdb` `engine::any` still pulls native engines | Cfg the factory and Surreal modules; do not compile `SurrealKv` on wasm32 |
| Tokio `time` / RNG gaps in browser | `wasm-bindgen` `js` features for `getrandom`/`uuid` if W2 smoke fails |
| Scope creep into dashboard-in-browser | ADR non-goal; reject PRs that compile Axum into the WASM profile |
| Confusing Story A with Story B | This doc + README table; Stage B work stays on Grapheme handler PRs |

## 10. Immediate next step

Ship **Phase W1 first cooking slice** as a dedicated PR:

1. Accept ADR-0009 (status flip) or keep Proposed until the check is green — prefer flip when `wasm.yml` passes.
2. Introduce `native` / `dashboard` features and target-specific Tokio.
3. Cfg-out `src/dashboard` and `src/bin/stasis_dashboard.rs` from `wasm32`.
4. Do not start `stasis-wasm` bindings or `rfkafka_wasi` in that PR.

## 11. Workstream map

| Track | Outcome | Starts |
| --- | --- | --- |
| **K — Kernel graph** | Optional native crates + wasm32 check | W1 |
| **R — Runtime smoke** | In-memory SDK/jobs on a WASM harness | W2 |
| **N — Network** | fetch HTTP + remote Surreal WS | W3 |
| **M — Memory** | Locus in-memory, then indxdb/WS | W4 |
| **B — Bindings** | optional `stasis-wasm` | W5 (after K+R) |

K is on the critical path. N/M can overlap after W2. B waits on K+R.
