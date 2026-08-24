# ADR-0009 WASM Target Profile for the Stasis Kernel

## Document Metadata

- Document Type: Architecture Standard
- Audience: Engineer, Architect, Platform Owner
- Stability: Evolving
- Last Verified: 2026-08-24
- Verified Against:
  - Cargo.toml
  - src/lib.rs
  - src/application/composition/runtime_composition.rs
  - src/infrastructure/memory/locus_node_store_factory.rs
  - src/infrastructure/runtime/kafka_wasm_transport_publisher.rs
  - docs/design/wasm-target-phase-plan.md
  - docs/design/locus-integration-rfc-plan.md

## Status

Proposed

## Date

2026-08-24

## Context

Sibling crates already have WASM profiles:

- **Locus 0.5.0 / locus-sdk 0.3.0** compiles in-memory stores, parsing, and services to `wasm32-unknown-unknown`. HTTP/genai are feature-gated. Browser hosts use `locus-wasm` plus `locus-surreal-adapter` (`kv-indxdb`, `kv-mem`, `protocol-ws`).
- **Grapheme 0.7.1** can compile workflow stdlib/Stage B artifacts *to* Wasm. Stasis still executes Grapheme through the in-process host SDK on native.

Stasis 0.9.3 consumed those bumps (`#7`, `#5`, `#6`) but the `stasis-rs` crate itself remains a native-only graph:

- `default = []` while Axum, Askama, Surreal filesystem engines, `reqwest`/`rustls`, `genai`, Grapheme host, and `tokio` `rt-multi-thread` are unconditional dependencies.
- There is no `#[cfg(target_arch = "wasm32")]` split and no `wasm.yml` CI gate.
- The only WASM-named adapter (`transport-kafka-wasm`) is a stub that errors until `rfkafka_wasi` is bound.
- `stasisd`, `stasis_dashboard`, file-secret bootstrap, TCP/Kafka/RabbitMQ, and OTLP/gRPC are process-host concerns.

Medousa and other browser/edge hosts cannot embed the Stasis kernel until the library can compile and run a slim profile on WASM.

Two WASM stories must stay distinct:

1. **Stasis hosts Wasm** — Grapheme Stage B / Wasix artifacts execute *inside* a native (or later WASM) Stasis worker. Already partially true on native.
2. **Stasis *is* Wasm** — the kernel compiles to `wasm32-unknown-unknown` (browser) and later `wasm32-wasip1` / WASIX (edge). This ADR.

## Decision

Make the **Stasis runtime kernel** (`stasis-rs` library) compilable for WASM environments. Keep native process hosts and native-only adapters out of that profile.

1. **Kernel vs host.** `stasis-rs` is the portable kernel. `stasisd`, `stasis_dashboard`, filesystem watch, systemd, and OTLP exporters stay native-only.
2. **Follow the Locus feature split.** Native defaults stay the full host experience. WASM builds use `--no-default-features` plus explicit opt-ins. Do not change runtime job semantics to get a compile.
3. **First WASM slice is in-memory.** Domain, ports, in-memory stores, `StasisSdk` / `RuntimeSdk`, and injected `AiChatClient` / memory ports must compile and run without filesystem Surreal, genai, Axum, or Grapheme host.
4. **Persistence on WASM is remote or browser-local.** `RuntimeBackend::SurrealKv` stays native. Browser/edge durability comes later via remote Surreal WebSocket and Locus IndexedDB (`indxdb://`), not embedded `surrealkv://`.
5. **Providers are ports.** WASM hosts inject chat/memory/transport adapters. `genai` and `reqwest`/`rustls` are native (or later explicit WASM-http) features, not kernel requirements.
6. **Grapheme host is native-first.** Workflow handlers stay behind a `grapheme` feature. Compiling the Grapheme *host* SDK into the Stasis WASM guest is out of the first slices. Stage B remains “Stasis hosts Wasm artifacts.”
7. **No `stasis-wasm` cdylib until the kernel checks.** Browser bindings wrap a compiling kernel; they are not the first PR.
8. **Kafka WASM stays a placeholder** until the kernel profile exists. Binding `rfkafka_wasi` is a later transport slice, not a compile-unblocking step.

## Non-Goals

1. Running `stasisd` or the Axum dashboard inside a browser.
2. Embedded SurrealKV / filesystem secrets on `wasm32-unknown-unknown`.
3. Changing job state machine, lease, or lineage contracts.
4. Shipping vendor agent adapters or Medousa UI in this repo.
5. Requiring every native feature to work on WASM.

## Consequences

### Positive

1. Browser and edge hosts can embed the same job/memory/tool contracts Medousa already uses natively.
2. Feature gates make the native graph honest (`--no-default-features` actually means slim).
3. Locus/Grapheme WASM work becomes consumable instead of version-only prep.

### Tradeoffs

1. Optional-dependency and `#[cfg]` churn across `lib.rs`, dashboard, Surreal adapters, and LLM/Grapheme modules.
2. `default = ["native"]` (or equivalent) is the first time default features mean something; document it for crates.io consumers.
3. Tokio must use target-specific features (`rt` on WASM, `rt-multi-thread` on native).
4. Some handlers (Grapheme, cluster TCP/Kafka, OTEL) will be absent on WASM until later slices.

## Follow-through

- Epic / phase board: [design/wasm-target-phase-plan.md](../design/wasm-target-phase-plan.md)
- Related: Locus WASM PRs EntasisLabs/locus#17–#19; Grapheme Stage B EntasisLabs/grapheme#14–#17; Stasis bumps #5–#7
