# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).


## [Unreleased]

## [0.10.0] - 2026-08-28

### Added

- **Runtime-neutral federated job contract (ADR-0010).** Optional `ProvenanceRef` input/output lineage replaces mandatory STTP fields (STTP retained via `SttpProvenanceAdapter`). Signed `RemoteJobEnvelope` carries job type, content-addressed payload descriptor, idempotency/correlation/causation, deadline, origin authority, terminal-delivery endpoint, and placement. Per-job `PlacementConstraints` are matched atomically in `JobStore::lease_due`. Fenced `OwnershipHandoffStore` (reserve/commit/abort) prevents dual execution of a resource generation. `BlobTransferPort` keeps artifacts out of band; `FederatedDeliveryPort`/`FederatedIngressPort` move signals and terminal results across independent runtimes without a shared DB.

### Changed

- **Job lineage fields are optional and scheme-neutral.** `NewJob` / `Job` / `RuntimeEvent` no longer require `sttp_input_node_id: String`. Use `input_provenance: Option<ProvenanceRef>` and `output_provenance: Option<ProvenanceRef>` (STTP helpers: `ProvenanceRef::sttp(...)`, `Job::sttp_input_node_id()`, `NewJob::with_sttp_input_node_id(...)`).
- **Successful execution is scheme-neutral.** `JobExecutionOutcome::Success` now carries `output_provenance: Option<ProvenanceRef>`; typed generic jobs use CAS output provenance instead of fabricating STTP ids. Job attempts and durable outbox events persist structured provenance while retaining STTP compatibility columns.
- **Placement is first-class on every job.** `NewJob` / `Job` include `placement: PlacementConstraints` (default unrestricted).
- **Workers can declare placement capabilities.** `JobStore::lease_due` takes `&WorkerCapabilities`; `RuntimeSdk::process_once_with_capabilities` and the backend equivalents expose the same contract. Existing `process_once` remains the unrestricted compatibility path.
- **Federated ingress authenticates every message type.** Remote jobs, signals, and terminal results have canonical signing/verification helpers. The reference in-memory bus requires registered verification keys, rejects expired/tampered work, and deduplicates deliveries.
- **Placement and handoff claims are enforced atomically.** Surreal lease CAS includes the selected placement representation; in-memory ownership handoff reservation performs conflict-check and insert under one state lock.

### Migration

```rust
use stasis::domain::runtime::placement::{PlacementConstraints, WorkerCapabilities};
use stasis::domain::runtime::provenance::ProvenanceRef;

// Before
NewJob {
    // ...
    sttp_input_node_id: "sttp:in:1".into(),
    // ...
}

// After
NewJob {
    // ...
    input_provenance: Some(ProvenanceRef::sttp("sttp:in:1")),
    placement: PlacementConstraints::default(),
    // ...
}

// Leasing
job_store
    .lease_due("default", "worker-1", now, 30, &WorkerCapabilities::any())
    .await?;

// Runtime facade worker placement
runtime
    .process_once_with_capabilities("default", "worker-1", &worker_capabilities)
    .await?;

// Successful generic handler outcome
JobExecutionOutcome::Success {
    output_provenance: Some(ProvenanceRef::cas(output_digest)),
    execution_id: Some(execution_id),
    diagnostics: None,
}
```

### Notes

- Surreal job/outbox rows keep STTP string columns for compatibility and populate them from the STTP adapter when scheme is `sttp`.
- `InMemoryFederatedBus` requires `register_verification_key(key_id, key)` before accepting signed federation messages.
- See `docs/adr/ADR-0010-federated-job-contract.md` and `docs/design/federated-job-contract.md`.

## [0.9.4] - 2026-08-25

### Changed

- **Locus 0.5.1 / locus-sdk 0.3.1** — `locus-core-rs` bumped to **0.5.1** and `locus-sdk` to **0.3.1**. Pulls `SttpDocumentBuilder` for strict canonical STTP composition (shallow content-slice merge, metadata-driven provenance/envelope, finalized metrics, `render_canonical()`), with round-trip coverage through `TreeSitterValidator` + `StrictTypedIr`.

### Notes

- Native defaults unchanged (`genai-provider` + `http-providers`).
- Stasis prompt/session memory helpers still emit STTP via existing templates; adopting `SttpDocumentBuilder` for `render_prompt_response_sttp_node` / `render_session_summary_sttp_node` is a follow-up (wire-format shift away from `⏣0` tagged templates).

## [0.9.3] - 2026-08-23

### Changed

- **Locus 0.5.0 / locus-sdk 0.3.0** — `locus-core-rs` bumped to **0.5.0** and `locus-sdk` to **0.3.0**. Pulls WASM compilation support (in-memory stores, parsing, and services on `wasm32-unknown-unknown`), the `surreal-runtime` feature gate for native Surreal filesystem hosts, and SDK feature splits (`http-providers`, `testing`) so browser builds can opt out of `reqwest`/`genai`.

### Notes

- Native Stasis defaults keep `locus-sdk` default features (`genai-provider` + `http-providers`).
- Locus's separate `locus-wasm` / `locus-surreal-adapter` crates enable IndexedDB (`kv-indxdb`) and remote WS Surreal clients in browser hosts; Stasis still runs the native in-memory Locus bootstrap via `.with_locus_memory()`.

## [0.9.2] - 2026-08-22

### Changed

- **Grapheme 0.7.1** — `grapheme-sdk`, `grapheme-compiler`, and `grapheme-lsp` bumped to **0.7.1**. Default Stasis build now opts into Grapheme's lean host profile (`default-features = false`, `features = ["host"]`) and drops the unused `grapheme-lsp` dependency from the default graph (Stage B / AOT container no longer pulled transitively).

### Notes

- Enable `dashboard-lsp` when you need the Grapheme language server wired into the dashboard build.
- `grapheme-full` still opts into extended stdlib modules (`data`, `pdf`, `image`, `plot`, `media`) and Stage B.

## [0.9.1] - 2026-08-22

### Changed

- **Grapheme 0.7.0** — `grapheme-sdk`, `grapheme-compiler`, and `grapheme-lsp` bumped to **0.7.0**. Brings executable parameters / tagged `using` scopes (RFC-0004) and Stage B slim Wasm AOT / host-fulfillment paths (RFC-0005). Default Stasis build stays lean (`default-features = false`); use `grapheme-full` for extended stdlib modules (`data`, `pdf`, `image`, `plot`, `media`).

### Notes

- Grapheme 0.7 requires **Rust 1.92+** (edition 2024).
- Stage B Wasix sandbox remains opt-in upstream (`wasix-runtime` / `prefer_stage_b_wasix`); Stasis continues to execute via the in-process SDK path by default.

## [0.9.0] - 2026-08-15

### Added

- **Typed jobs, durable waits, and fenced resource leases.** `StasisJob` / `JobConsumer` enqueue via `runtime.enqueue_job(payload).queue(...).retry(...).send().await`. Handlers receive `JobContext` (`heartbeat`, `progress`, `publish`, `enqueue`, `wait_for`). `wait_for` is re-entrant (`Deferred`); `runtime.signal` resumes waiters. `runtime.cancel` marks non-terminal jobs `Canceled` and trips in-flight watches. Resource leases (`acquire_lease` / `renew_lease` / `release_lease` / `transfer_lease` / `force_acquire_lease` / `validate_fence`) carry a generation fencing token. Raw `JobHandler::execute` and `enqueue(NewJob)` remain.

Migration:

```rust
#[derive(Serialize, Deserialize)]
struct PrepareReplica { replica_id: String }
impl StasisJob for PrepareReplica {
    const NAME: &'static str = "prepare_replica";
    const VERSION: u32 = 1;
    type Output = ();
}

runtime.register_consumer(MyConsumer)?;
runtime.enqueue_job(PrepareReplica { replica_id: "r1".into() })
    .queue("replicas")
    .retry(RetryPolicy::exponential(8))
    .send()
    .await?;
```

- **Job lifecycle recovery.** Expired `Leased`/`Running` jobs are recovered as retryable failures (attempt consumed, backoff, dead-letter at `max_attempts`). `JobContext::heartbeat` extends `lease_expires_at`. `JobHandler` / `JobConsumer::on_lifecycle` fires after persist for success, defer, retry, dead-letter, and cancel. `RuntimeSdk` adds `recover_stale`, `fail`, `delete` (terminal only), and `replay_dead_letter`. `cancel` completes pending durable waits and emits `JobCanceled`. `stasisd` sweeps stale leases before processing.

Migration:

```rust
async fn consume(&self, job: PrepareReplica, ctx: JobContext) -> JobResult<()> {
    mark_pending(&job)?;
    ctx.wait_for::<ReplicaReady>().correlated_by(&job.replica_id).await?;
    mark_finishing(&job)?;
    Ok(())
}

async fn on_lifecycle(&self, job: &Job, event: &JobLifecycleEvent) -> Result<()> {
    match event {
        JobLifecycleEvent::Succeeded => mark_done(job),
        JobLifecycleEvent::Deferred { .. } => revert_pending(job),
        JobLifecycleEvent::Canceled { .. } | JobLifecycleEvent::DeadLettered { .. } => fail_card(job),
        JobLifecycleEvent::RetryScheduled { .. } => revert_pending(job),
    }
}
```

### Changed

- **Breaking: bounded provider streaming.** `AiChatClient::complete_stream`, `PromptExecutionPipeline::complete_chat_stream`, and `ToolLoopPipeline::execute_with_stream*` now take `Option<&tokio::sync::mpsc::Sender<StreamDelta>>`. The caller owns channel capacity; every delta awaits capacity. A closed receiver returns `StasisError::StreamClosed` and is not treated as a successful completion. Chat middlewares forward `complete_stream` so backpressure reaches the provider.

Migration:

```rust
let (tx, mut rx) = tokio::sync::mpsc::channel::<StreamDelta>(32);
client.complete_stream(request, options, Some(&tx)).await?;
```

### Notes

- **`stasisd` is not published to crates.io** in this release (`publish = false`); ship/run from the workspace binary.
- Resource leases recover via TTL; this release does not scan-and-release leases on cancel/fail, and `JobContext::acquire_lease` is not added yet.
- `JobState::Failed` remains unused as a persisted intermediate; retries stay `Enqueued` with a future `scheduled_at`.
- No generic inbox — `runtime.signal` and `AgentEventIngress` stay the inbound paths.

## [0.8.0] - 2026-07-22

### Added

- **Agent platform runtime contracts (ADR-0007)** — vendor-neutral ports for comms, translation, and MCP tool bridge under `ports::outbound::agent`.
- **Canonical agent envelopes** — `AgentEnvelope` kinds plus `JsonAgentMessageCodec` encode/decode (JSON v1).
- **Agent event ingress** — `AgentEventIngress` with in-memory and wait-correlating adapters; idempotent accept for grant → complete/fail/cancel.
- **Waitable external turns** — `workflow.stasis.agent_turn.waitable` parks via `JobExecutionOutcome::Deferred` until ingress completes, fails, cancels, or times out (`TurnWaitStore`).
- **MCP tool bridge** — `McpToolProvider` / `McpToolExporter`; `McpBridgedToolRegistry` merges provider tools beside local `StasisTool`s; `AllowlistedLocalMcpExporter` + recursion budget.
- **Builder DI** — `StasisRuntimeBuilder` methods: `with_agent_message_codec`, `with_agent_event_ingress`, `with_agent_transport`, `with_turn_wait_store`, `with_mcp_tool_provider`, `with_mcp_tool_exporter`, `with_mcp_export_allowlist`, and `build_with_handles()` → `McpBridgeHandles`.
- **Declarative external participants** — `AgentParticipantKindPayload::{LocalToolLoop, External}` on session participants; `endpoint_ref` / `mcp_gateway_ref` / timeout fields for external turns.
- **`stasisd` workspace crate** (ADR-0008, `publish = false`) — YAML/TOML desired-state engine: validate, reconcile managed `stasisd:<id>` schedules, filesystem watch + tick host (materialize / process / publish), `--once` / `--strict`, optional `/healthz`, systemd unit + runbook.
- **Docs** — ADR-0007/0008, epic + delivery plans, docs-book pages for platform contracts / `stasisd` / external-participant cookbook.

### Changed

- **`AgentSessionParticipantPayload`** — additive fields with serde defaults (`kind` defaults to `local_tool_loop`; `tool_name` defaults empty). Rust struct literals must set the new fields.
- **Runtime composition** — tool registry is MCP-bridged when providers are configured; waitable agent-turn handler registered with default handlers.
- **Production examples / cookbook** — `MemoryPolicyPayload` initializers include `tenant_id`, `gamma`, and `filter`.

### Notes

- **`stasisd` is not published to crates.io** in this release (`publish = false`); ship/run from the workspace binary.
- Core Stasis still ships **no vendor agent adapters** — platform builders inject codecs/gateways at the composition root.
- Turn wait store remains **process-local** in this release (including when using Surreal job backends).

## [0.7.1] - 2026-06-23

### Changed

- **Locus 0.4.2 / locus-sdk 0.2.2** — fixes semantic tag/link `null` handling in the STTP parser and SurrealDB storage (`semantic_tags` / `semantic_links` treated as absent; optional fields written as `NONE` not `NULL`).

## [0.7.0] - 2026-06-23

### Added

- **Locus 0.4.1 / locus-sdk 0.2.1** — semantic tags, semantic index, eviction policy, and memory graph primitives (canonical sync-key tag index sync on ingest).
- **`LocusMemoryStore`** — shared in-memory bundle (`NodeStore` + `SemanticIndexStore`) wired through reader, writer, and operations adapters.
- **Semantic memory** — `MemoryNode.semantic_tags` / `semantic_links`; extended `MemoryFilter` (tag/link predicates, including `indexed_tags`); recall `gamma` and `filter`; `MemoryPolicyPayload.filter` for agent-time tag-aware recall.
- **`workflow.stasis.memory.evict`** — governed deletion with modes `by_sync_keys`, `by_node_ids`, `by_filter`, `purge_session`; `dry_run` (default `true`), `force`, reference safety.
- **`workflow.stasis.memory.graph`** — session topology, lineage, and semantic link edges at read time.
- **Transform ops** — `embed_tag_backfill`, `reindex_tag_embeddings` on the semantic tag index.

### Changed

- **Memory ports** — `MemoryContextReader::graph()`; `MemoryOperations::evict()`; `MemorySchemaResponse.evict_operations`.
- **Bootstrap** — `.with_locus_memory()` initializes semantic index and wires `with_semantic_index()` on Locus ingest, find/recall, transform, and evict paths.
- **Schema version** — Locus memory schema **`locus-sdk.memory.v3`**.

## [0.6.1] - 2026-06-02

### Added

- **`grapheme-full` Cargo feature** — opt-in `grapheme-sdk/full` + `grapheme-compiler/full` for extended stdlib modules (`data`, `pdf`, `image`, `plot`, `media`).
- **Grapheme 0.6.1** — language/compiler upgrades (fragments, state machines, flow/match sugar, typed signatures, AOT paths, lint warnings).
- **`lint_warnings`** and **`description`** on workflow execution/reflection diagnostics.

### Changed

- **Grapheme deps** — `grapheme-sdk`, `grapheme-compiler`, and `grapheme-lsp` bumped to **0.6.1** (default build stays lean; use `grapheme-full` for extended modules).
- **Import guardrails** — default allowlist is `grapheme/*` with prefix wildcard matching (docs corrected).

## [0.6.0] - 2026-06-02

### Added

- **genai 0.6.5 baseline** — Bedrock, Vertex, OpenRouter, native Ollama adapter, GPT-5 / Responses improvements, prompt cache hooks, streaming capture updates.
- **`reasoning_effort` on runtime job payloads** — optional string keywords on prompt, tool-loop, agent, and orchestration payloads; branch/stage/turn/route override → pattern default (same semantics as 0.5.0 concurrent overrides).
- **`chat_options_resolver`** — keyword validation, `PromptExecutionContext` → `ChatOptions`, model suffix fallback in `GenaiChatClient`.
- **Provider docs** — [llm-providers.md](docs-book/src/llm-providers.md); orchestration patterns updated for `reasoning_effort`.
- **Roadmap:** [genai-0.6.0-runtime-upgrade-roadmap.md](docs/design/genai-0.6.0-runtime-upgrade-roadmap.md)

### Changed

- **`PromptExecutionPipeline`** — passes resolved `ChatOptions` (reasoning effort) to `AiChatClient`.
- **Groq models** — require `groq::` namespace prefix (genai 0.6.x).

### Deferred

- Built-in provider WebSearch tools (Slice 6)
- `STASIS_LLM_REASONING_EFFORT` env alias
- Full `model_hint` model routing (Track B → ~0.7.0)

## [0.5.0] - 2026-06-02

### Added

- **Concurrent tool_loop branches** — `ConcurrentBranchExecutionMode` (`prompt` / `tool_loop`) on concurrent orchestration branches; branches can run full `ToolLoopPipeline` in parallel via the existing `JoinSet`.
- **Payload helpers** — `ConcurrentBranchJobPayload::prompt(...)` and `::tool_loop(...)`; pattern-level `tool_call_mode` and `memory_policy` defaults.
- **Concurrent tool branch memory** — identity snapshot + memory recall prepend and optional store per `tool_loop` branch (`concurrent_tool_branch_memory.rs`).
- **Roadmap:** [concurrent-capabilities-0.5.0-roadmap.md](docs/design/concurrent-capabilities-0.5.0-roadmap.md)

### Changed

- **`ConcurrentPatternJobHandler`** — wires `ToolRegistry` and memory/identity deps; reports `prompt_branch_count`, `tool_loop_branch_count`, and per-branch summaries (including memory fields) in diagnostics.

### Documentation

- **Orchestration patterns** — concurrent branch execution modes, memory policy semantics, updated cookbook example.

## [0.4.0]

### Added

- **Identity model 0.4.0 foundation** — `UserEntity.preferences`, `ContactEntity`, typed `RelationshipKind` enum (`knows`, `prefers`, `delegation`, `colleague` + structural kinds), and `GetIdentityContextRequest.mode` (`Full` / `Policy` / `Cognitive`) with shared mode filtering in both identity store adapters.
- **Roadmap:** [identity-model-0.4.0-roadmap.md](docs/design/identity-model-0.4.0-roadmap.md)

### Changed

- **Runtime identity compiler** — prompt path now requests `IdentityContextMode::Cognitive` and reports contact/preference counts in diagnostics snapshots.

### Documentation

- **Identity memory layer** — documents 0.4.0 model (`ContactEntity`, `UserEntity.preferences`, `RelationshipKind`, `IdentityContextMode`), Surreal schema additions, and updated cookbook recipes.

## [0.3.0] - 2026-06-04

### Added

- **OpenTelemetry first-class observability** behind optional Cargo feature `otel` (ADR-0006, [RFC plan](docs/design/opentelemetry-integration-rfc-plan.md)).
- **`RuntimeTracing` / `RuntimeTelemetry` ports** with `NoopRuntimeTracing`, `NoopRuntimeTelemetry`, and `OpenTelemetryTelemetry::from_env()`.
- **`StasisRuntimeBuilder::with_runtime_telemetry()`** and **`with_otel_from_env()`** — wires metrics + tracing into the job loop and chat middleware.
- **`stasis::telemetry_prelude`** — frozen metric keys, span names, propagation helpers, and telemetry types.
- **Span instrumentation** for worker loop, job execution, chat completion, memory recall, outbox publish, and grapheme execution.
- **W3C trace propagation** via `RuntimeWorkflowJobBuilder::with_traceparent()` / `with_trace_context()` and job-loop parent rehydration (`STASIS_OTEL_TRACE_PROPAGATION`).
- **Dashboard HTTP trace propagation** — incoming `traceparent` headers propagate to scheduler materialization and runtime spans during dashboard actions.
- **`dashboard::bootstrap`** — shared `build_dashboard_query_service()` for the standalone binary and embedded apps (`StasisRuntimeBuilder`, optional Locus memory, OTEL, demo seed).
- **`tests/otel_runtime_parity.rs`** and **`tests/dashboard_bootstrap.rs`** — OTEL parity and production-like dashboard bootstrap coverage.

### Changed

- **Dashboard bootstrap** — `stasis_dashboard` builds the runtime via `StasisRuntimeBuilder` with full default handlers; in-memory control-plane stores are shared with the runtime.
- **Dashboard workflow execute** — saved workflow execute enqueues a `workflow.grapheme.run` job from the latest persisted revision and runs it via `process_once` (empty queue falls back to the saved workflow queue).
- **Dashboard UI honesty pass** — relabeled synthetic cluster metrics, wired endpoint trends to delivery history where available, clarified queue lanes vs persisted workflows, draft canvas node status, lineage preview disclosures, honest diagnostics provider naming, and demo-seed badge in the shell.
- **`RuntimeWorkflowJobBuilder`** now generates a W3C-style trace id by default when none is supplied (replacing the previous job-id fallback).
- **Dashboard service** — consolidated runtime and control-plane dispatch helpers to remove duplicated in-memory/Surreal match arms.

## [0.2.4]

### Added

- **`stasis::config_prelude`** — safe environment helpers: `bootstrap()`, `non_empty()`, `required()`, `with_default()`, `first_non_empty()`, `truthy()`.
- **Optional `.env` loading** via `dotenvy` (never overrides existing process env). Alternate path via `STASIS_ENV_FILE`.
- **`STASIS_SECRETS_DIR` file-backed secrets** for Vault Agent / External Secrets file mounts, plus `SecretsSource` trait for custom vault clients.
- **`.env.example`** template and [Environment Configuration](docs-book/src/environment-configuration.md) reference doc.
- Dashboard binary now calls `bootstrap()` on startup.

## [0.2.3]

### Fixed

- **`MemoryRecallResponse` and `MemoryFindResponse` now include full `nodes`** (`MemoryNode` with `raw` STTP content and metadata), matching Locus `MemoryRecallResponseDto` / `MemoryFindResponseDto` instead of returning sync keys only.
- **Memory-enabled runtime handlers** (prompt, tool-loop, agent-turn, agent-session) now **inject recalled node context into the user prompt** before LLM execution.
- Memory recall/find workflow job diagnostics now include serialized `nodes` alongside `node_sync_keys`.

## [0.2.2]

### Changed

- SurrealDB authentication now uses **root-level sign-in** (`username` + `password`) before `use_ns` / `use_db`, matching typical secured remote deployments. Replaces the 0.2.1 database-scoped sign-in behavior.

## [0.2.1]

### Added

- **`SurrealAuth`** and optional `auth` on all Surreal `RuntimeBackend` variants (`SurrealMem`, `SurrealWs`, `SurrealKv`).
- **`RuntimeBackend::surreal_mem` / `surreal_ws` / `surreal_kv`** helper constructors and **`.with_surreal_auth(...)`** chaining.
- **`RuntimeSdk::surreal_*_with_auth(...)`** helpers for authenticated remote/KV runtimes.
- Environment variables for database sign-in: `STASIS_DASHBOARD_SURREAL_USERNAME`, `STASIS_DASHBOARD_SURREAL_PASSWORD` (and example equivalents).


### Fixed

- Remote SurrealDB connections no longer skip authentication — Stasis signs in with database credentials before selecting namespace/database, avoiding privilege errors on secured deployments.


## [0.2.0]

### Added

- **`workflow.stasis.memory.find`** — durable job for predicate-based memory inventory (filter, sort, paginate) without AVEC resonance scoring.
- **`MemoryContextReader::find`** — port method backed by Locus `MemoryFindService` in the default adapter.
- Port types: `MemoryFindRequest`, `MemoryFindResponse`, `MemoryFilter`, `MemoryMetricRange`, `MemorySortField`, `MemorySortDirection`.
- **`RuntimeWorkflowJobBuilder::for_memory_find(...)`** — enqueue helper for the find workflow.

### Changed

- **`locus-core-rs`** pinned `0.2.1` → **`0.3.0`**
- **`locus-sdk`** pinned `0.1.1` → **`0.1.2`**
- **`LocusContextWriter`** — updated for `StoreContextService::new(store, validator, SttpNodeParser::new())` required by `locus-core-rs` 0.3.0.

### Breaking

- Custom **`MemoryContextReader`** implementations must implement **`find()`** in addition to **`recall()`**.

### Notes

- **Bring-your-own memory unchanged.** Wire custom backends with `.with_memory_context_reader(...)`, `.with_memory_context_writer(...)`, and `.with_memory_operations(...)`. Explicit ports still override `.with_locus_memory()` defaults.
- Embedding migration and sync coordination remain available in Locus core but are not exposed as Stasis workflow handlers in this release.
