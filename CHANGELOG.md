# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).


## [Unreleased]

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
