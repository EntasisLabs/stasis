# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).


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
