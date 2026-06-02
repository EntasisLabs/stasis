# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

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
