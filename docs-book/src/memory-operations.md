# Memory Operations Reference

## Document Metadata

- Document Type: Reference Standard
- Audience: Engineer, Architect, SRE
- Stability: Evolving
- Last Verified: 2026-06-02
- Verified Against:
  - src/ports/outbound/memory/memory_operations.rs
  - src/ports/outbound/memory/memory_context_reader.rs
  - src/ports/outbound/memory/memory_context_writer.rs
  - src/ports/outbound/memory/memory_models.rs
  - src/application/runtime/memory_recall_job_handler.rs
  - src/application/runtime/memory_find_job_handler.rs
  - src/application/runtime/memory_aggregate_job_handler.rs
  - src/application/runtime/memory_transform_job_handler.rs
  - src/application/runtime/memory_rollup_job_handler.rs
  - src/application/runtime/memory_schema_job_handler.rs
  - tests/locus_memory_adapters.rs

## Purpose

Document the six Stasis memory operation workflows, their request/response contracts, default values, diagnostics keys, and the three memory port interfaces (`MemoryContextReader`, `MemoryContextWriter`, `MemoryOperations`).

## Invariants

1. All six memory workflow handlers are durable jobs — they inherit retry, dead-letter, and lineage semantics from the runtime.
2. Memory handlers are only registered when a `MemoryContextReader` or `MemoryOperations` port is provided to the builder. Missing ports cause the handler to be silently skipped at build time.
3. Invalid payloads produce a `FatalFailure` with `guardrail_code: POLICY_VIOLATION` — they are not retried.
4. `MemoryTransformRequest` defaults to `dry_run: true`. Callers must explicitly set `dry_run: false` to apply changes.
5. `MemoryScope` fields are optional. An empty scope means no session, tier, or time filtering — operations apply globally.

---

## Memory Port Interfaces

### MemoryContextReader

Used by handlers that need to retrieve prior context before execution (prompt, tool loop, agent turn, agent session).

```rust
#[async_trait]
pub trait MemoryContextReader: Send + Sync {
    async fn recall(&self, request: &MemoryRecallRequest) -> Result<MemoryRecallResponse>;

    async fn find(&self, request: &MemoryFindRequest) -> Result<MemoryFindResponse>;
}
```

Custom implementations must provide both methods. The default Locus adapter delegates `recall` to resonance/semantic retrieval and `find` to deterministic predicate-based inventory (no AVEC scoring).

### MemoryContextWriter

Used by handlers that need to persist execution output for future recall.

```rust
#[async_trait]
pub trait MemoryContextWriter: Send + Sync {
    async fn store_context(&self, request: &MemoryStoreRequest) -> Result<MemoryStoreResponse>;
}
```

### MemoryOperations

Used by the memory maintenance workflow handlers for aggregate, transform, rollup, and schema operations.

```rust
#[async_trait]
pub trait MemoryOperations: Send + Sync {
    async fn aggregate(&self, request: &MemoryAggregateRequest) -> Result<MemoryAggregateResponse>;
    async fn transform(&self, request: &MemoryTransformRequest) -> Result<MemoryTransformResponse>;
    async fn rollup(&self, request: &MemoryRollupRequest) -> Result<MemoryRollupResponse>;
    async fn schema(&self) -> Result<MemorySchemaResponse>;
}
```

---

## Shared Types

### MemoryScope

Filters applied to recall, find, aggregate, transform, and rollup operations.

| Field | Type | Description |
|---|---|---|
| `session_ids` | `Option<Vec<String>>` | Restrict to specific session IDs. `None` = all sessions |
| `tiers` | `Option<Vec<String>>` | Restrict to specific memory tiers. `None` = all tiers |
| `from_utc` | `Option<DateTime<Utc>>` | Lower bound on node timestamp |
| `to_utc` | `Option<DateTime<Utc>>` | Upper bound on node timestamp |

### MemoryAvecState

AVEC scores used for resonance-based recall ranking.

| Field | Type | Description |
|---|---|---|
| `stability` | `f32` | Stability dimension (0.0–1.0) |
| `friction` | `f32` | Friction dimension (0.0–1.0) |
| `logic` | `f32` | Logic dimension (0.0–1.0) |
| `autonomy` | `f32` | Autonomy dimension (0.0–1.0) |

### MemoryFilter

Predicate filters for find operations (and available on the Locus SDK recall path via adapter defaults).

| Field | Type | Description |
|---|---|---|
| `has_embedding` | `Option<bool>` | Restrict to nodes with or without embeddings |
| `embedding_model` | `Option<String>` | Restrict to a specific embedding model |
| `psi` | `Option<MemoryMetricRange>` | Filter by psi range |
| `rho` | `Option<MemoryMetricRange>` | Filter by rho range |
| `kappa` | `Option<MemoryMetricRange>` | Filter by kappa range |
| `text_contains` | `Option<String>` | Substring match on node text |

### MemoryMetricRange

| Field | Type | Description |
|---|---|---|
| `min` | `Option<f32>` | Inclusive lower bound |
| `max` | `Option<f32>` | Inclusive upper bound |

### MemorySortField / MemorySortDirection

Used by find operations for stable result ordering.

| Sort field | Values |
|---|---|
| `MemorySortField` | `Timestamp` (default), `UpdatedAt`, `Psi`, `Rho`, `Kappa` |
| `MemorySortDirection` | `Asc`, `Desc` (default) |

---

## Operation 1: Recall

**Job type:** `workflow.stasis.memory.recall`  
**Port:** `MemoryContextReader`

Retrieves memory nodes matching the provided scope and query parameters. Used inline by prompt/tool-loop/agent handlers and available as a standalone job.

### Request: `MemoryRecallRequest`

| Field | Type | Default | Description |
|---|---|---|---|
| `scope` | `MemoryScope` | empty | Scope filter |
| `current_avec` | `Option<MemoryAvecState>` | `None` | AVEC state for resonance ranking |
| `query_text` | `Option<String>` | `None` | Semantic query string |
| `limit` | `usize` | `20` | Maximum nodes to retrieve |
| `alpha` | `f32` | `0.7` | AVEC resonance weight |
| `beta` | `f32` | `0.3` | Semantic similarity weight |
| `fallback_policy` | `MemoryFallbackPolicy` | `OnEmpty` | Fallback behavior when results are empty |
| `strictness` | `MemoryStrictnessMode` | `Balanced` | Retrieval strictness |
| `include_explain` | `bool` | `false` | Include retrieval path explanation |

#### MemoryFallbackPolicy

| Variant | Behavior |
|---|---|
| `Never` | Return empty result if no matches found |
| `OnEmpty` (default) | Broaden query if initial result is empty |
| `Always` | Always attempt fallback broadening |

#### MemoryStrictnessMode

| Variant | Behavior |
|---|---|
| `Precision` | High precision, lower recall — only high-confidence matches |
| `Balanced` (default) | Balanced precision/recall |
| `Recall` | High recall, lower precision — broader matches |

### Response: `MemoryRecallResponse`

| Field | Type | Description |
|---|---|---|
| `retrieved` | `usize` | Number of nodes returned |
| `next_cursor` | `Option<String>` | Cursor for pagination |
| `has_more` | `bool` | Whether more results are available |
| `retrieval_path` | `Option<String>` | Explanation of how results were retrieved |
| `fallback_triggered` | `bool` | Whether fallback broadening was activated |
| `fallback_reason` | `Option<String>` | Reason fallback was triggered |
| `node_sync_keys` | `Vec<String>` | Sync keys of returned nodes |

---

## Operation 2: Find

**Job type:** `workflow.stasis.memory.find`  
**Port:** `MemoryContextReader`

Deterministic memory inventory: filters, sorts, and paginates nodes without AVEC resonance scoring. Use when you need stable predicate-based listing rather than semantic/recall ranking.

### Request: `MemoryFindRequest`

| Field | Type | Default | Description |
|---|---|---|---|
| `scope` | `MemoryScope` | empty | Scope filter |
| `filter` | `MemoryFilter` | empty | Predicate filters |
| `limit` | `usize` | `50` | Maximum nodes to retrieve |
| `cursor` | `Option<String>` | `None` | Pagination cursor from a prior response |
| `sort_field` | `MemorySortField` | `Timestamp` | Sort key |
| `sort_direction` | `MemorySortDirection` | `Desc` | Sort order |

### Job payload: `MemoryFindJobPayload`

JSON fields accepted by the find workflow handler (camelCase):

| Field | Type | Default | Description |
|---|---|---|---|
| `sessionIds` | `Option<Vec<String>>` | `None` | Session scope |
| `tiers` | `Option<Vec<String>>` | `None` | Tier scope |
| `fromUtc` / `toUtc` | `Option<DateTime<Utc>>` | `None` | Time bounds |
| `limit` | `Option<usize>` | `50` | Page size |
| `cursor` | `Option<String>` | `None` | Pagination cursor |
| `textContains` | `Option<String>` | `None` | Text substring filter |
| `sortField` | `Option<String>` | `timestamp` | `timestamp`, `updated_at`, `psi`, `rho`, or `kappa` |
| `sortDirection` | `Option<String>` | `desc` | `asc` or `desc` |

### Response: `MemoryFindResponse`

| Field | Type | Description |
|---|---|---|
| `retrieved` | `usize` | Number of nodes returned |
| `has_more` | `bool` | Whether more results are available |
| `next_cursor` | `Option<String>` | Cursor for the next page |
| `node_sync_keys` | `Vec<String>` | Sync keys of returned nodes |

### Diagnostics keys

| Key | Value |
|---|---|
| `provider` | `stasis-memory-find` |
| `status` | `success` or `failure` |
| `retrieved` | Result count |
| `has_more` | Pagination flag |
| `next_cursor` | Next page cursor when present |
| `node_sync_keys` | Returned node sync keys |

---

## Operation 3: Store Context

**Job type:** Inline only (no standalone job handler)  
**Port:** `MemoryContextWriter`

Persists a raw STTP node string to memory for future recall. Called automatically by memory-enabled handlers after successful execution.

### Request: `MemoryStoreRequest`

| Field | Type | Description |
|---|---|---|
| `session_id` | `String` | Session to associate the node with |
| `raw_node` | `String` | Raw STTP node JSON string |

### Response: `MemoryStoreResponse`

| Field | Type | Description |
|---|---|---|
| `node_id` | `String` | Assigned node ID |
| `psi` | `f32` | Node quality score (0.0–1.0) |
| `valid` | `bool` | Whether the node passed schema validation |
| `validation_error` | `Option<String>` | Validation failure reason if `valid` is false |

---

## Operation 4: Aggregate

**Job type:** `workflow.stasis.memory.aggregate`  
**Port:** `MemoryOperations`

Groups memory nodes within the scope and produces aggregate statistics. Used for memory health analysis and maintenance.

### Request: `MemoryAggregateRequest`

| Field | Type | Default | Description |
|---|---|---|---|
| `scope` | `MemoryScope` | empty | Scope filter |
| `max_groups` | `usize` | `30` | Maximum groups to produce |
| `max_nodes` | `usize` | `5000` | Maximum nodes to scan |

### Response: `MemoryAggregateResponse`

| Field | Type | Description |
|---|---|---|
| `total_groups` | `usize` | Number of groups produced |
| `scanned_nodes` | `usize` | Total nodes scanned |

### Diagnostics keys

| Key | Value |
|---|---|
| `provider` | `stasis-memory-aggregate` |
| `status` | `success` or `failure` |
| `total_groups` | Result group count |
| `scanned_nodes` | Nodes scanned |

---

## Operation 5: Transform

**Job type:** `workflow.stasis.memory.transform`  
**Port:** `MemoryOperations`

Applies a batch transformation operation to memory nodes (embedding backfill or reindex). Defaults to `dry_run: true` — the payload must explicitly set `dry_run: false` to apply changes.

### Request: `MemoryTransformRequest`

| Field | Type | Default | Description |
|---|---|---|---|
| `scope` | `MemoryScope` | empty | Scope filter |
| `operation` | `MemoryTransformOperation` | `EmbedBackfill` | Operation to apply |
| `dry_run` | `bool` | `true` | Preview only — no writes when `true` |
| `batch_size` | `usize` | `100` | Nodes processed per batch |
| `max_nodes` | `usize` | `5000` | Maximum nodes to process |
| `provider_id` | `Option<String>` | `None` | Embedding provider override |
| `model` | `Option<String>` | `None` | Embedding model override |

#### MemoryTransformOperation

| Variant | Description |
|---|---|
| `EmbedBackfill` (default) | Generate embeddings for nodes that are missing them |
| `ReindexEmbeddings` | Regenerate embeddings for all nodes in scope |

### Response: `MemoryTransformResponse`

| Field | Type | Description |
|---|---|---|
| `scanned` | `usize` | Total nodes examined |
| `selected` | `usize` | Nodes selected for transformation |
| `updated` | `usize` | Nodes successfully transformed |
| `skipped` | `usize` | Nodes skipped (already up-to-date) |
| `failed` | `usize` | Nodes that failed transformation |
| `duplicate` | `usize` | Nodes skipped as duplicates |
| `failures` | `Vec<String>` | Error messages for failed nodes |

### Diagnostics keys

| Key | Value |
|---|---|
| `provider` | `stasis-memory-transform` |
| `status` | `success` or `failure` |
| `scanned` | Total nodes scanned |
| `selected` | Nodes selected |
| `updated` | Nodes updated |
| `failed` | Node failure count |

---

## Operation 6: Rollup

**Job type:** `workflow.stasis.memory.rollup`  
**Port:** `MemoryOperations`

Creates monthly checkpoint rollups from stored nodes to reduce retrieval noise over long timelines.

### Request: `MemoryRollupRequest`

| Field | Type | Default | Description |
|---|---|---|---|
| `scope` | `MemoryScope` | empty | Scope filter |
| `max_days` | `usize` | `30` | Maximum days of history to roll up |
| `max_nodes` | `usize` | `5000` | Maximum nodes to process |

### Response: `MemoryRollupResponse`

| Field | Type | Description |
|---|---|---|
| `total_groups` | `usize` | Number of rollup groups produced |
| `scanned_nodes` | `usize` | Total nodes scanned |

---

## Operation 7: Schema

**Job type:** `workflow.stasis.memory.schema`  
**Port:** `MemoryOperations`

Returns the current memory schema version and capability descriptor. No payload fields are required beyond the job envelope.

### Response: `MemorySchemaResponse`

| Field | Type | Description |
|---|---|---|
| `schema_version` | `String` | Current STTP schema version |
| `sort_fields` | `Vec<String>` | Fields available for result sorting |
| `filter_fields` | `Vec<String>` | Fields available for scope filtering |
| `group_by_fields` | `Vec<String>` | Fields available for grouping |
| `fallback_policies` | `Vec<String>` | Supported fallback policy names |
| `strictness_modes` | `Vec<String>` | Supported strictness mode names |
| `transform_operations` | `Vec<String>` | Supported transform operation names |

### Diagnostics keys

| Key | Value |
|---|---|
| `provider` | `stasis-memory-schema` |
| `status` | `success` or `failure` |
| `schema_version` | Schema version string |
| `transform_operations` | List of supported operations |

---

## Non-Goals

- Memory operations do not perform agent execution. They are maintenance and retrieval workflows, not orchestration patterns.
- `MemoryContextReader` and `MemoryContextWriter` are separate interfaces from `MemoryOperations` by design — readers/writers can be wired without enabling bulk operation handlers.
- Embedding migration and sync coordination remain Locus-core capabilities; Stasis does not expose dedicated workflow handlers for them yet. Use custom `MemoryOperations` implementations or call Locus services directly in application code if needed.

## Locus dependency versions

Stasis pins Locus crates to prevent resolution drift:

- `locus-core-rs = 0.3.0`
- `locus-sdk = 0.1.2`

The default `.with_locus_memory()` bootstrap uses in-memory Locus adapters. Replace any port with your own implementation via `.with_memory_context_reader(...)`, `.with_memory_context_writer(...)`, or `.with_memory_operations(...)`.
