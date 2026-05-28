# Memory Maintenance and Rollups

## Outcome

Run routine memory health operations for aggregate visibility, embedding maintenance, and monthly rollup compaction.

## Recipe

### 1. Create memory operations adapter

```rust
use std::sync::Arc;

use stasis::infrastructure::memory::locus_memory_operations::LocusMemoryOperations;
use stasis::infrastructure::memory::locus_node_store_factory::LocusNodeStoreFactory;
use stasis::ports::outbound::memory::memory_operations::MemoryOperations;

let store = LocusNodeStoreFactory::in_memory().await?;
let ops = Arc::new(LocusMemoryOperations::new(store, None));
```

### 2. Run aggregate health snapshot

```rust
use stasis::ports::outbound::memory::memory_models::{MemoryAggregateRequest, MemoryScope};

let aggregate = ops
    .aggregate(&MemoryAggregateRequest {
        scope: MemoryScope::default(),
        max_groups: 30,
        max_nodes: 5000,
    })
    .await?;

println!("groups={} scanned={}", aggregate.total_groups, aggregate.scanned_nodes);
```

### 3. Preview embedding transform

```rust
use stasis::ports::outbound::memory::memory_models::{
    MemoryScope, MemoryTransformOperation, MemoryTransformRequest,
};

let preview = ops
    .transform(&MemoryTransformRequest {
        scope: MemoryScope::default(),
        operation: MemoryTransformOperation::EmbedBackfill,
        dry_run: true,
        batch_size: 100,
        max_nodes: 5000,
        provider_id: None,
        model: None,
    })
    .await?;

println!(
    "preview scanned={} selected={} updated={} failed={}",
    preview.scanned, preview.selected, preview.updated, preview.failed
);
```

### 4. Apply transform

```rust
let apply = ops
    .transform(&MemoryTransformRequest {
        scope: MemoryScope::default(),
        operation: MemoryTransformOperation::EmbedBackfill,
        dry_run: false,
        batch_size: 100,
        max_nodes: 5000,
        provider_id: None,
        model: None,
    })
    .await?;

println!(
    "apply scanned={} selected={} updated={} failed={}",
    apply.scanned, apply.selected, apply.updated, apply.failed
);
```

### 5. Run monthly rollup

```rust
use stasis::ports::outbound::memory::memory_models::{MemoryRollupRequest, MemoryScope};

let rollup = ops
    .rollup(&MemoryRollupRequest {
        scope: MemoryScope::default(),
        max_days: 30,
        max_nodes: 5000,
    })
    .await?;

println!("rollup groups={} scanned={}", rollup.total_groups, rollup.scanned_nodes);
```

### 6. Validate current schema descriptor

```rust
let schema = ops.schema().await?;

println!("schema_version={}", schema.schema_version);
println!("transform_ops={:?}", schema.transform_operations);
```

## Operational Cadence

Suggested baseline:

1. Aggregate daily.
2. Transform preview daily, apply during controlled windows.
3. Rollup weekly or monthly depending on retrieval noise profile.
4. Schema check in CI and before major migration windows.

## Guardrails

1. Keep dry_run=true in automation until preview quality is verified.
2. Bound max_nodes to avoid unplanned long-running batches.
3. Persist transform failure lists for follow-up remediation.
4. Use scoped operations for large multi-tenant deployments.
