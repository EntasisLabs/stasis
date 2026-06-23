use stasis::application::runtime::memory_persistence_helpers::{
    SttpPromptNodeFormat, render_prompt_response_sttp_node,
};
use stasis::infrastructure::memory::locus_context_reader::LocusContextReader;
use stasis::infrastructure::memory::locus_context_writer::LocusContextWriter;
use stasis::infrastructure::memory::locus_memory_operations::LocusMemoryOperations;
use stasis::infrastructure::memory::locus_node_store_factory::LocusNodeStoreFactory;
use stasis::ports::outbound::memory::memory_context_reader::MemoryContextReader;
use stasis::ports::outbound::memory::memory_context_writer::MemoryContextWriter;
use stasis::ports::outbound::memory::memory_models::{
    MemoryAggregateRequest, MemoryEvictMode, MemoryEvictRequest, MemoryFilter, MemoryFindRequest,
    MemoryGraphRequest, MemoryRecallRequest, MemoryRollupRequest, MemoryScope, MemoryStoreRequest,
    MemoryTransformRequest,
};
use stasis::ports::outbound::memory::memory_operations::MemoryOperations;

#[tokio::test]
async fn locus_node_store_factory_in_memory_initializes_store() {
    let memory = LocusNodeStoreFactory::in_memory()
        .await
        .expect("in-memory node store should initialize");

    let reader = LocusContextReader::new(memory);
    let response = reader
        .recall(&MemoryRecallRequest {
            scope: MemoryScope {
                session_ids: Some(vec!["session-factory-check".to_string()]),
                ..Default::default()
            },
            include_explain: false,
            ..Default::default()
        })
        .await
        .expect("recall should succeed with empty store");

    assert_eq!(response.retrieved, 0);
}

#[tokio::test]
async fn locus_context_reader_find_returns_empty_store_inventory() {
    let memory = LocusNodeStoreFactory::in_memory()
        .await
        .expect("in-memory node store should initialize");

    let reader = LocusContextReader::new(memory);
    let response = reader
        .find(&MemoryFindRequest {
            scope: MemoryScope {
                session_ids: Some(vec!["session-find-empty".to_string()]),
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .expect("find should succeed with empty store");

    assert_eq!(response.retrieved, 0);
    assert!(!response.has_more);
}

#[tokio::test]
async fn locus_context_reader_recall_returns_raw_nodes_after_store() {
    let session_id = "session-recall-raw";
    let memory = LocusNodeStoreFactory::in_memory()
        .await
        .expect("in-memory node store should initialize");
    let writer = LocusContextWriter::new(memory.clone());
    let reader = LocusContextReader::new(memory);

    let raw_node = render_prompt_response_sttp_node(
        session_id,
        "prior question",
        "prior answer about rust",
        SttpPromptNodeFormat::TaggedSchema,
    );
    writer
        .store_context(&MemoryStoreRequest {
            session_id: session_id.to_string(),
            raw_node,
        })
        .await
        .expect("valid STTP node should store");

    let response = reader
        .recall(&MemoryRecallRequest {
            scope: MemoryScope {
                session_ids: Some(vec![session_id.to_string()]),
                ..Default::default()
            },
            query_text: Some("rust".to_string()),
            ..Default::default()
        })
        .await
        .expect("recall should succeed");

    assert_eq!(response.retrieved, 1);
    assert_eq!(response.nodes.len(), 1);
    assert_eq!(response.node_sync_keys.len(), 1);
    assert!(
        response.nodes[0].raw.contains("prior answer about rust"),
        "recalled node should include raw STTP context"
    );
}

#[tokio::test]
async fn locus_context_reader_find_returns_raw_nodes_after_store() {
    let session_id = "session-find-raw";
    let memory = LocusNodeStoreFactory::in_memory()
        .await
        .expect("in-memory node store should initialize");
    let writer = LocusContextWriter::new(memory.clone());
    let reader = LocusContextReader::new(memory);

    let raw_node = render_prompt_response_sttp_node(
        session_id,
        "inventory question",
        "inventory answer",
        SttpPromptNodeFormat::TaggedSchema,
    );
    writer
        .store_context(&MemoryStoreRequest {
            session_id: session_id.to_string(),
            raw_node,
        })
        .await
        .expect("valid STTP node should store");

    let response = reader
        .find(&MemoryFindRequest {
            scope: MemoryScope {
                session_ids: Some(vec![session_id.to_string()]),
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .expect("find should succeed");

    assert_eq!(response.retrieved, 1);
    assert_eq!(response.nodes.len(), 1);
    assert!(
        response.nodes[0].raw.contains("inventory answer"),
        "found node should include raw STTP context"
    );
}

#[tokio::test]
async fn locus_context_writer_rejects_invalid_sttp_node() {
    let memory = LocusNodeStoreFactory::in_memory()
        .await
        .expect("in-memory node store should initialize");
    let writer = LocusContextWriter::new(memory);

    let result = writer
        .store_context(&MemoryStoreRequest {
            session_id: "session-invalid-node".to_string(),
            raw_node: "this is not valid sttp content".to_string(),
        })
        .await;

    assert!(result.is_err(), "invalid STTP should fail validation");
}

#[tokio::test]
async fn locus_memory_operations_schema_aggregate_rollup_work_on_empty_store() {
    let memory = LocusNodeStoreFactory::in_memory()
        .await
        .expect("in-memory node store should initialize");
    let operations = LocusMemoryOperations::new(memory, None);

    let schema = operations
        .schema()
        .await
        .expect("schema should be available");
    assert!(
        !schema.schema_version.trim().is_empty(),
        "schema version should be non-empty"
    );
    assert!(
        !schema.evict_operations.is_empty(),
        "schema should expose evict operations"
    );

    let aggregate = operations
        .aggregate(&MemoryAggregateRequest {
            scope: MemoryScope {
                session_ids: Some(vec!["session-aggregate-empty".to_string()]),
                ..Default::default()
            },
            max_groups: 10,
            max_nodes: 100,
        })
        .await
        .expect("aggregate should succeed on empty store");
    assert_eq!(aggregate.total_groups, 0);

    let rollup = operations
        .rollup(&MemoryRollupRequest {
            scope: MemoryScope {
                session_ids: Some(vec!["session-rollup-empty".to_string()]),
                ..Default::default()
            },
            max_days: 7,
            max_nodes: 100,
        })
        .await
        .expect("rollup should succeed on empty store");
    assert_eq!(rollup.total_groups, 0);
}

#[tokio::test]
async fn locus_memory_transform_requires_provider_registry() {
    let memory = LocusNodeStoreFactory::in_memory()
        .await
        .expect("in-memory node store should initialize");
    let operations = LocusMemoryOperations::new(memory, None);

    let result = operations
        .transform(&MemoryTransformRequest {
            scope: MemoryScope {
                session_ids: Some(vec!["session-transform".to_string()]),
                ..Default::default()
            },
            ..Default::default()
        })
        .await;

    assert!(result.is_err(), "transform without providers should fail");
    let message = result.expect_err("error should exist").to_string();
    assert!(
        message.contains("requires ai provider registry"),
        "unexpected error message: {message}"
    );
}

#[tokio::test]
async fn locus_semantic_tags_index_and_find_by_tag() {
    let session_id = "session-semantic-tags";
    let memory = LocusNodeStoreFactory::in_memory()
        .await
        .expect("in-memory node store should initialize");
    let writer = LocusContextWriter::new(memory.clone());
    let reader = LocusContextReader::new(memory);

    let raw_node = format!(
        r#"⊕⟨ {{ trigger: manual, response_format: temporal_node, origin_session: "{session_id}", compression_depth: 1, parent_node: null, prime: {{ attractor_config: {{ stability: 0.8, friction: 0.2, logic: 0.9, autonomy: 0.7 }}, context_summary: "semantic tags test", relevant_tier: raw, retrieval_budget: 3, semantic_tags: ["Grammar", "parser"] }} }} ⟩
⦿⟨ {{ timestamp: "2026-03-05T06:30:00Z", tier: raw, session_id: "{session_id}", user_avec: {{ stability: 0.8, friction: 0.2, logic: 0.9, autonomy: 0.7, psi: 2.6 }}, model_avec: {{ stability: 0.8, friction: 0.2, logic: 0.9, autonomy: 0.7, psi: 2.6 }} }} ⟩
◈⟨ {{ note(.99): "ok" }} ⟩
⍉⟨ {{ rho: 0.96, kappa: 0.94, psi: 2.6, compression_avec: {{ stability: 0.8, friction: 0.2, logic: 0.9, autonomy: 0.7, psi: 2.6 }} }} ⟩"#
    );

    writer
        .store_context(&MemoryStoreRequest {
            session_id: session_id.to_string(),
            raw_node,
        })
        .await
        .expect("semantic tagged node should store");

    let inventory = reader
        .find(&MemoryFindRequest {
            scope: MemoryScope {
                session_ids: Some(vec![session_id.to_string()]),
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .expect("inventory find should succeed");
    assert_eq!(inventory.retrieved, 1);
    assert_eq!(
        inventory.nodes[0].semantic_tags,
        Some(vec!["grammar".to_string(), "parser".to_string()])
    );

    let response = reader
        .find(&MemoryFindRequest {
            scope: MemoryScope {
                session_ids: Some(vec![session_id.to_string()]),
                ..Default::default()
            },
            filter: MemoryFilter {
                indexed_tags: Some(vec!["grammar".to_string()]),
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .expect("find by indexed tag should succeed");

    assert_eq!(response.retrieved, 1);
    assert_eq!(
        response.nodes[0].semantic_tags,
        Some(vec!["grammar".to_string(), "parser".to_string()])
    );
}

#[tokio::test]
async fn locus_evict_dry_run_then_delete_by_sync_key() {
    let session_id = "session-evict";
    let memory = LocusNodeStoreFactory::in_memory()
        .await
        .expect("in-memory node store should initialize");
    let writer = LocusContextWriter::new(memory.clone());
    let reader = LocusContextReader::new(memory.clone());
    let operations = LocusMemoryOperations::new(memory, None);

    let raw_node = render_prompt_response_sttp_node(
        session_id,
        "evict question",
        "evict answer",
        SttpPromptNodeFormat::TaggedSchema,
    );
    let stored = writer
        .store_context(&MemoryStoreRequest {
            session_id: session_id.to_string(),
            raw_node,
        })
        .await
        .expect("node should store");

    let recall_before = reader
        .find(&MemoryFindRequest {
            scope: MemoryScope {
                session_ids: Some(vec![session_id.to_string()]),
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .expect("find before evict");
    assert_eq!(recall_before.retrieved, 1);

    let dry_run = operations
        .evict(&MemoryEvictRequest {
            mode: MemoryEvictMode::ByNodeIds,
            scope: MemoryScope {
                session_ids: Some(vec![session_id.to_string()]),
                ..Default::default()
            },
            node_ids: Some(vec![stored.node_id.clone()]),
            dry_run: true,
            ..Default::default()
        })
        .await
        .expect("dry-run evict should succeed");
    assert!(dry_run.dry_run);
    assert_eq!(dry_run.deleted, 1);
    assert!(!dry_run.would_delete.is_empty());

    let applied = operations
        .evict(&MemoryEvictRequest {
            mode: MemoryEvictMode::ByNodeIds,
            scope: MemoryScope {
                session_ids: Some(vec![session_id.to_string()]),
                ..Default::default()
            },
            node_ids: Some(vec![stored.node_id]),
            dry_run: false,
            ..Default::default()
        })
        .await
        .expect("evict should succeed");
    assert_eq!(applied.deleted, 1);

    let recall_after = reader
        .find(&MemoryFindRequest {
            scope: MemoryScope {
                session_ids: Some(vec![session_id.to_string()]),
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .expect("find after evict");
    assert_eq!(recall_after.retrieved, 0);
}

#[tokio::test]
async fn locus_graph_returns_topology_after_store() {
    let session_id = "session-graph";
    let memory = LocusNodeStoreFactory::in_memory()
        .await
        .expect("in-memory node store should initialize");
    let writer = LocusContextWriter::new(memory.clone());
    let reader = LocusContextReader::new(memory);

    let raw_node = render_prompt_response_sttp_node(
        session_id,
        "graph question",
        "graph answer",
        SttpPromptNodeFormat::TaggedSchema,
    );
    writer
        .store_context(&MemoryStoreRequest {
            session_id: session_id.to_string(),
            raw_node,
        })
        .await
        .expect("node should store");

    let graph = reader
        .graph(&MemoryGraphRequest {
            scope: MemoryScope {
                session_ids: Some(vec![session_id.to_string()]),
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .expect("graph should succeed");

    assert!(graph.retrieved >= 1);
    assert!(!graph.nodes.is_empty());
}
