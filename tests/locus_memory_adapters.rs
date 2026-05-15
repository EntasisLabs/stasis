use stasis::prelude::{
    LocusContextReader, LocusContextWriter, LocusMemoryOperations, LocusNodeStoreFactory,
    MemoryAggregateRequest, MemoryContextReader, MemoryContextWriter, MemoryOperations,
    MemoryRecallRequest, MemoryScope, MemoryStoreRequest, MemoryTransformRequest,
};

#[tokio::test]
async fn locus_node_store_factory_in_memory_initializes_store() {
    let store = LocusNodeStoreFactory::in_memory()
        .await
        .expect("in-memory node store should initialize");

    let reader = LocusContextReader::new(store);
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
async fn locus_context_writer_rejects_invalid_sttp_node() {
    let store = LocusNodeStoreFactory::in_memory()
        .await
        .expect("in-memory node store should initialize");
    let writer = LocusContextWriter::new(store);

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
    let store = LocusNodeStoreFactory::in_memory()
        .await
        .expect("in-memory node store should initialize");
    let operations = LocusMemoryOperations::new(store, None);

    let schema = operations.schema().await.expect("schema should be available");
    assert!(
        !schema.schema_version.trim().is_empty(),
        "schema version should be non-empty"
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
        .rollup(&stasis::prelude::MemoryRollupRequest {
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
    let store = LocusNodeStoreFactory::in_memory()
        .await
        .expect("in-memory node store should initialize");
    let operations = LocusMemoryOperations::new(store, None);

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
    let message = result.err().expect("error should exist").to_string();
    assert!(
        message.contains("requires ai provider registry"),
        "unexpected error message: {message}"
    );
}
