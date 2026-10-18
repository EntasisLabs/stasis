//! One Locus node and tag scenario for every store implementation.

use std::sync::Arc;

use chrono::{TimeZone, Utc};
use locus_core_rs::domain::contracts::{NodeStore, SemanticIndexStore};
use locus_core_rs::domain::models::{
    AvecState, NodeDeleteRequest, NodeDeleteStatus, NodeQuery, NodeUpsertStatus,
    SemanticTagNodeRef, SttpNode, SyncCheckpoint, SyncCursor,
};

pub(crate) fn sample_node(session_id: &str, sync_key: &str, stability: f32) -> SttpNode {
    let avec = AvecState {
        stability,
        friction: 0.2,
        logic: 0.5,
        autonomy: 0.4,
    };
    SttpNode {
        raw: format!("raw-{sync_key}"),
        session_id: session_id.to_string(),
        tier: "raw".to_string(),
        timestamp: Utc.with_ymd_and_hms(2026, 3, 5, 6, 30, 0).unwrap(),
        compression_depth: 1,
        parent_node_id: None,
        sync_key: sync_key.to_string(),
        updated_at: Utc.with_ymd_and_hms(2026, 3, 5, 6, 30, 0).unwrap(),
        source_metadata: None,
        context_summary: Some(format!("summary-{sync_key}")),
        semantic_tags: Some(vec!["Alpha".to_string()]),
        semantic_links: None,
        embedding: None,
        embedding_model: None,
        embedding_dimensions: None,
        embedded_at: None,
        user_avec: avec,
        model_avec: avec,
        compression_avec: Some(avec),
        rho: 0.9,
        kappa: 0.8,
        psi: avec.psi(),
    }
}

pub(crate) async fn run_locus_contract(
    nodes: &Arc<dyn NodeStore>,
    tags: &Arc<dyn SemanticIndexStore>,
) {
    let created = nodes
        .upsert_node_async(sample_node("demo", "sync-1", 0.8))
        .await
        .expect("create");
    assert_eq!(created.status, NodeUpsertStatus::Created);

    let duplicate = nodes
        .upsert_node_async(sample_node("demo", "sync-1", 0.8))
        .await
        .expect("duplicate");
    assert_eq!(duplicate.status, NodeUpsertStatus::Duplicate);
    assert_eq!(duplicate.node_id, created.node_id);

    let mut changed = sample_node("demo", "sync-1", 0.8);
    changed.raw = "raw-changed".to_string();
    let updated = nodes.upsert_node_async(changed).await.expect("update");
    assert_eq!(updated.status, NodeUpsertStatus::Updated);
    assert_eq!(updated.node_id, created.node_id);

    nodes
        .upsert_node_async(sample_node("demo", "sync-far", 0.1))
        .await
        .expect("far node");
    let ranked = nodes
        .get_by_resonance_async(
            "demo",
            AvecState {
                stability: 0.8,
                friction: 0.2,
                logic: 0.5,
                autonomy: 0.4,
            },
            None,
            None,
            None,
            2,
        )
        .await
        .expect("resonance");
    assert_eq!(ranked.len(), 2);
    assert_eq!(ranked[0].sync_key, "sync-1");

    nodes
        .store_calibration_async(
            "demo",
            AvecState {
                stability: 0.7,
                friction: 0.2,
                logic: 0.5,
                autonomy: 0.4,
            },
            "manual",
        )
        .await
        .expect("calibration");
    let last = nodes
        .get_last_avec_async("demo")
        .await
        .expect("last avec")
        .expect("calibration row");
    assert!((last.stability - 0.7).abs() < 0.001);

    let cursor = SyncCursor {
        updated_at: Utc.with_ymd_and_hms(2026, 3, 5, 7, 0, 0).unwrap(),
        sync_key: "cursor-1".to_string(),
    };
    nodes
        .put_checkpoint_async(SyncCheckpoint {
            session_id: "demo".to_string(),
            connector_id: "connector".to_string(),
            cursor: Some(cursor.clone()),
            updated_at: cursor.updated_at,
            metadata: None,
        })
        .await
        .expect("checkpoint");
    let stored = nodes
        .get_checkpoint_async("demo", "connector")
        .await
        .expect("read checkpoint")
        .expect("checkpoint row");
    assert_eq!(stored.cursor.unwrap().sync_key, "cursor-1");

    let tenant_id = locus_core_rs::derive_tenant_id_from_session("demo");
    tags.sync_node_tags_async(
        SemanticTagNodeRef {
            tenant_id: tenant_id.clone(),
            session_id: "demo".to_string(),
            node_id: created.node_id.clone(),
            sync_key: "sync-1".to_string(),
        },
        &["Alpha".to_string(), "beta".to_string()],
        None,
    )
    .await
    .expect("tags");
    let keys = tags
        .find_sync_keys_by_tags_async(&tenant_id, &["alpha".to_string()], false, Some("demo"), 10)
        .await
        .expect("find tags");
    assert_eq!(keys, vec!["sync-1".to_string()]);

    let deleted = nodes
        .delete_nodes_async(NodeDeleteRequest {
            tenant_id,
            session_id: "demo".to_string(),
            sync_keys: vec!["sync-far".to_string()],
            node_ids: Vec::new(),
            dry_run: false,
        })
        .await
        .expect("delete");
    assert_eq!(deleted.deleted, 1);
    assert!(deleted.records.iter().any(|record| {
        record.sync_key == "sync-far" && record.status == NodeDeleteStatus::Deleted
    }));

    let remaining = nodes
        .query_nodes_async(NodeQuery {
            session_id: Some("demo".to_string()),
            limit: 10,
            ..NodeQuery::default()
        })
        .await
        .expect("query");
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].raw, "raw-changed");
}

#[tokio::test]
async fn in_memory_locus_contract() {
    use crate::infrastructure::memory::locus_node_store_factory::LocusNodeStoreFactory;

    let memory = LocusNodeStoreFactory::in_memory().await.expect("memory");
    run_locus_contract(&memory.node_store, &memory.semantic_index).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_and_executor_locus_contract_survives_reopen() {
    use std::sync::Arc;

    use crate::infrastructure::memory::locus_node_store_factory::LocusMemoryStore;
    use crate::infrastructure::sqlite::{RusqliteExecutor, SqliteSession};

    let executor = Arc::new(RusqliteExecutor::open_in_memory().expect("executor"));
    let memory = LocusMemoryStore::from_executor(executor.clone()).expect("locus");
    run_locus_contract(&memory.node_store, &memory.semantic_index).await;
    drop(memory);
    let reopened = LocusMemoryStore::from_executor(executor).expect("reopen executor");
    let rows = reopened
        .node_store
        .query_nodes_async(NodeQuery {
            session_id: Some("demo".to_string()),
            limit: 10,
            ..NodeQuery::default()
        })
        .await
        .expect("recall");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].raw, "raw-changed");

    let path = std::env::temp_dir().join(format!("stasis-locus-{}.sqlite", std::process::id()));
    let path = path.to_string_lossy().into_owned();
    {
        let memory =
            LocusMemoryStore::from_sqlite_session(SqliteSession::open(&path).expect("create"))
                .await
                .expect("file locus");
        memory
            .node_store
            .upsert_node_async(sample_node("kept", "sync-kept", 0.4))
            .await
            .expect("write");
    }
    let memory = LocusMemoryStore::from_sqlite_session(SqliteSession::open(&path).expect("reopen"))
        .await
        .expect("reopen file");
    let rows = memory
        .node_store
        .query_nodes_async(NodeQuery {
            session_id: Some("kept".to_string()),
            limit: 10,
            ..NodeQuery::default()
        })
        .await
        .expect("file recall");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].context_summary.as_deref(),
        Some("summary-sync-kept")
    );
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{path}-wal"));
    let _ = std::fs::remove_file(format!("{path}-shm"));
}

#[cfg(feature = "surreal")]
#[tokio::test]
async fn surreal_locus_contract_survives_reopen() {
    use surrealdb::Surreal;
    use surrealdb::engine::any::Any;

    use crate::infrastructure::memory::locus_node_store_factory::LocusMemoryStore;

    let db = Surreal::<Any>::init();
    db.connect("mem://").await.expect("connect");
    db.use_ns("test")
        .use_db("locus_contract")
        .await
        .expect("ns");
    let memory = LocusMemoryStore::from_surreal_db(db.clone())
        .await
        .expect("locus");
    memory
        .node_store
        .upsert_node_async(sample_node("demo", "sync-surreal", 0.55))
        .await
        .expect("write");
    drop(memory);
    let reopened = LocusMemoryStore::from_surreal_db(db).await.expect("reopen");
    let rows = reopened
        .node_store
        .query_nodes_async(NodeQuery {
            session_id: Some("demo".to_string()),
            limit: 10,
            ..NodeQuery::default()
        })
        .await
        .expect("recall");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].raw, "raw-sync-surreal");
    assert_eq!(
        rows[0].context_summary.as_deref(),
        Some("summary-sync-surreal")
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_runtime_locus_uses_the_job_database() {
    use crate::application::composition::runtime_composition::{RuntimeBackend, RuntimeFactory};
    use crate::infrastructure::memory::locus_node_store_factory::LocusMemoryStore;

    let path = std::env::temp_dir().join(format!(
        "stasis-runtime-locus-{}.sqlite",
        std::process::id()
    ));
    let path = path.to_string_lossy().into_owned();
    let runtime = RuntimeFactory::build(RuntimeBackend::Sqlite { path: path.clone() })
        .await
        .expect("runtime");
    let memory = RuntimeFactory::locus_memory_for(&runtime)
        .await
        .expect("wired locus");
    memory
        .node_store
        .upsert_node_async(sample_node("runtime", "sync-runtime", 0.6))
        .await
        .expect("write");
    drop(memory);
    drop(runtime);

    let recalled = LocusMemoryStore::from_sqlite_session(
        crate::infrastructure::sqlite::SqliteSession::open(&path).expect("reopen"),
    )
    .await
    .expect("store");
    let rows = recalled
        .node_store
        .query_nodes_async(NodeQuery {
            session_id: Some("runtime".to_string()),
            limit: 5,
            ..NodeQuery::default()
        })
        .await
        .expect("recall");
    assert_eq!(rows.len(), 1);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{path}-wal"));
    let _ = std::fs::remove_file(format!("{path}-shm"));
}

/// `:memory:` is a shared-cache URI so the adapter connection and the session
/// connection are one database.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_memory_adapter_shares_the_session_database() {
    use crate::infrastructure::memory::locus_node_store_factory::LocusMemoryStore;
    use crate::infrastructure::sqlite::SqliteSession;

    let session = SqliteSession::open(":memory:").expect("memory");
    assert!(session.path().contains("mode=memory"));
    let memory = LocusMemoryStore::from_sqlite_session(session.clone())
        .await
        .expect("adapter");
    memory
        .node_store
        .upsert_node_async(sample_node("mem", "sync-mem", 0.5))
        .await
        .expect("write");
    drop(memory);

    let via_session = LocusMemoryStore::from_executor(session.executor()).expect("executor");
    let rows = via_session
        .node_store
        .query_nodes_async(NodeQuery {
            session_id: Some("mem".to_string()),
            limit: 5,
            ..NodeQuery::default()
        })
        .await
        .expect("read");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].raw, "raw-sync-mem");
}

/// The native adapter and the wasm executor store must read each other's rows.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn adapter_and_executor_round_trip_one_file() {
    use std::sync::Arc;

    use locus_core_rs::domain::contracts::{NodeStoreInitializer, SemanticIndexStoreInitializer};
    use locus_sqlite_adapter::{SqliteDatabase, SqliteNodeStore, SqliteSemanticIndexStore};

    use crate::infrastructure::memory::executor_locus_store::{
        ExecutorLocusNodeStore, ExecutorSemanticIndexStore,
    };
    use crate::infrastructure::sqlite::SqliteSession;

    let path = std::env::temp_dir().join(format!(
        "stasis-locus-cross-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let path_str = path.to_string_lossy().into_owned();

    let db = Arc::new(SqliteDatabase::open(&path).expect("adapter open"));
    let adapter_nodes = Arc::new(SqliteNodeStore::new(db.clone()));
    let adapter_init: Arc<dyn NodeStoreInitializer> = adapter_nodes.clone();
    adapter_init
        .initialize_async()
        .await
        .expect("adapter schema");
    let adapter_tags = Arc::new(SqliteSemanticIndexStore::new(db));
    let adapter_tags_init: Arc<dyn SemanticIndexStoreInitializer> = adapter_tags.clone();
    adapter_tags_init
        .initialize_async()
        .await
        .expect("adapter tags");

    let created = adapter_nodes
        .upsert_node_async(sample_node("cross", "sync-from-adapter", 0.8))
        .await
        .expect("adapter write");
    assert_eq!(created.status, NodeUpsertStatus::Created);
    let tenant_id = locus_core_rs::derive_tenant_id_from_session("cross");
    adapter_tags
        .sync_node_tags_async(
            SemanticTagNodeRef {
                tenant_id: tenant_id.clone(),
                session_id: "cross".to_string(),
                node_id: created.node_id.clone(),
                sync_key: "sync-from-adapter".to_string(),
            },
            &["AdapterTag".to_string()],
            None,
        )
        .await
        .expect("adapter tag");

    let session = SqliteSession::open(&path_str).expect("session");
    let executor_nodes = Arc::new(ExecutorLocusNodeStore::new(session.executor()));
    executor_nodes.initialize().expect("executor schema");
    let executor_tags = Arc::new(ExecutorSemanticIndexStore::new(session.executor()));
    executor_tags.initialize().expect("executor tags");

    let from_adapter = executor_nodes
        .query_nodes_async(NodeQuery {
            session_id: Some("cross".to_string()),
            limit: 10,
            ..NodeQuery::default()
        })
        .await
        .expect("executor read");
    assert_eq!(from_adapter.len(), 1);
    assert_eq!(from_adapter[0].raw, "raw-sync-from-adapter");
    assert_eq!(from_adapter[0].sync_key, "sync-from-adapter");
    assert_eq!(
        from_adapter[0].context_summary.as_deref(),
        Some("summary-sync-from-adapter")
    );
    let adapter_keys = executor_tags
        .find_sync_keys_by_tags_async(
            &tenant_id,
            &["adaptertag".to_string()],
            false,
            Some("cross"),
            10,
        )
        .await
        .expect("executor tag read");
    assert_eq!(adapter_keys, vec!["sync-from-adapter".to_string()]);

    let ranked = executor_nodes
        .get_by_resonance_async(
            "cross",
            AvecState {
                stability: 0.8,
                friction: 0.2,
                logic: 0.5,
                autonomy: 0.4,
            },
            None,
            None,
            None,
            5,
        )
        .await
        .expect("executor rank");
    assert_eq!(ranked[0].sync_key, "sync-from-adapter");

    let executor_created = executor_nodes
        .upsert_node_async(sample_node("cross", "sync-from-executor", 0.2))
        .await
        .expect("executor write");
    assert_eq!(executor_created.status, NodeUpsertStatus::Created);
    executor_tags
        .sync_node_tags_async(
            SemanticTagNodeRef {
                tenant_id: tenant_id.clone(),
                session_id: "cross".to_string(),
                node_id: executor_created.node_id,
                sync_key: "sync-from-executor".to_string(),
            },
            &["ExecutorTag".to_string()],
            None,
        )
        .await
        .expect("executor tag");

    let from_executor = adapter_nodes
        .query_nodes_async(NodeQuery {
            session_id: Some("cross".to_string()),
            limit: 10,
            ..NodeQuery::default()
        })
        .await
        .expect("adapter read");
    let executor_row = from_executor
        .iter()
        .find(|node| node.sync_key == "sync-from-executor")
        .expect("executor row");
    assert_eq!(executor_row.raw, "raw-sync-from-executor");
    assert_eq!(
        executor_row.context_summary.as_deref(),
        Some("summary-sync-from-executor")
    );
    let executor_keys = adapter_tags
        .find_sync_keys_by_tags_async(
            &tenant_id,
            &["executortag".to_string()],
            false,
            Some("cross"),
            10,
        )
        .await
        .expect("adapter tag read");
    assert_eq!(executor_keys, vec!["sync-from-executor".to_string()]);

    let ranked = adapter_nodes
        .get_by_resonance_async(
            "cross",
            AvecState {
                stability: 0.8,
                friction: 0.2,
                logic: 0.5,
                autonomy: 0.4,
            },
            None,
            None,
            None,
            5,
        )
        .await
        .expect("adapter rank");
    assert_eq!(ranked[0].sync_key, "sync-from-adapter");
    assert_eq!(ranked[1].sync_key, "sync-from-executor");

    drop(adapter_nodes);
    drop(adapter_tags);
    drop(executor_nodes);
    drop(executor_tags);
    drop(session);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{path_str}-wal"));
    let _ = std::fs::remove_file(format!("{path_str}-shm"));
}
