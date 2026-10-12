//! Pre-#31 Surreal rows have epoch, version, incarnation, and terminal cycle zero.
//! Reconciliation must not trust those leases and must refuse a cycle it cannot prove.
use chrono::Utc;
use stasis::application::runtime::surreal_runtime::SurrealRuntime;
use stasis::domain::runtime::execution::JobExecutionOutcome;
use stasis::domain::runtime::job::JobState;
use stasis::infrastructure::runtime::surreal_job_store::SurrealJobStore;
use stasis::ports::outbound::runtime::durable_wait_store::DurableWaitStore;
use stasis::ports::outbound::runtime::job_continuation_store::JobContinuationStore;
use stasis::ports::outbound::runtime::job_store::JobStore;
async fn db(name: &str) -> surrealdb::Surreal<surrealdb::engine::any::Any> {
    let db = surrealdb::Surreal::<surrealdb::engine::any::Any>::init();
    db.connect("mem://").await.expect("surreal memory");
    db.use_ns("legacy").use_db(name).await.expect("ns");
    db
}

const CHILD_SPEC: &str = r#"{"job_type":"child","payload_ref":"{}","queue":"default","priority":0,"max_attempts":1,"backoff":{"base_delay_seconds":1,"max_delay_seconds":2},"idempotency_key":null,"correlation_id":null,"placement":{}}"#;

#[tokio::test]
async fn pre_identity_running_job_is_requeued_and_completes() {
    let db = db("requeue").await;
    db.query(
        "CREATE job:legacy SET job_id = 'legacy', queue = 'default', job_type = 'work', payload_ref = '{}', state = 'running', priority = 0, attempts = 1, max_attempts = 3, backoff_policy = { base_delay_seconds: 1, max_delay_seconds: 10 }, idempotency_key = 'legacy', correlation_id = 'c', causation_id = 'c', trace_id = 't', sttp_input_node_id = '', execution_epoch = 0, version = 0, incarnation = 0, terminal_cycle = 0, wakeup_pending = false, lease_owner = 'worker', lease_expires_at = time::now(), heartbeat_at = time::now(), scheduled_at = time::now(), started_at = time::now();
         CREATE durable_wait:w SET wait_id = 'w', job_id = 'legacy', incarnation = 0, signal_type = 'kind', correlation_key = 'key', status = 'pending', created_at = time::now(), updated_at = time::now(), consumed_signal_ids = [];
         CREATE job_continuation:edge SET continuation_id = 'edge', parent_job_id = 'legacy', parent_incarnation = 0, parent_terminal_cycle = 0, trigger = 'succeeded', status = 'pending', child_spec_json = $spec, created_at = time::now();
         CREATE durable_signal:sig SET signal_id = 'sig', signal_type = 'kind', correlation_key = 'key', payload_json = '{}', created_at = time::now();",
    )
    .bind(("spec", CHILD_SPEC))
    .await
    .unwrap()
    .check()
    .unwrap();

    let store = SurrealJobStore::new(db.clone());
    store.initialize_execution_schema().await.unwrap();
    let job = store.get("legacy").await.unwrap().unwrap();
    assert_eq!(job.state, JobState::Enqueued);
    assert!(job.lease_owner.is_none());
    assert!(job.incarnation > 0);
    assert_eq!(job.terminal_cycle, 1);
    assert_eq!(job.execution_epoch, 0);

    let rt = SurrealRuntime::new(db.clone());
    let wait = rt.wait_store.get_wait("w").await.unwrap().unwrap();
    assert_eq!(wait.incarnation, job.incarnation);
    let edge = rt.continuation_store.get("edge").await.unwrap().unwrap();
    assert_eq!(edge.parent_incarnation, job.incarnation);
    assert_eq!(edge.parent_terminal_cycle, 1);
    assert_eq!(edge.parent_version_floor, Some(job.version));
    let signal = rt.wait_store.get_signal("sig").await.unwrap().unwrap();
    assert!(signal.target_incarnations.is_none());

    let leased = rt
        .job_store
        .lease_due("default", "worker", Utc::now(), 60, &Default::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(leased.id, "legacy");
    assert!(leased.execution_epoch > 0);
    rt.job_store
        .commit_execution(
            &leased,
            JobExecutionOutcome::Success {
                output_provenance: None,
                execution_id: None,
                diagnostics: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        rt.job_store.get("legacy").await.unwrap().unwrap().state,
        JobState::Succeeded
    );
}

#[tokio::test]
async fn legacy_terminal_cycle_with_a_receipt_is_refused() {
    let db = db("receipt").await;
    db.query(
        "CREATE job:legacy SET job_id = 'legacy', queue = 'default', job_type = 'work', payload_ref = '{}', state = 'succeeded', priority = 0, attempts = 1, max_attempts = 3, backoff_policy = { base_delay_seconds: 1, max_delay_seconds: 10 }, idempotency_key = 'legacy', correlation_id = 'c', causation_id = 'c', trace_id = 't', sttp_input_node_id = '', execution_epoch = 1, version = 1, incarnation = 0, terminal_cycle = 0, scheduled_at = time::now(), finished_at = time::now();
         CREATE job_execution_receipt:r SET receipt_id = 'r', job_id = 'legacy', owner = 'worker', epoch = 1, version = 1, digest = 'd', receipt_json = '{}', delivered = true;",
    )
    .await
    .unwrap()
    .check()
    .unwrap();
    let error = SurrealJobStore::new(db)
        .initialize_execution_schema()
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("terminal cycle 0"),
        "unexpected reconcile error: {error}"
    );
}
