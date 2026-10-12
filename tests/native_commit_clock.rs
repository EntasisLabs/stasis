#![cfg(feature = "surreal-native")]
use chrono::Utc;
use stasis::application::runtime::surreal_runtime::SurrealRuntime;
use stasis::domain::runtime::{
    execution::{CommitResult, JobExecutionOutcome},
    job::{BackoffPolicy, JobState, NewJob},
    placement::WorkerCapabilities,
};
use stasis::ports::outbound::runtime::{
    job_attempt_store::JobAttemptStore, job_store::JobStore, outbox_store::OutboxStore,
};

#[tokio::test]
async fn native_commit_clock_rechecks_expiry_after_transaction_writes() {
    let db = surrealdb::Surreal::<surrealdb::engine::any::Any>::init();
    db.connect("mem://").await.unwrap();
    db.use_ns("foundation")
        .use_db("commit_clock")
        .await
        .unwrap();
    let rt = SurrealRuntime::new(db.clone());
    rt.job_store
        .insert(
            NewJob {
                id: "commit-clock".into(),
                queue: "q".into(),
                job_type: "inert".into(),
                payload_ref: "{}".into(),
                priority: 0,
                max_attempts: 3,
                idempotency_key: "commit-clock".into(),
                correlation_id: "commit-clock".into(),
                causation_id: "commit-clock".into(),
                trace_id: "commit-clock".into(),
                input_provenance: None,
                placement: Default::default(),
                scheduled_at: Utc::now(),
                backoff_policy: BackoffPolicy::default(),
            }
            .into_job(),
        )
        .await
        .unwrap();
    let snapshot = rt
        .job_store
        .lease_due("q", "worker", Utc::now(), 1, &WorkerCapabilities::any())
        .await
        .unwrap()
        .unwrap();
    // Native DB event pauses AFTER the guarded write. A worker's earlier validation cannot
    // authorize success if expiry passes before the final trusted transaction clock check.
    db.query("DEFINE EVENT delay_commit ON TABLE job WHEN $after.state = 'succeeded' THEN { SLEEP 1100ms; };").await.unwrap().check().unwrap();
    let output = JobExecutionOutcome::Success {
        output_provenance: None,
        execution_id: Some("expired-result".into()),
        diagnostics: Some("immutable-evidence-ref".into()),
    };
    assert!(
        rt.job_store
            .commit_execution(&snapshot, output)
            .await
            .is_err()
    );
    let current = rt.job_store.get(&snapshot.id).await.unwrap().unwrap();
    assert_eq!(current.state, JobState::Leased);
    assert_eq!(current.version, snapshot.version);
    assert!(current.output_provenance.is_none());
    assert!(
        rt.job_attempt_store
            .list_by_job_id(&snapshot.id)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        rt.outbox_store
            .list_by_job_id(&snapshot.id)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        rt.job_store
            .execution_receipt(&snapshot.id, snapshot.execution_epoch)
            .await
            .unwrap()
            .is_none()
    );
    db.query("REMOVE EVENT delay_commit ON TABLE job;")
        .await
        .unwrap()
        .check()
        .unwrap();
    assert_eq!(
        rt.job_store.recover_execution(&snapshot).await.unwrap(),
        CommitResult::Committed
    );
}
