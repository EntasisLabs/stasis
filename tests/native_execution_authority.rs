use chrono::Utc;
use stasis::application::runtime::{
    execution_commit::{CommitResult, ExecutionGrant},
    in_memory_runtime::{InMemoryRuntime, JobExecutionOutcome},
};
use stasis::domain::runtime::{
    job::{BackoffPolicy, Job, JobState, NewJob},
    placement::WorkerCapabilities,
};
use stasis::ports::outbound::runtime::{
    job_attempt_store::JobAttemptStore, job_store::JobStore, outbox_store::OutboxStore,
};
use std::sync::Arc;

fn job(id: &str) -> Job {
    NewJob {
        id: id.into(),
        queue: "q".into(),
        job_type: "inert".into(),
        payload_ref: "{}".into(),
        priority: 0,
        max_attempts: 4,
        idempotency_key: id.into(),
        correlation_id: id.into(),
        causation_id: id.into(),
        trace_id: id.into(),
        input_provenance: None,
        placement: Default::default(),
        scheduled_at: Utc::now(),
        backoff_policy: BackoffPolicy {
            base_delay_seconds: 0,
            max_delay_seconds: 0,
        },
    }
    .into_job()
}
fn success(output: &str) -> JobExecutionOutcome {
    JobExecutionOutcome::Success {
        output_provenance: Some(
            stasis::domain::runtime::provenance::SttpProvenanceAdapter::to_provenance(output),
        ),
        execution_id: Some(output.into()),
        diagnostics: Some(format!("{{\"output_memory_node_id\":\"{output}\"}}")),
    }
}
struct Backend {
    jobs: Arc<dyn JobStore>,
    attempts: Arc<dyn JobAttemptStore>,
    outbox: Arc<dyn OutboxStore>,
}
impl Backend {
    fn memory() -> Self {
        let rt = InMemoryRuntime::new();
        Self {
            jobs: Arc::new(rt.job_store),
            attempts: Arc::new(rt.job_attempt_store),
            outbox: Arc::new(rt.outbox_store),
        }
    }
    #[cfg(feature = "surreal-native")]
    async fn surreal() -> Self {
        use stasis::application::runtime::surreal_runtime::SurrealRuntime;
        let db = surrealdb::Surreal::<surrealdb::engine::any::Any>::init();
        db.connect("mem://").await.unwrap();
        db.use_ns("foundation").use_db("barriers").await.unwrap();
        let rt = SurrealRuntime::new(db);
        Self {
            jobs: Arc::new(rt.job_store),
            attempts: Arc::new(rt.job_attempt_store),
            outbox: Arc::new(rt.outbox_store),
        }
    }
    async fn lease(&self, id: &str, owner: &str) -> Job {
        let mut snapshot = self
            .jobs
            .lease_due("q", owner, Utc::now(), 120, &WorkerCapabilities::any())
            .await
            .unwrap()
            .expect("due native lease");
        assert_eq!(snapshot.id, id);
        assert!(snapshot.execution_epoch > 0);
        snapshot.state = JobState::Running;
        self.jobs.save(snapshot).await.unwrap();
        self.jobs.get(id).await.unwrap().unwrap()
    }
    async fn count(&self, id: &str) -> (usize, usize) {
        (
            self.attempts.list_by_job_id(id).await.unwrap().len(),
            self.outbox.list_by_job_id(id).await.unwrap().len(),
        )
    }
}
async fn barriers(backend: Backend) {
    let b = backend;
    b.jobs.insert(job("race")).await.unwrap();
    let a = b.lease("race", "same-owner").await;
    let a_grant = ExecutionGrant::from_job(&a).unwrap();
    // Validation before expiry is deliberately not sufficient authority to commit later.
    assert!(a_grant.permits(&a, Utc::now()));
    b.jobs
        .heartbeat_granted(&a_grant, Utc::now(), 0, None)
        .await
        .unwrap();
    assert_eq!(
        b.jobs
            .commit_execution(&a, success("expired-A"))
            .await
            .unwrap(),
        CommitResult::Stale
    );
    assert_eq!(b.count("race").await, (0, 0));
    assert_eq!(
        b.jobs.recover_execution(&a).await.unwrap(),
        CommitResult::Committed
    );
    let current = b.lease("race", "same-owner").await;
    assert!(current.execution_epoch > a.execution_epoch);
    let grant = ExecutionGrant::from_job(&current).unwrap();
    assert!(
        b.jobs
            .heartbeat_granted(&a_grant, Utc::now(), 120, Some("stale".into()))
            .await
            .is_err()
    );
    assert!(b.jobs.save(a.clone()).await.is_err());
    assert_eq!(
        b.jobs
            .commit_execution(&current, success("B"))
            .await
            .unwrap(),
        CommitResult::Committed
    );
    let counts = b.count("race").await;
    for stale in [
        success("A"),
        JobExecutionOutcome::FatalFailure {
            message: "A failed".into(),
            execution_id: None,
            diagnostics: None,
        },
        JobExecutionOutcome::Deferred {
            scheduled_at: Utc::now(),
            message: "A deferred".into(),
            execution_id: None,
            diagnostics: None,
        },
    ] {
        assert_ne!(
            b.jobs.commit_execution(&a, stale).await.unwrap(),
            CommitResult::Committed
        );
        assert_eq!(b.count("race").await, counts);
        assert_eq!(
            b.jobs.get("race").await.unwrap().unwrap().output_provenance,
            success_provenance("B")
        );
    }
    assert_eq!(
        b.jobs
            .commit_execution(&current, success("B"))
            .await
            .unwrap(),
        CommitResult::AlreadyCommittedSameDigest
    );
    assert_eq!(
        b.jobs
            .commit_execution(&current, success("different"))
            .await
            .unwrap(),
        CommitResult::Conflict
    );
    assert_eq!(b.count("race").await, counts);
    assert!(
        b.jobs
            .heartbeat_granted(&grant, Utc::now(), 120, None)
            .await
            .is_err()
    );
    assert_ne!(
        b.jobs.recover_execution(&current).await.unwrap(),
        CommitResult::Committed
    );
    // Delete and recreate must not restart the generation (same-owner ABA).
    b.jobs.delete("race").await.unwrap();
    b.jobs.insert(job("race")).await.unwrap();
    let recreated = b.lease("race", "same-owner").await;
    assert!(recreated.execution_epoch > current.execution_epoch);
    assert_ne!(
        b.jobs
            .commit_execution(&current, success("B"))
            .await
            .unwrap(),
        CommitResult::Committed
    );
    b.jobs.cancel_execution("race").await.unwrap().unwrap();
    let before = b.count("race").await;
    assert_ne!(
        b.jobs
            .commit_execution(&recreated, success("resurrection"))
            .await
            .unwrap(),
        CommitResult::Committed
    );
    assert_eq!(b.count("race").await, before);
    let cleanup = ExecutionGrant::from_job(&recreated).unwrap();
    assert_eq!(
        b.jobs
            .acknowledge_cleanup(
                &cleanup,
                "{\"reaped\":true,\"logs\":\"immutable-log-ref\"}".into()
            )
            .await
            .unwrap(),
        CommitResult::Committed
    );
    assert_eq!(
        b.jobs
            .acknowledge_cleanup(
                &cleanup,
                "{\"reaped\":true,\"logs\":\"immutable-log-ref\"}".into()
            )
            .await
            .unwrap(),
        CommitResult::AlreadyCommittedSameDigest
    );
    assert_eq!(
        b.jobs
            .acknowledge_cleanup(&cleanup, "different".into())
            .await
            .unwrap(),
        CommitResult::Conflict
    );
    assert_eq!(
        b.jobs.get("race").await.unwrap().unwrap().state,
        JobState::Canceled
    );
}
fn success_provenance(s: &str) -> Option<stasis::domain::runtime::provenance::ProvenanceRef> {
    Some(stasis::domain::runtime::provenance::SttpProvenanceAdapter::to_provenance(s))
}
async fn concurrent_barriers(b: Backend) {
    for index in 0..6 {
        let id = format!("concurrent-{index}");
        b.jobs.insert(job(&id)).await.unwrap();
        let snapshot = b.lease(&id, "owner").await;
        let grant = ExecutionGrant::from_job(&snapshot).unwrap();
        let (heartbeat, commit) = tokio::join!(
            b.jobs
                .heartbeat_granted(&grant, Utc::now(), 120, Some("current progress".into())),
            b.jobs
                .commit_execution(&snapshot, success("heartbeat-winner"))
        );
        assert_eq!(commit.unwrap(), CommitResult::Committed);
        let current = b.jobs.get(&id).await.unwrap().unwrap();
        assert_eq!(current.state, JobState::Succeeded);
        if heartbeat.is_ok() {
            assert_eq!(current.progress_json.as_deref(), Some("current progress"));
        }
        assert_ne!(
            b.jobs.recover_execution(&snapshot).await.unwrap(),
            CommitResult::Committed
        );
        assert_eq!(b.count(&id).await, (1, 1));
        let receipt = b
            .jobs
            .execution_receipt(&id, grant.epoch)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(receipt.grant, grant);
        b.jobs.delete(&id).await.unwrap();
        assert!(
            b.jobs
                .execution_receipt(&id, grant.epoch)
                .await
                .unwrap()
                .is_some()
        );
        let id = format!("same-{index}");
        b.jobs.insert(job(&id)).await.unwrap();
        let snapshot = b.lease(&id, "owner").await;
        let (a, c) = tokio::join!(
            b.jobs.commit_execution(&snapshot, success("same")),
            b.jobs.commit_execution(&snapshot, success("same"))
        );
        let outcomes = [a.unwrap(), c.unwrap()];
        assert_eq!(
            outcomes
                .iter()
                .filter(|r| **r == CommitResult::Committed)
                .count(),
            1
        );
        assert!(outcomes.contains(&CommitResult::AlreadyCommittedSameDigest));
        assert_eq!(b.count(&id).await, (1, 1));
        let id = format!("different-{index}");
        b.jobs.insert(job(&id)).await.unwrap();
        let snapshot = b.lease(&id, "owner").await;
        let (a, c) = tokio::join!(
            b.jobs.commit_execution(&snapshot, success("different-A")),
            b.jobs.commit_execution(&snapshot, success("different-B"))
        );
        let outcomes = [a.unwrap(), c.unwrap()];
        assert_eq!(
            outcomes
                .iter()
                .filter(|r| **r == CommitResult::Committed)
                .count(),
            1
        );
        assert!(outcomes.contains(&CommitResult::Conflict));
        assert_eq!(b.count(&id).await, (1, 1));
        let id = format!("cancel-{index}");
        b.jobs.insert(job(&id)).await.unwrap();
        let snapshot = b.lease(&id, "owner").await;
        let grant = ExecutionGrant::from_job(&snapshot).unwrap();
        assert!(grant.permits(&snapshot, Utc::now())); // Deliberately stale pre-validation is not commit authority.
        let (canceled, committed) = tokio::join!(
            b.jobs.cancel_execution(&id),
            b.jobs.commit_execution(&snapshot, success("cancel-race"))
        );
        let current = b.jobs.get(&id).await.unwrap().unwrap();
        match current.state {
            JobState::Succeeded => {
                assert_eq!(committed.unwrap(), CommitResult::Committed);
                assert!(!matches!(canceled, Ok(Some(_))));
                assert_eq!(b.count(&id).await, (1, 1));
            }
            JobState::Canceled => {
                assert_ne!(committed.unwrap(), CommitResult::Committed);
                assert!(canceled.unwrap().is_some());
                assert_eq!(b.count(&id).await, (0, 1));
                assert!(
                    b.jobs
                        .cleanup_receipt(&id, grant.epoch)
                        .await
                        .unwrap()
                        .is_some()
                );
            }
            other => panic!("non-atomic cancellation disposition: {other:?}"),
        }
    }
}
#[tokio::test]
async fn memory_concurrent_native_transactions() {
    concurrent_barriers(Backend::memory()).await;
}
#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn actual_surreal_concurrent_native_transactions() {
    concurrent_barriers(Backend::surreal().await).await;
}

#[tokio::test]
async fn memory_authority_barriers() {
    barriers(Backend::memory()).await;
}
#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn actual_surreal_authority_barriers() {
    barriers(Backend::surreal().await).await;
}
