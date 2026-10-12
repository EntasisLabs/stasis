// Reuse retained review fixtures without modifying their bytes.
include!("native_recipient_replay.rs");
use stasis::application::runtime::execution_commit::CommitResult;

async fn identity_genuine_edge(rt: Runtime) {
    let s = rt.services();
    let a = running(&s, "identity-genuine").await;
    let edge = context(&a, "A", s.clone())
        .continue_with(Child)
        .send()
        .await
        .unwrap();
    let mut forged = a.clone();
    forged.incarnation += 100;
    assert_eq!(
        s.job_store
            .commit_execution(&forged, success())
            .await
            .unwrap(),
        CommitResult::Committed
    );
    let receipt = s
        .job_store
        .execution_receipt(&a.id, a.execution_epoch)
        .await
        .unwrap()
        .unwrap();
    let published: Job = serde_json::from_str(&receipt.job_json).unwrap();
    assert_eq!(
        published.incarnation, a.incarnation,
        "receipt must retain native incarnation"
    );
    assert_eq!(
        s.job_store.get(&a.id).await.unwrap().unwrap().incarnation,
        a.incarnation
    );
    settle(&s, &published, &receipt.lifecycle).await.unwrap();
    materialized(&s, &edge.continuation_id).await;
    assert_eq!(
        s.job_store
            .commit_execution(&forged, success())
            .await
            .unwrap(),
        CommitResult::AlreadyCommittedSameDigest
    );
}

async fn identity_replacement_edge(rt: Runtime) {
    let s = rt.services();
    let a = running(&s, "identity-replacement").await;
    let old = producer(&s, &a.id).await;
    s.job_store.cancel_execution(&a.id).await.unwrap();
    assert!(s.job_store.delete(&a.id).await.unwrap());
    let b = running(&s, &a.id).await;
    assert_ne!(a.incarnation, b.incarnation);
    let new = context(&b, "A", s.clone())
        .continue_with(Child)
        .send()
        .await
        .unwrap();
    let mut forged = b.clone();
    forged.incarnation = a.incarnation;
    forged.terminal_cycle = a.terminal_cycle;
    assert_eq!(
        s.job_store
            .commit_execution(&forged, success())
            .await
            .unwrap(),
        CommitResult::Committed
    );
    let receipt = s
        .job_store
        .execution_receipt(&b.id, b.execution_epoch)
        .await
        .unwrap()
        .unwrap();
    let published: Job = serde_json::from_str(&receipt.job_json).unwrap();
    settle(&s, &published, &receipt.lifecycle).await.unwrap();
    assert_eq!(
        s.continuation_store
            .get(&old)
            .await
            .unwrap()
            .unwrap()
            .status,
        ContinuationStatus::Pending,
        "replacement success must not materialize predecessor obligation"
    );
    materialized(&s, &new.continuation_id).await;
    assert_eq!(published.incarnation, b.incarnation);
}

async fn identity_outcome_matrix(rt: Runtime) {
    let s = rt.services();
    for kind in 0..5 {
        let id = format!("identity-outcome-{kind}");
        let mut a = running(&s, &id).await;
        assert_eq!(a.id, id, "each matrix case must acquire its own job");
        a.started_at = Some(Utc::now() - Duration::seconds(5));
        s.job_store.save(a.clone()).await.unwrap();
        let a = s.job_store.get(&a.id).await.unwrap().unwrap();
        let mut forged = a.clone();
        forged.incarnation += 100;
        forged.terminal_cycle += 100;
        forged.queue = "forged-queue".into();
        forged.job_type = "forged-type".into();
        forged.payload_ref = "forged-payload".into();
        forged.priority = 999;
        forged.attempts = 999;
        forged.max_attempts = 1;
        forged.backoff_policy.base_delay_seconds = 999;
        forged.idempotency_key = "forged-idempotency".into();
        forged.correlation_id = "forged-correlation".into();
        forged.causation_id = "forged-causation".into();
        forged.trace_id = "forged-trace".into();
        forged.set_sttp_output_node_id("forged-output");
        forged.input_provenance = forged.output_provenance.clone();
        forged.started_at = None;
        forged.finished_at = Some(Utc::now());
        forged.scheduled_at = Utc::now() + Duration::days(30);
        let outcome = match kind {
            0 => success(),
            1 => JobExecutionOutcome::FatalFailure {
                message: "fatal".into(),
                execution_id: None,
                diagnostics: None,
            },
            2 | 4 => JobExecutionOutcome::RetryableFailure {
                message: if kind == 4 {
                    stasis::application::runtime::job_lifecycle::STALE_LEASE_MESSAGE.into()
                } else {
                    "retry".into()
                },
                execution_id: None,
                diagnostics: None,
            },
            _ => deferred(),
        };
        let result = if kind == 4 {
            s.job_store
                .heartbeat_granted(&ExecutionGrant::from_job(&a).unwrap(), Utc::now(), 0, None)
                .await
                .unwrap();
            s.job_store.recover_execution(&forged).await.unwrap()
        } else {
            s.job_store
                .commit_execution(&forged, outcome.clone())
                .await
                .unwrap()
        };
        assert_eq!(result, CommitResult::Committed);
        let row = s.job_store.get(&a.id).await.unwrap().unwrap();
        let receipt = s
            .job_store
            .execution_receipt(&a.id, a.execution_epoch)
            .await
            .unwrap()
            .unwrap();
        let retained: Job = serde_json::from_str(&receipt.job_json).unwrap();
        assert_eq!(
            serde_json::to_value(&row).unwrap(),
            serde_json::to_value(&retained).unwrap()
        );
        for published in [&row, &retained] {
            assert_eq!(published.incarnation, a.incarnation);
            assert_eq!(published.terminal_cycle, a.terminal_cycle);
            assert_eq!(published.queue, a.queue);
            assert_eq!(published.job_type, a.job_type);
            assert_eq!(published.payload_ref, a.payload_ref);
            assert_eq!(published.priority, a.priority);
            assert_eq!(published.max_attempts, a.max_attempts);
            assert_eq!(published.backoff_policy, a.backoff_policy);
            assert_eq!(published.idempotency_key, a.idempotency_key);
            assert_eq!(published.correlation_id, a.correlation_id);
            assert_eq!(published.causation_id, a.causation_id);
            assert_eq!(published.trace_id, a.trace_id);
            assert_eq!(published.input_provenance, a.input_provenance);
            assert_eq!(published.output_provenance, None);
            assert_eq!(published.started_at, a.started_at);
            assert_eq!(
                published.attempts,
                if matches!(kind, 1 | 2 | 4) {
                    a.attempts + 1
                } else {
                    a.attempts
                }
            );
        }
        assert_eq!(
            row.state,
            match kind {
                0 => JobState::Succeeded,
                1 => JobState::DeadLetter,
                _ => JobState::Enqueued,
            }
        );
        assert_eq!(receipt.grant, ExecutionGrant::from_job(&a).unwrap());
        use stasis::ports::outbound::runtime::{
            job_attempt_store::JobAttemptStore, outbox_store::OutboxStore,
        };
        let (attempts, events) = match &rt {
            Runtime::Memory(r) => (
                r.job_attempt_store.list_by_job_id(&a.id).await.unwrap(),
                r.outbox_store.list_by_job_id(&a.id).await.unwrap(),
            ),
            #[cfg(feature = "surreal-native")]
            Runtime::Surreal(r) => (
                r.job_attempt_store.list_by_job_id(&a.id).await.unwrap(),
                r.outbox_store.list_by_job_id(&a.id).await.unwrap(),
            ),
        };
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].attempt_number, a.attempts + 1);
        assert_eq!(attempts[0].started_at, a.started_at.unwrap());
        assert_eq!(attempts[0].output_provenance, None);
        let event = events
            .iter()
            .find(|e| e.event_id == format!("{}:execution:{}", a.id, a.execution_epoch))
            .unwrap();
        assert_eq!(event.event.correlation_id, a.correlation_id);
        assert_eq!(event.event.causation_id, a.causation_id);
        assert_eq!(event.event.trace_id, a.trace_id);
        assert_eq!(event.event.input_provenance, a.input_provenance);
        assert_eq!(event.event.output_provenance, None);
        assert_eq!(
            s.job_store
                .commit_execution(&forged, outcome)
                .await
                .unwrap(),
            CommitResult::AlreadyCommittedSameDigest
        );
        // Do not let a due retry from this case become the next case's lease.
        // Native deletion intentionally refuses live/enqueued jobs.
        s.job_store.cancel_execution(&a.id).await.unwrap();
        assert!(s.job_store.delete(&a.id).await.unwrap());
    }
}
#[tokio::test]
async fn identity_outcomes_memory() {
    identity_outcome_matrix(Runtime::memory()).await;
}
#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn identity_outcomes_surreal() {
    identity_outcome_matrix(Runtime::surreal().await).await;
}

#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn identity_kv_close_reopen() {
    use stasis::application::runtime::surreal_runtime::SurrealRuntime;
    async fn open(endpoint: &str) -> surrealdb::Surreal<surrealdb::engine::any::Any> {
        let db = surrealdb::Surreal::<surrealdb::engine::any::Any>::init();
        db.connect(endpoint).await.unwrap();
        db.use_ns("identity").use_db("restart").await.unwrap();
        db
    }
    let root = std::env::temp_dir().join(format!(
        "stasis-identity-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let endpoint = format!("surrealkv://{}", root.display());
    let (a, edge, forged) = {
        let rt = Runtime::Surreal(SurrealRuntime::new(open(&endpoint).await));
        let s = rt.services();
        let a = running(&s, "identity-kv").await;
        let edge = context(&a, "A", s.clone())
            .continue_with(Child)
            .send()
            .await
            .unwrap()
            .continuation_id;
        let mut forged = a.clone();
        forged.incarnation += 100;
        forged.correlation_id = "forged".into();
        assert_eq!(
            s.job_store
                .commit_execution(&forged, success())
                .await
                .unwrap(),
            CommitResult::Committed
        );
        (a, edge, forged)
    };
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let rt = Runtime::Surreal(SurrealRuntime::new(open(&endpoint).await));
    let s = rt.services();
    let r = s
        .job_store
        .execution_receipt(&a.id, a.execution_epoch)
        .await
        .unwrap()
        .unwrap();
    let p: Job = serde_json::from_str(&r.job_json).unwrap();
    assert_eq!(p.incarnation, a.incarnation);
    assert_eq!(p.correlation_id, a.correlation_id);
    settle(&s, &p, &r.lifecycle).await.unwrap();
    materialized(&s, &edge).await;
    assert_eq!(
        s.job_store
            .commit_execution(&forged, success())
            .await
            .unwrap(),
        CommitResult::AlreadyCommittedSameDigest
    );
}

#[tokio::test]
async fn identity_genuine_memory() {
    identity_genuine_edge(Runtime::memory()).await;
}
#[tokio::test]
async fn identity_replacement_memory() {
    identity_replacement_edge(Runtime::memory()).await;
}
#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn identity_genuine_surreal() {
    identity_genuine_edge(Runtime::surreal().await).await;
}
#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn identity_replacement_surreal() {
    identity_replacement_edge(Runtime::surreal().await).await;
}
