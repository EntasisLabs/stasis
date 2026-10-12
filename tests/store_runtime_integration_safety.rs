//! Safety assertions converted from the archived defect-positive audit probes.
use chrono::{Duration, Utc};
use stasis::application::runtime::{
    in_memory_runtime::InMemoryRuntime,
    job_context::{JobContext, JobResult},
    typed_job::JobConsumer,
};
use stasis::domain::runtime::{
    job::{BackoffPolicy, JobState, NewJob},
    resource_lease::{OwnerId, ResourceKey},
    typed_contract::StasisJob,
};
use stasis::ports::outbound::runtime::{
    job_attempt_store::JobAttemptStore, job_store::JobStore,
    resource_lease_store::ResourceLeaseStore,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::Notify;
#[derive(serde::Serialize, serde::Deserialize)]
struct Payload {}
impl StasisJob for Payload {
    const NAME: &'static str = "proof.inert";
    const VERSION: u32 = 1;
    type Output = String;
}
struct Consumer {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    calls: Arc<AtomicUsize>,
}
#[async_trait::async_trait]
impl JobConsumer<Payload> for Consumer {
    async fn consume(&self, _: Payload, ctx: JobContext) -> JobResult<String> {
        assert!(
            ctx.execution_grant.is_some(),
            "worker must carry native execution authority"
        );
        let i = self.calls.fetch_add(1, Ordering::SeqCst);
        if i == 0 {
            self.entered.notify_one();
            self.release.notified().await;
            Ok("OLD_A_OUTPUT".into())
        } else {
            Ok("CURRENT_B_OUTPUT".into())
        }
    }
}
#[tokio::test]
async fn old_owner_cannot_overwrite_new_owner() {
    let clock = Arc::new(TestClock::default());
    let rt = InMemoryRuntime::with_dependencies_and_metrics(
        clock.clone(),
        Arc::new(stasis::infrastructure::runtime::atomic_id_generator::AtomicIdGenerator::new(1)),
        Arc::new(stasis::infrastructure::runtime::noop_runtime_metrics::NoopRuntimeMetrics),
    );
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    rt.register_consumer::<Payload, _>(Consumer {
        entered: entered.clone(),
        release: release.clone(),
        calls,
    })
    .unwrap();
    let now = Utc::now();
    let key = ResourceKey("proof-job".into());
    let old_fence = rt
        .lease_store
        .acquire(
            key.clone(),
            OwnerId("A".into()),
            Duration::seconds(60),
            now,
            false,
        )
        .await
        .unwrap()
        .fencing_token;
    rt.enqueue(NewJob {
        id: "proof-job".into(),
        queue: "proof".into(),
        job_type: Payload::NAME.into(),
        payload_ref: r#"{"version":1,"payload":{}}"#.into(),
        priority: 100,
        max_attempts: 3,
        idempotency_key: "proof-idem".into(),
        correlation_id: "proof-corr".into(),
        causation_id: "proof-cause".into(),
        trace_id: "proof-trace".into(),
        input_provenance: None,
        placement: Default::default(),
        scheduled_at: now,
        backoff_policy: BackoffPolicy {
            base_delay_seconds: 1,
            max_delay_seconds: 1,
        },
    })
    .await
    .unwrap();
    let a = rt.clone();
    let handle = tokio::spawn(async move { a.process_once("proof", "A", now).await.unwrap() });
    tokio::time::timeout(std::time::Duration::from_secs(3), entered.notified())
        .await
        .expect("worker entered");
    assert!(
        rt.lease_store
            .validate_fence(&key, old_fence, now)
            .await
            .unwrap()
    );
    let later = now + Duration::seconds(1000);
    clock.set(later);
    rt.recover_stale(later).await.unwrap();
    let reassigned = rt
        .lease_store
        .acquire(
            key.clone(),
            OwnerId("B".into()),
            Duration::seconds(60),
            later,
            true,
        )
        .await
        .unwrap();
    assert_ne!(reassigned.fencing_token, old_fence);
    assert!(
        !rt.lease_store
            .validate_fence(&key, old_fence, later)
            .await
            .unwrap()
    );
    clock.set(later + Duration::seconds(2));
    assert_eq!(
        rt.process_once("proof", "B", later + Duration::seconds(2))
            .await
            .unwrap(),
        Some("proof-job".into())
    );
    let before = rt.job_store.get("proof-job").await.unwrap().unwrap();
    assert_eq!(before.state, JobState::Succeeded);
    let before_attempts = rt
        .job_attempt_store
        .list_by_job_id("proof-job")
        .await
        .unwrap();
    release.notify_one();
    handle.await.unwrap();
    let after = rt.job_store.get("proof-job").await.unwrap().unwrap();
    let attempts = rt
        .job_attempt_store
        .list_by_job_id("proof-job")
        .await
        .unwrap();
    assert_eq!(
        before.output_provenance, after.output_provenance,
        "stale A must not overwrite B output"
    );
    assert_eq!(
        after.attempts, before.attempts,
        "stale A must not reset retry count"
    );
    assert_eq!(
        attempts.len(),
        before_attempts.len(),
        "stale A must not append another attempt"
    );
    assert!(attempts.iter().any(|a| a.worker_id == "A"));
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_old_owner_cannot_overwrite_new_owner() {
    let clock = Arc::new(TestClock::default());
    let session = stasis::infrastructure::sqlite::SqliteSession::open(":memory:").unwrap();
    let rt =
        stasis::application::runtime::sqlite_runtime::SqliteRuntime::with_dependencies_and_metrics(
            session.clone(),
            clock.clone(),
            Arc::new(stasis::infrastructure::sqlite::durable_ids::DurableIds::native(session)),
            Arc::new(stasis::infrastructure::runtime::noop_runtime_metrics::NoopRuntimeMetrics),
        );
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    rt.register_consumer::<Payload, _>(Consumer {
        entered: entered.clone(),
        release: release.clone(),
        calls,
    })
    .unwrap();
    let now = Utc::now();
    let key = ResourceKey("proof-job".into());
    let old_fence = rt
        .lease_store
        .acquire(
            key.clone(),
            OwnerId("A".into()),
            Duration::seconds(60),
            now,
            false,
        )
        .await
        .unwrap()
        .fencing_token;
    rt.enqueue(NewJob {
        id: "proof-job".into(),
        queue: "proof".into(),
        job_type: Payload::NAME.into(),
        payload_ref: r#"{"version":1,"payload":{}}"#.into(),
        priority: 100,
        max_attempts: 3,
        idempotency_key: "proof-idem".into(),
        correlation_id: "proof-corr".into(),
        causation_id: "proof-cause".into(),
        trace_id: "proof-trace".into(),
        input_provenance: None,
        placement: Default::default(),
        scheduled_at: now,
        backoff_policy: BackoffPolicy {
            base_delay_seconds: 1,
            max_delay_seconds: 1,
        },
    })
    .await
    .unwrap();
    let a = rt.clone();
    let handle = tokio::spawn(async move { a.process_once("proof", "A", now).await.unwrap() });
    tokio::time::timeout(std::time::Duration::from_secs(3), entered.notified())
        .await
        .expect("worker entered");
    assert!(
        rt.lease_store
            .validate_fence(&key, old_fence, now)
            .await
            .unwrap()
    );
    let later = now + Duration::seconds(1000);
    clock.set(later);
    rt.recover_stale(later).await.unwrap();
    let reassigned = rt
        .lease_store
        .acquire(
            key.clone(),
            OwnerId("B".into()),
            Duration::seconds(60),
            later,
            true,
        )
        .await
        .unwrap();
    assert_ne!(reassigned.fencing_token, old_fence);
    assert!(
        !rt.lease_store
            .validate_fence(&key, old_fence, later)
            .await
            .unwrap()
    );
    clock.set(later + Duration::seconds(2));
    assert_eq!(
        rt.process_once("proof", "B", later + Duration::seconds(2))
            .await
            .unwrap(),
        Some("proof-job".into())
    );
    let before = rt.job_store.get("proof-job").await.unwrap().unwrap();
    assert_eq!(before.state, JobState::Succeeded);
    let before_attempts = rt
        .job_attempt_store
        .list_by_job_id("proof-job")
        .await
        .unwrap();
    release.notify_one();
    handle.await.unwrap();
    let after = rt.job_store.get("proof-job").await.unwrap().unwrap();
    let attempts = rt
        .job_attempt_store
        .list_by_job_id("proof-job")
        .await
        .unwrap();
    assert_eq!(
        before.output_provenance, after.output_provenance,
        "stale A must not overwrite B output"
    );
    assert_eq!(
        after.attempts, before.attempts,
        "stale A must not reset retry count"
    );
    assert_eq!(
        attempts.len(),
        before_attempts.len(),
        "stale A must not append another attempt"
    );
    assert!(attempts.iter().any(|a| a.worker_id == "A"));
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_restart_preserves_ids_and_original() {
    use stasis::application::runtime::sqlite_runtime::SqliteRuntime;
    use stasis::domain::runtime::inbound_trigger::InboundProtocol;
    let path = std::env::temp_dir().join(format!(
        "stasis-audit-restart-{}.sqlite",
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    ));
    let name = path.to_str().unwrap();
    let first = SqliteRuntime::open_path(name).unwrap();
    let a = first
        .accept_inbound_job(InboundProtocol::HttpWebhook, "first-request", Payload {})
        .await
        .unwrap();
    drop(first);
    let second = SqliteRuntime::open_path(name).unwrap();
    let b = second
        .accept_inbound_job(InboundProtocol::HttpWebhook, "second-request", Payload {})
        .await
        .unwrap();
    assert_ne!(
        a.job_id, b.job_id,
        "restart must allocate distinct persisted IDs"
    );
    let saved = second.job_store.get(&a.job_id).await.unwrap().unwrap();
    assert_eq!(
        saved.idempotency_key, "first-request",
        "first job must be preserved"
    );
    assert_eq!(
        second
            .job_store
            .get(&b.job_id)
            .await
            .unwrap()
            .unwrap()
            .idempotency_key,
        "second-request"
    );
    drop(second);
    std::fs::remove_file(path).unwrap();
}

#[derive(Default)]
struct TestClock(std::sync::Mutex<Option<chrono::DateTime<Utc>>>);
impl TestClock {
    fn set(&self, now: chrono::DateTime<Utc>) {
        *self.0.lock().unwrap() = Some(now);
    }
}
impl stasis::ports::outbound::runtime::clock::Clock for TestClock {
    fn now(&self) -> chrono::DateTime<Utc> {
        self.0.lock().unwrap().unwrap_or_else(Utc::now)
    }
}
