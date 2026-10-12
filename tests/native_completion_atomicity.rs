//! Deterministic native execution races. These fixtures launch no subprocess/CI workload.
use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};
#[cfg(feature = "surreal-native")]
use stasis::application::runtime::surreal_runtime::SurrealRuntime;
use stasis::application::runtime::{
    execution_commit::{CommitResult, ExecutionGrant},
    in_memory_runtime::{InMemoryRuntime, JobExecutionOutcome, JobHandler},
    job_context::{JobContext, JobResult},
    job_lifecycle::JobLifecycleEvent,
    typed_job::{JobConsumer, encode_typed_payload},
};
use stasis::domain::{
    errors::{Result, StasisError},
    runtime::{
        job::{BackoffPolicy, Job, JobState, NewJob},
        placement::WorkerCapabilities,
        typed_contract::{StasisEvent, StasisJob},
    },
};
use stasis::ports::outbound::runtime::{
    job_attempt_store::JobAttemptStore, job_continuation_store::JobContinuationStore,
    job_store::JobStore, outbox_store::OutboxStore,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::sync::Notify;

#[derive(Clone)]
enum Runtime {
    Memory(InMemoryRuntime),
    #[cfg(feature = "surreal-native")]
    Surreal(SurrealRuntime),
}
impl Runtime {
    async fn process(&self) -> Result<Option<String>> {
        match self {
            Self::Memory(r) => r.process_once("q", "same-owner", Utc::now()).await,
            #[cfg(feature = "surreal-native")]
            Self::Surreal(r) => r.process_once("q", "same-owner", Utc::now()).await,
        }
    }
    fn jobs(&self) -> Arc<dyn JobStore> {
        match self {
            Self::Memory(r) => Arc::new(r.job_store.clone()),
            #[cfg(feature = "surreal-native")]
            Self::Surreal(r) => Arc::new(r.job_store.clone()),
        }
    }
    async fn counts(&self, id: &str) -> (usize, usize) {
        match self {
            Self::Memory(r) => (
                r.job_attempt_store.list_by_job_id(id).await.unwrap().len(),
                r.outbox_store.list_by_job_id(id).await.unwrap().len(),
            ),
            #[cfg(feature = "surreal-native")]
            Self::Surreal(r) => (
                r.job_attempt_store.list_by_job_id(id).await.unwrap().len(),
                r.outbox_store.list_by_job_id(id).await.unwrap().len(),
            ),
        }
    }
    async fn continuation_exists(&self, id: &str) -> bool {
        match self {
            Self::Memory(r) => r.continuation_store.get(id).await.unwrap().is_some(),
            #[cfg(feature = "surreal-native")]
            Self::Surreal(r) => r.continuation_store.get(id).await.unwrap().is_some(),
        }
    }
    fn consumer(&self, c: PausedConsumer) {
        match self {
            Self::Memory(r) => r.register_consumer::<Payload, _>(c).unwrap(),
            #[cfg(feature = "surreal-native")]
            Self::Surreal(r) => r.register_consumer::<Payload, _>(c).unwrap(),
        }
    }
    fn handler(&self, h: Arc<dyn JobHandler>) {
        match self {
            Self::Memory(r) => r.register_handler(SharedHandler(h)).unwrap(),
            #[cfg(feature = "surreal-native")]
            Self::Surreal(r) => r.register_handler(SharedHandler(h)).unwrap(),
        }
    }
    async fn replay(&self) -> Result<usize> {
        match self {
            Self::Memory(r) => r.replay_lifecycle().await,
            #[cfg(feature = "surreal-native")]
            Self::Surreal(r) => r.replay_lifecycle().await,
        }
    }
}
fn job(id: &str, job_type: &str, payload: String) -> Job {
    NewJob {
        id: id.into(),
        queue: "q".into(),
        job_type: job_type.into(),
        payload_ref: payload,
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
fn success(s: &str) -> JobExecutionOutcome {
    JobExecutionOutcome::Success {
        output_provenance: Some(stasis::domain::runtime::provenance::ProvenanceRef::sttp(s)),
        execution_id: Some(s.into()),
        diagnostics: Some(format!("{{\"output_memory_node_id\":\"{s}\"}}")),
    }
}
#[derive(Clone, Serialize, Deserialize)]
struct Payload;
impl StasisJob for Payload {
    const NAME: &'static str = "foundation.paused";
    const VERSION: u32 = 1;
    type Output = String;
}
#[derive(Clone, Serialize, Deserialize)]
struct Child;
impl StasisJob for Child {
    const NAME: &'static str = "foundation.child";
    const VERSION: u32 = 1;
    type Output = String;
    fn declaration() -> stasis::domain::runtime::typed_contract::JobDeclaration {
        stasis::domain::runtime::typed_contract::JobDeclaration::default().queue("children")
    }
}
#[derive(Serialize, Deserialize)]
struct Effect;
impl StasisEvent for Effect {
    const NAME: &'static str = "foundation.effect";
    const VERSION: u32 = 1;
}
#[derive(Clone, Default)]
struct PausedConsumer {
    started: Arc<Notify>,
    release: Arc<Notify>,
    calls: Arc<AtomicUsize>,
    old: Arc<Mutex<Option<JobContext>>>,
    staged_ids: Arc<Mutex<Option<(String, String)>>>,
}
#[async_trait]
impl JobConsumer<Payload> for PausedConsumer {
    async fn consume(&self, _: Payload, ctx: JobContext) -> JobResult<String> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            assert_eq!(
                ctx.fencing_token.unwrap().0,
                ctx.execution_grant.as_ref().unwrap().epoch
            );
            let child = ctx.enqueue(Child).await?;
            let continuation = ctx.continue_with(Child).send().await?;
            ctx.publish(Effect).await?;
            *self.staged_ids.lock().unwrap() = Some((child, continuation.continuation_id));
            *self.old.lock().unwrap() = Some(ctx);
            self.started.notify_one();
            self.release.notified().await;
            Ok("OLD_A_OUTPUT".into())
        } else {
            Ok("CURRENT_B_OUTPUT".into())
        }
    }
}
async fn typed_automatic_race(rt: Runtime) {
    let consumer = PausedConsumer::default();
    rt.consumer(consumer.clone());
    rt.jobs()
        .insert(job(
            "automatic",
            Payload::NAME,
            encode_typed_payload(&Payload).unwrap(),
        ))
        .await
        .unwrap();
    let a_rt = rt.clone();
    let a = tokio::spawn(async move { a_rt.process().await });
    consumer.started.notified().await;
    let ctx = consumer.old.lock().unwrap().clone().unwrap();
    let (child, continuation) = consumer.staged_ids.lock().unwrap().clone().unwrap();
    assert!(rt.jobs().get(&child).await.unwrap().is_none());
    assert!(!rt.continuation_exists(&continuation).await);
    assert_eq!(rt.counts("automatic").await, (0, 0));
    rt.jobs()
        .heartbeat_granted(ctx.execution_grant.as_ref().unwrap(), Utc::now(), 0, None)
        .await
        .unwrap();
    let processed = rt.process().await.unwrap();
    assert_eq!(
        processed.as_deref(),
        Some("automatic"),
        "native job after recovery: {:?}",
        rt.jobs().get("automatic").await.unwrap()
    );
    let current = rt.jobs().get("automatic").await.unwrap().unwrap();
    assert_eq!(current.state, JobState::Succeeded);
    assert!(current.execution_epoch > ctx.execution_grant.as_ref().unwrap().epoch);
    let counts = rt.counts("automatic").await;
    assert_eq!(counts, (2, 2));
    assert!(ctx.heartbeat().await.is_err());
    assert!(ctx.progress("stale-progress").await.is_err());
    assert!(ctx.enqueue(Child).await.is_err());
    assert!(ctx.continue_with(Child).send().await.is_err());
    assert!(ctx.publish(Effect).await.is_err());
    assert!(ctx.wait_for::<Effect>().await.is_err());
    assert_eq!(
        ctx.acknowledge_cleanup("not-a-canceled-execution")
            .await
            .unwrap(),
        CommitResult::Stale
    );
    consumer.release.notify_one();
    a.await.unwrap().unwrap();
    let after = rt.jobs().get("automatic").await.unwrap().unwrap();
    assert_eq!(after.output_provenance, current.output_provenance);
    assert_eq!(after.version, current.version);
    assert_eq!(after.attempts, current.attempts);
    assert_eq!(rt.counts("automatic").await, counts);
    assert!(rt.jobs().get(&child).await.unwrap().is_none());
    assert!(!rt.continuation_exists(&continuation).await);
}
#[derive(Default)]
struct OrdinaryPaused {
    started: Notify,
    release: Notify,
    calls: AtomicUsize,
}
#[async_trait]
impl JobHandler for OrdinaryPaused {
    fn job_type(&self) -> &'static str {
        "ordinary"
    }
    async fn execute(&self, _: &Job) -> Result<JobExecutionOutcome> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.started.notify_one();
            self.release.notified().await;
            Ok(success("A"))
        } else {
            Ok(success("B"))
        }
    }
}
async fn ordinary_automatic_race(rt: Runtime) {
    let h = Arc::new(OrdinaryPaused::default());
    rt.handler(h.clone());
    rt.jobs()
        .insert(job("ordinary", "ordinary", "{}".into()))
        .await
        .unwrap();
    let other = rt.clone();
    let a = tokio::spawn(async move { other.process().await });
    h.started.notified().await;
    let snapshot = rt.jobs().get("ordinary").await.unwrap().unwrap();
    let grant = ExecutionGrant::from_job(&snapshot).unwrap();
    rt.jobs()
        .heartbeat_granted(&grant, Utc::now(), 0, None)
        .await
        .unwrap();
    assert_eq!(rt.process().await.unwrap().as_deref(), Some("ordinary"));
    let b = rt.jobs().get("ordinary").await.unwrap().unwrap();
    assert_eq!(b.state, JobState::Succeeded);
    let counts = rt.counts("ordinary").await;
    assert_eq!(counts, (2, 2));
    h.release.notify_one();
    a.await.unwrap().unwrap();
    assert_eq!(
        rt.jobs()
            .get("ordinary")
            .await
            .unwrap()
            .unwrap()
            .output_provenance,
        b.output_provenance
    );
    assert_eq!(rt.counts("ordinary").await, counts);
}
#[derive(Default)]
struct ReplayHook {
    keys: Mutex<std::collections::HashSet<String>>,
    effects: AtomicUsize,
    fail: AtomicBool,
}
#[async_trait]
impl JobHandler for ReplayHook {
    fn job_type(&self) -> &'static str {
        "ordinary"
    }
    async fn execute(&self, _: &Job) -> Result<JobExecutionOutcome> {
        Ok(success("committed-output"))
    }
    async fn on_lifecycle_committed(
        &self,
        _: &Job,
        _: &JobLifecycleEvent,
        key: &str,
    ) -> Result<()> {
        if self.keys.lock().unwrap().insert(key.into()) {
            self.effects.fetch_add(1, Ordering::SeqCst);
        }
        if self.fail.swap(false, Ordering::SeqCst) {
            return Err(StasisError::PortFailure(
                "injected crash after idempotent external effect".into(),
            ));
        }
        Ok(())
    }
}
async fn hook_replay(rt: Runtime) {
    let h = Arc::new(ReplayHook::default());
    h.fail.store(true, Ordering::SeqCst);
    rt.handler(h.clone());
    rt.jobs()
        .insert(job("hooks", "ordinary", "{}".into()))
        .await
        .unwrap();
    assert!(rt.process().await.is_err());
    let committed = rt.jobs().get("hooks").await.unwrap().unwrap();
    assert_eq!(committed.state, JobState::Succeeded);
    assert_eq!(rt.counts("hooks").await, (1, 1));
    assert_eq!(rt.replay().await.unwrap(), 1);
    assert_eq!(rt.replay().await.unwrap(), 0);
    assert_eq!(h.effects.load(Ordering::SeqCst), 1);
    assert_eq!(
        rt.jobs().get("hooks").await.unwrap().unwrap().version,
        committed.version
    );
}
#[tokio::test]
async fn memory_typed_automatic_stale_a_writes_nothing() {
    typed_automatic_race(Runtime::Memory(InMemoryRuntime::new())).await;
}
#[tokio::test]
async fn memory_ordinary_automatic_stale_a_writes_nothing() {
    ordinary_automatic_race(Runtime::Memory(InMemoryRuntime::new())).await;
}
#[tokio::test]
async fn memory_committed_hook_replay_is_idempotent() {
    hook_replay(Runtime::Memory(InMemoryRuntime::new())).await;
}
#[cfg(feature = "surreal-native")]
async fn db(endpoint: &str) -> surrealdb::Surreal<surrealdb::engine::any::Any> {
    let db = surrealdb::Surreal::<surrealdb::engine::any::Any>::init();
    db.connect(endpoint).await.unwrap();
    db.use_ns("foundation").use_db("acceptance").await.unwrap();
    db
}
#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn surreal_typed_automatic_stale_a_writes_nothing() {
    typed_automatic_race(Runtime::Surreal(SurrealRuntime::new(db("mem://").await))).await;
}
#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn surreal_ordinary_automatic_stale_a_writes_nothing() {
    ordinary_automatic_race(Runtime::Surreal(SurrealRuntime::new(db("mem://").await))).await;
}
#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn surreal_committed_hook_replay_is_idempotent() {
    hook_replay(Runtime::Surreal(SurrealRuntime::new(db("mem://").await))).await;
}
#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn surreal_crash_at_each_canonical_write_rolls_back_every_index() {
    let db = db("mem://").await;
    let rt = SurrealRuntime::new(db.clone());
    rt.job_store
        .insert(job("crash", "ordinary", "{}".into()))
        .await
        .unwrap();
    let snapshot = rt
        .job_store
        .lease_due("q", "worker", Utc::now(), 120, &WorkerCapabilities::any())
        .await
        .unwrap()
        .unwrap();
    let grant = ExecutionGrant::from_job(&snapshot).unwrap();
    let mut child = job("speculative-child", "ordinary", "{}".into());
    child.queue = "children".into();
    rt.job_store.stage_child(&grant, child).await.unwrap();
    for table in [
        "job",
        "job_attempt",
        "outbox_event",
        "job_execution_receipt",
    ] {
        let condition = if table == "job" {
            "$after.job_id = 'crash' AND $after.state = 'succeeded'"
        } else {
            "$after.job_id = 'crash'"
        };
        db.query(format!("DEFINE EVENT injected_boundary ON TABLE {table} WHEN {condition} THEN {{ THROW 'injected transaction write-boundary crash'; }};")).await.unwrap().check().unwrap();
        assert!(
            rt.job_store
                .commit_execution(&snapshot, success("crash-output"))
                .await
                .is_err(),
            "{table} must fail"
        );
        let current = rt.job_store.get("crash").await.unwrap().unwrap();
        assert_eq!(current.version, snapshot.version);
        assert_eq!(current.state, JobState::Leased);
        assert!(current.output_provenance.is_none());
        assert!(
            rt.job_attempt_store
                .list_by_job_id("crash")
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            rt.outbox_store
                .list_by_job_id("crash")
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            rt.job_store
                .get("speculative-child")
                .await
                .unwrap()
                .is_none()
        );
        assert!(rt.job_store.pending_lifecycle().await.unwrap().is_empty());
        db.query(format!("REMOVE EVENT injected_boundary ON TABLE {table};"))
            .await
            .unwrap()
            .check()
            .unwrap();
    }
    assert_eq!(
        rt.job_store
            .commit_execution(&snapshot, success("crash-output"))
            .await
            .unwrap(),
        CommitResult::Committed
    );
    assert!(
        rt.job_store
            .get("speculative-child")
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        rt.job_attempt_store
            .list_by_job_id("crash")
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        rt.job_store
            .commit_execution(&snapshot, success("crash-output"))
            .await
            .unwrap(),
        CommitResult::AlreadyCommittedSameDigest
    );
}
#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn actual_surreal_kv_restart_retains_grants_receipts_cleanup_and_pending_hook() {
    let root = std::env::temp_dir().join("stasis-store-runtime-safety-regressions")
        .join(format!(
            "restart-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
    std::fs::create_dir_all(&root).unwrap();
    let endpoint = format!("surrealkv://{}", root.display());
    let h = Arc::new(ReplayHook::default());
    h.fail.store(true, Ordering::SeqCst);
    let (snapshot, cancel_grant) = {
        let db = db(&endpoint).await;
        let rt = SurrealRuntime::new(db);
        rt.register_handler(SharedHandler(h.clone())).unwrap();
        rt.job_store
            .insert(job("restart-result", "ordinary", "{}".into()))
            .await
            .unwrap();
        let snapshot = rt
            .job_store
            .lease_due("q", "worker", Utc::now(), 120, &WorkerCapabilities::any())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            rt.job_store
                .commit_execution(&snapshot, success("persistent-result"))
                .await
                .unwrap(),
            CommitResult::Committed
        );
        rt.job_store
            .insert(job("restart-cleanup", "ordinary", "{}".into()))
            .await
            .unwrap();
        let canceled = rt
            .job_store
            .lease_due("q", "worker", Utc::now(), 120, &WorkerCapabilities::any())
            .await
            .unwrap()
            .unwrap();
        let grant = ExecutionGrant::from_job(&canceled).unwrap();
        rt.job_store.cancel_execution(&canceled.id).await.unwrap();
        assert_eq!(
            rt.job_store
                .acknowledge_cleanup(&grant, "finalized-cleanup-and-log-ref".into())
                .await
                .unwrap(),
            CommitResult::Committed
        );
        assert!(rt.replay_lifecycle().await.is_err());
        (snapshot, grant)
    };
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let rt = SurrealRuntime::new(db(&endpoint).await);
    rt.register_handler(SharedHandler(h.clone())).unwrap();
    assert_eq!(
        rt.job_store
            .commit_execution(&snapshot, success("persistent-result"))
            .await
            .unwrap(),
        CommitResult::AlreadyCommittedSameDigest
    );
    assert_eq!(
        rt.job_store
            .acknowledge_cleanup(&cancel_grant, "finalized-cleanup-and-log-ref".into())
            .await
            .unwrap(),
        CommitResult::AlreadyCommittedSameDigest
    );
    rt.replay_lifecycle().await.unwrap();
    assert_eq!(rt.replay_lifecycle().await.unwrap(), 0);
    let before = rt.job_store.get(&snapshot.id).await.unwrap().unwrap();
    rt.job_store.delete(&snapshot.id).await.unwrap();
    rt.job_store
        .insert(job(&snapshot.id, "ordinary", "{}".into()))
        .await
        .unwrap();
    let new = rt
        .job_store
        .lease_due("q", "worker", Utc::now(), 120, &WorkerCapabilities::any())
        .await
        .unwrap()
        .unwrap();
    assert!(new.execution_epoch > before.execution_epoch);
    assert_ne!(
        rt.job_store
            .commit_execution(&snapshot, success("stale-after-restart"))
            .await
            .unwrap(),
        CommitResult::Committed
    );
}

struct SharedHandler(Arc<dyn JobHandler>);
#[async_trait]
impl JobHandler for SharedHandler {
    fn job_type(&self) -> &'static str {
        self.0.job_type()
    }
    async fn execute(&self, job: &Job) -> Result<JobExecutionOutcome> {
        self.0.execute(job).await
    }
    async fn execute_with_context(
        &self,
        job: &Job,
        ctx: JobContext,
    ) -> Result<JobExecutionOutcome> {
        self.0.execute_with_context(job, ctx).await
    }
    async fn on_lifecycle_committed(
        &self,
        job: &Job,
        event: &JobLifecycleEvent,
        key: &str,
    ) -> Result<()> {
        self.0.on_lifecycle_committed(job, event, key).await
    }
}

async fn canceled_return_logs_are_cleanup_only(rt: Runtime) {
    let consumer = PausedConsumer::default();
    rt.consumer(consumer.clone());
    rt.jobs()
        .insert(job(
            "cleanup-auto",
            Payload::NAME,
            encode_typed_payload(&Payload).unwrap(),
        ))
        .await
        .unwrap();
    let worker = rt.clone();
    let a = tokio::spawn(async move { worker.process().await });
    consumer.started.notified().await;
    let ctx = consumer.old.lock().unwrap().clone().unwrap();
    let grant = ctx.execution_grant.clone().unwrap();
    rt.jobs()
        .cancel_execution("cleanup-auto")
        .await
        .unwrap()
        .unwrap();
    consumer.release.notify_one();
    a.await.unwrap().unwrap();
    let retired = rt
        .jobs()
        .cleanup_receipt("cleanup-auto", grant.epoch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retired.return_observations.len(), 1);
    assert!(retired.return_observations[0].contains("OLD_A_OUTPUT"));
    assert!(retired.acknowledged_at.is_none());
    assert!(retired.evidence_json.is_none());
    assert_eq!(
        rt.jobs().get("cleanup-auto").await.unwrap().unwrap().state,
        JobState::Canceled
    );
    assert_eq!(rt.counts("cleanup-auto").await, (0, 1));
    assert_eq!(
        ctx.acknowledge_cleanup(
            serde_json::json!({"descendants_reaped":true,"finalized_logs":"immutable-log-ref"})
        )
        .await
        .unwrap(),
        CommitResult::Committed
    );
    assert!(
        rt.jobs()
            .cleanup_receipt("cleanup-auto", grant.epoch)
            .await
            .unwrap()
            .unwrap()
            .acknowledged_at
            .is_some()
    );
    rt.jobs().delete("cleanup-auto").await.unwrap();
    rt.jobs()
        .insert(job(
            "cleanup-auto",
            Payload::NAME,
            encode_typed_payload(&Payload).unwrap(),
        ))
        .await
        .unwrap();
    rt.process().await.unwrap();
    let current = rt.jobs().get("cleanup-auto").await.unwrap().unwrap();
    assert_eq!(current.state, JobState::Succeeded);
    assert!(current.execution_epoch > grant.epoch);
    let counts = rt.counts("cleanup-auto").await;
    assert_eq!(
        ctx.acknowledge_cleanup(
            serde_json::json!({"descendants_reaped":true,"finalized_logs":"immutable-log-ref"})
        )
        .await
        .unwrap(),
        CommitResult::AlreadyCommittedSameDigest
    );
    assert_eq!(
        rt.jobs()
            .get("cleanup-auto")
            .await
            .unwrap()
            .unwrap()
            .version,
        current.version
    );
    assert_eq!(rt.counts("cleanup-auto").await, counts);
}
#[tokio::test]
async fn memory_canceled_automatic_return_retains_logs_without_success() {
    canceled_return_logs_are_cleanup_only(Runtime::Memory(InMemoryRuntime::new())).await;
}
#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn surreal_canceled_automatic_return_retains_logs_without_success() {
    canceled_return_logs_are_cleanup_only(Runtime::Surreal(SurrealRuntime::new(
        db("mem://").await,
    )))
    .await;
}
