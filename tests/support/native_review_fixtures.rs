// Shared regression suite; each target injects its storage backend only.
macro_rules! review_fixtures { ($backend:path, $open:expr) => {
use $backend as InMemoryRuntime;
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use stasis::application::runtime::{
    job_handler::{JobExecutionOutcome, JobHandler},
    job_context::{JobContext, JobContextServices, JobResult},
    job_continuation::ContinuationBuilder,
    job_lifecycle::JobLifecycleEvent,
    runtime_job_dx,
    typed_job::{JobConsumer, TypedJobHandler, encode_typed_payload},
};
use stasis::domain::{
    errors::Result,
    runtime::{
        execution::ExecutionGrant,
        job::{Job, JobState, NewJob},
        placement::WorkerCapabilities,
        typed_contract::{StasisEvent, StasisJob},
    },
};
use stasis::ports::outbound::runtime::job_store::JobStore;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::sync::Semaphore;

// The rejected pin has no extracted helper. This observation-only backport is
// its original save/reload sequence verbatim in semantics; no fence is added.
// Fresh builds call the production helper used by BOTH runtime entry points.
#[allow(unexpected_cfgs)]
mod transition {
    #[cfg(not(stasis_rejected_pin))]
    pub use stasis::application::runtime::runtime_job_dx::running_snapshot;
    #[cfg(stasis_rejected_pin)]
    pub async fn running_snapshot(
        store: &dyn super::JobStore,
        mut job: super::Job,
    ) -> super::Result<Option<super::Job>> {
        job.state = super::JobState::Running;
        store.save(job.clone()).await?;
        job = store.get(&job.id).await?.ok_or_else(|| {
            stasis::domain::errors::StasisError::PortFailure("leased job disappeared".into())
        })?;
        Ok(Some(job))
    }
}

#[derive(Serialize, Deserialize)]
struct Payload;
impl StasisJob for Payload {
    const NAME: &'static str = "correction.payload";
    const VERSION: u32 = 1;
    type Output = String;
}
#[derive(Serialize, Deserialize)]
struct Child;
impl StasisJob for Child {
    const NAME: &'static str = "correction.child";
    const VERSION: u32 = 1;
    type Output = String;
}
#[derive(Serialize, Deserialize)]
struct Signal;
impl StasisEvent for Signal {
    const NAME: &'static str = "correction.signal";
    const VERSION: u32 = 1;
}
fn job(id: &str) -> Job {
    NewJob {
        id: id.into(),
        queue: "q".into(),
        job_type: Payload::NAME.into(),
        payload_ref: encode_typed_payload(&Payload).unwrap(),
        priority: 0,
        max_attempts: 20,
        idempotency_key: id.into(),
        correlation_id: id.into(),
        causation_id: id.into(),
        trace_id: id.into(),
        input_provenance: None,
        placement: Default::default(),
        scheduled_at: Utc::now(),
        backoff_policy: stasis::domain::runtime::job::BackoffPolicy {
            base_delay_seconds: 0,
            max_delay_seconds: 0,
        },
    }
    .into_job()
}
fn success() -> JobExecutionOutcome {
    JobExecutionOutcome::Success {
        output_provenance: None,
        execution_id: None,
        diagnostics: None,
    }
}
fn deferred() -> JobExecutionOutcome {
    JobExecutionOutcome::Deferred {
        scheduled_at: Utc::now() + Duration::days(7),
        message: "wait".into(),
        execution_id: None,
        diagnostics: None,
    }
}
fn context(j: &Job, owner: &str, s: JobContextServices) -> JobContext {
    let (_, rx) = tokio::sync::watch::channel(false);
    JobContext::new(j, owner, rx, s, 120)
}

enum Runtime {
    Memory(InMemoryRuntime),
    #[cfg(feature = "surreal-native")]
    Surreal(stasis::application::runtime::surreal_runtime::SurrealRuntime),
}
impl Runtime {
    fn memory() -> Self {
        Self::Memory($open)
    }
    #[cfg(feature = "surreal-native")]
    async fn surreal() -> Self {
        let db = surrealdb::Surreal::<surrealdb::engine::any::Any>::init();
        db.connect("mem://").await.unwrap();
        db.use_ns("corrections").use_db("barriers").await.unwrap();
        Self::Surreal(stasis::application::runtime::surreal_runtime::SurrealRuntime::new(db))
    }
    fn services(&self) -> JobContextServices {
        match self {
            Self::Memory(r) => r.job_context_services(),
            #[cfg(feature = "surreal-native")]
            Self::Surreal(r) => r.job_context_services(),
        }
    }
    fn register(&self, h: Hook) {
        match self {
            Self::Memory(r) => r.register_handler(h).unwrap(),
            #[cfg(feature = "surreal-native")]
            Self::Surreal(r) => r.register_handler(h).unwrap(),
        }
    }
    async fn replay(&self) -> usize {
        match self {
            Self::Memory(r) => r.replay_lifecycle().await.unwrap(),
            #[cfg(feature = "surreal-native")]
            Self::Surreal(r) => r.replay_lifecycle().await.unwrap(),
        }
    }
}
async fn running(s: &JobContextServices, id: &str) -> Job {
    s.job_store.insert(job(id)).await.unwrap();
    let j = s
        .job_store
        .lease_due("q", "A", Utc::now(), 120, &WorkerCapabilities::any())
        .await
        .unwrap()
        .unwrap();
    transition::running_snapshot(s.job_store.as_ref(), j)
        .await
        .unwrap()
        .unwrap()
}

// This store is an observation seam only: native save completes, unrestricted reload
// is held until the real backend has expired A, recovered, and issued B.
struct ReloadBarrier {
    inner: Arc<dyn JobStore>,
    armed: AtomicBool,
    reached: Arc<Semaphore>,
    release: Arc<Semaphore>,
}
#[async_trait]
impl JobStore for ReloadBarrier {
    async fn insert(&self, j: Job) -> Result<()> {
        self.inner.insert(j).await
    }
    async fn save(&self, j: Job) -> Result<()> {
        self.inner.save(j).await?;
        self.armed.store(true, Ordering::SeqCst);
        Ok(())
    }
    async fn get(&self, id: &str) -> Result<Option<Job>> {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.reached.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
        self.inner.get(id).await
    }
    async fn lease_due(
        &self,
        q: &str,
        o: &str,
        n: DateTime<Utc>,
        t: i64,
        w: &WorkerCapabilities,
    ) -> Result<Option<Job>> {
        self.inner.lease_due(q, o, n, t, w).await
    }
    async fn heartbeat(&self, id: &str, o: &str, n: DateTime<Utc>, t: i64) -> Result<()> {
        self.inner.heartbeat(id, o, n, t).await
    }
    async fn list_by_state(&self, s: JobState) -> Result<Vec<Job>> {
        self.inner.list_by_state(s).await
    }
    async fn list_expired_leases(&self, n: DateTime<Utc>) -> Result<Vec<Job>> {
        self.inner.list_expired_leases(n).await
    }
    async fn delete(&self, id: &str) -> Result<bool> {
        self.inner.delete(id).await
    }
    async fn prune_terminal_before(&self, n: DateTime<Utc>) -> Result<usize> {
        self.inner.prune_terminal_before(n).await
    }
}
#[derive(Clone)]
struct Adversary(Arc<AtomicUsize>);
#[async_trait]
impl JobConsumer<Payload> for Adversary {
    async fn consume(&self, _: Payload, ctx: JobContext) -> JobResult<String> {
        self.0.fetch_add(1, Ordering::SeqCst);
        ctx.enqueue(Child).await?;
        ctx.continue_with(Child).send().await?;
        Ok("adopted successor".into())
    }
}
#[async_trait]
impl JobHandler for Adversary {
    fn job_type(&self) -> &'static str {
        Payload::NAME
    }
    async fn execute(&self, _: &Job) -> Result<JobExecutionOutcome> {
        panic!("context path required")
    }
    async fn execute_with_context(&self, _: &Job, ctx: JobContext) -> Result<JobExecutionOutcome> {
        self.0.fetch_add(1, Ordering::SeqCst);
        ctx.enqueue(Child).await?;
        ctx.continue_with(Child).send().await?;
        Ok(success())
    }
}
async fn reload_case(rt: Runtime, owner: &'static str, typed: bool) {
    let s = rt.services();
    let id = format!("reload-{owner}-{typed}");
    s.job_store.insert(job(&id)).await.unwrap();
    let a = s
        .job_store
        .lease_due("q", "A", Utc::now(), 120, &WorkerCapabilities::any())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a.id, id);
    let reached = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let seam = Arc::new(ReloadBarrier {
        inner: s.job_store.clone(),
        armed: AtomicBool::new(false),
        reached: reached.clone(),
        release: release.clone(),
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let consumer = Adversary(calls.clone());
    let handler: Arc<dyn JobHandler> = if typed {
        Arc::new(TypedJobHandler::<Payload, _>::new(consumer))
    } else {
        Arc::new(consumer)
    };
    let task_services = s.clone();
    let task = tokio::spawn(async move {
        if let Some(j) = transition::running_snapshot(seam.as_ref(), a)
            .await
            .unwrap()
        {
            let (outcome, _guard) = runtime_job_dx::execute_handler(
                Some(handler),
                &j,
                "A",
                task_services.clone(),
                &runtime_job_dx::new_in_flight_map(),
            )
            .await
            .unwrap();
            task_services
                .job_store
                .commit_execution(&j, outcome)
                .await
                .unwrap();
        }
    });
    reached.acquire().await.unwrap().forget();
    let a = s.job_store.get(&id).await.unwrap().unwrap();
    s.job_store
        .heartbeat_granted(&ExecutionGrant::from_job(&a).unwrap(), Utc::now(), 0, None)
        .await
        .unwrap();
    assert_eq!(
        s.job_store.recover_execution(&a).await.unwrap(),
        stasis::domain::runtime::execution::CommitResult::Committed
    );
    let b = s
        .job_store
        .lease_due(
            "q",
            owner,
            Utc::now() + Duration::seconds(1),
            120,
            &WorkerCapabilities::any(),
        )
        .await
        .unwrap()
        .unwrap();
    let b = transition::running_snapshot(s.job_store.as_ref(), b)
        .await
        .unwrap()
        .unwrap();
    assert!(b.execution_epoch > a.execution_epoch);
    release.add_permits(1);
    task.await.unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "stale A executed successor, typed={typed}, owner={owner}"
    );
    assert!(
        s.job_store
            .list_by_state(JobState::Enqueued)
            .await
            .unwrap()
            .is_empty(),
        "stale A published children"
    );
    assert!(
        s.continuation_store
            .list_pending_by_parent(&id)
            .await
            .unwrap()
            .is_empty(),
        "stale A published continuation"
    );
    let current = s.job_store.get(&id).await.unwrap().unwrap();
    assert_eq!(current.version, b.version);
    assert_eq!(current.state, JobState::Running);
    assert_eq!(
        s.job_store.commit_execution(&b, success()).await.unwrap(),
        stasis::domain::runtime::execution::CommitResult::Committed
    );
}
async fn signal_matrix(rt: Runtime) {
    let s = rt.services();
    for mode in ["success", "defer", "late"] {
        let id = format!("signal-{mode}");
        let a = running(&s, &id).await;
        let ctx = context(&a, "A", s.clone());
        assert!(ctx.wait_for::<Signal>().await.is_err());
        // Deterministic barrier: the handler has observed Pending but has not yet returned Deferred.
        let reached = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let rr = reached.clone();
        let rel = release.clone();
        let jobs = s.job_store.clone();
        let snapshot = a.clone();
        let outcome = if mode == "success" {
            success()
        } else {
            deferred()
        };
        let submitted = outcome.clone();
        let task = tokio::spawn(async move {
            rr.add_permits(1);
            rel.acquire().await.unwrap().forget();
            jobs.commit_execution(&snapshot, outcome).await.unwrap()
        });
        reached.acquire().await.unwrap().forget();
        if mode != "late" {
            runtime_job_dx::signal_event(
                s.job_store.as_ref(),
                s.wait_store.as_ref(),
                s.clock.as_ref(),
                id.clone(),
                Signal,
            )
            .await
            .unwrap();
            assert_eq!(
                s.job_store.get(&id).await.unwrap().unwrap().version,
                a.version,
                "signal revoked active version"
            );
            ctx.heartbeat().await.unwrap();
        }
        release.add_permits(1);
        assert_eq!(
            task.await.unwrap(),
            stasis::domain::runtime::execution::CommitResult::Committed
        );
        assert_eq!(
            s.job_store.commit_execution(&a, submitted).await.unwrap(),
            stasis::domain::runtime::execution::CommitResult::AlreadyCommittedSameDigest
        );
        if mode == "defer" {
            let receipt = s
                .job_store
                .execution_receipt(&id, a.execution_epoch)
                .await
                .unwrap()
                .unwrap();
            let committed: Job = serde_json::from_str(&receipt.job_json).unwrap();
            let actual = s.job_store.get(&id).await.unwrap().unwrap();
            assert_eq!(
                committed.scheduled_at, actual.scheduled_at,
                "receipt must retain effective native wakeup"
            );
            match receipt.lifecycle {
                JobLifecycleEvent::Deferred { scheduled_at, .. } => {
                    assert_eq!(scheduled_at, actual.scheduled_at)
                }
                _ => panic!("wrong native lifecycle"),
            }
        }
        if mode == "late" {
            runtime_job_dx::signal_event(
                s.job_store.as_ref(),
                s.wait_store.as_ref(),
                s.clock.as_ref(),
                id.clone(),
                Signal,
            )
            .await
            .unwrap();
        }
        let current = s.job_store.get(&id).await.unwrap().unwrap();
        if mode != "success" {
            assert!(current.scheduled_at <= Utc::now(), "Deferred lost wakeup");
            assert_eq!(current.state, JobState::Enqueued);
            let b = s
                .job_store
                .lease_due("q", "B", Utc::now(), 120, &WorkerCapabilities::any())
                .await
                .unwrap()
                .unwrap();
            let ctx = context(&b, "B", s.clone());
            ctx.wait_for::<Signal>()
                .await
                .expect("signal survives deferral");
            s.job_store.cancel_execution(&id).await.unwrap();
        }
    }
}
#[derive(Clone)]
struct Hook(Arc<Mutex<Vec<String>>>);
#[async_trait]
impl JobHandler for Hook {
    fn job_type(&self) -> &'static str {
        Payload::NAME
    }
    async fn execute(&self, _: &Job) -> Result<JobExecutionOutcome> {
        Ok(success())
    }
    async fn on_lifecycle_committed(
        &self,
        _: &Job,
        _: &JobLifecycleEvent,
        key: &str,
    ) -> Result<()> {
        self.0.lock().unwrap().push(key.into());
        Ok(())
    }
}
async fn controls(rt: Runtime) {
    let s = rt.services();
    let hooks = Arc::new(Mutex::new(Vec::new()));
    rt.register(Hook(hooks.clone()));
    for kind in ["cancel", "fail"] {
        let id = format!("recreated-{kind}");
        for _ in 0..3 {
            s.job_store.insert(job(&id)).await.unwrap();
            let j = s.job_store.get(&id).await.unwrap().unwrap();
            assert_eq!(j.version, 0);
            assert!(j.lease_owner.is_none());
            if kind == "cancel" {
                s.job_store.cancel_execution(&id).await.unwrap().unwrap();
            } else {
                s.job_store.fail_execution(&id).await.unwrap().unwrap();
            }
            s.job_store.delete(&id).await.unwrap();
        }
    }
    let events = s.outbox_store.list_pending(100).await.unwrap();
    assert_eq!(
        events.len(),
        6,
        "old undelivered native control outbox lost"
    );
    assert_eq!(
        events
            .iter()
            .map(|e| &e.event_id)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        6
    );
    assert_eq!(
        rt.replay().await,
        6,
        "old undelivered native control journals lost"
    );
    let keys = hooks.lock().unwrap();
    assert_eq!(keys.len(), 6);
    let unique: std::collections::HashSet<_> = keys.iter().collect();
    assert_eq!(unique.len(), 6, "hook identity reused");
    drop(keys);
    assert_eq!(rt.replay().await, 0);
}
async fn continuations(rt: Runtime) {
    let s = rt.services();
    let a = running(&s, "continuation").await;
    let mut snapshots = vec![(a.clone(), "wrong-owner"), (a.clone(), "A")];
    snapshots[1].0.execution_epoch = 0;
    let mut terminal = a.clone();
    terminal.state = JobState::Succeeded;
    snapshots.push((terminal, "A"));
    for (snapshot, owner) in snapshots {
        let ctx = context(&snapshot, owner, s.clone());
        assert!(ctx.execution_grant.is_none());
        assert!(ctx.continue_with(Child).send().await.is_err());
        assert!(ctx.enqueue(Child).await.is_err());
    }
    let mut ctx = context(&a, "A", s.clone());
    ctx.execution_grant = None;
    assert!(ctx.continue_with(Child).send().await.is_err());
    let ctx = context(&a, "A", s.clone());
    assert!(
        ctx.continue_with(Child)
            .execution_grant(None)
            .send()
            .await
            .is_err(),
        "optional setter downgraded worker to producer"
    );
    assert!(
        s.continuation_store
            .list_pending_by_parent(&a.id)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        s.job_store
            .list_by_state(JobState::Enqueued)
            .await
            .unwrap()
            .is_empty()
    );
    s.job_store.commit_execution(&a, success()).await.unwrap();
    let receipt = ContinuationBuilder::new(
        a.id,
        Child,
        s.clock.clone(),
        s.id_generator.clone(),
        s.job_store.clone(),
        s.continuation_store.clone(),
    )
    .send()
    .await
    .unwrap();
    assert!(
        receipt.child_job_id.is_some(),
        "producer API remains supported for terminal parents"
    );
}
async fn wake_before_running(rt: Runtime) {
    let s = rt.services();
    s.job_store.insert(job("leased-wake")).await.unwrap();
    let a = s
        .job_store
        .lease_due("q", "A", Utc::now(), 120, &WorkerCapabilities::any())
        .await
        .unwrap()
        .unwrap();
    let ctx = context(&a, "A", s.clone());
    assert!(ctx.wait_for::<Signal>().await.is_err());
    runtime_job_dx::signal_event(
        s.job_store.as_ref(),
        s.wait_store.as_ref(),
        s.clock.as_ref(),
        a.id.clone(),
        Signal,
    )
    .await
    .unwrap();
    let running = transition::running_snapshot(s.job_store.as_ref(), a)
        .await
        .unwrap()
        .unwrap();
    let ctx = context(&running, "A", s.clone());
    ctx.heartbeat().await.unwrap();
    assert_eq!(
        s.job_store
            .commit_execution(&running, deferred())
            .await
            .unwrap(),
        stasis::domain::runtime::execution::CommitResult::Committed
    );
    assert!(
        s.job_store
            .get(&running.id)
            .await
            .unwrap()
            .unwrap()
            .scheduled_at
            <= Utc::now()
    );
}
async fn old_hook_preserves_successor_wait(rt: Runtime) {
    let s = rt.services();
    s.job_store.insert(job("hook-wait")).await.unwrap();
    s.job_store.cancel_execution("hook-wait").await.unwrap();
    s.job_store.delete("hook-wait").await.unwrap();
    let b = running(&s, "hook-wait").await;
    let ctx = context(&b, "A", s.clone());
    assert!(ctx.wait_for::<Signal>().await.is_err());
    assert_eq!(rt.replay().await, 1);
    runtime_job_dx::signal_event(
        s.job_store.as_ref(),
        s.wait_store.as_ref(),
        s.clock.as_ref(),
        b.id.clone(),
        Signal,
    )
    .await
    .unwrap();
    ctx.wait_for::<Signal>()
        .await
        .expect("old hook must not cancel successor wait");
}
#[tokio::test]
async fn memory_wakeup_before_running() {
    wake_before_running(Runtime::memory()).await;
}
#[tokio::test]
async fn memory_old_hook_preserves_successor_wait() {
    old_hook_preserves_successor_wait(Runtime::memory()).await;
}
#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn surreal_wakeup_before_running() {
    wake_before_running(Runtime::surreal().await).await;
}
#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn surreal_old_hook_preserves_successor_wait() {
    old_hook_preserves_successor_wait(Runtime::surreal().await).await;
}
#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn actual_kv_reopen_preserves_control_incarnations_and_signal_receipt() {
    use stasis::application::runtime::surreal_runtime::SurrealRuntime;
    async fn open(endpoint: &str) -> surrealdb::Surreal<surrealdb::engine::any::Any> {
        let db = surrealdb::Surreal::<surrealdb::engine::any::Any>::init();
        db.connect(endpoint).await.unwrap();
        db.use_ns("correction-kv").use_db("reopen").await.unwrap();
        db
    }
    fn incarnation(j: &Job) -> u64 {
        serde_json::to_value(j)
            .unwrap()
            .get("incarnation")
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
    }
    let root = std::env::temp_dir().join("stasis-store-runtime-safety-regressions")
        .join(format!(
            "correction-kv-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
    std::fs::create_dir_all(&root).unwrap();
    let endpoint = format!("surrealkv://{}", root.display());
    let (a, outcome, last_incarnation) = {
        let rt = SurrealRuntime::new(open(&endpoint).await);
        let s = rt.job_context_services();
        let mut last = 0;
        for kind in ["cancel", "fail"] {
            for _ in 0..2 {
                let id = format!("kv-{kind}");
                s.job_store.insert(job(&id)).await.unwrap();
                let j = s.job_store.get(&id).await.unwrap().unwrap();
                assert_eq!(j.execution_epoch, 0);
                assert!(incarnation(&j) > last);
                last = incarnation(&j);
                // Incarnations are per logical job, not global. Reset cross-kind comparison.
                if kind == "cancel" {
                    s.job_store.cancel_execution(&id).await.unwrap();
                } else {
                    s.job_store.fail_execution(&id).await.unwrap();
                }
                s.job_store.delete(&id).await.unwrap();
            }
            last = 0;
        }
        s.job_store.insert(job("kv-new")).await.unwrap();
        let last = incarnation(&s.job_store.get("kv-new").await.unwrap().unwrap());
        s.job_store.cancel_execution("kv-new").await.unwrap();
        s.job_store.delete("kv-new").await.unwrap();
        let a = running(&s, "kv-signal").await;
        let ctx = context(&a, "A", s.clone());
        assert!(ctx.wait_for::<Signal>().await.is_err());
        runtime_job_dx::signal_event(
            s.job_store.as_ref(),
            s.wait_store.as_ref(),
            s.clock.as_ref(),
            a.id.clone(),
            Signal,
        )
        .await
        .unwrap();
        let outcome = deferred();
        s.job_store
            .commit_execution(&a, outcome.clone())
            .await
            .unwrap();
        (a, outcome, last)
    };
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    {
        let rt = SurrealRuntime::new(open(&endpoint).await);
        let s = rt.job_context_services();
        let current = s.job_store.get(&a.id).await.unwrap().unwrap();
        assert!(current.scheduled_at <= Utc::now());
        let receipt = s
            .job_store
            .execution_receipt(&a.id, a.execution_epoch)
            .await
            .unwrap()
            .unwrap();
        let published: Job = serde_json::from_str(&receipt.job_json).unwrap();
        assert_eq!(published.scheduled_at, current.scheduled_at);
        assert_eq!(
            s.job_store.commit_execution(&a, outcome).await.unwrap(),
            stasis::domain::runtime::execution::CommitResult::AlreadyCommittedSameDigest
        );
        assert_eq!(s.outbox_store.list_pending(100).await.unwrap().len(), 6);
        let hooks = Arc::new(Mutex::new(Vec::new()));
        rt.register_handler(Hook(hooks.clone())).unwrap();
        assert_eq!(rt.replay_lifecycle().await.unwrap(), 6);
        assert_eq!(
            hooks
                .lock()
                .unwrap()
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            6
        );
        s.job_store.insert(job("kv-new")).await.unwrap();
        let fresh = s.job_store.get("kv-new").await.unwrap().unwrap();
        assert!(incarnation(&fresh) > last_incarnation);
        s.job_store.cancel_execution("kv-new").await.unwrap();
        assert_eq!(rt.replay_lifecycle().await.unwrap(), 1);
        assert_eq!(
            hooks
                .lock()
                .unwrap()
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            7
        );
    }
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    std::fs::remove_dir_all(root).unwrap();
}
async fn child_identity(rt: Runtime) {
    let s = rt.services();
    let a = running(&s, "child-incarnation").await;
    let ctx = context(&a, "A", s.clone());
    let id = ctx.enqueue(Child).await.unwrap();
    s.job_store.commit_execution(&a, success()).await.unwrap();
    let child = s.job_store.get(&id).await.unwrap().unwrap();
    let data = serde_json::to_value(&child).unwrap();
    assert!(
        data.get("incarnation")
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
            > 0,
        "staged child must receive native incarnation at materialization"
    );
    let mut forged = data;
    forged["incarnation"] = serde_json::json!(0);
    let forged: Job = serde_json::from_value(forged).unwrap();
    assert!(
        s.job_store.save(forged).await.is_err(),
        "generic save rewrote immutable identity"
    );
    assert_eq!(
        serde_json::to_value(s.job_store.get(&id).await.unwrap().unwrap()).unwrap()["incarnation"],
        serde_json::to_value(child).unwrap()["incarnation"]
    );
}
#[tokio::test]
async fn memory_child_identity_is_native_immutable() {
    child_identity(Runtime::memory()).await;
}
#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn surreal_child_identity_is_native_immutable() {
    child_identity(Runtime::surreal().await).await;
}
#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn surreal_legacy_never_leased_incarnation_migration_is_idempotent() {
    let db = surrealdb::Surreal::<surrealdb::engine::any::Any>::init();
    db.connect("mem://").await.unwrap();
    db.use_ns("legacy").use_db("correction").await.unwrap();
    let rt = stasis::application::runtime::surreal_runtime::SurrealRuntime::new(db.clone());
    let s = rt.job_context_services();
    s.job_store
        .insert(job("legacy-never-leased"))
        .await
        .unwrap();
    db.query("UPDATE job SET incarnation = NONE WHERE job_id = 'legacy-never-leased'")
        .await
        .unwrap()
        .check()
        .unwrap();
    rt.job_store.initialize_execution_schema().await.unwrap();
    let migrated = s
        .job_store
        .get("legacy-never-leased")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(migrated.execution_epoch, 0);
    let incarnation = serde_json::to_value(migrated).unwrap()["incarnation"]
        .as_u64()
        .unwrap();
    assert!(incarnation > 0);
    rt.job_store.initialize_execution_schema().await.unwrap();
    assert_eq!(
        serde_json::to_value(
            s.job_store
                .get("legacy-never-leased")
                .await
                .unwrap()
                .unwrap()
        )
        .unwrap()["incarnation"],
        incarnation
    );
    s.job_store
        .cancel_execution("legacy-never-leased")
        .await
        .unwrap();
    s.job_store.delete("legacy-never-leased").await.unwrap();
    s.job_store
        .insert(job("legacy-never-leased"))
        .await
        .unwrap();
    assert!(
        serde_json::to_value(
            s.job_store
                .get("legacy-never-leased")
                .await
                .unwrap()
                .unwrap()
        )
        .unwrap()["incarnation"]
            .as_u64()
            .unwrap()
            > incarnation
    );
    s.job_store
        .cancel_execution("legacy-never-leased")
        .await
        .unwrap();
    assert_eq!(rt.replay_lifecycle().await.unwrap(), 2);
}
#[tokio::test]
async fn memory_reload_same_owner_ordinary() {
    reload_case(Runtime::memory(), "A", false).await;
}
#[tokio::test]
async fn memory_reload_same_owner_typed() {
    reload_case(Runtime::memory(), "A", true).await;
}
#[tokio::test]
async fn memory_reload_different_owner_ordinary() {
    reload_case(Runtime::memory(), "B", false).await;
}
#[tokio::test]
async fn memory_reload_different_owner_typed() {
    reload_case(Runtime::memory(), "B", true).await;
}
#[tokio::test]
async fn memory_signal_barrier_matrix() {
    signal_matrix(Runtime::memory()).await;
}
#[tokio::test]
async fn memory_control_incarnations() {
    controls(Runtime::memory()).await;
}
#[tokio::test]
async fn memory_continuations_fail_closed() {
    continuations(Runtime::memory()).await;
}
#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn surreal_reload_same_owner_ordinary() {
    reload_case(Runtime::surreal().await, "A", false).await;
}
#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn surreal_reload_same_owner_typed() {
    reload_case(Runtime::surreal().await, "A", true).await;
}
#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn surreal_reload_different_owner_ordinary() {
    reload_case(Runtime::surreal().await, "B", false).await;
}
#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn surreal_reload_different_owner_typed() {
    reload_case(Runtime::surreal().await, "B", true).await;
}
#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn surreal_signal_barrier_matrix() {
    signal_matrix(Runtime::surreal().await).await;
}
#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn surreal_control_incarnations() {
    controls(Runtime::surreal().await).await;
}
#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn surreal_continuations_fail_closed() {
    continuations(Runtime::surreal().await).await;
}

async fn retained_wait_probe(rt: Runtime) {
    let s=rt.services();
    let a=running(&s,"retained-wait").await;
    let c=context(&a,"A",s.clone());
    assert!(matches!(c.wait_for::<Signal>().await,Err(stasis::application::runtime::job_context::JobConsumeError::Deferred{..})));
    s.job_store.cancel_execution(&a.id).await.unwrap();
    assert!(s.job_store.delete(&a.id).await.unwrap());
    let b=running(&s,"retained-wait").await;
    assert!(b.incarnation>a.incarnation);
    let c=context(&b,"A",s.clone());
    let result=c.wait_for::<Signal>().await;
    println!("successor wait error: {:?}",result.as_ref().err());
    assert!(matches!(result,Err(stasis::application::runtime::job_context::JobConsumeError::Deferred{..})),"new incarnation inherited retired cancellation");
}
async fn retained_cont_probe(rt: Runtime) {
    let s=rt.services();
    let a=running(&s,"retained-cont").await;
    let receipt=context(&a,"A",s.clone()).continue_with(Child).send().await.unwrap();
    s.job_store.commit_execution(&a,success()).await.unwrap();
    assert!(s.job_store.delete(&a.id).await.unwrap());
    let b=running(&s,"retained-cont").await;
    s.job_store.fail_execution(&b.id).await.unwrap();
    rt.replay().await;
    let record=s.continuation_store.get(&receipt.continuation_id).await.unwrap().unwrap();
    println!("original success continuation after successor failure: {:?}",record.status);
    assert_ne!(record.status,stasis::domain::runtime::job_continuation::ContinuationStatus::Discarded,"successor failure discarded old successful incarnation continuation");
}
#[tokio::test] async fn review_memory_retained_wait(){retained_wait_probe(Runtime::memory()).await;}
#[tokio::test] async fn review_memory_retained_cont(){retained_cont_probe(Runtime::memory()).await;}
#[cfg(feature="surreal-native")] #[tokio::test] async fn review_surreal_retained_wait(){retained_wait_probe(Runtime::surreal().await).await;}
#[cfg(feature="surreal-native")] #[tokio::test] async fn review_surreal_retained_cont(){retained_cont_probe(Runtime::surreal().await).await;}
async fn progress_receipt_probe(rt:Runtime){
 let s=rt.services(); let a=running(&s,"progress-receipt").await;
 context(&a,"A",s.clone()).progress("final progress").await.unwrap();
 s.job_store.commit_execution(&a,success()).await.unwrap();
 let actual=s.job_store.get(&a.id).await.unwrap().unwrap();
 let receipt=s.job_store.execution_receipt(&a.id,a.execution_epoch).await.unwrap().unwrap();
 let retained:Job=serde_json::from_str(&receipt.job_json).unwrap();
 println!("native progress {:?}, receipt progress {:?}",actual.progress_json,retained.progress_json);
 assert_eq!(actual.progress_json,retained.progress_json,"receipt snapshot differs from native committed job");
}
#[tokio::test] async fn review_memory_progress_receipt(){progress_receipt_probe(Runtime::memory()).await;}
#[cfg(feature="surreal-native")] #[tokio::test] async fn review_surreal_progress_receipt(){progress_receipt_probe(Runtime::surreal().await).await;}
async fn staged_authority_probe(rt:Runtime){
 let s=rt.services(); let a=running(&s,"stage-authority").await;
 let mut child=job("caller-granted-child"); child.queue="children".into();
 child.state=JobState::Running; child.execution_epoch=500; child.version=7;
 child.lease_owner=Some("caller-picked-owner".into()); child.lease_expires_at=Some(Utc::now()+Duration::seconds(120));
 let staged=s.job_store.stage_child(&ExecutionGrant::from_job(&a).unwrap(),child).await;
 if staged.is_err(){return;}
 s.job_store.commit_execution(&a,success()).await.unwrap();
 let current=s.job_store.get("caller-granted-child").await.unwrap().unwrap();
 println!("materialized child state {:?} epoch {} owner {:?}",current.state,current.execution_epoch,current.lease_owner);
 let committed=s.job_store.commit_execution(&current,success()).await.unwrap();
 println!("caller-issued child grant outcome: {committed:?}");
 assert_ne!(committed,stasis::domain::runtime::execution::CommitResult::Committed,"stage_child minted native execution authority without native acquisition");
}
#[tokio::test] async fn review_memory_staged_authority(){staged_authority_probe(Runtime::memory()).await;}
#[cfg(feature="surreal-native")] #[tokio::test] async fn review_surreal_staged_authority(){staged_authority_probe(Runtime::surreal().await).await;}

}; }
