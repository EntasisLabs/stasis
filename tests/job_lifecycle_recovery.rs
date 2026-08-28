use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::Surreal;
use surrealdb::engine::any::Any;

use stasis::application::runtime::in_memory_runtime::{
    InMemoryRuntime, JobExecutionOutcome, JobHandler,
};
use stasis::application::runtime::job_context::{JobContext, JobResult};
use stasis::application::runtime::job_lifecycle::{JobLifecycleEvent, STALE_LEASE_MESSAGE};
use stasis::application::runtime::surreal_runtime::SurrealRuntime;
use stasis::application::runtime::typed_job::JobConsumer;
use stasis::domain::runtime::durable_wait::DurableWaitStatus;
use stasis::domain::runtime::job::{BackoffPolicy, Job, JobState, NewJob};
use stasis::domain::runtime::outbox::RuntimeEventType;
use stasis::domain::runtime::typed_contract::{RetryPolicy, StasisEvent, StasisJob};
use stasis::ports::outbound::runtime::durable_wait_store::DurableWaitStore;
use stasis::ports::outbound::runtime::job_store::JobStore;
use stasis::sdk::runtime_sdk::RuntimeSdk;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct PrepareReplica {
    replica_id: String,
}

impl StasisJob for PrepareReplica {
    const NAME: &'static str = "prepare_replica";
    const VERSION: u32 = 1;
    type Output = ();
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct ReplicaReady {
    replica_id: String,
}

impl StasisEvent for ReplicaReady {
    const NAME: &'static str = "replica_ready";
    const VERSION: u32 = 1;
}

#[derive(Clone)]
struct RecordingConsumer {
    events: Arc<Mutex<Vec<JobLifecycleEvent>>>,
    mode: RecordMode,
}

#[derive(Clone, Copy)]
enum RecordMode {
    Success,
    Wait,
}

#[async_trait]
impl JobConsumer<PrepareReplica> for RecordingConsumer {
    async fn consume(&self, job: PrepareReplica, ctx: JobContext) -> JobResult<()> {
        match self.mode {
            RecordMode::Success => Ok(()),
            RecordMode::Wait => {
                let _ready = ctx
                    .wait_for::<ReplicaReady>()
                    .correlated_by(&job.replica_id)
                    .timeout(Duration::from_secs(30))
                    .await?;
                Ok(())
            }
        }
    }

    async fn on_lifecycle(
        &self,
        _job: &Job,
        event: &JobLifecycleEvent,
    ) -> stasis::domain::errors::Result<()> {
        self.events.lock().expect("events lock").push(event.clone());
        Ok(())
    }
}

struct RetryOnceHandler {
    events: Arc<Mutex<Vec<JobLifecycleEvent>>>,
}

#[async_trait]
impl JobHandler for RetryOnceHandler {
    fn job_type(&self) -> &'static str {
        "retry.once"
    }

    async fn execute(&self, _job: &Job) -> stasis::domain::errors::Result<JobExecutionOutcome> {
        Ok(JobExecutionOutcome::RetryableFailure {
            message: "transient".into(),
            execution_id: None,
            diagnostics: None,
        })
    }

    async fn on_lifecycle(
        &self,
        _job: &Job,
        event: &JobLifecycleEvent,
    ) -> stasis::domain::errors::Result<()> {
        self.events.lock().expect("events lock").push(event.clone());
        Ok(())
    }
}

enum TestRuntime {
    Memory(InMemoryRuntime),
    Surreal(SurrealRuntime),
}

impl TestRuntime {
    fn memory() -> Self {
        Self::Memory(InMemoryRuntime::new())
    }

    async fn surreal(db_name: &str) -> Self {
        let db = Surreal::<Any>::init();
        db.connect("mem://")
            .await
            .expect("surreal mem should initialize");
        db.use_ns("stasis")
            .use_db(db_name)
            .await
            .expect("namespace and db should be selected");
        Self::Surreal(SurrealRuntime::new(db))
    }

    fn register_recording(&self, consumer: RecordingConsumer) {
        match self {
            Self::Memory(rt) => rt.register_consumer(consumer).unwrap(),
            Self::Surreal(rt) => rt.register_consumer(consumer).unwrap(),
        }
    }

    fn register_retry(&self, handler: RetryOnceHandler) {
        match self {
            Self::Memory(rt) => rt.register_handler(handler).unwrap(),
            Self::Surreal(rt) => rt.register_handler(handler).unwrap(),
        }
    }

    async fn enqueue_prepare(&self, replica_id: &str) -> String {
        let payload = PrepareReplica {
            replica_id: replica_id.to_string(),
        };
        match self {
            Self::Memory(rt) => rt
                .enqueue_job(payload)
                .queue("replicas")
                .idempotency_key(format!("idem-{replica_id}"))
                .retry(RetryPolicy::exponential(8))
                .send()
                .await
                .expect("enqueue"),
            Self::Surreal(rt) => rt
                .enqueue_job(payload)
                .queue("replicas")
                .idempotency_key(format!("idem-{replica_id}"))
                .retry(RetryPolicy::exponential(8))
                .send()
                .await
                .expect("enqueue"),
        }
    }

    async fn enqueue_raw(&self, job: NewJob) {
        match self {
            Self::Memory(rt) => rt.enqueue(job).await.unwrap(),
            Self::Surreal(rt) => rt.enqueue(job).await.unwrap(),
        }
    }

    async fn process(&self) -> Option<String> {
        match self {
            Self::Memory(rt) => rt
                .process_once_now("replicas", "worker-1")
                .await
                .expect("process"),
            Self::Surreal(rt) => rt
                .process_once_now("replicas", "worker-1")
                .await
                .expect("process"),
        }
    }

    async fn process_queue(&self, queue: &str) -> Option<String> {
        match self {
            Self::Memory(rt) => rt
                .process_once_now(queue, "worker-1")
                .await
                .expect("process"),
            Self::Surreal(rt) => rt
                .process_once_now(queue, "worker-1")
                .await
                .expect("process"),
        }
    }

    async fn get_job(&self, id: &str) -> Option<Job> {
        match self {
            Self::Memory(rt) => rt.job_store.get(id).await.unwrap(),
            Self::Surreal(rt) => rt.job_store.get(id).await.unwrap(),
        }
    }

    async fn save_job(&self, job: Job) {
        match self {
            Self::Memory(rt) => rt.job_store.save(job).await.unwrap(),
            Self::Surreal(rt) => rt.job_store.save(job).await.unwrap(),
        }
    }

    async fn lease(&self, queue: &str, worker: &str, now: chrono::DateTime<Utc>, ttl: i64) -> Job {
        match self {
            Self::Memory(rt) => rt
                .job_store
                .lease_due(queue, worker, now, ttl, &stasis::domain::runtime::placement::WorkerCapabilities::any())
                .await
                .unwrap()
                .expect("lease"),
            Self::Surreal(rt) => rt
                .job_store
                .lease_due(queue, worker, now, ttl, &stasis::domain::runtime::placement::WorkerCapabilities::any())
                .await
                .unwrap()
                .expect("lease"),
        }
    }

    async fn heartbeat(&self, job_id: &str, worker: &str, now: chrono::DateTime<Utc>, ttl: i64) {
        match self {
            Self::Memory(rt) => rt
                .job_store
                .heartbeat(job_id, worker, now, ttl)
                .await
                .unwrap(),
            Self::Surreal(rt) => rt
                .job_store
                .heartbeat(job_id, worker, now, ttl)
                .await
                .unwrap(),
        }
    }

    async fn recover_stale(&self, now: chrono::DateTime<Utc>) -> (usize, usize) {
        let report = match self {
            Self::Memory(rt) => rt.recover_stale(now).await.unwrap(),
            Self::Surreal(rt) => rt.recover_stale(now).await.unwrap(),
        };
        (report.recovered, report.dead_lettered)
    }

    async fn cancel(&self, id: &str) -> bool {
        match self {
            Self::Memory(rt) => rt.cancel(id).await.unwrap(),
            Self::Surreal(rt) => rt.cancel(id).await.unwrap(),
        }
    }

    async fn fail(&self, id: &str) -> bool {
        match self {
            Self::Memory(rt) => rt.fail(id).await.unwrap(),
            Self::Surreal(rt) => rt.fail(id).await.unwrap(),
        }
    }

    async fn delete(&self, id: &str) -> stasis::domain::errors::Result<bool> {
        match self {
            Self::Memory(rt) => rt.delete(id).await,
            Self::Surreal(rt) => rt.delete(id).await,
        }
    }

    async fn pending_waits(
        &self,
        job_id: &str,
    ) -> Vec<stasis::domain::runtime::durable_wait::DurableWaitRecord> {
        match self {
            Self::Memory(rt) => rt.wait_store.list_pending_by_job(job_id).await.unwrap(),
            Self::Surreal(rt) => rt.wait_store.list_pending_by_job(job_id).await.unwrap(),
        }
    }

    async fn get_wait(
        &self,
        wait_id: &str,
    ) -> Option<stasis::domain::runtime::durable_wait::DurableWaitRecord> {
        match self {
            Self::Memory(rt) => rt.wait_store.get_wait(wait_id).await.unwrap(),
            Self::Surreal(rt) => rt.wait_store.get_wait(wait_id).await.unwrap(),
        }
    }

    async fn lineage(&self, id: &str) -> Vec<stasis::domain::runtime::outbox::OutboxEvent> {
        match self {
            Self::Memory(rt) => rt.list_lineage_events(id).await.unwrap(),
            Self::Surreal(rt) => rt.list_lineage_events(id).await.unwrap(),
        }
    }
}

async fn for_each_backend<F, Fut>(name: &str, f: F)
where
    F: Fn(TestRuntime) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    f(TestRuntime::memory()).await;
    f(TestRuntime::surreal(name).await).await;
}

fn raw_job(id: &str, job_type: &str, now: chrono::DateTime<Utc>, max_attempts: u32) -> NewJob {
    NewJob {
        id: id.to_string(),
        queue: "default".to_string(),
        job_type: job_type.to_string(),
        payload_ref: "{}".to_string(),
        priority: 100,
        max_attempts,
        idempotency_key: format!("idem-{id}"),
        correlation_id: format!("corr-{id}"),
        causation_id: "cause-1".to_string(),
        trace_id: "trace-1".to_string(),
        input_provenance: Some(stasis::domain::runtime::provenance::ProvenanceRef::sttp("sttp:in:1".to_string())),
        placement: stasis::domain::runtime::placement::PlacementConstraints::default(),
        scheduled_at: now,
        backoff_policy: BackoffPolicy {
            base_delay_seconds: 0,
            max_delay_seconds: 0,
        },
    }
}

#[tokio::test]
async fn heartbeat_extends_lease_and_protects_long_jobs() {
    for_each_backend("lifecycle_heartbeat", |rt| async move {
        let now = Utc::now();
        rt.enqueue_raw(raw_job("job-hb", "retry.once", now, 3))
            .await;
        let leased = rt.lease("default", "worker-1", now, 5).await;
        assert_eq!(
            leased.lease_expires_at,
            Some(now + ChronoDuration::seconds(5))
        );

        let later = now + ChronoDuration::seconds(3);
        rt.heartbeat("job-hb", "worker-1", later, 30).await;
        let after = rt.get_job("job-hb").await.expect("job");
        assert_eq!(after.heartbeat_at, Some(later));
        assert_eq!(
            after.lease_expires_at,
            Some(later + ChronoDuration::seconds(30))
        );

        let (recovered, dead) = rt.recover_stale(now + ChronoDuration::seconds(10)).await;
        assert_eq!((recovered, dead), (0, 0));
        assert_eq!(rt.get_job("job-hb").await.unwrap().state, JobState::Leased);
    })
    .await;
}

#[tokio::test]
async fn recover_stale_retries_then_dead_letters_crashed_running_job() {
    for_each_backend("lifecycle_crash_running", |rt| async move {
        let now = Utc::now();
        rt.enqueue_raw(raw_job("job-crash", "retry.once", now, 2))
            .await;
        let mut leased = rt.lease("default", "worker-1", now, 1).await;
        leased.state = JobState::Running;
        rt.save_job(leased).await;

        let recover_at = now + ChronoDuration::seconds(2);
        let (recovered, dead) = rt.recover_stale(recover_at).await;
        assert_eq!((recovered, dead), (1, 0));
        let retried = rt.get_job("job-crash").await.unwrap();
        assert_eq!(retried.state, JobState::Enqueued);
        assert_eq!(retried.attempts, 1);
        assert_eq!(retried.last_error.as_deref(), Some(STALE_LEASE_MESSAGE));
        assert!(retried.lease_owner.is_none());

        let mut leased_again = rt.lease("default", "worker-2", recover_at, 1).await;
        leased_again.state = JobState::Running;
        rt.save_job(leased_again).await;

        let (recovered, dead) = rt
            .recover_stale(recover_at + ChronoDuration::seconds(2))
            .await;
        assert_eq!((recovered, dead), (0, 1));
        let dead_job = rt.get_job("job-crash").await.unwrap();
        assert_eq!(dead_job.state, JobState::DeadLetter);
        assert_eq!(dead_job.attempts, 2);
    })
    .await;
}

#[tokio::test]
async fn lifecycle_hooks_fire_for_success_defer_retry_dead_letter_and_cancel() {
    for_each_backend("lifecycle_hooks", |rt| async move {
        let success_events = Arc::new(Mutex::new(Vec::new()));
        rt.register_recording(RecordingConsumer {
            events: success_events.clone(),
            mode: RecordMode::Success,
        });
        let success_id = rt.enqueue_prepare("success").await;
        rt.process().await.expect("processed success");
        assert_eq!(
            success_events.lock().unwrap().as_slice(),
            &[JobLifecycleEvent::Succeeded]
        );
        assert_eq!(
            rt.get_job(&success_id).await.unwrap().state,
            JobState::Succeeded
        );

        let wait_events = Arc::new(Mutex::new(Vec::new()));
        // Re-register wait mode for the same job type (overwrites handler).
        rt.register_recording(RecordingConsumer {
            events: wait_events.clone(),
            mode: RecordMode::Wait,
        });
        let wait_id = rt.enqueue_prepare("wait").await;
        rt.process().await.expect("processed wait");
        {
            let events = wait_events.lock().unwrap();
            assert!(
                matches!(events.as_slice(), [JobLifecycleEvent::Deferred { .. }]),
                "expected defer hook, got {events:?}"
            );
        }
        let waits = rt.pending_waits(&wait_id).await;
        assert_eq!(waits.len(), 1);
        let wait_record_id = waits[0].wait_id.clone();

        assert!(rt.cancel(&wait_id).await);
        assert!(!rt.cancel(&wait_id).await);
        {
            let events = wait_events.lock().unwrap();
            assert!(
                matches!(
                    events.as_slice(),
                    [
                        JobLifecycleEvent::Deferred { .. },
                        JobLifecycleEvent::Canceled { .. }
                    ]
                ),
                "expected defer then cancel, got {events:?}"
            );
        }
        assert!(rt.pending_waits(&wait_id).await.is_empty());
        let completed = rt.get_wait(&wait_record_id).await.expect("wait row");
        assert_eq!(completed.status, DurableWaitStatus::Cancelled);
        let lineage = rt.lineage(&wait_id).await;
        assert!(
            lineage
                .iter()
                .any(|event| event.event.event_type == RuntimeEventType::JobCanceled)
        );

        let retry_events = Arc::new(Mutex::new(Vec::new()));
        rt.register_retry(RetryOnceHandler {
            events: retry_events.clone(),
        });
        let now = Utc::now();
        rt.enqueue_raw(raw_job("job-retry", "retry.once", now, 3))
            .await;
        rt.process_queue("default").await.expect("processed retry");
        {
            let events = retry_events.lock().unwrap();
            assert!(
                matches!(
                    events.as_slice(),
                    [JobLifecycleEvent::RetryScheduled { attempt: 1, .. }]
                ),
                "expected retry hook, got {events:?}"
            );
        }

        let fail_events = Arc::new(Mutex::new(Vec::new()));
        rt.register_retry(RetryOnceHandler {
            events: fail_events.clone(),
        });
        rt.enqueue_raw(raw_job("job-fail-hook", "retry.once", now, 3))
            .await;
        assert!(rt.fail("job-fail-hook").await);
        {
            let events = fail_events.lock().unwrap();
            assert!(
                matches!(
                    events.as_slice(),
                    [JobLifecycleEvent::DeadLettered { message }] if message == "operator fail"
                ),
                "expected dead-letter hook, got {events:?}"
            );
        }
    })
    .await;
}

#[tokio::test]
async fn fail_dead_letters_delete_refuses_running_and_removes_after_cancel() {
    for_each_backend("lifecycle_fail_delete", |rt| async move {
        let now = Utc::now();
        rt.enqueue_raw(raw_job("job-fail", "retry.once", now, 3))
            .await;
        assert!(rt.fail("job-fail").await);
        assert_eq!(
            rt.get_job("job-fail").await.unwrap().state,
            JobState::DeadLetter
        );
        assert!(!rt.fail("job-fail").await);

        rt.enqueue_raw(raw_job("job-running", "retry.once", now, 3))
            .await;
        let mut leased = rt.lease("default", "worker-1", now, 30).await;
        leased.state = JobState::Running;
        rt.save_job(leased).await;
        let err = rt.delete("job-running").await.expect_err("refuse running");
        assert!(
            err.to_string().contains("cancel or fail first"),
            "unexpected error: {err}"
        );
        assert_eq!(
            rt.get_job("job-running").await.unwrap().state,
            JobState::Running
        );

        assert!(rt.cancel("job-running").await);
        assert!(rt.delete("job-running").await.unwrap());
        assert!(rt.get_job("job-running").await.is_none());
    })
    .await;
}

#[tokio::test]
async fn runtime_sdk_exposes_recover_fail_delete_and_replay() {
    let sdk = RuntimeSdk::in_memory().await.expect("sdk");
    let now = Utc::now();
    sdk.enqueue(raw_job("sdk-fail", "missing.handler", now, 3))
        .await
        .unwrap();
    assert!(sdk.fail("sdk-fail").await.unwrap());
    assert!(sdk.replay_dead_letter("sdk-fail").await.unwrap());
    assert!(sdk.cancel("sdk-fail").await.unwrap());
    assert!(sdk.delete("sdk-fail").await.unwrap());
    let report = sdk.recover_stale().await.unwrap();
    assert_eq!(report.recovered, 0);
    assert_eq!(report.dead_lettered, 0);
}
