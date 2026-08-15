use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::json;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;

use stasis::application::runtime::in_memory_runtime::InMemoryRuntime;
use stasis::application::runtime::job_context::{JobConsumeError, JobContext, JobResult};
use stasis::application::runtime::surreal_runtime::SurrealRuntime;
use stasis::application::runtime::typed_job::JobConsumer;
use stasis::domain::runtime::job::{Job, JobState, NewJob};
use stasis::domain::runtime::outbox::RuntimeEventType;
use stasis::domain::runtime::resource_lease::FencingToken;
use stasis::domain::runtime::typed_contract::{RetryPolicy, StasisEvent, StasisJob};
use stasis::ports::outbound::runtime::job_store::JobStore;
use stasis::sdk::runtime_sdk::RuntimeSdk;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct PrepareReplica {
    replica_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct PrepareReplicaOutput {
    replica_id: String,
    child_id: Option<String>,
}

impl StasisJob for PrepareReplica {
    const NAME: &'static str = "prepare_replica";
    const VERSION: u32 = 1;
    type Output = PrepareReplicaOutput;
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct ReplicaReady {
    replica_id: String,
}

impl StasisEvent for ReplicaReady {
    const NAME: &'static str = "replica_ready";
    const VERSION: u32 = 1;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HangJob;

impl StasisJob for HangJob {
    const NAME: &'static str = "hang_until_cancel";
    const VERSION: u32 = 1;
    type Output = ();
}

struct EchoConsumer;

#[async_trait]
impl JobConsumer<PrepareReplica> for EchoConsumer {
    async fn consume(
        &self,
        job: PrepareReplica,
        ctx: JobContext,
    ) -> JobResult<PrepareReplicaOutput> {
        ctx.heartbeat().await?;
        ctx.progress(json!({ "pct": 40 })).await?;
        ctx.publish(ReplicaReady {
            replica_id: job.replica_id.clone(),
        })
        .await?;
        let child_id = ctx
            .enqueue(PrepareReplica {
                replica_id: format!("{}-child", job.replica_id),
            })
            .await?;
        Ok(PrepareReplicaOutput {
            replica_id: job.replica_id,
            child_id: Some(child_id),
        })
    }
}

struct WaitConsumer;

#[async_trait]
impl JobConsumer<PrepareReplica> for WaitConsumer {
    async fn consume(
        &self,
        job: PrepareReplica,
        ctx: JobContext,
    ) -> JobResult<PrepareReplicaOutput> {
        let ready = ctx
            .wait_for::<ReplicaReady>()
            .correlated_by(&job.replica_id)
            .timeout(Duration::from_secs(30))
            .await?;
        Ok(PrepareReplicaOutput {
            replica_id: ready.replica_id,
            child_id: None,
        })
    }
}

struct TimeoutWaitConsumer;

#[async_trait]
impl JobConsumer<PrepareReplica> for TimeoutWaitConsumer {
    async fn consume(
        &self,
        job: PrepareReplica,
        ctx: JobContext,
    ) -> JobResult<PrepareReplicaOutput> {
        let ready = ctx
            .wait_for::<ReplicaReady>()
            .correlated_by(&job.replica_id)
            .timeout(Duration::from_secs(0))
            .await?;
        Ok(PrepareReplicaOutput {
            replica_id: ready.replica_id,
            child_id: None,
        })
    }
}

struct HangConsumer;

#[async_trait]
impl JobConsumer<HangJob> for HangConsumer {
    async fn consume(&self, _job: HangJob, ctx: JobContext) -> JobResult<()> {
        loop {
            if ctx.is_cancelled() {
                return Err(JobConsumeError::Cancelled);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
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

    fn register_echo(&self) {
        match self {
            Self::Memory(rt) => rt.register_consumer(EchoConsumer).unwrap(),
            Self::Surreal(rt) => rt.register_consumer(EchoConsumer).unwrap(),
        }
    }

    fn register_wait(&self) {
        match self {
            Self::Memory(rt) => rt.register_consumer(WaitConsumer).unwrap(),
            Self::Surreal(rt) => rt.register_consumer(WaitConsumer).unwrap(),
        }
    }

    fn register_timeout_wait(&self) {
        match self {
            Self::Memory(rt) => rt.register_consumer(TimeoutWaitConsumer).unwrap(),
            Self::Surreal(rt) => rt.register_consumer(TimeoutWaitConsumer).unwrap(),
        }
    }

    fn register_hang(&self) {
        match self {
            Self::Memory(rt) => rt.register_consumer(HangConsumer).unwrap(),
            Self::Surreal(rt) => rt.register_consumer(HangConsumer).unwrap(),
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

    async fn process_default(&self) -> Option<String> {
        match self {
            Self::Memory(rt) => rt
                .process_once_now("default", "worker-1")
                .await
                .expect("process"),
            Self::Surreal(rt) => rt
                .process_once_now("default", "worker-1")
                .await
                .expect("process"),
        }
    }

    async fn get_job(&self, id: &str) -> Job {
        match self {
            Self::Memory(rt) => rt.job_store.get(id).await.unwrap().unwrap(),
            Self::Surreal(rt) => rt.job_store.get(id).await.unwrap().unwrap(),
        }
    }

    async fn lineage(&self, id: &str) -> Vec<stasis::domain::runtime::outbox::OutboxEvent> {
        match self {
            Self::Memory(rt) => rt.list_lineage_events(id).await.unwrap(),
            Self::Surreal(rt) => rt.list_lineage_events(id).await.unwrap(),
        }
    }

    async fn signal_ready(&self, replica_id: &str) -> bool {
        let event = ReplicaReady {
            replica_id: replica_id.to_string(),
        };
        match self {
            Self::Memory(rt) => rt.signal(replica_id, event).await.unwrap(),
            Self::Surreal(rt) => rt.signal(replica_id, event).await.unwrap(),
        }
    }

    async fn cancel(&self, id: &str) -> bool {
        match self {
            Self::Memory(rt) => rt.cancel(id).await.unwrap(),
            Self::Surreal(rt) => rt.cancel(id).await.unwrap(),
        }
    }

    async fn enqueue_raw(&self, job: NewJob) {
        match self {
            Self::Memory(rt) => rt.enqueue(job).await.unwrap(),
            Self::Surreal(rt) => rt.enqueue(job).await.unwrap(),
        }
    }

    async fn acquire(
        &self,
        resource: &str,
        owner: &str,
        ttl: Duration,
    ) -> stasis::domain::errors::Result<stasis::domain::runtime::resource_lease::ResourceLease>
    {
        match self {
            Self::Memory(rt) => rt.acquire_lease(resource, owner, ttl).await,
            Self::Surreal(rt) => rt.acquire_lease(resource, owner, ttl).await,
        }
    }

    async fn force_acquire(
        &self,
        resource: &str,
        owner: &str,
        ttl: Duration,
    ) -> stasis::domain::runtime::resource_lease::ResourceLease {
        match self {
            Self::Memory(rt) => rt.force_acquire_lease(resource, owner, ttl).await.unwrap(),
            Self::Surreal(rt) => rt.force_acquire_lease(resource, owner, ttl).await.unwrap(),
        }
    }

    async fn renew(
        &self,
        resource: &str,
        owner: &str,
        token: FencingToken,
        ttl: Duration,
    ) -> stasis::domain::errors::Result<stasis::domain::runtime::resource_lease::ResourceLease>
    {
        match self {
            Self::Memory(rt) => rt.renew_lease(resource, owner, token, ttl).await,
            Self::Surreal(rt) => rt.renew_lease(resource, owner, token, ttl).await,
        }
    }

    async fn release(&self, resource: &str, owner: &str, token: FencingToken) -> bool {
        match self {
            Self::Memory(rt) => rt.release_lease(resource, owner, token).await.unwrap(),
            Self::Surreal(rt) => rt.release_lease(resource, owner, token).await.unwrap(),
        }
    }

    async fn transfer(
        &self,
        resource: &str,
        from: &str,
        to: &str,
        token: FencingToken,
        ttl: Duration,
    ) -> stasis::domain::runtime::resource_lease::ResourceLease {
        match self {
            Self::Memory(rt) => rt
                .transfer_lease(resource, from, to, token, ttl)
                .await
                .unwrap(),
            Self::Surreal(rt) => rt
                .transfer_lease(resource, from, to, token, ttl)
                .await
                .unwrap(),
        }
    }

    async fn validate(&self, resource: &str, token: FencingToken) -> bool {
        match self {
            Self::Memory(rt) => rt.validate_fence(resource, token).await.unwrap(),
            Self::Surreal(rt) => rt.validate_fence(resource, token).await.unwrap(),
        }
    }

    async fn watch(
        &self,
        resource: &str,
    ) -> Option<stasis::domain::runtime::resource_lease::ResourceLease> {
        match self {
            Self::Memory(rt) => rt.watch_lease(resource).await.unwrap(),
            Self::Surreal(rt) => rt.watch_lease(resource).await.unwrap(),
        }
    }

    fn clone_runtime(&self) -> Self {
        match self {
            Self::Memory(rt) => Self::Memory(rt.clone()),
            Self::Surreal(rt) => Self::Surreal(rt.clone()),
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

#[tokio::test]
async fn typed_enqueue_round_trip_and_causation() {
    for_each_backend("typed_round_trip", |rt| async move {
        rt.register_echo();
        let id = rt.enqueue_prepare("replica-a").await;
        rt.process().await.expect("processed");
        let job = rt.get_job(&id).await;
        assert_eq!(job.state, JobState::Succeeded);
        assert_eq!(job.job_type, PrepareReplica::NAME);
        assert!(job.progress_json.unwrap().contains("\"pct\":40"));

        let events = rt.lineage(&id).await;
        let published = events
            .iter()
            .find(|event| event.event.event_type == RuntimeEventType::JobPublished)
            .expect("published event");
        assert_eq!(published.event.causation_id, id);
        assert!(
            published
                .event
                .message
                .as_deref()
                .unwrap()
                .starts_with("replica_ready:")
        );

        let children = match &rt {
            TestRuntime::Memory(inner) => inner
                .job_store
                .list_by_state(JobState::Enqueued)
                .await
                .unwrap(),
            TestRuntime::Surreal(inner) => inner
                .job_store
                .list_by_state(JobState::Enqueued)
                .await
                .unwrap(),
        };
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].causation_id, id);
        assert_eq!(children[0].correlation_id, job.correlation_id);
    })
    .await;
}

#[tokio::test]
async fn malformed_typed_payload_is_fatal() {
    for_each_backend("typed_malformed", |rt| async move {
        rt.register_echo();
        let now = chrono::Utc::now();
        rt.enqueue_raw(NewJob {
            id: "job-bad".into(),
            queue: "replicas".into(),
            job_type: PrepareReplica::NAME.into(),
            payload_ref: "not-json".into(),
            priority: 100,
            max_attempts: 3,
            idempotency_key: "idem-bad".into(),
            correlation_id: "corr-bad".into(),
            causation_id: "cause-bad".into(),
            trace_id: "trace-bad".into(),
            sttp_input_node_id: "sttp:in:bad".into(),
            scheduled_at: now,
            backoff_policy: Default::default(),
        })
        .await;
        rt.process().await.expect("processed");
        let job = rt.get_job("job-bad").await;
        assert_eq!(job.state, JobState::DeadLetter);
        assert!(job.last_error.unwrap().contains("malformed typed payload"));
    })
    .await;
}

#[tokio::test]
async fn wait_defers_then_signal_resumes_and_duplicate_is_ignored() {
    for_each_backend("typed_wait_signal", |rt| async move {
        rt.register_wait();
        let id = rt.enqueue_prepare("replica-wait").await;
        rt.process().await.expect("deferred");
        let deferred = rt.get_job(&id).await;
        assert_eq!(deferred.state, JobState::Enqueued);
        assert!(deferred.last_error.unwrap().contains("waiting for signal"));

        assert!(rt.signal_ready("replica-wait").await);
        assert!(
            !rt.signal_ready("replica-wait").await,
            "duplicate signal id"
        );

        rt.process().await.expect("resumed");
        let done = rt.get_job(&id).await;
        assert_eq!(done.state, JobState::Succeeded);
    })
    .await;
}

#[tokio::test]
async fn wait_timeout_fails_without_consuming_retry_budget_then_dead_letters() {
    for_each_backend("typed_wait_timeout", |rt| async move {
        rt.register_timeout_wait();
        let id = rt.enqueue_prepare("replica-timeout").await;
        rt.process().await.expect("first poll");
        let first = rt.get_job(&id).await;
        assert_eq!(first.state, JobState::Enqueued);
        assert_eq!(first.attempts, 0);

        rt.process().await.expect("timeout poll");
        let timed_out = rt.get_job(&id).await;
        assert_eq!(timed_out.state, JobState::DeadLetter);
        assert!(timed_out.last_error.unwrap().contains("timed out"));
    })
    .await;
}

#[tokio::test]
async fn cancel_prevents_deferred_wait_from_resuming() {
    for_each_backend("typed_wait_cancel", |rt| async move {
        rt.register_wait();
        let id = rt.enqueue_prepare("replica-cancel").await;
        rt.process().await.expect("deferred");
        assert!(rt.cancel(&id).await);
        assert!(rt.process().await.is_none());
        let job = rt.get_job(&id).await;
        assert_eq!(job.state, JobState::Canceled);
    })
    .await;
}

#[tokio::test]
async fn cancel_wakes_in_flight_consumer() {
    for_each_backend("typed_inflight_cancel", |rt| async move {
        rt.register_hang();
        let payload = HangJob;
        let id = match &rt {
            TestRuntime::Memory(inner) => inner.enqueue_job(payload).send().await.unwrap(),
            TestRuntime::Surreal(inner) => inner.enqueue_job(payload).send().await.unwrap(),
        };
        let worker = rt.clone_runtime();
        let process = tokio::spawn(async move { worker.process_default().await });
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(rt.cancel(&id).await);
        let processed = tokio::time::timeout(Duration::from_secs(2), process)
            .await
            .expect("process should finish after cancel")
            .expect("join");
        assert_eq!(processed.as_deref(), Some(id.as_str()));
        let job = rt.get_job(&id).await;
        assert_eq!(job.state, JobState::Canceled);
    })
    .await;
}

#[tokio::test]
async fn resource_leases_fence_and_force_acquire() {
    for_each_backend("typed_leases", |rt| async move {
        let ttl = Duration::from_secs(30);
        let first = rt.acquire("shard-1", "owner-a", ttl).await.unwrap();
        assert_eq!(first.generation, 1);
        assert_eq!(first.fencing_token, FencingToken(1));
        assert!(rt.validate("shard-1", first.fencing_token).await);
        assert!(rt.acquire("shard-1", "owner-b", ttl).await.is_err());

        let renewed = rt
            .renew("shard-1", "owner-a", first.fencing_token, ttl)
            .await
            .unwrap();
        assert_eq!(renewed.generation, 1);
        assert!(
            rt.renew("shard-1", "owner-a", FencingToken(99), ttl)
                .await
                .is_err()
        );

        let transferred = rt
            .transfer("shard-1", "owner-a", "owner-b", first.fencing_token, ttl)
            .await;
        assert_eq!(transferred.generation, 2);
        assert_eq!(transferred.owner.0, "owner-b");
        assert!(!rt.validate("shard-1", first.fencing_token).await);
        assert!(rt.validate("shard-1", transferred.fencing_token).await);

        assert!(
            rt.release("shard-1", "owner-b", transferred.fencing_token)
                .await
        );
        assert!(rt.watch("shard-1").await.is_none());

        let again = rt
            .acquire("shard-1", "owner-a", Duration::from_millis(1))
            .await
            .unwrap();
        assert_eq!(again.generation, 1);
        tokio::time::sleep(Duration::from_millis(5)).await;
        let after_expire = rt.acquire("shard-1", "owner-c", ttl).await.unwrap();
        assert_eq!(after_expire.generation, 2);

        let live = rt.acquire("shard-2", "owner-a", ttl).await.unwrap();
        let forced = rt.force_acquire("shard-2", "owner-z", ttl).await;
        assert_eq!(forced.generation, live.generation + 1);
        assert!(!rt.validate("shard-2", live.fencing_token).await);
        assert!(rt.validate("shard-2", forced.fencing_token).await);
    })
    .await;
}

#[tokio::test]
async fn runtime_sdk_exposes_typed_enqueue_and_leases() {
    let sdk = RuntimeSdk::in_memory().await.expect("sdk");
    sdk.register_consumer(EchoConsumer).unwrap();
    let id = sdk
        .enqueue_job(PrepareReplica {
            replica_id: "sdk-1".into(),
        })
        .queue("replicas")
        .send()
        .await
        .unwrap();
    sdk.process_once("replicas", "worker-sdk")
        .await
        .unwrap()
        .expect("processed");

    let lease = sdk
        .acquire_lease("sdk-resource", "sdk-owner", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        sdk.validate_fence("sdk-resource", lease.fencing_token)
            .await
            .unwrap()
    );
    assert!(
        sdk.release_lease("sdk-resource", "sdk-owner", lease.fencing_token)
            .await
            .unwrap()
    );
    let _ = id;
}

#[tokio::test]
async fn runtime_sdk_signal_is_backend_agnostic() {
    let sdk = RuntimeSdk::surreal_mem("stasis", "sdk_signal")
        .await
        .expect("surreal sdk");
    sdk.register_consumer(WaitConsumer).unwrap();
    let id = sdk
        .enqueue_job(PrepareReplica {
            replica_id: "sdk-wait".into(),
        })
        .queue("replicas")
        .send()
        .await
        .unwrap();
    sdk.process_once("replicas", "w").await.unwrap();
    assert!(
        sdk.signal(
            "sdk-wait",
            ReplicaReady {
                replica_id: "sdk-wait".into(),
            },
        )
        .await
        .unwrap()
    );
    sdk.process_once("replicas", "w").await.unwrap();
    let _ = id;
}
