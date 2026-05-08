use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use chrono::{Duration, Utc};
use surrealdb::Surreal;
use surrealdb::engine::local::Mem;

use stasis::application::runtime::in_memory_runtime::{InMemoryRuntime, JobExecutionOutcome, JobHandler};
use stasis::application::runtime::surreal_runtime::SurrealRuntime;
use stasis::domain::errors::Result;
use stasis::domain::runtime::job::{BackoffPolicy, Job, JobState, NewJob};
use stasis::domain::runtime::outbox::{OutboxStatus, RuntimeEventType};
use stasis::domain::runtime::recurring::RecurringDefinition;
use stasis::ports::outbound::runtime::event_publisher::EventPublisher;
use stasis::ports::outbound::runtime::job_store::JobStore;
use stasis::ports::outbound::runtime::outbox_store::OutboxStore;

struct AlwaysSuccessHandler;

#[async_trait]
impl JobHandler for AlwaysSuccessHandler {
    fn job_type(&self) -> &'static str {
        "test.success"
    }

    async fn execute(&self, _job: &Job) -> Result<JobExecutionOutcome> {
        Ok(JobExecutionOutcome::Success {
            sttp_output_node_id: "sttp:out:success".to_string(),
        })
    }
}

#[derive(Clone)]
struct CountingPublisher {
    count: Arc<AtomicUsize>,
}

#[async_trait]
impl EventPublisher for CountingPublisher {
    async fn publish(&self, _event: &stasis::domain::runtime::outbox::OutboxEvent) -> Result<()> {
        self.count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn build_new_job(job_type: &str, now: chrono::DateTime<Utc>) -> NewJob {
    NewJob {
        id: format!("job-{job_type}"),
        queue: "default".to_string(),
        job_type: job_type.to_string(),
        payload_ref: "payload:ref".to_string(),
        priority: 100,
        max_attempts: 3,
        idempotency_key: format!("idem-{job_type}"),
        correlation_id: "corr-1".to_string(),
        causation_id: "cause-1".to_string(),
        trace_id: "trace-1".to_string(),
        sttp_input_node_id: "sttp:in:1".to_string(),
        scheduled_at: now,
        backoff_policy: BackoffPolicy {
            base_delay_seconds: 1,
            max_delay_seconds: 8,
        },
    }
}

#[tokio::test]
async fn in_memory_runtime_emits_and_publishes_outbox_events() {
    let runtime = InMemoryRuntime::new();
    runtime
        .register_handler(AlwaysSuccessHandler)
        .expect("handler should register");

    let published_count = Arc::new(AtomicUsize::new(0));
    runtime
        .register_event_publisher(CountingPublisher {
            count: published_count.clone(),
        })
        .expect("publisher should register");

    let now = Utc::now();
    runtime
        .enqueue(build_new_job("test.success", now))
        .await
        .expect("job should enqueue");

    runtime
        .process_once("default", "worker-1", now)
        .await
        .expect("processing should succeed");

    let job = runtime
        .job_store
        .get("job-test.success")
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(job.state, JobState::Succeeded);

    let pending = runtime
        .outbox_store
        .list_pending(10)
        .await
        .expect("pending list should succeed");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].status, OutboxStatus::Pending);
    assert_eq!(pending[0].event.event_type, RuntimeEventType::JobSucceeded);

    let published = runtime
        .publish_pending_events(10, now + Duration::seconds(1))
        .await
        .expect("publish pending should succeed");

    assert_eq!(published, 1);
    assert_eq!(published_count.load(Ordering::SeqCst), 1);

    let still_pending = runtime
        .outbox_store
        .list_pending(10)
        .await
        .expect("pending list should succeed");
    assert!(still_pending.is_empty());
}

#[tokio::test]
async fn surreal_runtime_matches_core_flow_and_recurring_materialization() {
    let db = Surreal::new::<Mem>(())
        .await
        .expect("surreal mem should initialize");
    db.use_ns("test")
        .use_db("runtime_backend_parity")
        .await
        .expect("namespace and db should be selected");

    let runtime = SurrealRuntime::new(db);
    runtime
        .register_handler(AlwaysSuccessHandler)
        .expect("handler should register");

    let published_count = Arc::new(AtomicUsize::new(0));
    runtime
        .register_event_publisher(CountingPublisher {
            count: published_count.clone(),
        })
        .expect("publisher should register");

    let now = Utc::now();
    runtime
        .register_recurring(RecurringDefinition {
            id: "recur.scrape".to_string(),
            queue: "default".to_string(),
            job_type: "test.success".to_string(),
            payload_template_ref: "sttp:in:recurring".to_string(),
            interval_seconds: 60,
            jitter_seconds: 0,
            enabled: true,
            max_attempts: 4,
            next_run_at: now,
            last_run_at: None,
            lease_owner: None,
            lease_expires_at: None,
        })
        .await
        .expect("recurring should register");

    let created = runtime
        .materialize_recurring(now, "scheduler-1")
        .await
        .expect("materialization should succeed");
    assert_eq!(created, 1);

    let enqueued = runtime
        .job_store
        .list_by_state(JobState::Enqueued)
        .await
        .expect("list by state should succeed");
    assert_eq!(enqueued.len(), 1);

    runtime
        .process_once("default", "worker-1", now)
        .await
        .expect("processing should succeed");

    let succeeded = runtime
        .job_store
        .list_by_state(JobState::Succeeded)
        .await
        .expect("list by state should succeed");
    assert_eq!(succeeded.len(), 1);

    let pending = runtime
        .outbox_store
        .list_pending(10)
        .await
        .expect("pending list should succeed");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].event.event_type, RuntimeEventType::JobSucceeded);

    let published = runtime
        .publish_pending_events(10, now + Duration::seconds(1))
        .await
        .expect("publish pending should succeed");
    assert_eq!(published, 1);
    assert_eq!(published_count.load(Ordering::SeqCst), 1);
}
