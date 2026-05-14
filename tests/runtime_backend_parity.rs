use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration as StdDuration;

use async_trait::async_trait;
use chrono::{Duration, Utc};
use surrealdb::Surreal;
use surrealdb::engine::local::Mem;
use tokio::sync::Mutex;

use stasis::application::runtime::in_memory_runtime::{InMemoryRuntime, JobExecutionOutcome, JobHandler};
use stasis::application::runtime::grapheme_job_handler::GraphemeJobHandler;
use stasis::application::runtime::surreal_runtime::SurrealRuntime;
use stasis::domain::errors::Result;
use stasis::domain::runtime::job::{BackoffPolicy, Job, JobState, NewJob};
use stasis::domain::runtime::outbox::{OutboxPublishPolicy, OutboxStatus, RuntimeEventType};
use stasis::domain::runtime::recurring::RecurringDefinition;
use stasis::infrastructure::runtime::grapheme_sdk_workflow_engine::{
    GraphemeSdkWorkflowEngine, GraphemeWorkflowGuardrails,
};
use stasis::infrastructure::runtime::tokio_channel_event_publisher::TokioChannelEventPublisher;
use stasis::ports::outbound::runtime::event_publisher::EventPublisher;
use stasis::ports::outbound::runtime::job_attempt_store::JobAttemptStore;
use stasis::ports::outbound::runtime::job_store::JobStore;
use stasis::ports::outbound::runtime::outbox_store::OutboxStore;
use stasis::ports::outbound::runtime::workflow_engine::WorkflowEngine;

struct AlwaysSuccessHandler;

#[async_trait]
impl JobHandler for AlwaysSuccessHandler {
    fn job_type(&self) -> &'static str {
        "test.success"
    }

    async fn execute(&self, _job: &Job) -> Result<JobExecutionOutcome> {
        Ok(JobExecutionOutcome::Success {
            sttp_output_node_id: "sttp:out:success".to_string(),
            execution_id: None,
            diagnostics: None,
        })
    }
}

struct ParentSuccessHandler;

#[async_trait]
impl JobHandler for ParentSuccessHandler {
    fn job_type(&self) -> &'static str {
        "test.parent"
    }

    async fn execute(&self, _job: &Job) -> Result<JobExecutionOutcome> {
        Ok(JobExecutionOutcome::Success {
            sttp_output_node_id: "sttp:out:parent".to_string(),
            execution_id: None,
            diagnostics: None,
        })
    }
}

struct ChildSuccessHandler;

#[async_trait]
impl JobHandler for ChildSuccessHandler {
    fn job_type(&self) -> &'static str {
        "test.child"
    }

    async fn execute(&self, _job: &Job) -> Result<JobExecutionOutcome> {
        Ok(JobExecutionOutcome::Success {
            sttp_output_node_id: "sttp:out:child".to_string(),
            execution_id: None,
            diagnostics: None,
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

#[derive(Clone)]
struct FlakyPublisher {
    failures_before_success: usize,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl EventPublisher for FlakyPublisher {
    async fn publish(&self, _event: &stasis::domain::runtime::outbox::OutboxEvent) -> Result<()> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call <= self.failures_before_success {
            return Err(stasis::domain::errors::StasisError::PortFailure(
                "synthetic publish failure".to_string(),
            ));
        }

        Ok(())
    }
}

struct FatalThenSuccessHandler {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl JobHandler for FatalThenSuccessHandler {
    fn job_type(&self) -> &'static str {
        "test.fatal_then_success"
    }

    async fn execute(&self, _job: &Job) -> Result<JobExecutionOutcome> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call == 1 {
            return Ok(JobExecutionOutcome::FatalFailure {
                message: "first run fails".to_string(),
                execution_id: None,
                diagnostics: None,
            });
        }

        Ok(JobExecutionOutcome::Success {
            sttp_output_node_id: "sttp:out:replayed".to_string(),
            execution_id: None,
            diagnostics: None,
        })
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
            cron_expr: "0/1 * * * * * *".to_string(),
            timezone: "UTC".to_string(),
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

#[tokio::test]
async fn surreal_runtime_replays_dead_letter_and_retries_outbox_publish() {
    let db = Surreal::new::<Mem>(())
        .await
        .expect("surreal mem should initialize");
    db.use_ns("test")
        .use_db("runtime_retry_replay")
        .await
        .expect("namespace and db should be selected");

    let runtime = SurrealRuntime::new(db);
    runtime
        .configure_outbox_publish_policy(OutboxPublishPolicy {
            max_attempts: 3,
            base_delay_seconds: 1,
            max_delay_seconds: 8,
        })
        .expect("policy should configure");

    let handler_calls = Arc::new(AtomicUsize::new(0));
    runtime
        .register_handler(FatalThenSuccessHandler {
            calls: handler_calls.clone(),
        })
        .expect("handler should register");

    let publisher_calls = Arc::new(AtomicUsize::new(0));
    runtime
        .register_event_publisher(FlakyPublisher {
            failures_before_success: 1,
            calls: publisher_calls.clone(),
        })
        .expect("publisher should register");

    let now = Utc::now();
    runtime
        .enqueue(build_new_job("test.fatal_then_success", now))
        .await
        .expect("job should enqueue");

    runtime
        .process_once("default", "worker-1", now)
        .await
        .expect("first processing should complete");

    let dead_lettered = runtime
        .job_store
        .get("job-test.fatal_then_success")
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(dead_lettered.state, JobState::DeadLetter);

    let replayed = runtime
        .replay_dead_letter("job-test.fatal_then_success", now + Duration::seconds(1))
        .await
        .expect("replay should succeed");
    assert!(replayed);

    runtime
        .process_once("default", "worker-2", now + Duration::seconds(1))
        .await
        .expect("second processing should complete");

    let succeeded = runtime
        .job_store
        .get("job-test.fatal_then_success")
        .await
        .expect("job get should succeed")
        .expect("job should exist");
    assert_eq!(succeeded.state, JobState::Succeeded);

    let first_publish = runtime
        .publish_pending_events(10, now + Duration::seconds(1))
        .await
        .expect("first publish should complete");
    assert!(first_publish >= 1);

    let pending = runtime
        .outbox_store
        .list_pending(10)
        .await
        .expect("pending list should succeed");
    assert_eq!(pending.len(), 1);
    assert!(pending.iter().all(|evt| evt.publish_attempts >= 1));

    let second_publish = runtime
        .publish_pending_events(10, now + Duration::seconds(2))
        .await
        .expect("second publish should complete");
    assert!(second_publish >= 1);
    assert!(publisher_calls.load(Ordering::SeqCst) >= 2);
}

#[tokio::test]
async fn tokio_channel_publisher_adapter_receives_outbox_events() {
    let runtime = InMemoryRuntime::new();
    runtime
        .register_handler(AlwaysSuccessHandler)
        .expect("handler should register");

    let (publisher, rx) = TokioChannelEventPublisher::channel();
    runtime
        .register_event_publisher(publisher)
        .expect("publisher should register");

    let shared_rx = Arc::new(Mutex::new(rx));
    let now = Utc::now();
    runtime
        .enqueue(build_new_job("test.success", now))
        .await
        .expect("job should enqueue");
    runtime
        .process_once("default", "worker-1", now)
        .await
        .expect("processing should succeed");
    runtime
        .publish_pending_events(10, now + Duration::seconds(1))
        .await
        .expect("publish pending should succeed");

    let mut guard = shared_rx.lock().await;
    let received = guard
        .recv()
        .await
        .expect("publisher channel should receive event");
    assert_eq!(received.event.event_type, RuntimeEventType::JobSucceeded);
}

#[tokio::test]
async fn surreal_job_leasing_allows_only_one_winner_under_contention() {
    let db = Surreal::new::<Mem>(())
        .await
        .expect("surreal mem should initialize");
    db.use_ns("test")
        .use_db("runtime_lease_contention")
        .await
        .expect("namespace and db should be selected");

    let runtime = SurrealRuntime::new(db);
    let now = Utc::now();
    runtime
        .enqueue(build_new_job("test.success", now))
        .await
        .expect("job should enqueue");

    let (a, b) = tokio::join!(
        runtime.job_store.lease_due("default", "worker-a", now, 30),
        runtime.job_store.lease_due("default", "worker-b", now, 30)
    );

    let leased_a = a.expect("lease call a should succeed");
    let leased_b = b.expect("lease call b should succeed");

    let winners = [leased_a, leased_b].iter().filter(|job| job.is_some()).count();
    assert_eq!(winners, 1);
}

#[tokio::test]
async fn surreal_job_lease_expiry_allows_recovery_by_another_worker() {
    let db = Surreal::new::<Mem>(())
        .await
        .expect("surreal mem should initialize");
    db.use_ns("test")
        .use_db("runtime_lease_recovery")
        .await
        .expect("namespace and db should be selected");

    let runtime = SurrealRuntime::new(db);
    let now = Utc::now();
    runtime
        .enqueue(build_new_job("test.success", now))
        .await
        .expect("job should enqueue");

    let first = runtime
        .job_store
        .lease_due("default", "worker-1", now, 1)
        .await
        .expect("first lease should succeed")
        .expect("first lease should acquire job");
    assert_eq!(first.lease_owner.as_deref(), Some("worker-1"));

    let during_lease = runtime
        .job_store
        .lease_due("default", "worker-2", now, 1)
        .await
        .expect("second lease call should succeed");
    assert!(during_lease.is_none());

    let recovered = runtime
        .job_store
        .lease_due("default", "worker-2", now + Duration::seconds(2), 30)
        .await
        .expect("recovery lease should succeed")
        .expect("recovery lease should acquire job");

    assert_eq!(recovered.lease_owner.as_deref(), Some("worker-2"));
}

#[tokio::test]
async fn in_memory_event_driven_continuation_job_executes_end_to_end() {
    let runtime = InMemoryRuntime::new();
    runtime
        .register_handler(ParentSuccessHandler)
        .expect("parent handler should register");
    runtime
        .register_handler(ChildSuccessHandler)
        .expect("child handler should register");

    let (publisher, mut rx) = TokioChannelEventPublisher::channel();
    runtime
        .register_event_publisher(publisher)
        .expect("publisher should register");

    let now = Utc::now();
    let parent_job_id = "job-parent-1".to_string();
    runtime
        .enqueue(NewJob {
            id: parent_job_id.clone(),
            queue: "default".to_string(),
            job_type: "test.parent".to_string(),
            payload_ref: "payload:parent".to_string(),
            priority: 100,
            max_attempts: 3,
            idempotency_key: "idem-parent-1".to_string(),
            correlation_id: "corr-parent-1".to_string(),
            causation_id: "cause-parent-1".to_string(),
            trace_id: "trace-parent-1".to_string(),
            sttp_input_node_id: "sttp:in:parent".to_string(),
            scheduled_at: now,
            backoff_policy: BackoffPolicy {
                base_delay_seconds: 1,
                max_delay_seconds: 8,
            },
        })
        .await
        .expect("parent job should enqueue");

    runtime
        .process_once("default", "worker-parent", now)
        .await
        .expect("parent processing should succeed");

    runtime
        .publish_pending_events(10, now + Duration::seconds(1))
        .await
        .expect("outbox publish should succeed");

    let evt = rx.recv().await.expect("should receive runtime event");
    assert_eq!(evt.event.event_type, RuntimeEventType::JobSucceeded);
    assert_eq!(evt.event.job_id, parent_job_id);

    let parent_output = evt
        .event
        .sttp_output_node_id
        .clone()
        .expect("parent output node id should exist");

    let child_job_id = "job-child-1".to_string();
    runtime
        .enqueue(NewJob {
            id: child_job_id.clone(),
            queue: "default".to_string(),
            job_type: "test.child".to_string(),
            payload_ref: "payload:child".to_string(),
            priority: 100,
            max_attempts: 3,
            idempotency_key: "idem-child-1".to_string(),
            correlation_id: "corr-parent-1".to_string(),
            causation_id: parent_job_id,
            trace_id: "trace-parent-1".to_string(),
            sttp_input_node_id: parent_output,
            scheduled_at: now + Duration::seconds(1),
            backoff_policy: BackoffPolicy {
                base_delay_seconds: 1,
                max_delay_seconds: 8,
            },
        })
        .await
        .expect("child job should enqueue");

    runtime
        .process_once("default", "worker-child", now + Duration::seconds(1))
        .await
        .expect("child processing should succeed");

    let child = runtime
        .job_store
        .get(&child_job_id)
        .await
        .expect("child get should succeed")
        .expect("child should exist");

    assert_eq!(child.state, JobState::Succeeded);
    assert_eq!(child.sttp_input_node_id, "sttp:out:parent");
    assert_eq!(child.correlation_id, "corr-parent-1");
    assert_eq!(child.trace_id, "trace-parent-1");
}

#[tokio::test]
async fn in_memory_grapheme_sdk_workflow_job_executes_successfully() {
    let runtime = InMemoryRuntime::new();
    let workflow_engine = Arc::new(GraphemeSdkWorkflowEngine::new());

    runtime
        .register_handler(GraphemeJobHandler::new(workflow_engine))
        .expect("grapheme handler should register");

    let source = r#"import core from "grapheme/core"

query Hello {
    core.echo(message: "hello from stasis grapheme handler") {
        state { current }
    }
}
"#;

    let now = Utc::now();
    let job_id = "job-grapheme-1".to_string();

    runtime
        .enqueue(NewJob {
            id: job_id.clone(),
            queue: "default".to_string(),
            job_type: "workflow.grapheme.run".to_string(),
            payload_ref: format!("grapheme:inline:{}", source),
            priority: 100,
            max_attempts: 3,
            idempotency_key: "idem-grapheme-1".to_string(),
            correlation_id: "corr-grapheme-1".to_string(),
            causation_id: "cause-grapheme-1".to_string(),
            trace_id: "trace-grapheme-1".to_string(),
            sttp_input_node_id: "sttp:in:grapheme:1".to_string(),
            scheduled_at: now,
            backoff_policy: BackoffPolicy {
                base_delay_seconds: 1,
                max_delay_seconds: 8,
            },
        })
        .await
        .expect("grapheme job should enqueue");

    runtime
        .process_once("default", "worker-grapheme", now)
        .await
        .expect("grapheme processing should succeed");

    let job = runtime
        .job_store
        .get(&job_id)
        .await
        .expect("job get should succeed")
        .expect("job should exist");

    assert_eq!(job.state, JobState::Succeeded);
    assert!(
        job.sttp_output_node_id
            .as_deref()
            .unwrap_or_default()
            .starts_with("sttp:grapheme:")
    );

    let attempts = runtime
        .job_attempt_store
        .list_by_job_id(&job_id)
        .await
        .expect("attempt list should succeed");
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].attempt_number, 1);
    assert!(
        attempts[0]
            .execution_id
            .as_deref()
            .unwrap_or_default()
            .starts_with("grapheme:")
    );

    let pending = runtime
        .outbox_store
        .list_pending(10)
        .await
        .expect("pending list should succeed");
    let event = pending
        .iter()
        .find(|evt| evt.event.job_id == job_id)
        .expect("outbox event should exist for grapheme job");
    assert!(
        event
            .event
            .execution_id
            .as_deref()
            .unwrap_or_default()
            .starts_with("grapheme:")
    );
}

#[tokio::test]
async fn grapheme_sdk_rejects_non_allowlisted_import() {
    let engine = GraphemeSdkWorkflowEngine::new();
    let source = r#"import sql from "grapheme/sql"

query Run {
  sql.query(connection: "local", sql: "select 1") {
    rows
  }
}
"#;

    let err = engine
        .execute_grapheme_source(source)
        .await
        .expect_err("non-allowlisted import should be rejected");

    assert!(
        err.to_string().contains("not allowlisted"),
        "expected allowlist policy violation, got: {err}"
    );
}

#[tokio::test]
async fn grapheme_sdk_rejects_zero_execution_timeout() {
    let guardrails = GraphemeWorkflowGuardrails {
        execution_timeout: StdDuration::from_millis(0),
        ..GraphemeWorkflowGuardrails::default()
    };
    let engine = GraphemeSdkWorkflowEngine::with_guardrails(guardrails);
    let source = r#"import core from "grapheme/core"

query Hello {
  core.echo(message: "hello") {
    state { current }
  }
}
"#;

    let err = engine
        .execute_grapheme_source(source)
        .await
        .expect_err("zero timeout should reject execution");

    assert!(
        err.to_string().contains("timeout must be greater than 0ms"),
        "expected timeout policy violation, got: {err}"
    );
}
