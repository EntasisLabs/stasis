#![cfg(feature = "sqlite")]
//! Failure-boundary and reconstruction tests for the row-level runtime tables.
use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};
use stasis::application::runtime::{job_context::JobContext, sqlite_runtime::SqliteRuntime};
use stasis::domain::{
    errors::{Result, StasisError},
    runtime::{
        execution::{CommitResult, ExecutionGrant, JobExecutionOutcome},
        job::{Job, JobState, NewJob},
        typed_contract::{StasisEvent, StasisJob},
    },
};
use stasis::infrastructure::sqlite::{
    DurableSqliteKernel, RusqliteExecutor, SqlExecutor, SqlRow, SqlValue,
};
use stasis::ports::outbound::runtime::{
    durable_wait_store::DurableWaitStore, job_attempt_store::JobAttemptStore,
    job_continuation_store::JobContinuationStore, job_store::JobStore, outbox_store::OutboxStore,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[derive(Clone, Serialize, Deserialize)]
struct Child {}
impl StasisJob for Child {
    const NAME: &'static str = "ledger.child";
    const VERSION: u32 = 1;
    type Output = ();
}
#[derive(Serialize, Deserialize)]
struct Signal {}
impl StasisEvent for Signal {
    const NAME: &'static str = "ledger.signal";
    const VERSION: u32 = 1;
}
fn job(id: &str) -> Job {
    NewJob {
        id: id.into(),
        queue: "default".into(),
        job_type: "ledger.parent".into(),
        payload_ref: "{}".into(),
        priority: 0,
        max_attempts: 3,
        idempotency_key: id.into(),
        correlation_id: id.into(),
        causation_id: id.into(),
        trace_id: id.into(),
        input_provenance: None,
        placement: Default::default(),
        scheduled_at: Utc::now(),
        backoff_policy: Default::default(),
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

struct FailWrite {
    inner: RusqliteExecutor,
    armed: AtomicBool,
}
fn completion_write(sql: &str) -> bool {
    let sql = sql.trim_start();
    (sql.starts_with("INSERT") || sql.starts_with("UPDATE") || sql.starts_with("DELETE"))
        && (sql.contains("runtime_job")
            || sql.contains("runtime_receipt")
            || sql.contains("runtime_attempt")
            || sql.contains("runtime_event")
            || sql.contains("runtime_continuation"))
}

impl SqlExecutor for FailWrite {
    fn execute(&self, sql: &str, p: &[SqlValue]) -> Result<u64> {
        self.inner.execute(sql, p)
    }
    fn query(&self, sql: &str, p: &[SqlValue]) -> Result<Vec<SqlRow>> {
        let rows = self.inner.query(sql, p)?;
        if self.armed.load(Ordering::SeqCst) && completion_write(sql) {
            Err(StasisError::PortFailure("injected after row write".into()))
        } else {
            Ok(rows)
        }
    }
    fn transaction(&self, body: &mut dyn FnMut() -> Result<()>) -> Result<()> {
        self.inner.transaction(body)
    }
}

#[tokio::test]
async fn executor_write_failure_rolls_back_entire_completion_and_retry_is_idempotent() {
    let exec = Arc::new(FailWrite {
        inner: RusqliteExecutor::open_in_memory().unwrap(),
        armed: AtomicBool::new(false),
    });
    let rt = DurableSqliteKernel::open(exec.clone())
        .unwrap()
        .into_runtime();
    rt.job_store.insert(job("parent")).await.unwrap();
    let leased = rt
        .job_store
        .lease_due("default", "worker", Utc::now(), 120, &Default::default())
        .await
        .unwrap()
        .unwrap();
    let ctx = JobContext::new(
        &leased,
        "worker",
        tokio::sync::watch::channel(false).1,
        rt.job_context_services(),
        120,
    );
    let child = ctx.enqueue(Child {}).await.unwrap();
    let edge = ctx.continue_with(Child {}).send().await.unwrap();
    ctx.publish(Signal {}).await.unwrap();
    exec.armed.store(true, Ordering::SeqCst);
    assert!(
        rt.job_store
            .commit_execution(&leased, success())
            .await
            .is_err()
    );
    exec.armed.store(false, Ordering::SeqCst);
    assert_eq!(
        rt.job_store.get("parent").await.unwrap().unwrap().state,
        JobState::Leased
    );
    assert!(rt.job_store.get(&child).await.unwrap().is_none());
    assert!(
        rt.job_attempt_store
            .list_by_job_id("parent")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        rt.outbox_store
            .list_by_job_id("parent")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        rt.continuation_store
            .get(&edge.continuation_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        rt.job_store
            .execution_receipt("parent", leased.execution_epoch)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        rt.job_store
            .commit_execution(&leased, success())
            .await
            .unwrap(),
        CommitResult::Committed
    );
    assert_eq!(
        rt.job_store
            .commit_execution(&leased, success())
            .await
            .unwrap(),
        CommitResult::AlreadyCommittedSameDigest
    );
    assert!(rt.job_store.get(&child).await.unwrap().is_some());
    assert_eq!(
        rt.job_attempt_store
            .list_by_job_id("parent")
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        rt.outbox_store
            .list_by_job_id("parent")
            .await
            .unwrap()
            .len(),
        2 // staged signal plus native lifecycle publication
    );
    assert!(
        rt.continuation_store
            .get(&edge.continuation_id)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn native_sql_abort_rolls_back_job_attempt_and_staged_child() {
    let rt = SqliteRuntime::open_memory().unwrap();
    rt.job_store.insert(job("parent")).await.unwrap();
    let leased = rt
        .job_store
        .lease_due("default", "worker", Utc::now(), 120, &Default::default())
        .await
        .unwrap()
        .unwrap();
    let grant = ExecutionGrant::from_job(&leased).unwrap();
    rt.job_store
        .stage_child(&grant, job("child"))
        .await
        .unwrap();
    let session = rt.job_store.session();
    session.execute("CREATE TRIGGER fail_completion BEFORE UPDATE ON runtime_job BEGIN SELECT RAISE(ABORT,'injected native completion'); END",[]).unwrap();
    assert!(
        rt.job_store
            .commit_execution(&leased, success())
            .await
            .is_err()
    );
    assert_eq!(
        rt.job_store.get("parent").await.unwrap().unwrap().state,
        JobState::Leased
    );
    assert!(rt.job_store.get("child").await.unwrap().is_none());
    assert!(
        rt.job_attempt_store
            .list_by_job_id("parent")
            .await
            .unwrap()
            .is_empty()
    );
    session.execute("DROP TRIGGER fail_completion", []).unwrap();
    assert_eq!(
        rt.job_store
            .commit_execution(&leased, success())
            .await
            .unwrap(),
        CommitResult::Committed
    );
    assert!(rt.job_store.get("child").await.unwrap().is_some());
}

#[tokio::test]
async fn executor_reconstruction_retains_auxiliary_state_receipts_and_cleanup() {
    let exec = Arc::new(RusqliteExecutor::open_in_memory().unwrap());
    let rt = DurableSqliteKernel::open(exec.clone())
        .unwrap()
        .into_runtime();
    rt.job_store.insert(job("parent")).await.unwrap();
    let leased = rt
        .job_store
        .lease_due("default", "worker", Utc::now(), 120, &Default::default())
        .await
        .unwrap()
        .unwrap();
    let ctx = JobContext::new(
        &leased,
        "worker",
        tokio::sync::watch::channel(false).1,
        rt.job_context_services(),
        120,
    );
    ctx.publish(Signal {}).await.unwrap();
    let edge = ctx.continue_with(Child {}).send().await.unwrap();
    let now = Utc::now();
    assert!(
        ctx.wait_for::<Signal>()
            .timeout(std::time::Duration::from_secs(3600))
            .await
            .is_err()
    );
    let wait_id = rt.wait_store.list_pending_by_job("parent").await.unwrap()[0]
        .wait_id
        .clone();
    rt.job_store
        .commit_execution(
            &leased,
            JobExecutionOutcome::Deferred {
                scheduled_at: now + Duration::hours(1),
                message: "park".into(),
                execution_id: None,
                diagnostics: None,
            },
        )
        .await
        .unwrap();
    drop(ctx);
    drop(rt);
    let rt = DurableSqliteKernel::open(exec.clone())
        .unwrap()
        .into_runtime();
    assert_eq!(
        rt.outbox_store
            .list_by_job_id("parent")
            .await
            .unwrap()
            .len(),
        2 // staged signal plus native lifecycle publication
    );
    assert!(
        rt.continuation_store
            .get(&edge.continuation_id)
            .await
            .unwrap()
            .is_some()
    );
    assert!(rt.wait_store.get_wait(&wait_id).await.unwrap().is_some());
    assert!(
        rt.job_store
            .execution_receipt("parent", leased.execution_epoch)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        rt.job_attempt_store
            .list_by_job_id("parent")
            .await
            .unwrap()
            .len(),
        1
    );
    let active = rt
        .job_store
        .lease_due(
            "default",
            "worker",
            now + Duration::hours(2),
            120,
            &Default::default(),
        )
        .await
        .unwrap()
        .unwrap();
    rt.job_store.cancel_execution("parent").await.unwrap();
    drop(rt);
    let rt = DurableSqliteKernel::open(exec).unwrap().into_runtime();
    assert_eq!(
        rt.job_store.get("parent").await.unwrap().unwrap().state,
        JobState::Canceled
    );
    assert!(
        rt.job_store
            .cleanup_receipt("parent", active.execution_epoch)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn retired_v1_table_is_not_the_runtime_after_upgrade() {
    let session = stasis::infrastructure::sqlite::SqliteSession::open(":memory:").unwrap();
    session.execute("INSERT INTO durable_signal(signal_id,signal_type,correlation_key,payload_json,created_at) VALUES('old','signal','key','{}',0)",[]).unwrap();
    let rt = SqliteRuntime::new(session.clone());
    rt.job_store.insert(job("new")).await.unwrap();
    assert!(rt.job_store.get("new").await.unwrap().is_some());
    assert!(
        rt.wait_store.get_signal("old").await.unwrap().is_none(),
        "rows inserted into the retired v1 table after migration are not runtime state"
    );
}
