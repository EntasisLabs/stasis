#![cfg(feature = "sqlite")]
//! Publication must still be owned when SQL finishes writing, not only when
//! the memory transition begins. This fixture advances the trusted clock in exec.
use chrono::{DateTime, Duration, Utc};
use stasis::domain::{
    errors::Result,
    runtime::{
        execution::JobExecutionOutcome,
        job::{JobState, NewJob},
    },
};
use stasis::infrastructure::sqlite::{
    DurableSqliteKernel, RusqliteExecutor, SqlExecutor, SqlRow, SqlValue,
    executor_attempt_store::ExecutorJobAttemptStore, executor_job_store::ExecutorJobStore,
};
use stasis::ports::outbound::runtime::{
    clock::Clock, job_attempt_store::JobAttemptStore, job_store::JobStore,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
struct BoundaryClock {
    base: DateTime<Utc>,
    expired: Arc<AtomicBool>,
}
impl Clock for BoundaryClock {
    fn now(&self) -> DateTime<Utc> {
        if self.expired.load(Ordering::SeqCst) {
            self.base + Duration::hours(1)
        } else {
            self.base
        }
    }
}
struct BoundaryExecutor {
    inner: RusqliteExecutor,
    armed: AtomicBool,
    expired: Arc<AtomicBool>,
}
fn completion_write(sql: &str) -> bool {
    let sql = sql.trim_start();
    (sql.starts_with("INSERT") || sql.starts_with("UPDATE"))
        && (sql.contains("runtime_job") || sql.contains("runtime_receipt"))
}

impl SqlExecutor for BoundaryExecutor {
    fn execute(&self, s: &str, p: &[SqlValue]) -> Result<u64> {
        self.inner.execute(s, p)
    }
    fn query(&self, s: &str, p: &[SqlValue]) -> Result<Vec<SqlRow>> {
        let rows = self.inner.query(s, p)?;
        if self.armed.load(Ordering::SeqCst) && completion_write(s) {
            self.expired.store(true, Ordering::SeqCst);
        }
        Ok(rows)
    }
    fn transaction(&self, f: &mut dyn FnMut() -> Result<()>) -> Result<()> {
        self.inner.transaction(f)
    }
}
#[tokio::test]
async fn expiry_after_row_write_rolls_back_publication_and_receipt() {
    let expired = Arc::new(AtomicBool::new(false));
    let now = Utc::now();
    let clock = Arc::new(BoundaryClock {
        base: now,
        expired: expired.clone(),
    });
    let exec = Arc::new(BoundaryExecutor {
        inner: RusqliteExecutor::open_in_memory().unwrap(),
        armed: AtomicBool::new(false),
        expired: expired.clone(),
    });
    drop(DurableSqliteKernel::open(exec.clone()).unwrap());
    let store = ExecutorJobStore::new(exec.clone()).with_clock(clock);
    let job = NewJob {
        id: "boundary".into(),
        queue: "default".into(),
        job_type: "boundary".into(),
        payload_ref: "{}".into(),
        priority: 0,
        max_attempts: 3,
        idempotency_key: "boundary".into(),
        correlation_id: "boundary".into(),
        causation_id: "boundary".into(),
        trace_id: "boundary".into(),
        input_provenance: None,
        placement: Default::default(),
        scheduled_at: now,
        backoff_policy: Default::default(),
    }
    .into_job();
    store.insert(job).await.unwrap();
    let leased = store
        .lease_due("default", "worker", now, 30, &Default::default())
        .await
        .unwrap()
        .unwrap();
    exec.armed.store(true, Ordering::SeqCst);
    let result = store
        .commit_execution(
            &leased,
            JobExecutionOutcome::Success {
                output_provenance: None,
                execution_id: None,
                diagnostics: None,
            },
        )
        .await;
    assert!(
        result.is_err(),
        "late expiry must roll back rather than publish success"
    );
    exec.armed.store(false, Ordering::SeqCst);
    expired.store(false, Ordering::SeqCst);
    assert_eq!(
        store.get("boundary").await.unwrap().unwrap().state,
        JobState::Leased
    );
    assert!(
        store
            .execution_receipt("boundary", leased.execution_epoch)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        ExecutorJobAttemptStore::new(exec)
            .list_by_job_id("boundary")
            .await
            .unwrap()
            .is_empty()
    );
}
