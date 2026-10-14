//! The in-memory transitions are the reference. The same operation sequence must
//! leave native SQLite and the Durable Object executor in the same observable state.
#![cfg(feature = "sqlite")]
use chrono::{DateTime, Duration, Utc};
use stasis::application::runtime::in_memory_runtime::InMemoryRuntime;
use stasis::application::runtime::retention::RetentionPolicy;
use stasis::application::runtime::sqlite_runtime::SqliteRuntime;
use stasis::application::runtime::store_runtime::StoreRuntime;
use stasis::domain::errors::{Result, StasisError};
use stasis::domain::runtime::execution::{CommitResult, ExecutionGrant, JobExecutionOutcome};
use stasis::domain::runtime::job::{Job, JobState, NewJob};
use stasis::domain::runtime::job_attempt::JobAttemptOutcome;
use stasis::domain::runtime::placement::PlacementConstraints;
use stasis::infrastructure::runtime::atomic_id_generator::AtomicIdGenerator;
use stasis::infrastructure::runtime::noop_runtime_metrics::NoopRuntimeMetrics;
use stasis::infrastructure::sqlite::safety_stores::{
    ExecutorDurableWaitStore, ExecutorJobContinuationStore, ExecutorOutboxStore,
    ExecutorRecurringStore,
};
use stasis::infrastructure::sqlite::{
    DurableSqliteKernel, RusqliteExecutor, SqlExecutor, SqlRow, SqlValue,
    executor_attempt_store::ExecutorJobAttemptStore,
    executor_inbound_store::ExecutorInboundTriggerStore, executor_job_store::ExecutorJobStore,
    executor_lease_store::ExecutorResourceLeaseStore,
};
use stasis::infrastructure::telemetry::NoopRuntimeTracing;
use stasis::ports::outbound::runtime::clock::Clock;
use stasis::ports::outbound::runtime::job_attempt_store::JobAttemptStore;
use stasis::ports::outbound::runtime::job_store::JobStore;
use stasis::ports::outbound::runtime::outbox_store::OutboxStore;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

const SEEDS: u64 = 32;
const STEPS: usize = 40;

struct SharedClock {
    base: DateTime<Utc>,
    offset_secs: AtomicI64,
}
impl Clock for SharedClock {
    fn now(&self) -> DateTime<Utc> {
        self.base + Duration::seconds(self.offset_secs.load(Ordering::SeqCst))
    }
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() as usize) % n
    }
}

#[derive(Clone, Copy)]
enum Op {
    Insert { slot: u32, queue: u8, priority: i32 },
    Lease { queue: u8 },
    Heartbeat { slot: u32 },
    Commit { slot: u32, kind: u8 },
    Duplicate { slot: u32 },
    Cancel { slot: u32 },
    Fail { slot: u32 },
    Recover { slot: u32 },
    Advance,
}

fn script(seed: u64) -> Vec<Op> {
    let mut rng = Rng(seed);
    (0..STEPS)
        .map(|_| match rng.below(9) {
            0 => Op::Insert {
                slot: rng.below(6) as u32,
                queue: rng.below(2) as u8,
                priority: rng.below(3) as i32,
            },
            1 => Op::Lease {
                queue: rng.below(2) as u8,
            },
            2 => Op::Heartbeat {
                slot: rng.below(6) as u32,
            },
            3 => Op::Commit {
                slot: rng.below(6) as u32,
                kind: rng.below(3) as u8,
            },
            4 => Op::Duplicate {
                slot: rng.below(6) as u32,
            },
            5 => Op::Cancel {
                slot: rng.below(6) as u32,
            },
            6 => Op::Fail {
                slot: rng.below(6) as u32,
            },
            7 => Op::Recover {
                slot: rng.below(6) as u32,
            },
            _ => Op::Advance,
        })
        .collect()
}

fn job_id(slot: u32) -> String {
    format!("job-{slot}")
}
fn queue_name(queue: u8) -> String {
    format!("q{queue}")
}
fn make_job(slot: u32, queue: u8, priority: i32, when: DateTime<Utc>) -> Job {
    let id = job_id(slot);
    NewJob {
        id: id.clone(),
        queue: queue_name(queue),
        job_type: "eq".into(),
        payload_ref: "{}".into(),
        priority,
        max_attempts: 3,
        idempotency_key: id.clone(),
        correlation_id: id.clone(),
        causation_id: id,
        trace_id: "eq".into(),
        input_provenance: None,
        placement: Default::default(),
        scheduled_at: when,
        backoff_policy: Default::default(),
    }
    .into_job()
}
fn outcome(kind: u8) -> JobExecutionOutcome {
    match kind % 3 {
        0 => JobExecutionOutcome::Success {
            output_provenance: None,
            execution_id: None,
            diagnostics: None,
        },
        1 => JobExecutionOutcome::RetryableFailure {
            message: "again".into(),
            execution_id: None,
            diagnostics: None,
        },
        _ => JobExecutionOutcome::FatalFailure {
            message: "stop".into(),
            execution_id: None,
            diagnostics: None,
        },
    }
}

type JobColumns = (String, String, u64, u64, u64, u64, Option<String>);

#[derive(Clone, Debug, PartialEq, Eq)]
struct View {
    jobs: Vec<JobColumns>,
    attempts: Vec<(String, String, u8)>,
    events: Vec<(String, String)>,
    receipts: Vec<(String, u64, String)>,
}

async fn project(
    jobs: &impl JobStore,
    attempts: &impl JobAttemptStore,
    outbox: &impl OutboxStore,
) -> View {
    let mut rows = Vec::new();
    for state in [
        JobState::Enqueued,
        JobState::Leased,
        JobState::Running,
        JobState::Succeeded,
        JobState::Failed,
        JobState::DeadLetter,
        JobState::Canceled,
    ] {
        for job in jobs.list_by_state(state).await.unwrap() {
            rows.push((
                job.id,
                format!("{state:?}"),
                job.execution_epoch,
                job.version,
                job.incarnation,
                job.terminal_cycle,
                job.lease_owner,
            ));
        }
    }
    rows.sort();
    let mut attempt_rows = Vec::new();
    let mut event_rows = Vec::new();
    let mut receipt_rows = Vec::new();
    for (id, _, epoch, _, _, _, _) in &rows {
        for attempt in attempts.list_by_job_id(id).await.unwrap() {
            let code = match attempt.outcome {
                JobAttemptOutcome::Succeeded => 0,
                JobAttemptOutcome::RetryableFailure => 1,
                JobAttemptOutcome::FatalFailure => 2,
                JobAttemptOutcome::Deferred => 3,
            };
            attempt_rows.push((id.clone(), attempt.attempt_id, code));
        }
        for event in outbox.list_by_job_id(id).await.unwrap() {
            event_rows.push((id.clone(), event.event_id));
        }
        if let Some(receipt) = jobs.execution_receipt(id, *epoch).await.unwrap() {
            receipt_rows.push((id.clone(), *epoch, receipt.digest));
        }
    }
    attempt_rows.sort();
    event_rows.sort();
    receipt_rows.sort();
    View {
        jobs: rows,
        attempts: attempt_rows,
        events: event_rows,
        receipts: receipt_rows,
    }
}

struct Lane<J, A, O> {
    jobs: J,
    attempts: A,
    outbox: O,
    open: HashMap<String, Job>,
    done: HashMap<String, (Job, JobExecutionOutcome)>,
}

impl<J, A, O> Lane<J, A, O>
where
    J: JobStore,
    A: JobAttemptStore,
    O: OutboxStore,
{
    async fn apply(&mut self, op: Op, now: DateTime<Utc>) -> Result<String> {
        match op {
            Op::Insert {
                slot,
                queue,
                priority,
            } => {
                let id = job_id(slot);
                match self.jobs.insert(make_job(slot, queue, priority, now)).await {
                    Ok(()) => Ok(format!("insert {id}")),
                    Err(_) => Ok(format!("insert-err {id}")),
                }
            }
            Op::Lease { queue } => {
                let leased = self
                    .jobs
                    .lease_due(&queue_name(queue), "worker", now, 30, &Default::default())
                    .await?;
                if let Some(job) = &leased {
                    self.open.insert(job.id.clone(), job.clone());
                }
                Ok(format!(
                    "lease {}",
                    leased.as_ref().map(|job| job.id.as_str()).unwrap_or("-")
                ))
            }
            Op::Heartbeat { slot } => {
                let id = job_id(slot);
                let Some(job) = self.open.get(&id).cloned() else {
                    return Ok(format!("heartbeat-skip {id}"));
                };
                let Some(grant) = ExecutionGrant::from_job(&job) else {
                    return Ok(format!("heartbeat-skip {id}"));
                };
                match self.jobs.heartbeat_granted(&grant, now, 30, None).await {
                    Ok(()) => Ok(format!("heartbeat {id}")),
                    Err(_) => Ok(format!("heartbeat-err {id}")),
                }
            }
            Op::Commit { slot, kind } => self.finish(slot, outcome(kind), false).await,
            Op::Duplicate { slot } => {
                let id = job_id(slot);
                let Some((job, outcome)) = self.done.get(&id).cloned() else {
                    return Ok(format!("duplicate-skip {id}"));
                };
                let result = self.jobs.commit_execution(&job, outcome).await?;
                Ok(format!("duplicate {id} {result:?}"))
            }
            Op::Cancel { slot } => {
                let id = job_id(slot);
                let result = self.jobs.cancel_execution(&id).await?;
                self.open.remove(&id);
                Ok(format!("cancel {id} {}", result.is_some()))
            }
            Op::Fail { slot } => {
                let id = job_id(slot);
                let result = self.jobs.fail_execution(&id).await?;
                self.open.remove(&id);
                Ok(format!("fail {id} {}", result.is_some()))
            }
            Op::Recover { slot } => {
                let id = job_id(slot);
                let Some(job) = self.open.get(&id).cloned() else {
                    return Ok(format!("recover-skip {id}"));
                };
                let result = self.jobs.recover_execution(&job).await?;
                if !matches!(result, CommitResult::Stale) {
                    self.open.remove(&id);
                }
                Ok(format!("recover {id} {result:?}"))
            }
            Op::Advance => Ok("advance".into()),
        }
    }

    async fn finish(
        &mut self,
        slot: u32,
        outcome: JobExecutionOutcome,
        crash: bool,
    ) -> Result<String> {
        let id = job_id(slot);
        let Some(job) = self.open.get(&id).cloned() else {
            return Ok(format!("commit-skip {id}"));
        };
        let result = self.jobs.commit_execution(&job, outcome.clone()).await;
        if crash {
            return Ok(format!("crash {id}"));
        }
        match result {
            Ok(CommitResult::Committed) | Ok(CommitResult::AlreadyCommittedSameDigest) => {
                self.open.remove(&id);
                self.done.insert(id.clone(), (job, outcome));
                Ok(format!("commit {id} committed"))
            }
            Ok(other) => Ok(format!("commit {id} {other:?}")),
            Err(_) => Ok(format!("commit-err {id}")),
        }
    }

    async fn view(&self) -> View {
        project(&self.jobs, &self.attempts, &self.outbox).await
    }
}

struct Flaky {
    inner: RusqliteExecutor,
    armed: AtomicBool,
}
impl SqlExecutor for Flaky {
    fn execute(&self, sql: &str, params: &[SqlValue]) -> Result<u64> {
        self.inner.execute(sql, params)
    }
    fn query(&self, sql: &str, params: &[SqlValue]) -> Result<Vec<SqlRow>> {
        let rows = self.inner.query(sql, params)?;
        if self.armed.load(Ordering::SeqCst) && sql.contains("runtime_receipt") {
            return Err(StasisError::PortFailure("injected rollback".into()));
        }
        Ok(rows)
    }
    fn transaction(&self, body: &mut dyn FnMut() -> Result<()>) -> Result<()> {
        self.inner.transaction(body)
    }
}

fn durable_runtime(
    exec: Arc<dyn SqlExecutor>,
    clock: Arc<dyn Clock>,
) -> StoreRuntime<
    ExecutorJobStore,
    ExecutorRecurringStore,
    ExecutorOutboxStore,
    ExecutorJobAttemptStore,
    ExecutorDurableWaitStore,
    ExecutorJobContinuationStore,
    ExecutorInboundTriggerStore,
    ExecutorResourceLeaseStore,
> {
    DurableSqliteKernel::open(exec.clone()).unwrap();
    StoreRuntime::from_stores(
        ExecutorJobStore::new(exec.clone()).with_clock(clock.clone()),
        ExecutorRecurringStore::new(exec.clone()).with_clock(clock.clone()),
        ExecutorOutboxStore::new(exec.clone()).with_clock(clock.clone()),
        ExecutorJobAttemptStore::new(exec.clone()).with_clock(clock.clone()),
        ExecutorDurableWaitStore::new(exec.clone()).with_clock(clock.clone()),
        ExecutorJobContinuationStore::new(exec.clone()).with_clock(clock.clone()),
        ExecutorInboundTriggerStore::new(exec.clone()),
        ExecutorResourceLeaseStore::new(exec),
        clock,
        Arc::new(AtomicIdGenerator::new(1)),
        Arc::new(NoopRuntimeMetrics),
        Arc::new(NoopRuntimeTracing),
    )
}

#[tokio::test]
async fn randomized_sequences_match_memory_sqlite_and_executor() {
    for seed in 1..=SEEDS {
        let clock = Arc::new(SharedClock {
            base: DateTime::parse_from_rfc3339("2024-06-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            offset_secs: AtomicI64::new(0),
        });
        let memory =
            InMemoryRuntime::with_dependencies(clock.clone(), Arc::new(AtomicIdGenerator::new(1)));
        let sqlite = SqliteRuntime::with_dependencies(
            stasis::infrastructure::sqlite::SqliteSession::open(":memory:").unwrap(),
            clock.clone(),
            Arc::new(AtomicIdGenerator::new(1)),
        );
        let exec = Arc::new(Flaky {
            inner: RusqliteExecutor::open_in_memory().unwrap(),
            armed: AtomicBool::new(false),
        });
        let durable = durable_runtime(exec.clone(), clock.clone());
        let mut memory_lane = Lane {
            jobs: memory.job_store.clone(),
            attempts: memory.job_attempt_store.clone(),
            outbox: memory.outbox_store.clone(),
            open: HashMap::new(),
            done: HashMap::new(),
        };
        let mut sqlite_lane = Lane {
            jobs: sqlite.job_store.clone(),
            attempts: sqlite.job_attempt_store.clone(),
            outbox: sqlite.outbox_store.clone(),
            open: HashMap::new(),
            done: HashMap::new(),
        };
        let mut durable_lane = Lane {
            jobs: durable.job_store.clone(),
            attempts: durable.job_attempt_store.clone(),
            outbox: durable.outbox_store.clone(),
            open: HashMap::new(),
            done: HashMap::new(),
        };
        for (step, op) in script(seed.wrapping_mul(0x9E37_79B9))
            .into_iter()
            .enumerate()
        {
            if matches!(op, Op::Advance) {
                clock.offset_secs.fetch_add(120, Ordering::SeqCst);
            }
            let now = clock.now();
            let memory_report = memory_lane
                .apply(op, now)
                .await
                .unwrap_or_else(|error| format!("err {error}"));
            let sqlite_report = sqlite_lane
                .apply(op, now)
                .await
                .unwrap_or_else(|error| format!("err {error}"));
            let durable_report = durable_lane
                .apply(op, now)
                .await
                .unwrap_or_else(|error| format!("err {error}"));
            assert_eq!(
                memory_report, sqlite_report,
                "seed {seed} step {step} sqlite report"
            );
            assert_eq!(
                memory_report, durable_report,
                "seed {seed} step {step} executor report"
            );
            assert_eq!(
                memory_lane.view().await,
                sqlite_lane.view().await,
                "seed {seed} step {step} sqlite state"
            );
            assert_eq!(
                memory_lane.view().await,
                durable_lane.view().await,
                "seed {seed} step {step} executor state"
            );
        }
    }
}

#[tokio::test]
async fn injected_rollback_matches_a_skipped_memory_commit() {
    let clock = Arc::new(SharedClock {
        base: Utc::now(),
        offset_secs: AtomicI64::new(0),
    });
    let memory =
        InMemoryRuntime::with_dependencies(clock.clone(), Arc::new(AtomicIdGenerator::new(1)));
    let sqlite = SqliteRuntime::with_dependencies(
        stasis::infrastructure::sqlite::SqliteSession::open(":memory:").unwrap(),
        clock.clone(),
        Arc::new(AtomicIdGenerator::new(1)),
    );
    let exec = Arc::new(Flaky {
        inner: RusqliteExecutor::open_in_memory().unwrap(),
        armed: AtomicBool::new(false),
    });
    let durable = durable_runtime(exec.clone(), clock.clone());
    let now = clock.now();
    for jobs in [
        &memory.job_store as &dyn JobStore,
        &sqlite.job_store,
        &durable.job_store,
    ] {
        jobs.insert(make_job(0, 0, 0, now)).await.unwrap();
        let leased = jobs
            .lease_due("q0", "worker", now, 30, &Default::default())
            .await
            .unwrap()
            .unwrap();
        jobs.stage_child(
            &ExecutionGrant::from_job(&leased).unwrap(),
            make_job(1, 0, 0, now),
        )
        .await
        .unwrap();
    }
    let leased = memory.job_store.get("job-0").await.unwrap().unwrap();
    // Memory does not attempt the commit. The SQL lanes write it and roll it back.
    sqlite.job_store.session().execute(
        "CREATE TRIGGER fail_completion BEFORE UPDATE ON runtime_job BEGIN SELECT RAISE(ABORT,'injected native completion'); END",
        [],
    ).unwrap();
    assert!(
        sqlite
            .job_store
            .commit_execution(&leased, outcome(0))
            .await
            .is_err()
    );
    sqlite
        .job_store
        .session()
        .execute("DROP TRIGGER fail_completion", [])
        .unwrap();
    exec.armed.store(true, Ordering::SeqCst);
    let durable_lease = durable.job_store.get("job-0").await.unwrap().unwrap();
    assert!(
        durable
            .job_store
            .commit_execution(&durable_lease, outcome(0))
            .await
            .is_err()
    );
    exec.armed.store(false, Ordering::SeqCst);
    let memory_view = project(
        &memory.job_store,
        &memory.job_attempt_store,
        &memory.outbox_store,
    )
    .await;
    let sqlite_view = project(
        &sqlite.job_store,
        &sqlite.job_attempt_store,
        &sqlite.outbox_store,
    )
    .await;
    let durable_view = project(
        &durable.job_store,
        &durable.job_attempt_store,
        &durable.outbox_store,
    )
    .await;
    assert_eq!(memory_view, sqlite_view);
    assert_eq!(memory_view, durable_view);
    assert!(memory.job_store.get("job-1").await.unwrap().is_none());
    assert_eq!(
        memory.job_store.get("job-0").await.unwrap().unwrap().state,
        JobState::Leased
    );
}

#[tokio::test]
async fn due_lookup_does_not_read_another_queue() {
    let seen = Arc::new(Mutex::new(0usize));
    struct Counting {
        inner: RusqliteExecutor,
        seen: Arc<Mutex<usize>>,
    }
    impl SqlExecutor for Counting {
        fn execute(&self, sql: &str, params: &[SqlValue]) -> Result<u64> {
            self.inner.execute(sql, params)
        }
        fn query(&self, sql: &str, params: &[SqlValue]) -> Result<Vec<SqlRow>> {
            let rows = self.inner.query(sql, params)?;
            if sql.contains("runtime_job j") && sql.contains("j.queue") {
                *self.seen.lock().unwrap() += rows.len();
            }
            Ok(rows)
        }
        fn transaction(&self, body: &mut dyn FnMut() -> Result<()>) -> Result<()> {
            self.inner.transaction(body)
        }
    }
    let exec = Arc::new(Counting {
        inner: RusqliteExecutor::open_in_memory().unwrap(),
        seen: seen.clone(),
    });
    let rt = durable_runtime(
        exec,
        Arc::new(stasis::infrastructure::runtime::system_clock::SystemClock),
    );
    let now = Utc::now();
    for index in 0..40u32 {
        rt.job_store
            .insert(make_job(index, 0, 0, now))
            .await
            .unwrap();
    }
    rt.job_store.insert(make_job(41, 1, 0, now)).await.unwrap();
    *seen.lock().unwrap() = 0;
    let leased = rt
        .job_store
        .lease_due("q1", "worker", now, 30, &Default::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(leased.id, "job-41");
    assert_eq!(*seen.lock().unwrap(), 1);
}

#[tokio::test]
async fn lease_stops_at_the_first_placement_match() {
    let now = Utc::now();
    let mut blocked = make_job(0, 0, 0, now);
    blocked.placement = PlacementConstraints::default().require_capability("gpu");
    let open = make_job(1, 0, 0, now);
    let mut later = Vec::new();
    for index in 2..40u32 {
        later.push(make_job(index, 0, 0, now));
    }

    let memory = InMemoryRuntime::with_dependencies(
        Arc::new(SharedClock {
            base: now,
            offset_secs: AtomicI64::new(0),
        }),
        Arc::new(AtomicIdGenerator::new(1)),
    );
    memory.job_store.insert(blocked.clone()).await.unwrap();
    memory.job_store.insert(open.clone()).await.unwrap();
    for job in &later {
        memory.job_store.insert(job.clone()).await.unwrap();
    }
    let memory_id = memory
        .job_store
        .lease_due("q0", "worker", now, 30, &Default::default())
        .await
        .unwrap()
        .unwrap()
        .id;

    let seen = Arc::new(Mutex::new(0usize));
    struct Counting {
        inner: RusqliteExecutor,
        seen: Arc<Mutex<usize>>,
    }
    impl SqlExecutor for Counting {
        fn execute(&self, sql: &str, params: &[SqlValue]) -> Result<u64> {
            self.inner.execute(sql, params)
        }
        fn query(&self, sql: &str, params: &[SqlValue]) -> Result<Vec<SqlRow>> {
            let rows = self.inner.query(sql, params)?;
            if sql.contains("runtime_job j") && sql.contains("ORDER BY j.scheduled_at") {
                *self.seen.lock().unwrap() += rows.len();
            }
            Ok(rows)
        }
        fn transaction(&self, body: &mut dyn FnMut() -> Result<()>) -> Result<()> {
            self.inner.transaction(body)
        }
    }
    let exec = Arc::new(Counting {
        inner: RusqliteExecutor::open_in_memory().unwrap(),
        seen: seen.clone(),
    });
    let rt = durable_runtime(
        exec,
        Arc::new(stasis::infrastructure::runtime::system_clock::SystemClock),
    );
    rt.job_store.insert(blocked).await.unwrap();
    rt.job_store.insert(open).await.unwrap();
    for job in later {
        rt.job_store.insert(job).await.unwrap();
    }
    *seen.lock().unwrap() = 0;
    let leased = rt
        .job_store
        .lease_due("q0", "worker", now, 30, &Default::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(leased.id, memory_id);
    assert_eq!(leased.id, "job-1");
    assert_eq!(*seen.lock().unwrap(), 2);
}

#[tokio::test]
async fn receipt_retention_keeps_duplicate_completion_until_the_window_closes() {
    let clock = Arc::new(SharedClock {
        base: Utc::now(),
        offset_secs: AtomicI64::new(0),
    });
    let rt = SqliteRuntime::with_dependencies(
        stasis::infrastructure::sqlite::SqliteSession::open(":memory:").unwrap(),
        clock.clone(),
        Arc::new(AtomicIdGenerator::new(1)),
    );
    rt.configure_retention_policy(RetentionPolicy {
        terminal_ttl_days: 1,
        receipt_ttl_days: 30,
        cleanup_ttl_days: 30,
    })
    .unwrap();
    let now = clock.now();
    rt.job_store.insert(make_job(0, 0, 0, now)).await.unwrap();
    let leased = rt
        .job_store
        .lease_due("q0", "worker", now, 30, &Default::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        rt.job_store
            .commit_execution(&leased, outcome(0))
            .await
            .unwrap(),
        CommitResult::Committed
    );
    rt.job_store
        .prune_terminal_before(now + Duration::days(2))
        .await
        .unwrap();
    assert!(rt.job_store.get("job-0").await.unwrap().is_none());
    assert_eq!(
        rt.job_store
            .commit_execution(&leased, outcome(0))
            .await
            .unwrap(),
        CommitResult::AlreadyCommittedSameDigest
    );
    let pruned = rt
        .job_store
        .prune_retained_before(now + Duration::days(1), now + Duration::days(1))
        .await
        .unwrap();
    assert!(pruned.0 >= 1);
    assert_eq!(
        rt.job_store
            .commit_execution(&leased, outcome(0))
            .await
            .unwrap(),
        CommitResult::Stale
    );
}

#[tokio::test]
async fn row_store_insert_and_lease_cost_is_independent_of_a_full_snapshot() {
    let rt = SqliteRuntime::open_memory().unwrap();
    let now = Utc::now();
    let started = std::time::Instant::now();
    for index in 0..500 {
        let mut job = make_job(0, 0, 0, now);
        job.id = format!("bulk-{index}");
        job.idempotency_key = job.id.clone();
        rt.job_store.insert(job).await.unwrap();
    }
    let inserted = started.elapsed();
    let started = std::time::Instant::now();
    for _ in 0..20 {
        let _ = rt
            .job_store
            .lease_due("q0", "worker", now, 30, &Default::default())
            .await
            .unwrap();
    }
    let leased = started.elapsed();
    eprintln!("row-level 500 inserts {inserted:?}; 20 leases {leased:?}");
    assert!(inserted.as_secs() < 30);
    assert!(leased.as_secs() < 10);
}
