//! Row-level native state. Each operation loads an indexed working set, runs the
//! in-memory transition, and writes only the rows that changed.
//!
//! Updates compare-and-swap on the previously read payload so a concurrent
//! writer fails the transaction instead of publishing a stale effect.
use super::codec::{decode_json, encode_json, encode_time, sql_err};
use super::executor::{SqlExecutor, SqlRow, SqlValue};
use super::schema::MAX_ROW_PAYLOAD_BYTES;
use crate::application::runtime::in_memory_runtime::{NativeState, StagedEffects};
use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::durable_wait::{
    DurableSignalRecord, DurableWaitRecord, DurableWaitStatus,
};
use crate::domain::runtime::execution::{CleanupRecord, ExecutionReceipt};
use crate::domain::runtime::job::{Job, JobState};
use crate::domain::runtime::job_attempt::JobAttempt;
use crate::domain::runtime::job_continuation::JobContinuation;
use crate::domain::runtime::outbox::{OutboxEvent, OutboxStatus};
use crate::domain::runtime::placement::WorkerCapabilities;
use crate::domain::runtime::recurring::RecurringDefinition;
use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet};

pub(crate) trait SqlIo {
    fn exec(&mut self, sql: &str, params: &[SqlValue]) -> Result<u64>;
    fn query(&mut self, sql: &str, params: &[SqlValue]) -> Result<Vec<SqlRow>>;
}

pub(crate) struct ExecIo<'a>(pub &'a dyn SqlExecutor);

impl SqlIo for ExecIo<'_> {
    fn exec(&mut self, sql: &str, params: &[SqlValue]) -> Result<u64> {
        self.0.execute(sql, params)
    }
    fn query(&mut self, sql: &str, params: &[SqlValue]) -> Result<Vec<SqlRow>> {
        self.0.query(sql, params)
    }
}

#[derive(Clone, Debug)]
pub(crate) enum Scope {
    Empty,
    Job(String),
    Due {
        queue: String,
        now_ns: i64,
        worker: WorkerCapabilities,
    },
    ByState(String),
    Expired {
        now_ns: i64,
    },
    PruneJobs {
        cutoff_ns: i64,
    },
    Commit {
        job_id: String,
        epoch: u64,
    },
    Cancel(String),
    Fail(String),
    Stage {
        job_id: String,
        epoch: u64,
    },
    Poll {
        job_id: String,
        signal_type: String,
        correlation_key: String,
    },
    Settle {
        parent_id: String,
        continuation_id: String,
        child_id: Option<String>,
    },
    Delete(String),
    AttemptsJob(String),
    AttemptsGuardrail(String),
    AttemptsExecution(String),
    PruneAttempts {
        cutoff_ns: i64,
    },
    Event(String),
    EventsPending {
        limit: u64,
    },
    EventsJob(String),
    EventsThread(String),
    EventsPrefix(String),
    EventsExecution(String),
    PruneEvents {
        cutoff_ns: i64,
    },
    Wait(String),
    WaitsJobSignal {
        job_id: String,
        signal_type: String,
        correlation_key: String,
    },
    WaitsSignal {
        signal_type: String,
        correlation_key: String,
    },
    WaitsJob(String),
    Signal(String),
    SignalsKey {
        signal_type: String,
        correlation_key: String,
    },
    Continuation(String),
    ContinuationsParent(String),
    ContinuationChild(String),
    Recurring(String),
    RecurringDue {
        now_ns: i64,
    },
    RecurringAll,
    LifecyclePending,
    LifecycleKey(String),
    Receipt {
        job_id: String,
        epoch: u64,
    },
    Cleanup {
        job_id: String,
        epoch: u64,
    },
}

pub(crate) fn job_state_name(state: JobState) -> &'static str {
    match state {
        JobState::Enqueued => "Enqueued",
        JobState::Leased => "Leased",
        JobState::Running => "Running",
        JobState::Succeeded => "Succeeded",
        JobState::Failed => "Failed",
        JobState::DeadLetter => "DeadLetter",
        JobState::Canceled => "Canceled",
    }
}

fn err(message: impl std::fmt::Display) -> StasisError {
    sql_err("runtime row store", message)
}

fn check_payload(payload: &str) -> Result<()> {
    if payload.len() > MAX_ROW_PAYLOAD_BYTES {
        return Err(err(
            "row payload exceeds supported 1 MiB profile; no volatile fallback",
        ));
    }
    Ok(())
}

fn json_same(left: &str, right: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(left).ok()
        == serde_json::from_str::<serde_json::Value>(right).ok()
}

fn as_i64(value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| err("integer exceeds sqlite range"))
}

fn time_val(value: Option<DateTime<Utc>>) -> SqlValue {
    match value {
        Some(value) => SqlValue::Integer(encode_time(value)),
        None => SqlValue::Null,
    }
}

fn opt_text(value: &Option<String>) -> SqlValue {
    match value {
        Some(value) => SqlValue::Text(value.clone()),
        None => SqlValue::Null,
    }
}

fn must_cas(io: &mut dyn SqlIo, sql: &str, params: &[SqlValue], what: &str) -> Result<()> {
    let rows = io.query(sql, params)?;
    if rows.is_empty() {
        return Err(err(format!(
            "compare-and-swap rejected for {what}; the row changed or disappeared"
        )));
    }
    Ok(())
}

#[derive(Default)]
pub(crate) struct Snapshot {
    state: NativeState,
    recurring: HashMap<String, RecurringDefinition>,
    job_payload: HashMap<String, String>,
    epoch: HashMap<String, i64>,
    attempt_payload: HashMap<String, String>,
    attempt_seq: HashMap<String, i64>,
    event_payload: HashMap<String, String>,
    receipt_payload: HashMap<(String, i64), String>,
    cancel_payload: HashMap<String, String>,
    cleanup_payload: HashMap<(String, i64), String>,
    staged_payload: HashMap<(String, i64, i64), String>,
    wait_payload: HashMap<String, String>,
    signal_payload: HashMap<String, String>,
    continuation_payload: HashMap<String, String>,
    recurring_payload: HashMap<String, String>,
}

pub(crate) struct WorkingSet {
    pub(crate) before: Snapshot,
    pub state: NativeState,
    pub recurring: HashMap<String, RecurringDefinition>,
}

pub(crate) fn load(io: &mut dyn SqlIo, scope: &Scope) -> Result<WorkingSet> {
    let mut snap = Snapshot::default();
    match scope {
        Scope::Empty => {}
        Scope::Job(id) => {
            load_job(io, &mut snap, id)?;
            load_epoch(io, &mut snap, id)?;
        }
        Scope::Due {
            queue,
            now_ns,
            worker,
        } => load_due(io, &mut snap, queue, *now_ns, worker)?,
        Scope::ByState(state) => {
            load_jobs_where(io, &mut snap, "state = ?1", &[SqlValue::text(state)])?
        }
        Scope::Expired { now_ns } => load_jobs_where(
            io,
            &mut snap,
            "state IN ('Leased','Running') AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?1",
            &[SqlValue::Integer(*now_ns)],
        )?,
        Scope::PruneJobs { cutoff_ns } => {
            load_jobs_where(
                io,
                &mut snap,
                "state IN ('Succeeded','Failed','DeadLetter','Canceled') AND finished_at IS NOT NULL AND finished_at <= ?1",
                &[SqlValue::Integer(*cutoff_ns)],
            )?;
            let rows = io.query(
                "SELECT parent_id, CAST(epoch AS TEXT), CAST(version AS TEXT), payload FROM runtime_staged \
                 WHERE parent_id IN (SELECT job_id FROM runtime_job WHERE state IN ('Succeeded','Failed','DeadLetter','Canceled') AND finished_at IS NOT NULL AND finished_at <= ?1)",
                &[SqlValue::Integer(*cutoff_ns)],
            )?;
            for row in rows {
                put_staged(&mut snap, row)?;
            }
        }
        Scope::Commit { job_id, epoch } => {
            load_job(io, &mut snap, job_id)?;
            load_epoch(io, &mut snap, job_id)?;
            load_receipt(io, &mut snap, job_id, *epoch)?;
            load_staged_epoch(io, &mut snap, job_id, *epoch)?;
            expand_staged(io, &mut snap)?;
        }
        Scope::Cancel(job_id) | Scope::Fail(job_id) => {
            load_job(io, &mut snap, job_id)?;
            load_waits_job(io, &mut snap, job_id)?;
            load_staged_parent(io, &mut snap, job_id)?;
            if matches!(scope, Scope::Fail(_)) {
                load_attempts_job(io, &mut snap, job_id)?;
            }
            if let Some(job) = snap.state.jobs.get(job_id).cloned() {
                let cancel_key = format!(
                    "{}:incarnation:{}:cancel:{}",
                    job.id, job.incarnation, job.version
                );
                let fail_key = format!(
                    "{}:incarnation:{}:operator-fail:{}",
                    job.id, job.incarnation, job.version
                );
                load_cancel_key(io, &mut snap, &cancel_key)?;
                load_cancel_key(io, &mut snap, &fail_key)?;
                load_event(io, &mut snap, &cancel_key)?;
                load_event(io, &mut snap, &fail_key)?;
                load_cleanup(io, &mut snap, job_id, job.execution_epoch)?;
            }
        }
        Scope::Stage { job_id, epoch } => {
            load_job(io, &mut snap, job_id)?;
            load_staged_epoch(io, &mut snap, job_id, *epoch)?;
        }
        Scope::Poll {
            job_id,
            signal_type,
            correlation_key,
        } => {
            load_job(io, &mut snap, job_id)?;
            load_waits_job(io, &mut snap, job_id)?;
            load_signals_key(io, &mut snap, signal_type, correlation_key)?;
        }
        Scope::Settle {
            parent_id,
            continuation_id,
            child_id,
        } => {
            load_job(io, &mut snap, parent_id)?;
            load_continuation(io, &mut snap, continuation_id)?;
            if let Some(child_id) = child_id {
                load_job(io, &mut snap, child_id)?;
                load_epoch(io, &mut snap, child_id)?;
            }
            load_receipts_job(io, &mut snap, parent_id)?;
            load_cancels_job(io, &mut snap, parent_id)?;
        }
        Scope::Delete(id) => {
            load_job(io, &mut snap, id)?;
            load_staged_parent(io, &mut snap, id)?;
        }
        Scope::AttemptsJob(id) => load_attempts_job(io, &mut snap, id)?,
        Scope::AttemptsGuardrail(code) => load_attempts_where(
            io,
            &mut snap,
            "guardrail_code = ?1",
            &[SqlValue::text(code)],
        )?,
        Scope::AttemptsExecution(id) => {
            load_attempts_where(io, &mut snap, "execution_id = ?1", &[SqlValue::text(id)])?
        }
        Scope::PruneAttempts { cutoff_ns } => load_attempts_where(
            io,
            &mut snap,
            "finished_at <= ?1",
            &[SqlValue::Integer(*cutoff_ns)],
        )?,
        Scope::Event(id) => load_event(io, &mut snap, id)?,
        Scope::EventsPending { limit } => {
            let rows = io.query(
                "SELECT payload FROM runtime_event WHERE status = 'Pending' \
                 ORDER BY COALESCE(next_attempt_at, occurred_at) ASC, event_id ASC LIMIT ?1",
                &[SqlValue::Integer(i64::try_from(*limit).unwrap_or(i64::MAX))],
            )?;
            for row in rows {
                put_event(&mut snap, row.text(0)?)?;
            }
        }
        Scope::EventsJob(id) => {
            load_events_where(io, &mut snap, "job_id = ?1", &[SqlValue::text(id)])?
        }
        Scope::EventsThread(id) => {
            load_events_where(io, &mut snap, "thread_id = ?1", &[SqlValue::text(id)])?
        }
        Scope::EventsPrefix(prefix) => load_events_where(
            io,
            &mut snap,
            "thread_id LIKE ?1 ESCAPE '\\'",
            &[SqlValue::text(like_prefix(prefix))],
        )?,
        Scope::EventsExecution(id) => {
            load_events_where(io, &mut snap, "execution_id = ?1", &[SqlValue::text(id)])?
        }
        Scope::PruneEvents { cutoff_ns } => load_events_where(
            io,
            &mut snap,
            "status != 'Pending' AND occurred_at <= ?1",
            &[SqlValue::Integer(*cutoff_ns)],
        )?,
        Scope::Wait(id) => load_wait(io, &mut snap, id)?,
        Scope::WaitsJobSignal {
            job_id,
            signal_type,
            correlation_key,
        } => {
            let rows = io.query(
                "SELECT payload FROM runtime_wait WHERE job_id = ?1 AND signal_type = ?2 AND correlation_key = ?3",
                &[SqlValue::text(job_id), SqlValue::text(signal_type), SqlValue::text(correlation_key)],
            )?;
            for row in rows {
                put_wait(&mut snap, row.text(0)?)?;
            }
        }
        Scope::WaitsSignal {
            signal_type,
            correlation_key,
        } => load_waits_signal(io, &mut snap, signal_type, correlation_key)?,
        Scope::WaitsJob(id) => load_waits_job(io, &mut snap, id)?,
        Scope::Signal(id) => load_signal(io, &mut snap, id)?,
        Scope::SignalsKey {
            signal_type,
            correlation_key,
        } => load_signals_key(io, &mut snap, signal_type, correlation_key)?,
        Scope::Continuation(id) => load_continuation(io, &mut snap, id)?,
        Scope::ContinuationsParent(id) => {
            let rows = io.query(
                "SELECT payload FROM runtime_continuation WHERE parent_job_id = ?1 AND status = 'pending'",
                &[SqlValue::text(id)],
            )?;
            for row in rows {
                put_continuation(&mut snap, row.text(0)?)?;
            }
        }
        Scope::ContinuationChild(id) => {
            let rows = io.query(
                "SELECT payload FROM runtime_continuation WHERE child_job_id = ?1",
                &[SqlValue::text(id)],
            )?;
            for row in rows {
                put_continuation(&mut snap, row.text(0)?)?;
            }
        }
        Scope::Recurring(id) => load_recurring(io, &mut snap, id)?,
        Scope::RecurringDue { now_ns } => {
            let rows = io.query(
                "SELECT payload FROM runtime_recurring WHERE enabled = 1 AND next_run_at <= ?1 \
                 AND (lease_expires_at IS NULL OR lease_expires_at <= ?1)",
                &[SqlValue::Integer(*now_ns)],
            )?;
            for row in rows {
                put_recurring(&mut snap, row.text(0)?)?;
            }
        }
        Scope::RecurringAll => {
            let rows = io.query("SELECT payload FROM runtime_recurring", &[])?;
            for row in rows {
                put_recurring(&mut snap, row.text(0)?)?;
            }
        }
        Scope::LifecyclePending => {
            let rows = io.query(
                "SELECT payload FROM runtime_receipt WHERE delivered = 0",
                &[],
            )?;
            for row in rows {
                put_receipt(&mut snap, row.text(0)?)?;
            }
            let rows = io.query(
                "SELECT cancel_key, payload FROM runtime_cancellation WHERE delivered = 0",
                &[],
            )?;
            for row in rows {
                put_cancel(&mut snap, row.text(0)?, row.text(1)?)?;
            }
        }
        Scope::LifecycleKey(key) => {
            if let Some(id) = key.strip_prefix("control:") {
                load_cancel_key(io, &mut snap, id)?;
            } else {
                let (job_id, epoch): (String, u64) = serde_json::from_str(key).map_err(err)?;
                load_receipt(io, &mut snap, &job_id, epoch)?;
            }
        }
        Scope::Receipt { job_id, epoch } => load_receipt(io, &mut snap, job_id, *epoch)?,
        Scope::Cleanup { job_id, epoch } => load_cleanup(io, &mut snap, job_id, *epoch)?,
    }
    let state = snap.state.clone();
    let recurring = snap.recurring.clone();
    Ok(WorkingSet {
        before: snap,
        state,
        recurring,
    })
}

pub(crate) fn flush(io: &mut dyn SqlIo, working: &WorkingSet) -> Result<()> {
    flush_jobs(io, working)?;
    flush_epochs(io, working)?;
    flush_attempts(io, working)?;
    flush_events(io, working)?;
    flush_receipts(io, working)?;
    flush_cancels(io, working)?;
    flush_cleanups(io, working)?;
    flush_staged(io, working)?;
    flush_waits(io, working)?;
    flush_signals(io, working)?;
    flush_continuations(io, working)?;
    flush_recurring(io, working)?;
    Ok(())
}

pub(crate) fn insert_fresh(
    io: &mut dyn SqlIo,
    state: &NativeState,
    recurring: &HashMap<String, RecurringDefinition>,
) -> Result<()> {
    let working = WorkingSet {
        before: Snapshot::default(),
        state: state.clone(),
        recurring: recurring.clone(),
    };
    flush(io, &working)
}

pub(crate) fn allocate_id(io: &mut dyn SqlIo, prefix: &str) -> Result<String> {
    let rows = io.query(
        "INSERT INTO runtime_id_alloc (id, next) VALUES (1, 1) \
         ON CONFLICT(id) DO UPDATE SET next = next + 1 \
         RETURNING CAST(next AS TEXT)",
        &[],
    )?;
    let Some(row) = rows.first() else {
        return Err(err("identifier allocation returned no row"));
    };
    let next = row.i64(0)?;
    if next <= 0 {
        return Err(err("identifier sequence exhausted"));
    }
    Ok(format!("{prefix}-{next}"))
}

pub(crate) fn set_sequence(io: &mut dyn SqlIo, next: u64) -> Result<()> {
    let next = as_i64(next)?;
    must_cas(
        io,
        "INSERT INTO runtime_id_alloc (id, next) VALUES (1, ?1) \
         ON CONFLICT(id) DO UPDATE SET next = excluded.next \
         RETURNING id",
        &[SqlValue::Integer(next)],
        "id sequence",
    )
}

pub(crate) fn prune_retained(
    io: &mut dyn SqlIo,
    receipt_cutoff: i64,
    cleanup_cutoff: i64,
) -> Result<(usize, usize)> {
    let receipts = delete_count(
        io,
        "DELETE FROM runtime_receipt WHERE retained_at IS NOT NULL AND retained_at <= ?1 RETURNING job_id",
        receipt_cutoff,
    )? + delete_count(
        io,
        "DELETE FROM runtime_cancellation WHERE retained_at IS NOT NULL AND retained_at <= ?1 RETURNING cancel_key",
        receipt_cutoff,
    )?;
    let cleanups = delete_count(
        io,
        "DELETE FROM runtime_cleanup WHERE retained_at IS NOT NULL AND retained_at <= ?1 RETURNING job_id",
        cleanup_cutoff,
    )?;
    Ok((receipts, cleanups))
}

fn delete_count(io: &mut dyn SqlIo, sql: &str, cutoff: i64) -> Result<usize> {
    let rows = io.query(sql, &[SqlValue::Integer(cutoff)])?;
    Ok(rows.len())
}

fn like_prefix(prefix: &str) -> String {
    let mut out = String::new();
    for ch in prefix.chars() {
        if matches!(ch, '\\' | '%' | '_') {
            out.push('\\');
        }
        out.push(ch);
    }
    out.push('%');
    out
}

/// Walk due rows in `(scheduled_at, priority, job_id)` order and keep only the
/// first placement match. That key is the in-memory `min_by_key`, so stopping
/// there selects the same job. A lease that matches none still reads every due
/// row in the queue. Columns that disagree with a selectable payload fail closed.
fn load_due(
    io: &mut dyn SqlIo,
    snap: &mut Snapshot,
    queue: &str,
    now_ns: i64,
    worker: &WorkerCapabilities,
) -> Result<()> {
    let mut cursor: Option<(i64, i64, String)> = None;
    loop {
        let mut sql = String::from(
            "SELECT j.payload, e.job_id, CAST(e.generation AS TEXT), \
             CAST(j.scheduled_at AS TEXT), CAST(j.priority AS TEXT), j.job_id \
             FROM runtime_job j \
             LEFT JOIN runtime_epoch e ON e.job_id = j.job_id \
             WHERE j.queue = ?1 AND j.state = 'Enqueued' AND j.scheduled_at <= ?2 \
             AND (j.lease_expires_at IS NULL OR j.lease_expires_at <= ?2)",
        );
        let mut params = vec![SqlValue::text(queue), SqlValue::Integer(now_ns)];
        if let Some((scheduled, priority, id)) = &cursor {
            sql.push_str(
                " AND (j.scheduled_at > ?3 \
                 OR (j.scheduled_at = ?3 AND j.priority > ?4) \
                 OR (j.scheduled_at = ?3 AND j.priority = ?4 AND j.job_id > ?5))",
            );
            params.push(SqlValue::Integer(*scheduled));
            params.push(SqlValue::Integer(*priority));
            params.push(SqlValue::text(id));
        }
        sql.push_str(" ORDER BY j.scheduled_at, j.priority, j.job_id LIMIT 1");
        let rows = io.query(&sql, &params)?;
        let Some(row) = rows.into_iter().next() else {
            return Ok(());
        };
        let scheduled = row.i64(3)?;
        let priority = row.i64(4)?;
        let job_id = row.text(5)?;
        let next = (scheduled, priority, job_id);
        if cursor.as_ref().is_some_and(|previous| previous >= &next) {
            return Err(err("due scan did not advance"));
        }
        let payload = row.text(0)?;
        check_payload(&payload)?;
        let job: Job = decode_json(&payload)?;
        let payload_due = job.queue == queue
            && job.state == JobState::Enqueued
            && encode_time(job.scheduled_at) <= now_ns
            && job
                .lease_expires_at
                .map(|expiry| encode_time(expiry) <= now_ns)
                .unwrap_or(true);
        let sort_matches = job.id == next.2
            && i64::from(job.priority) == priority
            && encode_time(job.scheduled_at) == scheduled;
        if payload_due && job.placement.matches(worker) {
            if !sort_matches {
                return Err(err(
                    "due row order columns disagree with the job payload; refusing to lease",
                ));
            }
            if let Some(epoch_id) = row.text_opt(1)? {
                let generation = row.i64(2)?;
                snap.epoch.insert(epoch_id.clone(), generation);
                snap.state.epochs.insert(
                    epoch_id,
                    u64::try_from(generation).map_err(|_| err("epoch"))?,
                );
            }
            put_job(snap, payload)?;
            return Ok(());
        }
        cursor = Some(next);
    }
}

fn load_jobs_where(
    io: &mut dyn SqlIo,
    snap: &mut Snapshot,
    filter: &str,
    params: &[SqlValue],
) -> Result<()> {
    let rows = io.query(
        &format!("SELECT payload FROM runtime_job WHERE {filter}"),
        params,
    )?;
    for row in rows {
        put_job(snap, row.text(0)?)?;
    }
    Ok(())
}

fn load_job(io: &mut dyn SqlIo, snap: &mut Snapshot, id: &str) -> Result<()> {
    if snap.job_payload.contains_key(id) {
        return Ok(());
    }
    let rows = io.query(
        "SELECT payload FROM runtime_job WHERE job_id = ?1",
        &[SqlValue::text(id)],
    )?;
    if let Some(row) = rows.first() {
        put_job(snap, row.text(0)?)?;
    }
    Ok(())
}

fn put_job(snap: &mut Snapshot, payload: String) -> Result<()> {
    check_payload(&payload)?;
    let job: Job = decode_json(&payload)?;
    snap.job_payload.insert(job.id.clone(), payload);
    snap.state.jobs.insert(job.id.clone(), job);
    Ok(())
}

fn load_epoch(io: &mut dyn SqlIo, snap: &mut Snapshot, id: &str) -> Result<()> {
    if snap.epoch.contains_key(id) {
        return Ok(());
    }
    let rows = io.query(
        "SELECT CAST(generation AS TEXT) FROM runtime_epoch WHERE job_id = ?1",
        &[SqlValue::text(id)],
    )?;
    if let Some(row) = rows.first() {
        let generation = row.i64(0)?;
        snap.epoch.insert(id.to_string(), generation);
        snap.state.epochs.insert(
            id.to_string(),
            u64::try_from(generation).map_err(|_| err("epoch"))?,
        );
    }
    Ok(())
}

fn load_receipt(io: &mut dyn SqlIo, snap: &mut Snapshot, job_id: &str, epoch: u64) -> Result<()> {
    let rows = io.query(
        "SELECT payload FROM runtime_receipt WHERE job_id = ?1 AND epoch = ?2",
        &[SqlValue::text(job_id), SqlValue::Integer(as_i64(epoch)?)],
    )?;
    if let Some(row) = rows.first() {
        put_receipt(snap, row.text(0)?)?;
    }
    Ok(())
}

fn load_receipts_job(io: &mut dyn SqlIo, snap: &mut Snapshot, job_id: &str) -> Result<()> {
    let rows = io.query(
        "SELECT payload FROM runtime_receipt WHERE job_id = ?1",
        &[SqlValue::text(job_id)],
    )?;
    for row in rows {
        put_receipt(snap, row.text(0)?)?;
    }
    Ok(())
}

fn put_receipt(snap: &mut Snapshot, payload: String) -> Result<()> {
    check_payload(&payload)?;
    let receipt: ExecutionReceipt = decode_json(&payload)?;
    let epoch = as_i64(receipt.grant.epoch)?;
    snap.receipt_payload
        .insert((receipt.grant.job_id.clone(), epoch), payload);
    snap.state
        .receipts
        .insert((receipt.grant.job_id.clone(), receipt.grant.epoch), receipt);
    Ok(())
}

fn load_cleanup(io: &mut dyn SqlIo, snap: &mut Snapshot, job_id: &str, epoch: u64) -> Result<()> {
    let rows = io.query(
        "SELECT payload FROM runtime_cleanup WHERE job_id = ?1 AND epoch = ?2",
        &[SqlValue::text(job_id), SqlValue::Integer(as_i64(epoch)?)],
    )?;
    if let Some(row) = rows.first() {
        put_cleanup(snap, job_id, epoch, row.text(0)?)?;
    }
    Ok(())
}

fn put_cleanup(snap: &mut Snapshot, job_id: &str, epoch: u64, payload: String) -> Result<()> {
    check_payload(&payload)?;
    let record: CleanupRecord = decode_json(&payload)?;
    snap.cleanup_payload
        .insert((job_id.to_string(), as_i64(epoch)?), payload);
    snap.state
        .cleanup
        .insert((job_id.to_string(), epoch), record);
    Ok(())
}

fn load_cancel_key(io: &mut dyn SqlIo, snap: &mut Snapshot, key: &str) -> Result<()> {
    let rows = io.query(
        "SELECT payload FROM runtime_cancellation WHERE cancel_key = ?1",
        &[SqlValue::text(key)],
    )?;
    if let Some(row) = rows.first() {
        put_cancel(snap, key.to_string(), row.text(0)?)?;
    }
    Ok(())
}

fn load_cancels_job(io: &mut dyn SqlIo, snap: &mut Snapshot, job_id: &str) -> Result<()> {
    let rows = io.query(
        "SELECT cancel_key, payload FROM runtime_cancellation WHERE job_id = ?1",
        &[SqlValue::text(job_id)],
    )?;
    for row in rows {
        put_cancel(snap, row.text(0)?, row.text(1)?)?;
    }
    Ok(())
}

fn put_cancel(snap: &mut Snapshot, key: String, payload: String) -> Result<()> {
    check_payload(&payload)?;
    let receipt: ExecutionReceipt = decode_json(&payload)?;
    snap.cancel_payload.insert(key.clone(), payload);
    snap.state.cancellations.insert(key, receipt);
    Ok(())
}

fn load_staged_epoch(
    io: &mut dyn SqlIo,
    snap: &mut Snapshot,
    parent: &str,
    epoch: u64,
) -> Result<()> {
    let rows = io.query(
        "SELECT parent_id, CAST(epoch AS TEXT), CAST(version AS TEXT), payload FROM runtime_staged WHERE parent_id = ?1 AND epoch = ?2",
        &[SqlValue::text(parent), SqlValue::Integer(as_i64(epoch)?)],
    )?;
    for row in rows {
        put_staged(snap, row)?;
    }
    Ok(())
}

fn load_staged_parent(io: &mut dyn SqlIo, snap: &mut Snapshot, parent: &str) -> Result<()> {
    let rows = io.query(
        "SELECT parent_id, CAST(epoch AS TEXT), CAST(version AS TEXT), payload FROM runtime_staged WHERE parent_id = ?1",
        &[SqlValue::text(parent)],
    )?;
    for row in rows {
        put_staged(snap, row)?;
    }
    Ok(())
}

fn put_staged(snap: &mut Snapshot, row: SqlRow) -> Result<()> {
    let parent = row.text(0)?;
    let epoch = row.i64(1)?;
    let version = row.i64(2)?;
    let payload = row.text(3)?;
    check_payload(&payload)?;
    let effects: StagedEffects = decode_json(&payload)?;
    snap.staged_payload
        .insert((parent.clone(), epoch, version), payload);
    snap.state.staged.insert(
        (
            parent,
            u64::try_from(epoch).map_err(|_| err("epoch"))?,
            u64::try_from(version).map_err(|_| err("version"))?,
        ),
        effects,
    );
    Ok(())
}

fn expand_staged(io: &mut dyn SqlIo, snap: &mut Snapshot) -> Result<()> {
    let effects: Vec<StagedEffects> = snap.state.staged.values().cloned().collect();
    for effect in effects {
        for child in &effect.children {
            load_job(io, snap, &child.id)?;
            load_epoch(io, snap, &child.id)?;
        }
        for continuation in &effect.continuations {
            load_continuation(io, snap, &continuation.id)?;
        }
        for event in &effect.events {
            load_event(io, snap, &event.event_id)?;
        }
    }
    Ok(())
}

fn load_attempts_job(io: &mut dyn SqlIo, snap: &mut Snapshot, job_id: &str) -> Result<()> {
    load_attempts_where(
        io,
        snap,
        "job_id = ?1 ORDER BY seq ASC",
        &[SqlValue::text(job_id)],
    )
}

fn load_attempts_where(
    io: &mut dyn SqlIo,
    snap: &mut Snapshot,
    filter: &str,
    params: &[SqlValue],
) -> Result<()> {
    let rows = io.query(
        &format!("SELECT payload, CAST(seq AS TEXT) FROM runtime_attempt WHERE {filter}"),
        params,
    )?;
    let mut ordered: Vec<(i64, JobAttempt, String)> = Vec::new();
    for row in rows {
        let payload = row.text(0)?;
        check_payload(&payload)?;
        let attempt: JobAttempt = decode_json(&payload)?;
        let seq = row.i64(1)?;
        if snap.attempt_payload.contains_key(&attempt.attempt_id) {
            continue;
        }
        snap.attempt_payload
            .insert(attempt.attempt_id.clone(), payload.clone());
        snap.attempt_seq.insert(attempt.attempt_id.clone(), seq);
        ordered.push((seq, attempt, payload));
    }
    ordered.sort_by_key(|(seq, attempt, _)| (*seq, attempt.attempt_id.clone()));
    for (_, attempt, _) in ordered {
        snap.state
            .attempts
            .entry(attempt.job_id.clone())
            .or_default()
            .push(attempt);
    }
    Ok(())
}

fn load_event(io: &mut dyn SqlIo, snap: &mut Snapshot, id: &str) -> Result<()> {
    if snap.event_payload.contains_key(id) {
        return Ok(());
    }
    let rows = io.query(
        "SELECT payload FROM runtime_event WHERE event_id = ?1",
        &[SqlValue::text(id)],
    )?;
    if let Some(row) = rows.first() {
        put_event(snap, row.text(0)?)?;
    }
    Ok(())
}

fn load_events_where(
    io: &mut dyn SqlIo,
    snap: &mut Snapshot,
    filter: &str,
    params: &[SqlValue],
) -> Result<()> {
    let rows = io.query(
        &format!("SELECT payload FROM runtime_event WHERE {filter}"),
        params,
    )?;
    for row in rows {
        put_event(snap, row.text(0)?)?;
    }
    Ok(())
}

fn put_event(snap: &mut Snapshot, payload: String) -> Result<()> {
    check_payload(&payload)?;
    let event: OutboxEvent = decode_json(&payload)?;
    snap.event_payload.insert(event.event_id.clone(), payload);
    snap.state.events.insert(event.event_id.clone(), event);
    Ok(())
}

fn load_wait(io: &mut dyn SqlIo, snap: &mut Snapshot, id: &str) -> Result<()> {
    if snap.wait_payload.contains_key(id) {
        return Ok(());
    }
    let rows = io.query(
        "SELECT payload FROM runtime_wait WHERE wait_id = ?1",
        &[SqlValue::text(id)],
    )?;
    if let Some(row) = rows.first() {
        put_wait(snap, row.text(0)?)?;
    }
    Ok(())
}

fn load_waits_job(io: &mut dyn SqlIo, snap: &mut Snapshot, job_id: &str) -> Result<()> {
    let rows = io.query(
        "SELECT payload FROM runtime_wait WHERE job_id = ?1",
        &[SqlValue::text(job_id)],
    )?;
    for row in rows {
        put_wait(snap, row.text(0)?)?;
    }
    Ok(())
}

fn load_waits_signal(
    io: &mut dyn SqlIo,
    snap: &mut Snapshot,
    signal_type: &str,
    correlation_key: &str,
) -> Result<()> {
    let rows = io.query(
        "SELECT payload FROM runtime_wait WHERE signal_type = ?1 AND correlation_key = ?2",
        &[SqlValue::text(signal_type), SqlValue::text(correlation_key)],
    )?;
    for row in rows {
        put_wait(snap, row.text(0)?)?;
    }
    Ok(())
}

fn put_wait(snap: &mut Snapshot, payload: String) -> Result<()> {
    check_payload(&payload)?;
    let wait: DurableWaitRecord = decode_json(&payload)?;
    snap.wait_payload.insert(wait.wait_id.clone(), payload);
    snap.state.waits.insert(wait.wait_id.clone(), wait);
    Ok(())
}

fn load_signal(io: &mut dyn SqlIo, snap: &mut Snapshot, id: &str) -> Result<()> {
    if snap.signal_payload.contains_key(id) {
        return Ok(());
    }
    let rows = io.query(
        "SELECT payload FROM runtime_signal WHERE signal_id = ?1",
        &[SqlValue::text(id)],
    )?;
    if let Some(row) = rows.first() {
        put_signal(snap, row.text(0)?)?;
    }
    Ok(())
}

fn load_signals_key(
    io: &mut dyn SqlIo,
    snap: &mut Snapshot,
    signal_type: &str,
    correlation_key: &str,
) -> Result<()> {
    let rows = io.query(
        "SELECT payload FROM runtime_signal WHERE signal_type = ?1 AND correlation_key = ?2",
        &[SqlValue::text(signal_type), SqlValue::text(correlation_key)],
    )?;
    for row in rows {
        put_signal(snap, row.text(0)?)?;
    }
    Ok(())
}

fn put_signal(snap: &mut Snapshot, payload: String) -> Result<()> {
    check_payload(&payload)?;
    let signal: DurableSignalRecord = decode_json(&payload)?;
    snap.signal_payload
        .insert(signal.signal_id.clone(), payload);
    snap.state.signals.insert(signal.signal_id.clone(), signal);
    Ok(())
}

fn load_continuation(io: &mut dyn SqlIo, snap: &mut Snapshot, id: &str) -> Result<()> {
    if snap.continuation_payload.contains_key(id) {
        return Ok(());
    }
    let rows = io.query(
        "SELECT payload FROM runtime_continuation WHERE continuation_id = ?1",
        &[SqlValue::text(id)],
    )?;
    if let Some(row) = rows.first() {
        put_continuation(snap, row.text(0)?)?;
    }
    Ok(())
}

fn put_continuation(snap: &mut Snapshot, payload: String) -> Result<()> {
    check_payload(&payload)?;
    let record: JobContinuation = decode_json(&payload)?;
    snap.continuation_payload.insert(record.id.clone(), payload);
    snap.state.continuations.insert(record.id.clone(), record);
    Ok(())
}

fn load_recurring(io: &mut dyn SqlIo, snap: &mut Snapshot, id: &str) -> Result<()> {
    if snap.recurring_payload.contains_key(id) {
        return Ok(());
    }
    let rows = io.query(
        "SELECT payload FROM runtime_recurring WHERE recurring_id = ?1",
        &[SqlValue::text(id)],
    )?;
    if let Some(row) = rows.first() {
        put_recurring(snap, row.text(0)?)?;
    }
    Ok(())
}

fn put_recurring(snap: &mut Snapshot, payload: String) -> Result<()> {
    check_payload(&payload)?;
    let record: RecurringDefinition = decode_json(&payload)?;
    snap.recurring_payload.insert(record.id.clone(), payload);
    snap.recurring.insert(record.id.clone(), record);
    Ok(())
}

fn status_name(status: OutboxStatus) -> &'static str {
    match status {
        OutboxStatus::Pending => "Pending",
        OutboxStatus::Published => "Published",
        OutboxStatus::Failed => "Failed",
    }
}

fn wait_status(status: &DurableWaitStatus) -> &'static str {
    match status {
        DurableWaitStatus::Pending => "pending",
        DurableWaitStatus::Signaled => "signaled",
        DurableWaitStatus::TimedOut => "timed_out",
        DurableWaitStatus::Cancelled => "cancelled",
    }
}

fn flush_jobs(io: &mut dyn SqlIo, working: &WorkingSet) -> Result<()> {
    for (id, job) in &working.state.jobs {
        let payload = encode_json(job)?;
        check_payload(&payload)?;
        let params_tail = [
            SqlValue::text(&job.queue),
            SqlValue::text(job_state_name(job.state)),
            SqlValue::Integer(i64::from(job.priority)),
            SqlValue::Integer(encode_time(job.scheduled_at)),
            time_val(job.lease_expires_at),
            time_val(job.finished_at),
            SqlValue::Integer(as_i64(job.execution_epoch)?),
            SqlValue::Integer(as_i64(job.version)?),
            SqlValue::Integer(as_i64(job.incarnation)?),
            SqlValue::Integer(as_i64(job.terminal_cycle)?),
            SqlValue::Text(payload.clone()),
        ];
        match working.before.job_payload.get(id) {
            None => {
                let mut params = vec![SqlValue::text(id)];
                params.extend(params_tail);
                must_cas(
                    io,
                    "INSERT INTO runtime_job (job_id, queue, state, priority, scheduled_at, lease_expires_at, finished_at, execution_epoch, version, incarnation, terminal_cycle, payload) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12) RETURNING job_id",
                    &params,
                    "job insert",
                )?;
            }
            Some(old) if json_same(old, &payload) => {}
            Some(old) => {
                let mut params = vec![SqlValue::text(id)];
                params.extend(params_tail);
                params.push(SqlValue::Text(old.clone()));
                must_cas(
                    io,
                    "UPDATE runtime_job SET queue=?2, state=?3, priority=?4, scheduled_at=?5, lease_expires_at=?6, finished_at=?7, execution_epoch=?8, version=?9, incarnation=?10, terminal_cycle=?11, payload=?12 WHERE job_id=?1 AND payload=?13 RETURNING job_id",
                    &params,
                    "job update",
                )?;
            }
        }
    }
    for (id, old) in &working.before.job_payload {
        if !working.state.jobs.contains_key(id) {
            must_cas(
                io,
                "DELETE FROM runtime_job WHERE job_id = ?1 AND payload = ?2 RETURNING job_id",
                &[SqlValue::text(id), SqlValue::Text(old.clone())],
                "job delete",
            )?;
        }
    }
    Ok(())
}

fn flush_epochs(io: &mut dyn SqlIo, working: &WorkingSet) -> Result<()> {
    for (id, generation) in &working.state.epochs {
        let generation = as_i64(*generation)?;
        match working.before.epoch.get(id) {
            None => must_cas(
                io,
                "INSERT INTO runtime_epoch (job_id, generation) VALUES (?1, ?2) RETURNING job_id",
                &[SqlValue::text(id), SqlValue::Integer(generation)],
                "epoch insert",
            )?,
            Some(old) if *old == generation => {}
            Some(old) => must_cas(
                io,
                "UPDATE runtime_epoch SET generation = ?2 WHERE job_id = ?1 AND generation = ?3 RETURNING job_id",
                &[
                    SqlValue::text(id),
                    SqlValue::Integer(generation),
                    SqlValue::Integer(*old),
                ],
                "epoch update",
            )?,
        }
    }
    Ok(())
}

fn flush_attempts(io: &mut dyn SqlIo, working: &WorkingSet) -> Result<()> {
    let mut seen = HashSet::new();
    let mut next_seq: HashMap<String, i64> = HashMap::new();
    for (id, seq) in &working.before.attempt_seq {
        let job_id = working.before.attempt_payload.get(id).and_then(|payload| {
            decode_json::<JobAttempt>(payload)
                .ok()
                .map(|attempt| attempt.job_id)
        });
        if let Some(job_id) = job_id {
            next_seq
                .entry(job_id)
                .and_modify(|current| *current = (*current).max(*seq))
                .or_insert(*seq);
        }
    }
    for attempts in working.state.attempts.values() {
        for attempt in attempts {
            if !seen.insert(attempt.attempt_id.clone()) {
                return Err(err(format!("duplicate attempt id {}", attempt.attempt_id)));
            }
            let payload = encode_json(attempt)?;
            check_payload(&payload)?;
            match working.before.attempt_payload.get(&attempt.attempt_id) {
                None => {
                    let seq = next_seq.entry(attempt.job_id.clone()).or_insert(0);
                    *seq = seq.saturating_add(1);
                    must_cas(
                        io,
                        "INSERT INTO runtime_attempt (attempt_id, job_id, attempt_number, guardrail_code, execution_id, finished_at, seq, payload) VALUES (?1,?2,?3,?4,?5,?6,?7,?8) RETURNING attempt_id",
                        &[
                            SqlValue::text(&attempt.attempt_id),
                            SqlValue::text(&attempt.job_id),
                            SqlValue::Integer(i64::from(attempt.attempt_number)),
                            opt_text(&attempt.guardrail_code),
                            opt_text(&attempt.execution_id),
                            SqlValue::Integer(encode_time(attempt.finished_at)),
                            SqlValue::Integer(*seq),
                            SqlValue::Text(payload),
                        ],
                        "attempt insert",
                    )?;
                }
                Some(old) if json_same(old, &payload) => {}
                Some(old) => {
                    let seq = working
                        .before
                        .attempt_seq
                        .get(&attempt.attempt_id)
                        .copied()
                        .unwrap_or(0);
                    must_cas(
                        io,
                        "UPDATE runtime_attempt SET job_id=?2, attempt_number=?3, guardrail_code=?4, execution_id=?5, finished_at=?6, seq=?7, payload=?8 WHERE attempt_id=?1 AND payload=?9 RETURNING attempt_id",
                        &[
                            SqlValue::text(&attempt.attempt_id),
                            SqlValue::text(&attempt.job_id),
                            SqlValue::Integer(i64::from(attempt.attempt_number)),
                            opt_text(&attempt.guardrail_code),
                            opt_text(&attempt.execution_id),
                            SqlValue::Integer(encode_time(attempt.finished_at)),
                            SqlValue::Integer(seq),
                            SqlValue::Text(payload),
                            SqlValue::Text(old.clone()),
                        ],
                        "attempt update",
                    )?;
                }
            }
        }
    }
    for (id, old) in &working.before.attempt_payload {
        if !seen.contains(id) {
            must_cas(
                io,
                "DELETE FROM runtime_attempt WHERE attempt_id = ?1 AND payload = ?2 RETURNING attempt_id",
                &[SqlValue::text(id), SqlValue::Text(old.clone())],
                "attempt delete",
            )?;
        }
    }
    Ok(())
}

fn flush_events(io: &mut dyn SqlIo, working: &WorkingSet) -> Result<()> {
    for (id, event) in &working.state.events {
        let payload = encode_json(event)?;
        check_payload(&payload)?;
        let cols = [
            SqlValue::text(&event.event.job_id),
            SqlValue::text(status_name(event.status.clone())),
            opt_text(&event.event.thread_id),
            opt_text(&event.event.execution_id),
            SqlValue::Integer(encode_time(event.event.occurred_at)),
            time_val(event.next_attempt_at),
            SqlValue::Text(payload.clone()),
        ];
        match working.before.event_payload.get(id) {
            None => {
                let mut params = vec![SqlValue::text(id)];
                params.extend(cols);
                must_cas(
                    io,
                    "INSERT INTO runtime_event (event_id, job_id, status, thread_id, execution_id, occurred_at, next_attempt_at, payload) VALUES (?1,?2,?3,?4,?5,?6,?7,?8) RETURNING event_id",
                    &params,
                    "event insert",
                )?;
            }
            Some(old) if json_same(old, &payload) => {}
            Some(old) => {
                let mut params = vec![SqlValue::text(id)];
                params.extend(cols);
                params.push(SqlValue::Text(old.clone()));
                must_cas(
                    io,
                    "UPDATE runtime_event SET job_id=?2, status=?3, thread_id=?4, execution_id=?5, occurred_at=?6, next_attempt_at=?7, payload=?8 WHERE event_id=?1 AND payload=?9 RETURNING event_id",
                    &params,
                    "event update",
                )?;
            }
        }
    }
    for (id, old) in &working.before.event_payload {
        if !working.state.events.contains_key(id) {
            must_cas(
                io,
                "DELETE FROM runtime_event WHERE event_id = ?1 AND payload = ?2 RETURNING event_id",
                &[SqlValue::text(id), SqlValue::Text(old.clone())],
                "event delete",
            )?;
        }
    }
    Ok(())
}

fn flush_receipts(io: &mut dyn SqlIo, working: &WorkingSet) -> Result<()> {
    for ((job_id, epoch), receipt) in &working.state.receipts {
        let payload = encode_json(receipt)?;
        check_payload(&payload)?;
        let epoch_i = as_i64(*epoch)?;
        let snapshot: Job = decode_json(&receipt.job_json)?;
        let cols = receipt_cols(receipt, &snapshot, &payload, epoch_i)?;
        match working
            .before
            .receipt_payload
            .get(&(job_id.clone(), epoch_i))
        {
            None => {
                let mut params = vec![SqlValue::text(job_id)];
                params.extend(cols);
                must_cas(
                    io,
                    "INSERT INTO runtime_receipt (job_id, epoch, delivered, retained_at, digest, snapshot_version, snapshot_incarnation, snapshot_cycle, snapshot_state, payload) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10) RETURNING job_id",
                    &params,
                    "receipt insert",
                )?;
            }
            Some(old) if json_same(old, &payload) => {}
            Some(old) => {
                let mut params = vec![SqlValue::text(job_id)];
                params.extend(cols);
                params.push(SqlValue::Text(old.clone()));
                must_cas(
                    io,
                    "UPDATE runtime_receipt SET epoch=?2, delivered=?3, retained_at=?4, digest=?5, snapshot_version=?6, snapshot_incarnation=?7, snapshot_cycle=?8, snapshot_state=?9, payload=?10 WHERE job_id=?1 AND epoch=?2 AND payload=?11 RETURNING job_id",
                    &params,
                    "receipt update",
                )?;
            }
        }
    }
    for ((job_id, epoch), old) in &working.before.receipt_payload {
        let epoch_u = u64::try_from(*epoch).unwrap_or(0);
        if !working
            .state
            .receipts
            .contains_key(&(job_id.clone(), epoch_u))
        {
            must_cas(
                io,
                "DELETE FROM runtime_receipt WHERE job_id = ?1 AND epoch = ?2 AND payload = ?3 RETURNING job_id",
                &[
                    SqlValue::text(job_id),
                    SqlValue::Integer(*epoch),
                    SqlValue::Text(old.clone()),
                ],
                "receipt delete",
            )?;
        }
    }
    Ok(())
}

fn receipt_cols(
    receipt: &ExecutionReceipt,
    snapshot: &Job,
    payload: &str,
    epoch: i64,
) -> Result<[SqlValue; 9]> {
    Ok([
        SqlValue::Integer(epoch),
        SqlValue::Integer(i64::from(receipt.delivered)),
        time_val(receipt.retained_at),
        SqlValue::text(&receipt.digest),
        SqlValue::Integer(as_i64(snapshot.version)?),
        SqlValue::Integer(as_i64(snapshot.incarnation)?),
        SqlValue::Integer(as_i64(snapshot.terminal_cycle)?),
        SqlValue::text(job_state_name(snapshot.state)),
        SqlValue::Text(payload.to_string()),
    ])
}

fn flush_cancels(io: &mut dyn SqlIo, working: &WorkingSet) -> Result<()> {
    for (key, receipt) in &working.state.cancellations {
        let payload = encode_json(receipt)?;
        check_payload(&payload)?;
        let cols = [
            SqlValue::text(&receipt.grant.job_id),
            SqlValue::Integer(i64::from(receipt.delivered)),
            time_val(receipt.retained_at),
            SqlValue::Text(payload.clone()),
        ];
        match working.before.cancel_payload.get(key) {
            None => {
                let mut params = vec![SqlValue::text(key)];
                params.extend(cols);
                must_cas(
                    io,
                    "INSERT INTO runtime_cancellation (cancel_key, job_id, delivered, retained_at, payload) VALUES (?1,?2,?3,?4,?5) RETURNING cancel_key",
                    &params,
                    "cancellation insert",
                )?;
            }
            Some(old) if json_same(old, &payload) => {}
            Some(old) => {
                let mut params = vec![SqlValue::text(key)];
                params.extend(cols);
                params.push(SqlValue::Text(old.clone()));
                must_cas(
                    io,
                    "UPDATE runtime_cancellation SET job_id=?2, delivered=?3, retained_at=?4, payload=?5 WHERE cancel_key=?1 AND payload=?6 RETURNING cancel_key",
                    &params,
                    "cancellation update",
                )?;
            }
        }
    }
    for (key, old) in &working.before.cancel_payload {
        if !working.state.cancellations.contains_key(key) {
            must_cas(
                io,
                "DELETE FROM runtime_cancellation WHERE cancel_key = ?1 AND payload = ?2 RETURNING cancel_key",
                &[SqlValue::text(key), SqlValue::Text(old.clone())],
                "cancellation delete",
            )?;
        }
    }
    Ok(())
}

fn flush_cleanups(io: &mut dyn SqlIo, working: &WorkingSet) -> Result<()> {
    for ((job_id, epoch), record) in &working.state.cleanup {
        let payload = encode_json(record)?;
        check_payload(&payload)?;
        let epoch_i = as_i64(*epoch)?;
        match working
            .before
            .cleanup_payload
            .get(&(job_id.clone(), epoch_i))
        {
            None => must_cas(
                io,
                "INSERT INTO runtime_cleanup (job_id, epoch, retained_at, payload) VALUES (?1,?2,?3,?4) RETURNING job_id",
                &[
                    SqlValue::text(job_id),
                    SqlValue::Integer(epoch_i),
                    time_val(record.retained_at),
                    SqlValue::Text(payload),
                ],
                "cleanup insert",
            )?,
            Some(old) if json_same(old, &payload) => {}
            Some(old) => must_cas(
                io,
                "UPDATE runtime_cleanup SET retained_at=?3, payload=?4 WHERE job_id=?1 AND epoch=?2 AND payload=?5 RETURNING job_id",
                &[
                    SqlValue::text(job_id),
                    SqlValue::Integer(epoch_i),
                    time_val(record.retained_at),
                    SqlValue::Text(payload),
                    SqlValue::Text(old.clone()),
                ],
                "cleanup update",
            )?,
        }
    }
    for ((job_id, epoch), old) in &working.before.cleanup_payload {
        let epoch_u = u64::try_from(*epoch).unwrap_or(0);
        if !working
            .state
            .cleanup
            .contains_key(&(job_id.clone(), epoch_u))
        {
            must_cas(
                io,
                "DELETE FROM runtime_cleanup WHERE job_id=?1 AND epoch=?2 AND payload=?3 RETURNING job_id",
                &[
                    SqlValue::text(job_id),
                    SqlValue::Integer(*epoch),
                    SqlValue::Text(old.clone()),
                ],
                "cleanup delete",
            )?;
        }
    }
    Ok(())
}

fn flush_staged(io: &mut dyn SqlIo, working: &WorkingSet) -> Result<()> {
    for ((parent, epoch, version), effects) in &working.state.staged {
        let payload = encode_json(effects)?;
        check_payload(&payload)?;
        let epoch_i = as_i64(*epoch)?;
        let version_i = as_i64(*version)?;
        match working
            .before
            .staged_payload
            .get(&(parent.clone(), epoch_i, version_i))
        {
            None => must_cas(
                io,
                "INSERT INTO runtime_staged (parent_id, epoch, version, payload) VALUES (?1,?2,?3,?4) RETURNING parent_id",
                &[
                    SqlValue::text(parent),
                    SqlValue::Integer(epoch_i),
                    SqlValue::Integer(version_i),
                    SqlValue::Text(payload),
                ],
                "staged insert",
            )?,
            Some(old) if json_same(old, &payload) => {}
            Some(old) => must_cas(
                io,
                "UPDATE runtime_staged SET payload=?4 WHERE parent_id=?1 AND epoch=?2 AND version=?3 AND payload=?5 RETURNING parent_id",
                &[
                    SqlValue::text(parent),
                    SqlValue::Integer(epoch_i),
                    SqlValue::Integer(version_i),
                    SqlValue::Text(payload),
                    SqlValue::Text(old.clone()),
                ],
                "staged update",
            )?,
        }
    }
    for ((parent, epoch, version), old) in &working.before.staged_payload {
        let key = (
            parent.clone(),
            u64::try_from(*epoch).unwrap_or(0),
            u64::try_from(*version).unwrap_or(0),
        );
        if !working.state.staged.contains_key(&key) {
            must_cas(
                io,
                "DELETE FROM runtime_staged WHERE parent_id=?1 AND epoch=?2 AND version=?3 AND payload=?4 RETURNING parent_id",
                &[
                    SqlValue::text(parent),
                    SqlValue::Integer(*epoch),
                    SqlValue::Integer(*version),
                    SqlValue::Text(old.clone()),
                ],
                "staged delete",
            )?;
        }
    }
    Ok(())
}

fn flush_waits(io: &mut dyn SqlIo, working: &WorkingSet) -> Result<()> {
    for (id, wait) in &working.state.waits {
        let payload = encode_json(wait)?;
        check_payload(&payload)?;
        let cols = [
            SqlValue::text(&wait.job_id),
            SqlValue::Integer(as_i64(wait.incarnation)?),
            SqlValue::text(&wait.signal_type),
            SqlValue::text(&wait.correlation_key),
            SqlValue::text(wait_status(&wait.status)),
            SqlValue::Text(payload.clone()),
        ];
        match working.before.wait_payload.get(id) {
            None => {
                let mut params = vec![SqlValue::text(id)];
                params.extend(cols);
                must_cas(
                    io,
                    "INSERT INTO runtime_wait (wait_id, job_id, incarnation, signal_type, correlation_key, status, payload) VALUES (?1,?2,?3,?4,?5,?6,?7) RETURNING wait_id",
                    &params,
                    "wait insert",
                )?;
            }
            Some(old) if json_same(old, &payload) => {}
            Some(old) => {
                let mut params = vec![SqlValue::text(id)];
                params.extend(cols);
                params.push(SqlValue::Text(old.clone()));
                must_cas(
                    io,
                    "UPDATE runtime_wait SET job_id=?2, incarnation=?3, signal_type=?4, correlation_key=?5, status=?6, payload=?7 WHERE wait_id=?1 AND payload=?8 RETURNING wait_id",
                    &params,
                    "wait update",
                )?;
            }
        }
    }
    for (id, old) in &working.before.wait_payload {
        if !working.state.waits.contains_key(id) {
            must_cas(
                io,
                "DELETE FROM runtime_wait WHERE wait_id=?1 AND payload=?2 RETURNING wait_id",
                &[SqlValue::text(id), SqlValue::Text(old.clone())],
                "wait delete",
            )?;
        }
    }
    Ok(())
}

fn flush_signals(io: &mut dyn SqlIo, working: &WorkingSet) -> Result<()> {
    for (id, signal) in &working.state.signals {
        let payload = encode_json(signal)?;
        check_payload(&payload)?;
        match working.before.signal_payload.get(id) {
            None => must_cas(
                io,
                "INSERT INTO runtime_signal (signal_id, signal_type, correlation_key, created_at, payload) VALUES (?1,?2,?3,?4,?5) RETURNING signal_id",
                &[
                    SqlValue::text(id),
                    SqlValue::text(&signal.signal_type),
                    SqlValue::text(&signal.correlation_key),
                    SqlValue::Integer(encode_time(signal.created_at)),
                    SqlValue::Text(payload),
                ],
                "signal insert",
            )?,
            Some(old) if json_same(old, &payload) => {}
            Some(old) => must_cas(
                io,
                "UPDATE runtime_signal SET signal_type=?2, correlation_key=?3, created_at=?4, payload=?5 WHERE signal_id=?1 AND payload=?6 RETURNING signal_id",
                &[
                    SqlValue::text(id),
                    SqlValue::text(&signal.signal_type),
                    SqlValue::text(&signal.correlation_key),
                    SqlValue::Integer(encode_time(signal.created_at)),
                    SqlValue::Text(payload),
                    SqlValue::Text(old.clone()),
                ],
                "signal update",
            )?,
        }
    }
    for (id, old) in &working.before.signal_payload {
        if !working.state.signals.contains_key(id) {
            must_cas(
                io,
                "DELETE FROM runtime_signal WHERE signal_id=?1 AND payload=?2 RETURNING signal_id",
                &[SqlValue::text(id), SqlValue::Text(old.clone())],
                "signal delete",
            )?;
        }
    }
    Ok(())
}

fn flush_continuations(io: &mut dyn SqlIo, working: &WorkingSet) -> Result<()> {
    for (id, record) in &working.state.continuations {
        let payload = encode_json(record)?;
        check_payload(&payload)?;
        let cols = [
            SqlValue::text(&record.parent_job_id),
            opt_text(&record.child_job_id),
            SqlValue::text(record.status.as_str()),
            SqlValue::Text(payload.clone()),
        ];
        match working.before.continuation_payload.get(id) {
            None => {
                let mut params = vec![SqlValue::text(id)];
                params.extend(cols);
                must_cas(
                    io,
                    "INSERT INTO runtime_continuation (continuation_id, parent_job_id, child_job_id, status, payload) VALUES (?1,?2,?3,?4,?5) RETURNING continuation_id",
                    &params,
                    "continuation insert",
                )?;
            }
            Some(old) if json_same(old, &payload) => {}
            Some(old) => {
                let mut params = vec![SqlValue::text(id)];
                params.extend(cols);
                params.push(SqlValue::Text(old.clone()));
                must_cas(
                    io,
                    "UPDATE runtime_continuation SET parent_job_id=?2, child_job_id=?3, status=?4, payload=?5 WHERE continuation_id=?1 AND payload=?6 RETURNING continuation_id",
                    &params,
                    "continuation update",
                )?;
            }
        }
    }
    for (id, old) in &working.before.continuation_payload {
        if !working.state.continuations.contains_key(id) {
            must_cas(
                io,
                "DELETE FROM runtime_continuation WHERE continuation_id=?1 AND payload=?2 RETURNING continuation_id",
                &[SqlValue::text(id), SqlValue::Text(old.clone())],
                "continuation delete",
            )?;
        }
    }
    Ok(())
}

fn flush_recurring(io: &mut dyn SqlIo, working: &WorkingSet) -> Result<()> {
    for (id, record) in &working.recurring {
        let payload = encode_json(record)?;
        check_payload(&payload)?;
        let cols = [
            SqlValue::Integer(i64::from(record.enabled)),
            SqlValue::Integer(encode_time(record.next_run_at)),
            time_val(record.lease_expires_at),
            SqlValue::Text(payload.clone()),
        ];
        match working.before.recurring_payload.get(id) {
            None => {
                let mut params = vec![SqlValue::text(id)];
                params.extend(cols);
                must_cas(
                    io,
                    "INSERT INTO runtime_recurring (recurring_id, enabled, next_run_at, lease_expires_at, payload) VALUES (?1,?2,?3,?4,?5) RETURNING recurring_id",
                    &params,
                    "recurring insert",
                )?;
            }
            Some(old) if json_same(old, &payload) => {}
            Some(old) => {
                let mut params = vec![SqlValue::text(id)];
                params.extend(cols);
                params.push(SqlValue::Text(old.clone()));
                must_cas(
                    io,
                    "UPDATE runtime_recurring SET enabled=?2, next_run_at=?3, lease_expires_at=?4, payload=?5 WHERE recurring_id=?1 AND payload=?6 RETURNING recurring_id",
                    &params,
                    "recurring update",
                )?;
            }
        }
    }
    for (id, old) in &working.before.recurring_payload {
        if !working.recurring.contains_key(id) {
            must_cas(
                io,
                "DELETE FROM runtime_recurring WHERE recurring_id=?1 AND payload=?2 RETURNING recurring_id",
                &[SqlValue::text(id), SqlValue::Text(old.clone())],
                "recurring delete",
            )?;
        }
    }
    Ok(())
}
