//! One-shot import of pre-row runtime data into the row schema.
//!
//! Sources are a format-1 `runtime_safety_ledger` and the v1 runtime tables.
//! The import runs inside the schema transaction. It refuses when both sources
//! hold data, when the ledger format or JSON cannot be read, or when a v1 wait
//! or continuation has no parent job. In-flight v1 leases are re-enqueued.
use super::atomic_state::Ledger;
use super::codec::{decode_json, decode_time, sql_err};
use super::executor::SqlRow;
use super::row_engine::{self, SqlIo};
use crate::application::runtime::in_memory_runtime::NativeState;
use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::durable_wait::{
    DurableSignalRecord, DurableWaitRecord, DurableWaitStatus,
};
use crate::domain::runtime::job::{BackoffPolicy, Job, JobState};
use crate::domain::runtime::job_attempt::{JobAttempt, JobAttemptOutcome};
use crate::domain::runtime::job_continuation::{
    ContinuationStatus, ContinuationTrigger, JobContinuation,
};
use crate::domain::runtime::outbox::{OutboxEvent, OutboxStatus, RuntimeEvent, RuntimeEventType};
use crate::domain::runtime::provenance::{ProvenanceRef, SttpProvenanceAdapter};
use crate::domain::runtime::recurring::RecurringDefinition;
use chrono::Utc;
use std::collections::HashMap;

fn err(message: impl std::fmt::Display) -> StasisError {
    sql_err("runtime storage migration", message)
}

pub(crate) fn import_legacy_sources(io: &mut dyn SqlIo) -> Result<()> {
    let ledger = read_ledger(io)?;
    let v1_core = count(
        io,
        "SELECT CAST((SELECT COUNT(*) FROM job) + (SELECT COUNT(*) FROM job_attempt) + (SELECT COUNT(*) FROM outbox_event) + (SELECT COUNT(*) FROM durable_wait) + (SELECT COUNT(*) FROM durable_signal) + (SELECT COUNT(*) FROM job_continuation) AS TEXT)",
    )?;
    let v1_recurring = count(
        io,
        "SELECT CAST(COUNT(*) AS TEXT) FROM recurring_definition",
    )?;
    let ledger_nonempty = ledger.as_ref().is_some_and(ledger_has_data);
    if ledger_nonempty && (v1_core > 0 || v1_recurring > 0) {
        return Err(err(
            "both a safety ledger and v1 runtime rows are populated; refusing to guess which source is canonical",
        ));
    }
    if ledger_nonempty {
        let ledger = ledger.ok_or_else(|| err("ledger disappeared"))?;
        let mut state = ledger.state;
        stamp_retention(&mut state);
        let sequence = occupied_sequence(
            &state,
            &ledger.recurring,
            ledger.sequence.max(sequence_floor(io)?),
        );
        row_engine::insert_fresh(io, &state, &ledger.recurring)?;
        if sequence > 0 {
            row_engine::set_sequence(io, sequence)?;
        }
    } else if v1_core > 0 || v1_recurring > 0 {
        let (state, recurring) = read_v1(io)?;
        let sequence = occupied_sequence(&state, &recurring, sequence_floor(io)?);
        row_engine::insert_fresh(io, &state, &recurring)?;
        if sequence > 0 {
            row_engine::set_sequence(io, sequence)?;
        }
    }
    io.exec("DELETE FROM job", &[])?;
    io.exec("DELETE FROM job_attempt", &[])?;
    io.exec("DELETE FROM outbox_event", &[])?;
    io.exec("DELETE FROM durable_wait", &[])?;
    io.exec("DELETE FROM durable_signal", &[])?;
    io.exec("DELETE FROM job_continuation", &[])?;
    io.exec("DELETE FROM recurring_definition", &[])?;
    io.exec("DELETE FROM runtime_safety_ledger", &[])?;
    Ok(())
}

fn stamp_retention(state: &mut NativeState) {
    let now = Utc::now();
    for receipt in state.receipts.values_mut() {
        if receipt.retained_at.is_none() {
            receipt.retained_at = Some(now);
        }
    }
    for receipt in state.cancellations.values_mut() {
        if receipt.retained_at.is_none() {
            receipt.retained_at = Some(now);
        }
    }
    for record in state.cleanup.values_mut() {
        if record.retained_at.is_none() {
            record.retained_at = Some(now);
        }
    }
}

fn ledger_has_data(ledger: &Ledger) -> bool {
    let state = &ledger.state;
    ledger.sequence > 0
        || !ledger.recurring.is_empty()
        || !state.jobs.is_empty()
        || !state.continuations.is_empty()
        || !state.staged.is_empty()
        || !state.waits.is_empty()
        || !state.signals.is_empty()
        || !state.epochs.is_empty()
        || !state.attempts.is_empty()
        || !state.events.is_empty()
        || !state.receipts.is_empty()
        || !state.cleanup.is_empty()
        || !state.cancellations.is_empty()
}

fn read_ledger(io: &mut dyn SqlIo) -> Result<Option<Ledger>> {
    let rows = io.query(
        "SELECT CAST(format AS TEXT), payload FROM runtime_safety_ledger WHERE id = 1",
        &[],
    )?;
    let Some(row) = rows.into_iter().next() else {
        return Ok(None);
    };
    if row.i64(0)? != 1 {
        return Err(err(format!(
            "unsupported safety ledger format {}; refusing to import",
            row.text(0)?
        )));
    }
    let payload = row.text(1)?;
    serde_json::from_str(&payload).map(Some).map_err(|error| {
        err(format!(
            "safety ledger payload is not readable and was not imported: {error}"
        ))
    })
}

fn sequence_floor(io: &mut dyn SqlIo) -> Result<u64> {
    let exists = io.query(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'stasis_id_seq'",
        &[],
    )?;
    if exists.is_empty() {
        return Ok(0);
    }
    let rows = io.query(
        "SELECT CAST(COALESCE(MAX(next), 0) AS TEXT) FROM stasis_id_seq",
        &[],
    )?;
    let value = rows.first().map(|row| row.i64(0)).transpose()?.unwrap_or(0);
    u64::try_from(value).map_err(|_| err("negative id sequence"))
}

fn occupied_sequence(
    state: &NativeState,
    recurring: &HashMap<String, RecurringDefinition>,
    floor: u64,
) -> u64 {
    let mut sequence = floor;
    let bump = |sequence: &mut u64, id: &str| {
        if let Some((_, suffix)) = id.rsplit_once('-')
            && let Ok(value) = suffix.parse::<u64>()
        {
            *sequence = (*sequence).max(value);
        }
    };
    for id in state.jobs.keys() {
        bump(&mut sequence, id);
    }
    for attempts in state.attempts.values() {
        for attempt in attempts {
            bump(&mut sequence, &attempt.attempt_id);
        }
    }
    for id in state.events.keys() {
        bump(&mut sequence, id);
    }
    for id in state.waits.keys() {
        bump(&mut sequence, id);
    }
    for id in state.signals.keys() {
        bump(&mut sequence, id);
    }
    for id in state.continuations.keys() {
        bump(&mut sequence, id);
    }
    for id in recurring.keys() {
        bump(&mut sequence, id);
    }
    sequence
}

fn count(io: &mut dyn SqlIo, sql: &str) -> Result<i64> {
    let rows = io.query(sql, &[])?;
    rows.first().ok_or_else(|| err("count missing"))?.i64(0)
}

fn read_v1(io: &mut dyn SqlIo) -> Result<(NativeState, HashMap<String, RecurringDefinition>)> {
    let mut state = NativeState::default();
    for row in io.query(
        "SELECT job_id, queue, job_type, payload_ref, state, CAST(priority AS TEXT), CAST(attempts AS TEXT), CAST(max_attempts AS TEXT), CAST(backoff_base_delay_seconds AS TEXT), CAST(backoff_max_delay_seconds AS TEXT), idempotency_key, correlation_id, causation_id, trace_id, sttp_input_node_id, sttp_output_node_id, input_provenance_json, output_provenance_json, placement_json, lease_owner, CAST(lease_expires_at AS TEXT), CAST(heartbeat_at AS TEXT), CAST(scheduled_at AS TEXT), CAST(started_at AS TEXT), CAST(finished_at AS TEXT), last_error, progress_json FROM job",
        &[],
    )? {
        let job = map_v1_job(&row)?;
        let id = job.id.clone();
        if state.jobs.insert(id.clone(), job).is_some() {
            return Err(err(format!("duplicate v1 job id {id}")));
        }
        state.epochs.insert(id, 1);
    }
    for row in io.query(
        "SELECT attempt_id, job_id, CAST(attempt_number AS TEXT), worker_id, CAST(started_at AS TEXT), CAST(finished_at AS TEXT), outcome, error_message, sttp_output_node_id, output_provenance_json, execution_id, guardrail_code, policy_reason, CAST(duration_ms AS TEXT), diagnostics FROM job_attempt",
        &[],
    )? {
        let attempt = map_v1_attempt(&row)?;
        state.attempts.entry(attempt.job_id.clone()).or_default().push(attempt);
    }
    for row in io.query(
        "SELECT event_id, status, CAST(publish_attempts AS TEXT), CAST(published_at AS TEXT), CAST(next_attempt_at AS TEXT), last_publish_error, event_type, job_id, thread_id, correlation_id, causation_id, trace_id, sttp_input_node_id, sttp_output_node_id, input_provenance_json, output_provenance_json, execution_id, input_memory_query_id, input_memory_query_fingerprint, output_memory_node_id, retrieval_path, CAST(occurred_at AS TEXT), message FROM outbox_event",
        &[],
    )? {
        let event = map_v1_event(&row)?;
        if state.events.insert(event.event_id.clone(), event.clone()).is_some() {
            return Err(err(format!("duplicate v1 outbox event {}", event.event_id)));
        }
    }
    for row in io.query(
        "SELECT wait_id, job_id, signal_type, correlation_key, status, CAST(deadline_at AS TEXT), CAST(created_at AS TEXT), CAST(updated_at AS TEXT), signal_payload, consumed_signal_ids_json FROM durable_wait",
        &[],
    )? {
        let mut wait = map_v1_wait(&row)?;
        let parent = state.jobs.get(&wait.job_id).ok_or_else(|| {
            err(format!("v1 wait {} has no job {}; refusing to bind it to a later job", wait.wait_id, wait.job_id))
        })?;
        wait.incarnation = parent.incarnation;
        if state.waits.insert(wait.wait_id.clone(), wait.clone()).is_some() {
            return Err(err(format!("duplicate v1 wait {}", wait.wait_id)));
        }
    }
    for row in io.query(
        "SELECT signal_id, signal_type, correlation_key, payload_json, CAST(created_at AS TEXT) FROM durable_signal",
        &[],
    )? {
        let signal = DurableSignalRecord {
            signal_id: row.text(0)?,
            target_incarnations: None,
            signal_type: row.text(1)?,
            correlation_key: row.text(2)?,
            payload_json: row.text(3)?,
            created_at: decode_time(row.i64(4)?)?,
        };
        if state.signals.insert(signal.signal_id.clone(), signal.clone()).is_some() {
            return Err(err(format!("duplicate v1 signal {}", signal.signal_id)));
        }
    }
    for row in io.query(
        "SELECT continuation_id, parent_job_id, trigger, status, child_spec_json, child_job_id, parent_output_json, claim_token, CAST(created_at AS TEXT) FROM job_continuation",
        &[],
    )? {
        let mut record = map_v1_continuation(&row)?;
        let parent = state.jobs.get(&record.parent_job_id).ok_or_else(|| {
            err(format!("v1 continuation {} has no parent job {}; refusing to adopt a later job", record.id, record.parent_job_id))
        })?;
        record.parent_incarnation = parent.incarnation;
        record.parent_terminal_cycle = parent.terminal_cycle;
        record.parent_version_floor = Some(parent.version);
        if state.continuations.insert(record.id.clone(), record.clone()).is_some() {
            return Err(err(format!("duplicate v1 continuation {}", record.id)));
        }
    }
    let mut recurring = HashMap::new();
    for row in io.query(
        "SELECT recurring_id, queue, job_type, payload_template_ref, cron_expr, timezone, CAST(jitter_seconds AS TEXT), CAST(enabled AS TEXT), CAST(max_attempts AS TEXT), CAST(next_run_at AS TEXT), CAST(last_run_at AS TEXT), lease_owner, CAST(lease_expires_at AS TEXT) FROM recurring_definition",
        &[],
    )? {
        let record = map_v1_recurring(&row)?;
        recurring.insert(record.id.clone(), record);
    }
    Ok((state, recurring))
}

fn map_v1_job(row: &SqlRow) -> Result<Job> {
    let state = match row.text(4)?.as_str() {
        "enqueued" => JobState::Enqueued,
        "leased" => JobState::Leased,
        "running" => JobState::Running,
        "succeeded" => JobState::Succeeded,
        "failed" => JobState::Failed,
        "dead_letter" => JobState::DeadLetter,
        "canceled" => JobState::Canceled,
        other => {
            return Err(err(format!(
                "v1 job {} has unknown state {other}",
                row.text(0)?
            )));
        }
    };
    let in_flight = matches!(state, JobState::Leased | JobState::Running);
    let input = SttpProvenanceAdapter::merge_legacy(
        row.text_opt(16)?
            .as_deref()
            .and_then(|raw| decode_json::<ProvenanceRef>(raw).ok()),
        &row.text(14)?,
    );
    let legacy_output = row.text_opt(15)?;
    let output = row
        .text_opt(17)?
        .as_deref()
        .and_then(|raw| decode_json::<ProvenanceRef>(raw).ok())
        .or_else(|| {
            legacy_output
                .as_deref()
                .and_then(SttpProvenanceAdapter::from_compat_string)
        });
    let job_id = row.text(0)?;
    let priority = i32::try_from(row.i64(5)?)
        .map_err(|_| err(format!("v1 job {job_id} priority does not fit")))?;
    let attempts = u32::try_from(row.i64(6)?)
        .map_err(|_| err(format!("v1 job {job_id} attempts do not fit")))?;
    let max_attempts = u32::try_from(row.i64(7)?)
        .map_err(|_| err(format!("v1 job {job_id} max_attempts does not fit")))?;
    Ok(Job {
        id: job_id,
        queue: row.text(1)?,
        job_type: row.text(2)?,
        payload_ref: row.text(3)?,
        state: if in_flight { JobState::Enqueued } else { state },
        priority,
        attempts,
        max_attempts,
        backoff_policy: BackoffPolicy {
            base_delay_seconds: row.i64(8)?,
            max_delay_seconds: row.i64(9)?,
        },
        idempotency_key: row.text(10)?,
        correlation_id: row.text(11)?,
        causation_id: row.text(12)?,
        trace_id: row.text(13)?,
        input_provenance: input,
        output_provenance: output,
        placement: row
            .text_opt(18)?
            .as_deref()
            .and_then(|raw| decode_json(raw).ok())
            .unwrap_or_default(),
        execution_epoch: 0,
        incarnation: 1,
        terminal_cycle: 1,
        wakeup_pending: false,
        version: 0,
        lease_owner: None,
        lease_expires_at: None,
        heartbeat_at: None,
        scheduled_at: decode_time(row.i64(22)?)?,
        started_at: row.i64_opt(23)?.map(decode_time).transpose()?,
        finished_at: row.i64_opt(24)?.map(decode_time).transpose()?,
        last_error: row.text_opt(25)?,
        progress_json: row.text_opt(26)?,
    })
}

fn map_v1_attempt(row: &SqlRow) -> Result<JobAttempt> {
    let outcome = match row.text(6)?.as_str() {
        "succeeded" => JobAttemptOutcome::Succeeded,
        "retryable_failure" => JobAttemptOutcome::RetryableFailure,
        "fatal_failure" => JobAttemptOutcome::FatalFailure,
        "deferred" => JobAttemptOutcome::Deferred,
        other => {
            return Err(err(format!(
                "v1 attempt {} has unknown outcome {other}",
                row.text(0)?
            )));
        }
    };
    let legacy_output = row.text_opt(8)?;
    let output = row
        .text_opt(9)?
        .as_deref()
        .and_then(|raw| decode_json::<ProvenanceRef>(raw).ok())
        .or_else(|| {
            legacy_output
                .as_deref()
                .and_then(SttpProvenanceAdapter::from_compat_string)
        });
    Ok(JobAttempt {
        attempt_id: row.text(0)?,
        job_id: row.text(1)?,
        attempt_number: u32::try_from(row.i64(2)?).unwrap_or(0),
        worker_id: row.text(3)?,
        started_at: decode_time(row.i64(4)?)?,
        finished_at: decode_time(row.i64(5)?)?,
        outcome,
        error_message: row.text_opt(7)?,
        output_provenance: output,
        execution_id: row.text_opt(10)?,
        guardrail_code: row.text_opt(11)?,
        policy_reason: row.text_opt(12)?,
        duration_ms: row.i64_opt(13)?.and_then(|value| u64::try_from(value).ok()),
        diagnostics: row.text_opt(14)?,
    })
}

fn map_v1_event(row: &SqlRow) -> Result<OutboxEvent> {
    let status = match row.text(1)?.as_str() {
        "pending" => OutboxStatus::Pending,
        "published" => OutboxStatus::Published,
        "failed" => OutboxStatus::Failed,
        other => {
            return Err(err(format!(
                "v1 outbox {} has unknown status {other}",
                row.text(0)?
            )));
        }
    };
    let event_type = match row.text(6)?.as_str() {
        "job_succeeded" => RuntimeEventType::JobSucceeded,
        "job_retry_scheduled" => RuntimeEventType::JobRetryScheduled,
        "job_dead_lettered" => RuntimeEventType::JobDeadLettered,
        "job_published" => RuntimeEventType::JobPublished,
        "job_canceled" => RuntimeEventType::JobCanceled,
        other => {
            return Err(err(format!(
                "v1 outbox {} has unknown event type {other}",
                row.text(0)?
            )));
        }
    };
    Ok(OutboxEvent {
        event_id: row.text(0)?,
        status,
        publish_attempts: u32::try_from(row.i64(2)?).unwrap_or(0),
        published_at: row.i64_opt(3)?.map(decode_time).transpose()?,
        next_attempt_at: row.i64_opt(4)?.map(decode_time).transpose()?,
        last_publish_error: row.text_opt(5)?,
        event: RuntimeEvent {
            event_type,
            job_id: row.text(7)?,
            thread_id: row.text_opt(8)?,
            correlation_id: row.text(9)?,
            causation_id: row.text(10)?,
            trace_id: row.text(11)?,
            input_provenance: SttpProvenanceAdapter::merge_legacy(
                row.text_opt(14)?
                    .as_deref()
                    .and_then(|raw| decode_json(raw).ok()),
                &row.text(12)?,
            ),
            output_provenance: {
                let legacy_output = row.text_opt(13)?;
                row.text_opt(15)?
                    .as_deref()
                    .and_then(|raw| decode_json(raw).ok())
                    .or_else(|| {
                        legacy_output
                            .as_deref()
                            .and_then(SttpProvenanceAdapter::from_compat_string)
                    })
            },
            execution_id: row.text_opt(16)?,
            input_memory_query_id: row.text_opt(17)?,
            input_memory_query_fingerprint: row.text_opt(18)?,
            output_memory_node_id: row.text_opt(19)?,
            retrieval_path: row.text_opt(20)?,
            occurred_at: decode_time(row.i64(21)?)?,
            message: row.text_opt(22)?,
        },
    })
}

fn map_v1_wait(row: &SqlRow) -> Result<DurableWaitRecord> {
    let status = match row.text(4)?.as_str() {
        "pending" => DurableWaitStatus::Pending,
        "signaled" => DurableWaitStatus::Signaled,
        "timed_out" => DurableWaitStatus::TimedOut,
        "cancelled" => DurableWaitStatus::Cancelled,
        other => {
            return Err(err(format!(
                "v1 wait {} has unknown status {other}",
                row.text(0)?
            )));
        }
    };
    Ok(DurableWaitRecord {
        wait_id: row.text(0)?,
        job_id: row.text(1)?,
        incarnation: 0,
        signal_type: row.text(2)?,
        correlation_key: row.text(3)?,
        status,
        deadline_at: row.i64_opt(5)?.map(decode_time).transpose()?,
        created_at: decode_time(row.i64(6)?)?,
        updated_at: decode_time(row.i64(7)?)?,
        signal_payload: row.text_opt(8)?,
        consumed_signal_ids: row
            .text_opt(9)?
            .map(|raw| decode_json(&raw))
            .transpose()?
            .unwrap_or_default(),
    })
}

fn map_v1_continuation(row: &SqlRow) -> Result<JobContinuation> {
    let id = row.text(0)?;
    let trigger_raw = row.text(2)?;
    let status_raw = row.text(3)?;
    let trigger = ContinuationTrigger::parse(&trigger_raw).ok_or_else(|| {
        err(format!(
            "v1 continuation {id} has unknown trigger {trigger_raw}"
        ))
    })?;
    let status = ContinuationStatus::parse(&status_raw).ok_or_else(|| {
        err(format!(
            "v1 continuation {id} has unknown status {status_raw}"
        ))
    })?;
    Ok(JobContinuation {
        id: row.text(0)?,
        parent_job_id: row.text(1)?,
        parent_incarnation: 0,
        parent_terminal_cycle: 0,
        parent_version_floor: None,
        trigger,
        status,
        child: decode_json(&row.text(4)?)?,
        child_job_id: row.text_opt(5)?,
        parent_output_json: row.text_opt(6)?,
        claim_token: row.text_opt(7)?,
        created_at: decode_time(row.i64(8)?)?,
    })
}

fn map_v1_recurring(row: &SqlRow) -> Result<RecurringDefinition> {
    Ok(RecurringDefinition {
        id: row.text(0)?,
        queue: row.text(1)?,
        job_type: row.text(2)?,
        payload_template_ref: row.text(3)?,
        cron_expr: row.text(4)?,
        timezone: row.text(5)?,
        jitter_seconds: row.i64(6)?,
        enabled: row.i64(7)? != 0,
        max_attempts: u32::try_from(row.i64(8)?).unwrap_or(1),
        next_run_at: decode_time(row.i64(9)?)?,
        last_run_at: row.i64_opt(10)?.map(decode_time).transpose()?,
        lease_owner: row.text_opt(11)?,
        lease_expires_at: row.i64_opt(12)?.map(decode_time).transpose()?,
    })
}
