use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::errors::Result;
use crate::domain::runtime::job_attempt::{JobAttempt, JobAttemptOutcome};
use crate::domain::runtime::provenance::{ProvenanceRef, SttpProvenanceAdapter};
use crate::ports::outbound::runtime::job_attempt_store::JobAttemptStore;

use super::codec::{decode_json, decode_time, encode_json, encode_time, sql_err};
use super::session::SqliteSession;

#[derive(Clone)]
pub struct SqliteJobAttemptStore {
    session: SqliteSession,
}

impl SqliteJobAttemptStore {
    pub fn new(session: SqliteSession) -> Self {
        Self { session }
    }
}

const COLS: &str = "attempt_id, job_id, attempt_number, worker_id, started_at, finished_at, outcome, \
     error_message, sttp_output_node_id, output_provenance_json, execution_id, guardrail_code, \
     policy_reason, duration_ms, diagnostics";

fn outcome_str(outcome: JobAttemptOutcome) -> &'static str {
    match outcome {
        JobAttemptOutcome::Succeeded => "succeeded",
        JobAttemptOutcome::RetryableFailure => "retryable_failure",
        JobAttemptOutcome::FatalFailure => "fatal_failure",
        JobAttemptOutcome::Deferred => "deferred",
    }
}

fn parse_outcome(raw: &str) -> Result<JobAttemptOutcome> {
    match raw {
        "succeeded" => Ok(JobAttemptOutcome::Succeeded),
        "retryable_failure" => Ok(JobAttemptOutcome::RetryableFailure),
        "fatal_failure" => Ok(JobAttemptOutcome::FatalFailure),
        "deferred" => Ok(JobAttemptOutcome::Deferred),
        other => Err(sql_err("decode job attempt outcome", other)),
    }
}

fn map_attempt(row: &rusqlite::Row<'_>) -> rusqlite::Result<JobAttempt> {
    let convert = |err: crate::domain::errors::StasisError| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::other(err.to_string())),
        )
    };
    let outcome = parse_outcome(&row.get::<_, String>(6)?).map_err(convert)?;
    let started_at: i64 = row.get(4)?;
    let finished_at: i64 = row.get(5)?;
    let output_json: Option<String> = row.get(9)?;
    let sttp_output: Option<String> = row.get(8)?;
    let duration: Option<i64> = row.get(13)?;
    Ok(JobAttempt {
        attempt_id: row.get(0)?,
        job_id: row.get(1)?,
        attempt_number: u32::try_from(row.get::<_, i64>(2)?).unwrap_or(0),
        worker_id: row.get(3)?,
        started_at: decode_time(started_at).map_err(convert)?,
        finished_at: decode_time(finished_at).map_err(convert)?,
        outcome,
        error_message: row.get(7)?,
        output_provenance: output_json
            .as_deref()
            .and_then(|json| decode_json::<ProvenanceRef>(json).ok())
            .or_else(|| {
                sttp_output
                    .as_deref()
                    .and_then(SttpProvenanceAdapter::from_compat_string)
            }),
        execution_id: row.get(10)?,
        guardrail_code: row.get(11)?,
        policy_reason: row.get(12)?,
        duration_ms: duration.and_then(|value| u64::try_from(value).ok()),
        diagnostics: row.get(14)?,
    })
}

#[async_trait]
impl JobAttemptStore for SqliteJobAttemptStore {
    async fn insert(&self, attempt: JobAttempt) -> Result<()> {
        let output_json = attempt
            .output_provenance
            .as_ref()
            .map(encode_json)
            .transpose()?;
        self.session.execute(
            &format!(
                "INSERT INTO job_attempt ({COLS}) VALUES ({})
                 ON CONFLICT(attempt_id) DO UPDATE SET
                    job_id = excluded.job_id,
                    attempt_number = excluded.attempt_number,
                    worker_id = excluded.worker_id,
                    started_at = excluded.started_at,
                    finished_at = excluded.finished_at,
                    outcome = excluded.outcome,
                    error_message = excluded.error_message,
                    sttp_output_node_id = excluded.sttp_output_node_id,
                    output_provenance_json = excluded.output_provenance_json,
                    execution_id = excluded.execution_id,
                    guardrail_code = excluded.guardrail_code,
                    policy_reason = excluded.policy_reason,
                    duration_ms = excluded.duration_ms,
                    diagnostics = excluded.diagnostics",
                (1..=15).map(|n| format!("?{n}")).collect::<Vec<_>>().join(", ")
            ),
            rusqlite::params![
                attempt.attempt_id,
                attempt.job_id,
                i64::from(attempt.attempt_number),
                attempt.worker_id,
                encode_time(attempt.started_at),
                encode_time(attempt.finished_at),
                outcome_str(attempt.outcome),
                attempt.error_message,
                SttpProvenanceAdapter::from_optional(attempt.output_provenance.as_ref()),
                output_json,
                attempt.execution_id,
                attempt.guardrail_code,
                attempt.policy_reason,
                attempt.duration_ms.map(|value| value as i64),
                attempt.diagnostics,
            ],
        )?;
        Ok(())
    }

    async fn list_by_job_id(&self, job_id: &str) -> Result<Vec<JobAttempt>> {
        self.session.query_map(
            &format!(
                "SELECT {COLS} FROM job_attempt WHERE job_id = ?1 ORDER BY attempt_number ASC"
            ),
            [job_id],
            map_attempt,
        )
    }

    async fn list_by_guardrail_code(&self, guardrail_code: &str) -> Result<Vec<JobAttempt>> {
        self.session.query_map(
            &format!(
                "SELECT {COLS} FROM job_attempt WHERE guardrail_code = ?1 ORDER BY attempt_number ASC"
            ),
            [guardrail_code],
            map_attempt,
        )
    }

    async fn list_by_execution_id(&self, execution_id: &str) -> Result<Vec<JobAttempt>> {
        self.session.query_map(
            &format!(
                "SELECT {COLS} FROM job_attempt WHERE execution_id = ?1 ORDER BY attempt_number ASC"
            ),
            [execution_id],
            map_attempt,
        )
    }

    async fn prune_finished_before(&self, cutoff: DateTime<Utc>) -> Result<usize> {
        self.session.execute(
            "DELETE FROM job_attempt WHERE finished_at <= ?1",
            [encode_time(cutoff)],
        )
    }
}
