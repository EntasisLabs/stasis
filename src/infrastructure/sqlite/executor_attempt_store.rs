use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::errors::Result;
use crate::domain::runtime::job_attempt::{JobAttempt, JobAttemptOutcome};
use crate::domain::runtime::provenance::{ProvenanceRef, SttpProvenanceAdapter};
use crate::ports::outbound::runtime::job_attempt_store::JobAttemptStore;

use super::codec::{decode_json, decode_time, encode_json, encode_time};
use super::executor::{SqlExecutor, SqlRow, SqlValue};

const COLS_SELECT: &str = "attempt_id, job_id, CAST(attempt_number AS TEXT), worker_id, \
     CAST(started_at AS TEXT), CAST(finished_at AS TEXT), outcome, error_message, \
     sttp_output_node_id, output_provenance_json, execution_id, guardrail_code, \
     policy_reason, CAST(duration_ms AS TEXT), diagnostics";

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
        other => Err(super::codec::sql_err("decode job attempt outcome", other)),
    }
}

fn map_attempt(row: &SqlRow) -> Result<JobAttempt> {
    let output_json = row.text_opt(9)?;
    let sttp_output = row.text_opt(8)?;
    Ok(JobAttempt {
        attempt_id: row.text(0)?,
        job_id: row.text(1)?,
        attempt_number: u32::try_from(row.i64(2)?).unwrap_or(0),
        worker_id: row.text(3)?,
        started_at: decode_time(row.i64(4)?)?,
        finished_at: decode_time(row.i64(5)?)?,
        outcome: parse_outcome(&row.text(6)?)?,
        error_message: row.text_opt(7)?,
        output_provenance: output_json
            .as_deref()
            .and_then(|json| decode_json::<ProvenanceRef>(json).ok())
            .or_else(|| {
                sttp_output
                    .as_deref()
                    .and_then(SttpProvenanceAdapter::from_compat_string)
            }),
        execution_id: row.text_opt(10)?,
        guardrail_code: row.text_opt(11)?,
        policy_reason: row.text_opt(12)?,
        duration_ms: row.i64_opt(13)?.and_then(|value| u64::try_from(value).ok()),
        diagnostics: row.text_opt(14)?,
    })
}

#[derive(Clone)]
pub struct ExecutorJobAttemptStore {
    executor: Arc<dyn SqlExecutor>,
}

impl ExecutorJobAttemptStore {
    pub fn new(executor: Arc<dyn SqlExecutor>) -> Self {
        Self { executor }
    }
}

#[async_trait]
impl JobAttemptStore for ExecutorJobAttemptStore {
    async fn insert(&self, attempt: JobAttempt) -> Result<()> {
        let output_json = attempt
            .output_provenance
            .as_ref()
            .map(encode_json)
            .transpose()?;
        self.executor.execute(
            "INSERT INTO job_attempt (
                attempt_id, job_id, attempt_number, worker_id, started_at, finished_at, outcome,
                error_message, sttp_output_node_id, output_provenance_json, execution_id,
                guardrail_code, policy_reason, duration_ms, diagnostics
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
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
            &[
                SqlValue::from(attempt.attempt_id),
                SqlValue::from(attempt.job_id),
                SqlValue::from(i64::from(attempt.attempt_number)),
                SqlValue::from(attempt.worker_id),
                SqlValue::from(encode_time(attempt.started_at)),
                SqlValue::from(encode_time(attempt.finished_at)),
                SqlValue::from(outcome_str(attempt.outcome)),
                SqlValue::from(attempt.error_message),
                SqlValue::from(SttpProvenanceAdapter::from_optional(
                    attempt.output_provenance.as_ref(),
                )),
                SqlValue::from(output_json),
                SqlValue::from(attempt.execution_id),
                SqlValue::from(attempt.guardrail_code),
                SqlValue::from(attempt.policy_reason),
                SqlValue::from(attempt.duration_ms.map(|value| value as i64)),
                SqlValue::from(attempt.diagnostics),
            ],
        )?;
        Ok(())
    }

    async fn list_by_job_id(&self, job_id: &str) -> Result<Vec<JobAttempt>> {
        let rows = self.executor.query(
            &format!(
                "SELECT {COLS_SELECT} FROM job_attempt WHERE job_id = ?1 ORDER BY attempt_number ASC"
            ),
            &[SqlValue::text(job_id)],
        )?;
        rows.iter().map(map_attempt).collect()
    }

    async fn list_by_guardrail_code(&self, guardrail_code: &str) -> Result<Vec<JobAttempt>> {
        let rows = self.executor.query(
            &format!(
                "SELECT {COLS_SELECT} FROM job_attempt WHERE guardrail_code = ?1 ORDER BY attempt_number ASC"
            ),
            &[SqlValue::text(guardrail_code)],
        )?;
        rows.iter().map(map_attempt).collect()
    }

    async fn list_by_execution_id(&self, execution_id: &str) -> Result<Vec<JobAttempt>> {
        let rows = self.executor.query(
            &format!(
                "SELECT {COLS_SELECT} FROM job_attempt WHERE execution_id = ?1 ORDER BY attempt_number ASC"
            ),
            &[SqlValue::text(execution_id)],
        )?;
        rows.iter().map(map_attempt).collect()
    }

    async fn prune_finished_before(&self, cutoff: DateTime<Utc>) -> Result<usize> {
        let rows = self.executor.query(
            "DELETE FROM job_attempt WHERE finished_at <= ?1 RETURNING attempt_id",
            &[SqlValue::from(encode_time(cutoff))],
        )?;
        Ok(rows.len())
    }
}
