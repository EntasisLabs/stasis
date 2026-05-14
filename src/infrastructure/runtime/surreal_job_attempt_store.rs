use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::{Surreal, engine::local::Db};
use surrealdb_types::SurrealValue;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::job_attempt::{JobAttempt, JobAttemptOutcome};
use crate::ports::outbound::runtime::job_attempt_store::JobAttemptStore;

#[derive(Clone)]
pub struct SurrealJobAttemptStore {
    db: Surreal<Db>,
    table: String,
}

impl SurrealJobAttemptStore {
    pub fn new(db: Surreal<Db>) -> Self {
        Self {
            db,
            table: "job_attempt".to_string(),
        }
    }

    fn port_err(prefix: &str, err: impl std::fmt::Display) -> StasisError {
        StasisError::PortFailure(format!("{prefix}: {err}"))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
struct JobAttemptRecord {
    attempt_id: String,
    job_id: String,
    attempt_number: u32,
    worker_id: String,
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
    outcome: String,
    error_message: Option<String>,
    sttp_output_node_id: Option<String>,
    execution_id: Option<String>,
    diagnostics: Option<String>,
}

impl From<JobAttempt> for JobAttemptRecord {
    fn from(value: JobAttempt) -> Self {
        Self {
            attempt_id: value.attempt_id,
            job_id: value.job_id,
            attempt_number: value.attempt_number,
            worker_id: value.worker_id,
            started_at: value.started_at,
            finished_at: value.finished_at,
            outcome: match value.outcome {
                JobAttemptOutcome::Succeeded => "succeeded".to_string(),
                JobAttemptOutcome::RetryableFailure => "retryable_failure".to_string(),
                JobAttemptOutcome::FatalFailure => "fatal_failure".to_string(),
            },
            error_message: value.error_message,
            sttp_output_node_id: value.sttp_output_node_id,
            execution_id: value.execution_id,
            diagnostics: value.diagnostics,
        }
    }
}

impl TryFrom<JobAttemptRecord> for JobAttempt {
    type Error = StasisError;

    fn try_from(value: JobAttemptRecord) -> std::result::Result<Self, Self::Error> {
        let outcome = match value.outcome.as_str() {
            "succeeded" => JobAttemptOutcome::Succeeded,
            "retryable_failure" => JobAttemptOutcome::RetryableFailure,
            "fatal_failure" => JobAttemptOutcome::FatalFailure,
            other => {
                return Err(StasisError::PortFailure(format!(
                    "invalid job attempt outcome: {other}"
                )));
            }
        };

        Ok(Self {
            attempt_id: value.attempt_id,
            job_id: value.job_id,
            attempt_number: value.attempt_number,
            worker_id: value.worker_id,
            started_at: value.started_at,
            finished_at: value.finished_at,
            outcome,
            error_message: value.error_message,
            sttp_output_node_id: value.sttp_output_node_id,
            execution_id: value.execution_id,
            diagnostics: value.diagnostics,
        })
    }
}

#[async_trait]
impl JobAttemptStore for SurrealJobAttemptStore {
    async fn insert(&self, attempt: JobAttempt) -> Result<()> {
        let record: JobAttemptRecord = attempt.into();
        self.db
            .query("UPSERT type::record($table, $id) CONTENT $data")
            .bind(("table", self.table.clone()))
            .bind(("id", record.attempt_id.clone()))
            .bind(("data", record))
            .await
            .map_err(|e| Self::port_err("save job attempt", e))?;

        Ok(())
    }

    async fn list_by_job_id(&self, job_id: &str) -> Result<Vec<JobAttempt>> {
        let mut response = self
            .db
            .query("SELECT * FROM type::table($table) WHERE job_id = $job_id")
            .bind(("table", self.table.clone()))
            .bind(("job_id", job_id.to_string()))
            .await
            .map_err(|e| Self::port_err("list job attempts", e))?;

        let rows: Vec<JobAttemptRecord> = response
            .take(0)
            .map_err(|e| Self::port_err("decode job attempts", e))?;

        let mut attempts: Vec<JobAttempt> = rows
            .into_iter()
            .filter_map(|row| JobAttempt::try_from(row).ok())
            .collect();

        attempts.sort_by_key(|attempt| attempt.attempt_number);
        Ok(attempts)
    }
}
