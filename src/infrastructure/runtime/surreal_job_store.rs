use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::{Surreal, engine::local::Db};
use surrealdb_types::SurrealValue;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::job::{BackoffPolicy, Job, JobState};
use crate::ports::outbound::runtime::job_store::JobStore;

#[derive(Clone)]
pub struct SurrealJobStore {
    db: Surreal<Db>,
    table: String,
}

impl SurrealJobStore {
    pub fn new(db: Surreal<Db>) -> Self {
        Self {
            db,
            table: "job".to_string(),
        }
    }

    pub fn with_table(db: Surreal<Db>, table: impl Into<String>) -> Self {
        Self {
            db,
            table: table.into(),
        }
    }

    pub fn db(&self) -> Surreal<Db> {
        self.db.clone()
    }

    fn port_err(prefix: &str, err: impl std::fmt::Display) -> StasisError {
        StasisError::PortFailure(format!("{prefix}: {err}"))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
struct JobRecord {
    job_id: String,
    queue: String,
    job_type: String,
    payload_ref: String,
    state: String,
    priority: i32,
    attempts: u32,
    max_attempts: u32,
    backoff_policy: BackoffPolicy,
    idempotency_key: String,
    correlation_id: String,
    causation_id: String,
    trace_id: String,
    sttp_input_node_id: String,
    sttp_output_node_id: Option<String>,
    lease_owner: Option<String>,
    lease_expires_at: Option<DateTime<Utc>>,
    heartbeat_at: Option<DateTime<Utc>>,
    scheduled_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
    last_error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
struct LeaseCandidateRecord {
    job_id: String,
    scheduled_at: DateTime<Utc>,
    priority: i32,
}

impl From<Job> for JobRecord {
    fn from(job: Job) -> Self {
        Self {
            job_id: job.id,
            queue: job.queue,
            job_type: job.job_type,
            payload_ref: job.payload_ref,
            state: match job.state {
                JobState::Enqueued => "enqueued".to_string(),
                JobState::Leased => "leased".to_string(),
                JobState::Running => "running".to_string(),
                JobState::Succeeded => "succeeded".to_string(),
                JobState::Failed => "failed".to_string(),
                JobState::DeadLetter => "dead_letter".to_string(),
                JobState::Canceled => "canceled".to_string(),
            },
            priority: job.priority,
            attempts: job.attempts,
            max_attempts: job.max_attempts,
            backoff_policy: job.backoff_policy,
            idempotency_key: job.idempotency_key,
            correlation_id: job.correlation_id,
            causation_id: job.causation_id,
            trace_id: job.trace_id,
            sttp_input_node_id: job.sttp_input_node_id,
            sttp_output_node_id: job.sttp_output_node_id,
            lease_owner: job.lease_owner,
            lease_expires_at: job.lease_expires_at,
            heartbeat_at: job.heartbeat_at,
            scheduled_at: job.scheduled_at,
            started_at: job.started_at,
            finished_at: job.finished_at,
            last_error: job.last_error,
        }
    }
}

impl TryFrom<JobRecord> for Job {
    type Error = StasisError;

    fn try_from(record: JobRecord) -> std::result::Result<Self, Self::Error> {
        let state = match record.state.as_str() {
            "enqueued" => JobState::Enqueued,
            "leased" => JobState::Leased,
            "running" => JobState::Running,
            "succeeded" => JobState::Succeeded,
            "failed" => JobState::Failed,
            "dead_letter" => JobState::DeadLetter,
            "canceled" => JobState::Canceled,
            other => {
                return Err(StasisError::PortFailure(format!(
                    "invalid persisted job state: {other}"
                )));
            }
        };

        Ok(Self {
            id: record.job_id,
            queue: record.queue,
            job_type: record.job_type,
            payload_ref: record.payload_ref,
            state,
            priority: record.priority,
            attempts: record.attempts,
            max_attempts: record.max_attempts,
            backoff_policy: record.backoff_policy,
            idempotency_key: record.idempotency_key,
            correlation_id: record.correlation_id,
            causation_id: record.causation_id,
            trace_id: record.trace_id,
            sttp_input_node_id: record.sttp_input_node_id,
            sttp_output_node_id: record.sttp_output_node_id,
            lease_owner: record.lease_owner,
            lease_expires_at: record.lease_expires_at,
            heartbeat_at: record.heartbeat_at,
            scheduled_at: record.scheduled_at,
            started_at: record.started_at,
            finished_at: record.finished_at,
            last_error: record.last_error,
        })
    }
}

#[async_trait]
impl JobStore for SurrealJobStore {
    async fn insert(&self, job: Job) -> Result<()> {
        let record: JobRecord = job.into();
        self.db
            .query("CREATE type::record($table, $id) CONTENT $data")
            .bind(("table", self.table.clone()))
            .bind(("id", record.job_id.clone()))
            .bind(("data", record))
            .await
            .map_err(|e| Self::port_err("insert job", e))?;

        Ok(())
    }

    async fn save(&self, job: Job) -> Result<()> {
        let record: JobRecord = job.into();
        self.db
            .query("UPSERT type::record($table, $id) CONTENT $data")
            .bind(("table", self.table.clone()))
            .bind(("id", record.job_id.clone()))
            .bind(("data", record))
            .await
            .map_err(|e| Self::port_err("save job", e))?;

        Ok(())
    }

    async fn get(&self, id: &str) -> Result<Option<Job>> {
        let mut response = self
            .db
            .query("SELECT * FROM type::record($table, $id)")
            .bind(("table", self.table.clone()))
            .bind(("id", id.to_string()))
            .await
            .map_err(|e| Self::port_err("get job", e))?;

        let row: Option<JobRecord> = response
            .take(0)
            .map_err(|e| Self::port_err("decode job", e))?;

        row.map(Job::try_from).transpose()
    }

    async fn lease_due(
        &self,
        queue: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_seconds: i64,
    ) -> Result<Option<Job>> {
        let lease_expires_at = now + chrono::Duration::seconds(lease_seconds);

        // Retry candidate selection a few times in case another worker wins the CAS update.
        for _ in 0..3 {
            let mut candidate_response = self
                .db
                .query(
                    "SELECT job_id, scheduled_at, priority FROM type::table($table) \
                     WHERE queue = $queue \
                                             AND (state = 'enqueued' OR state = 'leased') \
                       AND scheduled_at <= $now \
                       AND (lease_expires_at = NONE OR lease_expires_at <= $now) \
                     ORDER BY scheduled_at ASC, priority ASC \
                     LIMIT 1",
                )
                .bind(("table", self.table.clone()))
                .bind(("queue", queue.to_string()))
                .bind(("now", now))
                .await
                .map_err(|e| Self::port_err("select lease candidate", e))?;

            let mut candidates: Vec<LeaseCandidateRecord> = candidate_response
                .take(0)
                .map_err(|e| Self::port_err("decode lease candidate", e))?;

            let Some(candidate) = candidates.pop() else {
                return Ok(None);
            };

            let mut update_response = self
                .db
                .query(
                    "UPDATE type::record($table, $id) \
                     SET state = 'leased', lease_owner = $worker_id, lease_expires_at = $lease_expires_at, heartbeat_at = $now \
                     WHERE queue = $queue \
                                             AND (state = 'enqueued' OR state = 'leased') \
                       AND scheduled_at <= $now \
                       AND (lease_expires_at = NONE OR lease_expires_at <= $now) \
                     RETURN AFTER"
                )
                .bind(("table", self.table.clone()))
                .bind(("id", candidate.job_id))
                .bind(("queue", queue.to_string()))
                .bind(("worker_id", worker_id.to_string()))
                .bind(("now", now))
                .bind(("lease_expires_at", lease_expires_at))
                .await
                .map_err(|e| Self::port_err("lease due job", e))?;

            let row: Option<JobRecord> = update_response
                .take(0)
                .map_err(|e| Self::port_err("decode leased job", e))?;

            if let Some(record) = row {
                return Ok(Some(Job::try_from(record)?));
            }
        }

        Ok(None)
    }

    async fn heartbeat(&self, job_id: &str, worker_id: &str, now: DateTime<Utc>) -> Result<()> {
        let Some(mut job) = self.get(job_id).await? else {
            return Ok(());
        };

        if job.lease_owner.as_deref() == Some(worker_id) {
            job.heartbeat_at = Some(now);
            self.save(job).await?;
        }

        Ok(())
    }

    async fn list_by_state(&self, state: JobState) -> Result<Vec<Job>> {
        let mut response = self
            .db
            .query("SELECT * FROM type::table($table)")
            .bind(("table", self.table.clone()))
            .await
            .map_err(|e| Self::port_err("list jobs by state", e))?;

        let rows: Vec<JobRecord> = response
            .take(0)
            .map_err(|e| Self::port_err("decode jobs by state", e))?;

        let mut jobs = Vec::new();
        for row in rows {
            let job = Job::try_from(row)?;
            if job.state == state {
                jobs.push(job);
            }
        }

        Ok(jobs)
    }

    async fn prune_terminal_before(&self, cutoff: DateTime<Utc>) -> Result<usize> {
        let mut response = self
            .db
            .query("SELECT * FROM type::table($table)")
            .bind(("table", self.table.clone()))
            .await
            .map_err(|e| Self::port_err("list jobs for prune", e))?;

        let rows: Vec<JobRecord> = response
            .take(0)
            .map_err(|e| Self::port_err("decode jobs for prune", e))?;

        let mut removed = 0usize;
        for row in rows {
            let job = Job::try_from(row.clone())?;
            let terminal = matches!(
                job.state,
                JobState::Succeeded | JobState::Failed | JobState::DeadLetter | JobState::Canceled
            );
            let old_enough = job.finished_at.map(|t| t <= cutoff).unwrap_or(false);
            if terminal && old_enough {
                self.db
                    .query("DELETE type::record($table, $id)")
                    .bind(("table", self.table.clone()))
                    .bind(("id", row.job_id))
                    .await
                    .map_err(|e| Self::port_err("delete pruned job", e))?;
                removed += 1;
            }
        }

        Ok(removed)
    }
}
