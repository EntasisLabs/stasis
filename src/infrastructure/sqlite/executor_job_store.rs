use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::errors::Result;
use crate::domain::runtime::job::{BackoffPolicy, Job, JobState};
use crate::domain::runtime::placement::{PlacementConstraints, WorkerCapabilities};
use crate::domain::runtime::provenance::{ProvenanceRef, SttpProvenanceAdapter};
use crate::ports::outbound::runtime::job_store::JobStore;

use super::codec::{decode_json, decode_time, encode_json, encode_time, sql_err};
use super::executor::{SqlExecutor, SqlRow, SqlValue, query_one, transaction_result};

const JOB_SELECT: &str = "job_id, queue, job_type, payload_ref, state, \
     CAST(priority AS TEXT), CAST(attempts AS TEXT), CAST(max_attempts AS TEXT), \
     CAST(backoff_base_delay_seconds AS TEXT), CAST(backoff_max_delay_seconds AS TEXT), \
     idempotency_key, correlation_id, causation_id, trace_id, sttp_input_node_id, \
     sttp_output_node_id, input_provenance_json, output_provenance_json, placement_json, \
     lease_owner, CAST(lease_expires_at AS TEXT), CAST(heartbeat_at AS TEXT), \
     CAST(scheduled_at AS TEXT), CAST(started_at AS TEXT), CAST(finished_at AS TEXT), \
     last_error, progress_json";

fn state_str(state: JobState) -> &'static str {
    match state {
        JobState::Enqueued => "enqueued",
        JobState::Leased => "leased",
        JobState::Running => "running",
        JobState::Succeeded => "succeeded",
        JobState::Failed => "failed",
        JobState::DeadLetter => "dead_letter",
        JobState::Canceled => "canceled",
    }
}

fn parse_state(raw: &str) -> Result<JobState> {
    match raw {
        "enqueued" => Ok(JobState::Enqueued),
        "leased" => Ok(JobState::Leased),
        "running" => Ok(JobState::Running),
        "succeeded" => Ok(JobState::Succeeded),
        "failed" => Ok(JobState::Failed),
        "dead_letter" => Ok(JobState::DeadLetter),
        "canceled" => Ok(JobState::Canceled),
        other => Err(sql_err("decode job state", other)),
    }
}

fn map_job(row: &SqlRow) -> Result<Job> {
    let state = parse_state(&row.text(4)?)?;
    let legacy_input = row.text(14)?;
    let input_json = row.text_opt(16)?;
    let sttp_output = row.text_opt(15)?;
    let output_json = row.text_opt(17)?;
    let input_provenance = SttpProvenanceAdapter::merge_legacy(
        input_json
            .as_deref()
            .and_then(|json| decode_json::<ProvenanceRef>(json).ok()),
        &legacy_input,
    );
    let output_provenance = output_json
        .as_deref()
        .and_then(|json| decode_json::<ProvenanceRef>(json).ok())
        .or_else(|| {
            sttp_output
                .as_deref()
                .and_then(SttpProvenanceAdapter::from_compat_string)
        });
    Ok(Job {
        id: row.text(0)?,
        queue: row.text(1)?,
        job_type: row.text(2)?,
        payload_ref: row.text(3)?,
        state,
        priority: i32::try_from(row.i64(5)?).unwrap_or(0),
        attempts: u32::try_from(row.i64(6)?).unwrap_or(0),
        max_attempts: u32::try_from(row.i64(7)?).unwrap_or(0),
        backoff_policy: BackoffPolicy {
            base_delay_seconds: row.i64(8)?,
            max_delay_seconds: row.i64(9)?,
        },
        idempotency_key: row.text(10)?,
        correlation_id: row.text(11)?,
        causation_id: row.text(12)?,
        trace_id: row.text(13)?,
        input_provenance,
        output_provenance,
        placement: row
            .text_opt(18)?
            .and_then(|json| decode_json(&json).ok())
            .unwrap_or_default(),
        lease_owner: row.text_opt(19)?,
        lease_expires_at: row.i64_opt(20)?.map(decode_time).transpose()?,
        heartbeat_at: row.i64_opt(21)?.map(decode_time).transpose()?,
        scheduled_at: decode_time(row.i64(22)?)?,
        started_at: row.i64_opt(23)?.map(decode_time).transpose()?,
        finished_at: row.i64_opt(24)?.map(decode_time).transpose()?,
        last_error: row.text_opt(25)?,
        progress_json: row.text_opt(26)?,
    })
}

fn job_params(job: &Job) -> Result<Vec<SqlValue>> {
    let input_json = match &job.input_provenance {
        Some(value) => Some(encode_json(value)?),
        None => None,
    };
    let output_json = match &job.output_provenance {
        Some(value) => Some(encode_json(value)?),
        None => None,
    };
    let placement = if job.placement.is_unrestricted() {
        None
    } else {
        Some(encode_json(&job.placement)?)
    };
    Ok(vec![
        SqlValue::from(job.id.clone()),
        SqlValue::from(job.queue.clone()),
        SqlValue::from(job.job_type.clone()),
        SqlValue::from(job.payload_ref.clone()),
        SqlValue::from(state_str(job.state)),
        SqlValue::from(i64::from(job.priority)),
        SqlValue::from(i64::from(job.attempts)),
        SqlValue::from(i64::from(job.max_attempts)),
        SqlValue::from(job.backoff_policy.base_delay_seconds),
        SqlValue::from(job.backoff_policy.max_delay_seconds),
        SqlValue::from(job.idempotency_key.clone()),
        SqlValue::from(job.correlation_id.clone()),
        SqlValue::from(job.causation_id.clone()),
        SqlValue::from(job.trace_id.clone()),
        SqlValue::from(SttpProvenanceAdapter::to_compat_string(
            job.input_provenance.as_ref(),
        )),
        SqlValue::from(SttpProvenanceAdapter::from_optional(
            job.output_provenance.as_ref(),
        )),
        SqlValue::from(input_json),
        SqlValue::from(output_json),
        SqlValue::from(placement),
        SqlValue::from(job.lease_owner.clone()),
        SqlValue::from(job.lease_expires_at.map(encode_time)),
        SqlValue::from(job.heartbeat_at.map(encode_time)),
        SqlValue::from(encode_time(job.scheduled_at)),
        SqlValue::from(job.started_at.map(encode_time)),
        SqlValue::from(job.finished_at.map(encode_time)),
        SqlValue::from(job.last_error.clone()),
        SqlValue::from(job.progress_json.clone()),
    ])
}

fn upsert_sql() -> String {
    format!(
        "INSERT INTO job ({columns}) VALUES ({placeholders})
         ON CONFLICT(job_id) DO UPDATE SET
            queue = excluded.queue,
            job_type = excluded.job_type,
            payload_ref = excluded.payload_ref,
            state = excluded.state,
            priority = excluded.priority,
            attempts = excluded.attempts,
            max_attempts = excluded.max_attempts,
            backoff_base_delay_seconds = excluded.backoff_base_delay_seconds,
            backoff_max_delay_seconds = excluded.backoff_max_delay_seconds,
            idempotency_key = excluded.idempotency_key,
            correlation_id = excluded.correlation_id,
            causation_id = excluded.causation_id,
            trace_id = excluded.trace_id,
            sttp_input_node_id = excluded.sttp_input_node_id,
            sttp_output_node_id = excluded.sttp_output_node_id,
            input_provenance_json = excluded.input_provenance_json,
            output_provenance_json = excluded.output_provenance_json,
            placement_json = excluded.placement_json,
            lease_owner = excluded.lease_owner,
            lease_expires_at = excluded.lease_expires_at,
            heartbeat_at = excluded.heartbeat_at,
            scheduled_at = excluded.scheduled_at,
            started_at = excluded.started_at,
            finished_at = excluded.finished_at,
            last_error = excluded.last_error,
            progress_json = excluded.progress_json",
        columns = "job_id, queue, job_type, payload_ref, state, priority, attempts, max_attempts, \
            backoff_base_delay_seconds, backoff_max_delay_seconds, idempotency_key, correlation_id, \
            causation_id, trace_id, sttp_input_node_id, sttp_output_node_id, input_provenance_json, \
            output_provenance_json, placement_json, lease_owner, lease_expires_at, heartbeat_at, \
            scheduled_at, started_at, finished_at, last_error, progress_json",
        placeholders = (1..=27)
            .map(|n| format!("?{n}"))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

#[derive(Clone)]
pub struct ExecutorJobStore {
    executor: Arc<dyn SqlExecutor>,
}

impl ExecutorJobStore {
    pub fn new(executor: Arc<dyn SqlExecutor>) -> Self {
        Self { executor }
    }

    fn save_job(&self, job: &Job) -> Result<()> {
        let params = job_params(job)?;
        self.executor.execute(&upsert_sql(), &params)?;
        Ok(())
    }

    fn load(&self, id: &str) -> Result<Option<Job>> {
        let row = query_one(
            self.executor.as_ref(),
            &format!("SELECT {JOB_SELECT} FROM job WHERE job_id = ?1"),
            &[SqlValue::text(id)],
        )?;
        row.as_ref().map(map_job).transpose()
    }

    pub fn heartbeat_if_owner(
        &self,
        job_id: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_seconds: i64,
    ) -> Result<bool> {
        let row = query_one(
            self.executor.as_ref(),
            "UPDATE job SET heartbeat_at = ?1, lease_expires_at = ?2
             WHERE job_id = ?3 AND lease_owner = ?4
             RETURNING job_id",
            &[
                SqlValue::from(encode_time(now)),
                SqlValue::from(encode_time(
                    now + chrono::Duration::seconds(lease_seconds.max(0)),
                )),
                SqlValue::text(job_id),
                SqlValue::text(worker_id),
            ],
        )?;
        Ok(row.is_some())
    }
}

#[async_trait]
impl JobStore for ExecutorJobStore {
    async fn insert(&self, job: Job) -> Result<()> {
        self.save_job(&job)
    }

    async fn save(&self, job: Job) -> Result<()> {
        self.save_job(&job)
    }

    async fn get(&self, id: &str) -> Result<Option<Job>> {
        self.load(id)
    }

    async fn lease_due(
        &self,
        queue: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_seconds: i64,
        worker: &WorkerCapabilities,
    ) -> Result<Option<Job>> {
        let now_ns = encode_time(now);
        let lease_expires_at = encode_time(now + chrono::Duration::seconds(lease_seconds));
        for _ in 0..5 {
            let candidates = self.executor.query(
                "SELECT job_id, placement_json FROM job
                 WHERE queue = ?1
                   AND state = 'enqueued'
                   AND scheduled_at <= ?2
                   AND (lease_expires_at IS NULL OR lease_expires_at <= ?2)
                 ORDER BY scheduled_at ASC, priority ASC
                 LIMIT 16",
                &[SqlValue::text(queue), SqlValue::from(now_ns)],
            )?;
            if candidates.is_empty() {
                return Ok(None);
            }
            for row in candidates {
                let job_id = row.text(0)?;
                let placement_json = row.text_opt(1)?;
                let placement: PlacementConstraints = placement_json
                    .as_deref()
                    .and_then(|json| decode_json(json).ok())
                    .unwrap_or_default();
                if !placement.matches(worker) {
                    continue;
                }
                let won = transaction_result(self.executor.as_ref(), &mut || {
                    let updated = query_one(
                        self.executor.as_ref(),
                        "UPDATE job
                         SET state = 'leased',
                             lease_owner = ?1,
                             lease_expires_at = ?2,
                             heartbeat_at = ?3
                         WHERE job_id = ?4
                           AND queue = ?5
                           AND state = 'enqueued'
                           AND scheduled_at <= ?3
                           AND (lease_expires_at IS NULL OR lease_expires_at <= ?3)
                           AND ((?6 IS NULL AND placement_json IS NULL) OR placement_json = ?6)
                         RETURNING job_id",
                        &[
                            SqlValue::text(worker_id),
                            SqlValue::from(lease_expires_at),
                            SqlValue::from(now_ns),
                            SqlValue::text(&job_id),
                            SqlValue::text(queue),
                            SqlValue::from(placement_json.clone()),
                        ],
                    )?;
                    Ok(updated.is_some())
                })?;
                if won {
                    return self.load(&job_id);
                }
            }
        }
        Ok(None)
    }

    async fn heartbeat(
        &self,
        job_id: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_seconds: i64,
    ) -> Result<()> {
        self.heartbeat_if_owner(job_id, worker_id, now, lease_seconds)?;
        Ok(())
    }

    async fn list_by_state(&self, state: JobState) -> Result<Vec<Job>> {
        let rows = self.executor.query(
            &format!("SELECT {JOB_SELECT} FROM job WHERE state = ?1"),
            &[SqlValue::from(state_str(state))],
        )?;
        rows.iter().map(map_job).collect()
    }

    async fn list_expired_leases(&self, now: DateTime<Utc>) -> Result<Vec<Job>> {
        let rows = self.executor.query(
            &format!(
                "SELECT {JOB_SELECT} FROM job
                 WHERE state IN ('leased', 'running')
                   AND lease_expires_at IS NOT NULL
                   AND lease_expires_at <= ?1"
            ),
            &[SqlValue::from(encode_time(now))],
        )?;
        rows.iter().map(map_job).collect()
    }

    async fn delete(&self, id: &str) -> Result<bool> {
        let row = query_one(
            self.executor.as_ref(),
            "DELETE FROM job WHERE job_id = ?1 RETURNING job_id",
            &[SqlValue::text(id)],
        )?;
        Ok(row.is_some())
    }

    async fn prune_terminal_before(&self, cutoff: DateTime<Utc>) -> Result<usize> {
        let rows = self.executor.query(
            "DELETE FROM job
             WHERE state IN ('succeeded', 'failed', 'dead_letter', 'canceled')
               AND finished_at IS NOT NULL
               AND finished_at <= ?1
             RETURNING job_id",
            &[SqlValue::from(encode_time(cutoff))],
        )?;
        Ok(rows.len())
    }
}
