use async_trait::async_trait;
use chrono::{DateTime, Utc};
use crate::domain::errors::Result;
use crate::domain::runtime::job::{BackoffPolicy, Job, JobState};
use crate::domain::runtime::placement::{PlacementConstraints, WorkerCapabilities};
use crate::domain::runtime::provenance::{ProvenanceRef, SttpProvenanceAdapter};
use crate::ports::outbound::runtime::job_store::JobStore;

use super::codec::{decode_json, decode_time, encode_json, encode_time, sql_err};
use super::session::SqliteSession;

#[derive(Clone)]
pub struct SqliteJobStore {
    session: SqliteSession,
}

impl SqliteJobStore {
    pub fn new(session: SqliteSession) -> Self {
        Self { session }
    }

    pub fn session(&self) -> SqliteSession {
        self.session.clone()
    }
}

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

fn encode_provenance(value: &Option<ProvenanceRef>) -> Result<Option<String>> {
    match value {
        Some(prov) => Ok(Some(encode_json(prov)?)),
        None => Ok(None),
    }
}

fn decode_provenance(raw: Option<String>) -> Option<ProvenanceRef> {
    raw.and_then(|json| decode_json(&json).ok())
}

fn encode_placement(placement: &PlacementConstraints) -> Result<Option<String>> {
    if placement.is_unrestricted() {
        Ok(None)
    } else {
        Ok(Some(encode_json(placement)?))
    }
}

fn decode_placement(raw: Option<String>) -> PlacementConstraints {
    raw.and_then(|json| decode_json(&json).ok())
        .unwrap_or_default()
}

const JOB_COLUMNS: &str = "job_id, queue, job_type, payload_ref, state, priority, attempts, max_attempts, \
     backoff_base_delay_seconds, backoff_max_delay_seconds, idempotency_key, correlation_id, causation_id, \
     trace_id, sttp_input_node_id, sttp_output_node_id, input_provenance_json, output_provenance_json, \
     placement_json, lease_owner, lease_expires_at, heartbeat_at, scheduled_at, started_at, finished_at, \
     last_error, progress_json";

fn map_job(row: &rusqlite::Row<'_>) -> rusqlite::Result<Job> {
    let state_raw: String = row.get(4)?;
    let state = parse_state(&state_raw).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(
            4,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::other(err.to_string())),
        )
    })?;
    let input_provenance_json: Option<String> = row.get(16)?;
    let output_provenance_json: Option<String> = row.get(17)?;
    let sttp_input: String = row.get(14)?;
    let sttp_output: Option<String> = row.get(15)?;
    let placement_json: Option<String> = row.get(18)?;
    let lease_expires_at: Option<i64> = row.get(20)?;
    let heartbeat_at: Option<i64> = row.get(21)?;
    let scheduled_at: i64 = row.get(22)?;
    let started_at: Option<i64> = row.get(23)?;
    let finished_at: Option<i64> = row.get(24)?;

    let input_provenance = SttpProvenanceAdapter::merge_legacy(
        decode_provenance(input_provenance_json),
        &sttp_input,
    );
    let output_provenance = decode_provenance(output_provenance_json).or_else(|| {
        sttp_output
            .as_deref()
            .and_then(SttpProvenanceAdapter::from_compat_string)
    });

    let decode = |value: i64| {
        decode_time(value).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Integer,
                Box::new(std::io::Error::other(err.to_string())),
            )
        })
    };

    Ok(Job {
        id: row.get(0)?,
        queue: row.get(1)?,
        job_type: row.get(2)?,
        payload_ref: row.get(3)?,
        state,
        priority: row.get(5)?,
        attempts: u32::try_from(row.get::<_, i64>(6)?).unwrap_or(0),
        max_attempts: u32::try_from(row.get::<_, i64>(7)?).unwrap_or(0),
        backoff_policy: BackoffPolicy {
            base_delay_seconds: row.get(8)?,
            max_delay_seconds: row.get(9)?,
        },
        idempotency_key: row.get(10)?,
        correlation_id: row.get(11)?,
        causation_id: row.get(12)?,
        trace_id: row.get(13)?,
        input_provenance,
        output_provenance,
        placement: decode_placement(placement_json),
        lease_owner: row.get(19)?,
        lease_expires_at: lease_expires_at.map(decode).transpose()?,
        heartbeat_at: heartbeat_at.map(decode).transpose()?,
        scheduled_at: decode(scheduled_at)?,
        started_at: started_at.map(decode).transpose()?,
        finished_at: finished_at.map(decode).transpose()?,
        last_error: row.get(25)?,
        progress_json: row.get(26)?,
    })
}

fn upsert_sql() -> String {
    format!(
        "INSERT INTO job ({JOB_COLUMNS}) VALUES ({})
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
        (1..=27).map(|n| format!("?{n}")).collect::<Vec<_>>().join(", ")
    )
}

fn job_params(job: &Job) -> Result<Vec<Box<dyn rusqlite::types::ToSql>>> {
    let input_json = encode_provenance(&job.input_provenance)?;
    let output_json = encode_provenance(&job.output_provenance)?;
    let placement = encode_placement(&job.placement)?;
    let sttp_input = SttpProvenanceAdapter::to_compat_string(job.input_provenance.as_ref());
    let sttp_output = SttpProvenanceAdapter::from_optional(job.output_provenance.as_ref());
    Ok(vec![
        Box::new(job.id.clone()),
        Box::new(job.queue.clone()),
        Box::new(job.job_type.clone()),
        Box::new(job.payload_ref.clone()),
        Box::new(state_str(job.state).to_string()),
        Box::new(job.priority),
        Box::new(i64::from(job.attempts)),
        Box::new(i64::from(job.max_attempts)),
        Box::new(job.backoff_policy.base_delay_seconds),
        Box::new(job.backoff_policy.max_delay_seconds),
        Box::new(job.idempotency_key.clone()),
        Box::new(job.correlation_id.clone()),
        Box::new(job.causation_id.clone()),
        Box::new(job.trace_id.clone()),
        Box::new(sttp_input),
        Box::new(sttp_output),
        Box::new(input_json),
        Box::new(output_json),
        Box::new(placement),
        Box::new(job.lease_owner.clone()),
        Box::new(job.lease_expires_at.map(encode_time)),
        Box::new(job.heartbeat_at.map(encode_time)),
        Box::new(encode_time(job.scheduled_at)),
        Box::new(job.started_at.map(encode_time)),
        Box::new(job.finished_at.map(encode_time)),
        Box::new(job.last_error.clone()),
        Box::new(job.progress_json.clone()),
    ])
}

impl SqliteJobStore {
    fn save_job(&self, job: &Job) -> Result<()> {
        let params = job_params(job)?;
        let refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();
        self.session
            .execute(&upsert_sql(), rusqlite::params_from_iter(refs.iter().copied()))?;
        Ok(())
    }

    fn load(&self, id: &str) -> Result<Option<Job>> {
        self.session.query_opt(
            &format!("SELECT {JOB_COLUMNS} FROM job WHERE job_id = ?1"),
            [id],
            map_job,
        )
    }
}

#[async_trait]
impl JobStore for SqliteJobStore {
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
            let candidates: Vec<(String, Option<String>)> = self.session.query_map(
                "SELECT job_id, placement_json FROM job
                 WHERE queue = ?1
                   AND state = 'enqueued'
                   AND scheduled_at <= ?2
                   AND (lease_expires_at IS NULL OR lease_expires_at <= ?2)
                 ORDER BY scheduled_at ASC, priority ASC
                 LIMIT 16",
                rusqlite::params![queue, now_ns],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            if candidates.is_empty() {
                return Ok(None);
            }
            for (job_id, placement_json) in candidates {
                let placement = decode_placement(placement_json.clone());
                if !placement.matches(worker) {
                    continue;
                }
                let won = self.session.with_tx(|tx| {
                    let changed = tx
                        .execute(
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
                               AND ((?6 IS NULL AND placement_json IS NULL) OR placement_json = ?6)",
                            rusqlite::params![
                                worker_id,
                                lease_expires_at,
                                now_ns,
                                job_id,
                                queue,
                                placement_json
                            ],
                        )
                        .map_err(|err| sql_err("lease due job", err))?;
                    Ok(changed == 1)
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
        let now_ns = encode_time(now);
        let lease_expires_at = encode_time(now + chrono::Duration::seconds(lease_seconds.max(0)));
        self.session.execute(
            "UPDATE job SET heartbeat_at = ?1, lease_expires_at = ?2
             WHERE job_id = ?3 AND lease_owner = ?4",
            rusqlite::params![now_ns, lease_expires_at, job_id, worker_id],
        )?;
        Ok(())
    }

    async fn list_by_state(&self, state: JobState) -> Result<Vec<Job>> {
        self.session.query_map(
            &format!("SELECT {JOB_COLUMNS} FROM job WHERE state = ?1"),
            [state_str(state)],
            map_job,
        )
    }

    async fn list_expired_leases(&self, now: DateTime<Utc>) -> Result<Vec<Job>> {
        self.session.query_map(
            &format!(
                "SELECT {JOB_COLUMNS} FROM job
                 WHERE state IN ('leased', 'running')
                   AND lease_expires_at IS NOT NULL
                   AND lease_expires_at <= ?1"
            ),
            [encode_time(now)],
            map_job,
        )
    }

    async fn delete(&self, id: &str) -> Result<bool> {
        let changed = self
            .session
            .execute("DELETE FROM job WHERE job_id = ?1", [id])?;
        Ok(changed > 0)
    }

    async fn prune_terminal_before(&self, cutoff: DateTime<Utc>) -> Result<usize> {
        self.session.execute(
            "DELETE FROM job
             WHERE state IN ('succeeded', 'failed', 'dead_letter', 'canceled')
               AND finished_at IS NOT NULL
               AND finished_at <= ?1",
            [encode_time(cutoff)],
        )
    }
}
