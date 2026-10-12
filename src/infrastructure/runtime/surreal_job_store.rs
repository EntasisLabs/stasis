use crate::infrastructure::runtime::surreal_durable_wait_store::WaitRow;
use crate::infrastructure::runtime::surreal_job_continuation_store::ContinuationRow;
#[path = "execution_transactions.surql.rs"]
mod execution_transactions;
use crate::application::runtime::execution_commit::{
    CleanupRecord, CommitResult, ExecutionGrant, ExecutionReceipt, cancellation, outcome_digest,
    publication,
};
use crate::application::runtime::in_memory_runtime::JobExecutionOutcome;
use crate::domain::runtime::outbox::OutboxEvent;
use crate::infrastructure::runtime::surreal_job_attempt_store::JobAttemptRecord;
use crate::infrastructure::runtime::surreal_outbox_store::OutboxRecord;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::{Surreal, engine::any::Any};
use surrealdb_types::SurrealValue;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::job::{BackoffPolicy, Job, JobState};
use crate::domain::runtime::placement::{PlacementConstraints, WorkerCapabilities};
use crate::domain::runtime::provenance::{ProvenanceRef, SttpProvenanceAdapter};
use crate::ports::outbound::runtime::job_store::JobStore;

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
struct BackoffPolicyRecord {
    base_delay_seconds: i64,
    max_delay_seconds: i64,
}

impl From<BackoffPolicy> for BackoffPolicyRecord {
    fn from(value: BackoffPolicy) -> Self {
        Self {
            base_delay_seconds: value.base_delay_seconds,
            max_delay_seconds: value.max_delay_seconds,
        }
    }
}

impl From<BackoffPolicyRecord> for BackoffPolicy {
    fn from(value: BackoffPolicyRecord) -> Self {
        Self {
            base_delay_seconds: value.base_delay_seconds,
            max_delay_seconds: value.max_delay_seconds,
        }
    }
}

#[derive(Clone)]
pub struct SurrealJobStore {
    db: Surreal<Any>,
    table: String,
}

impl SurrealJobStore {
    pub fn new(db: Surreal<Any>) -> Self {
        Self {
            db,
            table: "job".to_string(),
        }
    }

    pub fn with_table(db: Surreal<Any>, table: impl Into<String>) -> Self {
        Self {
            db,
            table: table.into(),
        }
    }

    pub fn db(&self) -> Surreal<Any> {
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
    backoff_policy: BackoffPolicyRecord,
    idempotency_key: String,
    correlation_id: String,
    causation_id: String,
    trace_id: String,
    sttp_input_node_id: String,
    sttp_output_node_id: Option<String>,
    #[serde(default)]
    input_provenance_json: Option<String>,
    #[serde(default)]
    output_provenance_json: Option<String>,
    #[serde(default)]
    placement_json: Option<String>,
    #[serde(default)]
    execution_epoch: u64,
    #[serde(default)]
    version: u64,
    #[serde(default)]
    incarnation: u64,
    #[serde(default)]
    terminal_cycle: u64,
    #[serde(default)]
    wakeup_pending: bool,
    lease_owner: Option<String>,
    lease_expires_at: Option<DateTime<Utc>>,
    heartbeat_at: Option<DateTime<Utc>>,
    scheduled_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
    last_error: Option<String>,
    #[serde(default)]
    progress_json: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
struct LeaseCandidateRecord {
    job_id: String,
    scheduled_at: DateTime<Utc>,
    priority: i32,
}

fn encode_provenance(value: &Option<ProvenanceRef>) -> Option<String> {
    value
        .as_ref()
        .and_then(|prov| serde_json::to_string(prov).ok())
}

fn decode_provenance(raw: Option<&str>) -> Option<ProvenanceRef> {
    raw.and_then(|json| serde_json::from_str(json).ok())
}

fn encode_placement(placement: &PlacementConstraints) -> Option<String> {
    if placement.is_unrestricted() {
        None
    } else {
        serde_json::to_string(placement).ok()
    }
}

fn decode_placement(raw: Option<&str>) -> PlacementConstraints {
    raw.and_then(|json| serde_json::from_str(json).ok())
        .unwrap_or_default()
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
            backoff_policy: job.backoff_policy.into(),
            idempotency_key: job.idempotency_key,
            correlation_id: job.correlation_id,
            causation_id: job.causation_id,
            trace_id: job.trace_id,
            sttp_input_node_id: SttpProvenanceAdapter::to_compat_string(
                job.input_provenance.as_ref(),
            ),
            sttp_output_node_id: SttpProvenanceAdapter::from_optional(
                job.output_provenance.as_ref(),
            ),
            input_provenance_json: encode_provenance(&job.input_provenance),
            output_provenance_json: encode_provenance(&job.output_provenance),
            placement_json: encode_placement(&job.placement),
            execution_epoch: job.execution_epoch,
            version: job.version,
            incarnation: job.incarnation,
            terminal_cycle: job.terminal_cycle,
            wakeup_pending: job.wakeup_pending,
            lease_owner: job.lease_owner,
            lease_expires_at: job.lease_expires_at,
            heartbeat_at: job.heartbeat_at,
            scheduled_at: job.scheduled_at,
            started_at: job.started_at,
            finished_at: job.finished_at,
            last_error: job.last_error,
            progress_json: job.progress_json,
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

        let input_provenance = SttpProvenanceAdapter::merge_legacy(
            decode_provenance(record.input_provenance_json.as_deref()),
            &record.sttp_input_node_id,
        );
        let output_provenance = decode_provenance(record.output_provenance_json.as_deref())
            .or_else(|| {
                record
                    .sttp_output_node_id
                    .as_deref()
                    .and_then(SttpProvenanceAdapter::from_compat_string)
            });

        Ok(Self {
            id: record.job_id,
            queue: record.queue,
            job_type: record.job_type,
            payload_ref: record.payload_ref,
            state,
            priority: record.priority,
            attempts: record.attempts,
            max_attempts: record.max_attempts,
            backoff_policy: record.backoff_policy.into(),
            idempotency_key: record.idempotency_key,
            correlation_id: record.correlation_id,
            causation_id: record.causation_id,
            trace_id: record.trace_id,
            input_provenance,
            output_provenance,
            placement: decode_placement(record.placement_json.as_deref()),
            execution_epoch: record.execution_epoch,
            version: record.version,
            incarnation: record.incarnation,
            terminal_cycle: record.terminal_cycle,
            wakeup_pending: record.wakeup_pending,
            lease_owner: record.lease_owner,
            lease_expires_at: record.lease_expires_at,
            heartbeat_at: record.heartbeat_at,
            scheduled_at: record.scheduled_at,
            started_at: record.started_at,
            finished_at: record.finished_at,
            last_error: record.last_error,
            progress_json: record.progress_json,
        })
    }
}

crate::async_store! {
impl JobStore for SurrealJobStore {
    async fn pending_lifecycle(&self) -> Result<Vec<(String, ExecutionReceipt)>> {
        SurrealJobStore::pending_lifecycle(self).await
    }
    async fn mark_lifecycle_delivered(&self, key: &str, digest: &str) -> Result<()> {
        SurrealJobStore::mark_lifecycle_delivered(self, key, digest).await
    }
    async fn insert(&self, mut job: Job) -> Result<()> {
        if job.state != JobState::Enqueued || job.output_provenance.is_some() {
            return Err(Self::port_err(
                "insert",
                "requires a new enqueued job without terminal output",
            ));
        }
        job.normalize_new()?;
        self.initialize_execution_schema().await?;
        let record: JobRecord = job.into();
        self.db
            .query("BEGIN TRANSACTION; LET $generation = UPSERT type::record($generation_table,$id) SET epoch = (epoch ?? 0) + 1 RETURN AFTER; CREATE type::record($table, $id) CONTENT $data; UPDATE type::record($table,$id) SET incarnation = $generation[0].epoch, terminal_cycle = 1, wakeup_pending = false; COMMIT TRANSACTION;")
            .bind(("generation_table", format!("{}_execution_generation", self.table)))
            .bind(("table", self.table.clone()))
            .bind(("id", record.job_id.clone()))
            .bind(("data", record))
            .await
            .map_err(|e| Self::port_err("insert job", e))?
            .check()
            .map_err(|e| Self::port_err("insert job", e))?;

        Ok(())
    }

    async fn save(&self, mut job: Job) -> Result<()> {
        let expected = job.version;
        job.version = expected
            .checked_add(1)
            .ok_or_else(|| Self::port_err("save", "version exhausted"))?;
        let record: JobRecord = job.into();
        let mut response = self.db.query("BEGIN TRANSACTION; LET $current = SELECT * FROM ONLY type::record($table,$id); LET $saved = UPDATE type::record($table, $id) CONTENT $data WHERE (version ?? 0) = $version AND (execution_epoch ?? 0) = $epoch AND (incarnation ?? 0) = $data.incarnation AND (terminal_cycle ?? 0) = $data.terminal_cycle AND (state != 'dead_letter' OR $data.state != 'enqueued' OR (terminal_cycle ?? 0) > 0) AND $data.state NOT IN ['succeeded','failed','dead_letter','canceled'] AND (state NOT IN ['succeeded','failed','dead_letter','canceled'] OR (state = 'dead_letter' AND $data.state = 'enqueued')) AND ($data.lease_owner = lease_owner OR ($data.lease_owner = NONE AND lease_owner = NONE)) AND (($data.lease_expires_at = lease_expires_at) OR ($data.lease_expires_at = NONE AND lease_expires_at = NONE)) AND (($data.heartbeat_at = heartbeat_at) OR ($data.heartbeat_at = NONE AND heartbeat_at = NONE)) AND (state NOT IN ['leased','running'] OR lease_expires_at > time::now()) AND ($data.state != 'running' OR state IN ['leased','running']) RETURN AFTER; IF array::len($saved) > 0 { UPDATE type::record($table,$id) SET terminal_cycle = IF $current.state = 'dead_letter' AND $data.state = 'enqueued' { $current.terminal_cycle + 1 } ELSE { $current.terminal_cycle ?? 0 }, wakeup_pending = $current.wakeup_pending ?? false, scheduled_at = IF ($current.wakeup_pending ?? false) { $current.scheduled_at } ELSE { $data.scheduled_at }; }; RETURN $saved; COMMIT TRANSACTION;")
            .bind(("table", self.table.clone())).bind(("id", record.job_id.clone()))
            .bind(("version", expected)).bind(("epoch", record.execution_epoch)).bind(("data", record))
            .await.map_err(|e| Self::port_err("conditional save", e))?.check().map_err(|e| Self::port_err("conditional save", e))?;
        let mut saved = false;
        for index in 0..response.num_statements() {
            if response
                .take::<Vec<JobRecord>>(index)
                .is_ok_and(|rows| !rows.is_empty())
            {
                saved = true;
                break;
            }
        }
        if !saved {
            return Err(Self::port_err("save", "stale job snapshot"));
        }
        Ok(())
    }
    async fn wake_incarnation(&self, job_id: &str, incarnation: u64) -> Result<()> {
        self.db.query("UPDATE type::record($table,$id) SET scheduled_at = time::now(), wakeup_pending = state IN ['leased','running'] WHERE state IN ['enqueued','leased','running'] AND incarnation = $incarnation AND $incarnation > 0")
            .bind(("table",self.table.clone())).bind(("id",job_id.to_string())).bind(("incarnation",incarnation))
            .await.map_err(|e|Self::port_err("incarnation wakeup",e))?.check().map_err(|e|Self::port_err("incarnation wakeup",e))?;
        Ok(())
    }
    async fn wake_execution(&self, job_id: &str) -> Result<()> {
        self.db.query("UPDATE type::record($table,$id) SET scheduled_at = time::now(), wakeup_pending = state IN ['leased','running'] WHERE state IN ['enqueued','leased','running']")
            .bind(("table",self.table.clone())).bind(("id",job_id.to_string())).await
            .map_err(|e|Self::port_err("atomic wakeup",e))?.check().map_err(|e|Self::port_err("atomic wakeup",e))?;
        Ok(())
    }
    async fn commit_execution(
        &self,
        snapshot: &Job,
        outcome: JobExecutionOutcome,
    ) -> Result<CommitResult> {
        self.commit_native(snapshot, outcome, false).await
    }
    async fn recover_execution(&self, snapshot: &Job) -> Result<CommitResult> {
        self.commit_native(
            snapshot,
            JobExecutionOutcome::RetryableFailure {
                message: crate::application::runtime::job_lifecycle::STALE_LEASE_MESSAGE.into(),
                execution_id: None,
                diagnostics: None,
            },
            true,
        )
        .await
    }
    async fn heartbeat_granted(
        &self,
        grant: &ExecutionGrant,
        _now: DateTime<Utc>,
        lease_seconds: i64,
        progress: Option<String>,
    ) -> Result<()> {
        let sql = if progress.is_some() {
            "UPDATE type::record($table, $id) SET progress_json = $progress WHERE lease_owner = $owner AND execution_epoch = $epoch AND version = $version AND state IN ['leased','running'] AND lease_expires_at > time::now() RETURN AFTER"
        } else {
            "LET $native_now = time::now(); UPDATE type::record($table, $id) SET heartbeat_at = $native_now, lease_expires_at = $native_now + $ttl WHERE lease_owner = $owner AND execution_epoch = $epoch AND version = $version AND state IN ['leased','running'] AND lease_expires_at > time::now() RETURN AFTER"
        };
        let mut response = self
            .db
            .query(sql)
            .bind(("table", self.table.clone()))
            .bind(("id", grant.job_id.clone()))
            .bind(("owner", grant.owner.clone()))
            .bind(("epoch", grant.epoch))
            .bind(("version", grant.version))
            .bind(("progress", progress))
            .bind((
                "ttl",
                std::time::Duration::from_secs(lease_seconds.max(0) as u64),
            ))
            .await
            .map_err(|e| Self::port_err("granted update", e))?
            .check()
            .map_err(|e| Self::port_err("granted update", e))?;
        let mut row = None;
        for index in (0..response.num_statements()).rev() {
            if let Ok(Some(record)) = response.take::<Option<JobRecord>>(index) {
                row = Some(record);
                break;
            }
        }
        if row.is_none() {
            return Err(Self::port_err("granted update", "stale execution grant"));
        }
        Ok(())
    }
    async fn cancel_execution(&self, job_id: &str) -> Result<Option<Job>> {
        self.initialize_execution_schema().await?;
        let Some(current) = self
            .get(job_id)
            .await?
            .filter(|j| !crate::application::runtime::runtime_job_dx::is_terminal(&j.state))
        else {
            return Ok(None);
        };
        let (job, event, receipt) = cancellation(&current, Utc::now());
        let key = event.event_id.clone();
        let cleanup = ExecutionGrant::from_job(&current).map(|grant| CleanupRecord {
            grant,
            evidence_json: None,
            return_observations: Vec::new(),
            acknowledged_at: None,
            retained_at: Some(Utc::now()),
        });
        let cleanup = cleanup.map(CleanupDbRecord::from_cleanup).transpose()?;
        let result = self
            .db
            .query(execution_transactions::CANCEL_EXECUTION)
            .bind(("table", self.table.clone()))
            .bind(("receipt_table", self.receipt_table()))
            .bind(("cleanup_table", self.cleanup_table()))
            .bind(("staged_table", format!("{}_execution_staged", self.table)))
            .bind(("job_id", job_id.to_string()))
            .bind(("epoch", current.execution_epoch))
            .bind(("version", current.version))
            .bind((
                "cleanup_key",
                format!("{}:execution:{}", job_id, current.execution_epoch),
            ))
            .bind(("cleanup_data", cleanup))
            .bind(("key", key.clone()))
            .bind(("job_data", JobRecord::from(job.clone())))
            .bind(("outbox_data", OutboxRecord::from(event)))
            .bind(("receipt_data", ReceiptRecord::from_receipt(&key, &receipt)?))
            .bind(("attempt_data", None::<JobAttemptRecord>))
            .await
            .map_err(|e| Self::port_err("atomic cancellation", e))?;
        Ok(if Self::decision(result)? == CommitResult::Committed {
            Some(job)
        } else {
            None
        })
    }
    async fn acknowledge_cleanup(
        &self,
        grant: &ExecutionGrant,
        evidence_json: String,
    ) -> Result<CommitResult> {
        let response = self.db.query("BEGIN TRANSACTION; LET $old = SELECT * FROM ONLY type::record($table, $id); LET $decision = IF $old = NONE OR $old.owner != $owner OR $old.epoch != $epoch OR $old.version != $version { 'stale' } ELSE IF $old.evidence_json != NONE { IF $old.evidence_json = $evidence { 'same' } ELSE { 'conflict' } } ELSE { 'committed' }; IF $decision = 'committed' { UPDATE type::record($table, $id) SET evidence_json = $evidence, acknowledged_at = time::now(); }; RETURN $decision; COMMIT TRANSACTION;")
            .bind(("table", self.cleanup_table())).bind(("id", format!("{}:execution:{}", grant.job_id, grant.epoch)))
            .bind(("owner", grant.owner.clone())).bind(("epoch", grant.epoch)).bind(("version", grant.version)).bind(("evidence", evidence_json))
            .await.map_err(|e| Self::port_err("cleanup acknowledgement", e))?;
        Self::decision(response)
    }
    async fn publish_granted(&self, grant: &ExecutionGrant, event: OutboxEvent) -> Result<()> {
        if event.event.job_id != grant.job_id {
            return Err(Self::port_err("stage event", "wrong parent"));
        }
        self.stage(
            grant,
            event.event_id.clone(),
            "event",
            None,
            None,
            Some(event.into()),
        )
        .await
    }
    async fn stage_child(&self, grant: &ExecutionGrant, mut child: Job) -> Result<()> {
        child.normalize_new()?;
        self.stage(
            grant,
            child.id.clone(),
            "child",
            Some(child.into()),
            None,
            None,
        )
        .await
    }
    async fn stage_continuation(
        &self,
        grant: &ExecutionGrant,
        continuation: crate::domain::runtime::job_continuation::JobContinuation,
    ) -> Result<()> {
        if continuation.parent_job_id != grant.job_id {
            return Err(Self::port_err("stage continuation", "wrong parent"));
        }
        let id = continuation.id.clone();
        self.stage(
            grant,
            id,
            "continuation",
            None,
            Some(continuation.try_into()?),
            None,
        )
        .await
    }
    async fn settle_continuation(
        &self,
        parent: &Job,
        record: crate::domain::runtime::job_continuation::JobContinuation,
        child: Option<Job>,
        detail: Option<String>,
    ) -> Result<bool> {
        let child = child
            .map(|mut j| {
                j.normalize_new()?;
                Ok::<_, StasisError>(j)
            })
            .transpose()?;
        let child_id = child.as_ref().map(|j| j.id.clone());
        let response = self.db.query("BEGIN TRANSACTION; LET $parent = SELECT * FROM ONLY type::record($table, $parent_id); LET $record = SELECT * FROM ONLY type::record('job_continuation', $id); LET $receipts = SELECT * FROM type::table($receipt_table) WHERE job_snapshot.job_id = $parent_id AND job_snapshot.version = $version AND job_snapshot.execution_epoch = $epoch AND job_snapshot.incarnation = $incarnation AND job_snapshot.terminal_cycle = $cycle AND job_snapshot.state = $parent_state AND job_snapshot.state IN ['succeeded','failed','dead_letter','canceled']; LET $valid = (($parent != NONE AND $parent.version = $version AND $parent.execution_epoch = $epoch AND $parent.state = $parent_state AND $parent.state IN ['succeeded','failed','dead_letter','canceled'] AND ($parent.incarnation ?? 0) = $incarnation AND ($parent.terminal_cycle ?? 0) = $cycle) OR array::len($receipts) > 0) AND $record != NONE AND $record.parent_job_id = $parent_id AND ($record.parent_incarnation ?? 0) = $incarnation AND $cycle > 0 AND ($record.parent_terminal_cycle ?? 0) = $cycle AND $record.parent_version_floor != NONE AND $record.parent_version_floor = $floor AND $version >= $record.parent_version_floor AND $record.status IN ['pending','settling'] AND (($child != NONE) = ($record.trigger = 'any_terminal' OR ($record.trigger = 'succeeded' AND $parent_state = 'succeeded') OR ($record.trigger = 'canceled' AND $parent_state = 'canceled') OR ($record.trigger = 'failed' AND $parent_state IN ['failed','dead_letter']))); IF $valid { IF $child != NONE { LET $generation = UPSERT type::record($generation_table,$child_id) SET epoch = (epoch ?? 0) + 1 RETURN AFTER; CREATE type::record($table, $child_id) CONTENT $child; UPDATE type::record($table,$child_id) SET incarnation = $generation[0].epoch, terminal_cycle = 1, wakeup_pending = false, execution_epoch = 0, version = 0, lease_owner = NONE, lease_expires_at = NONE, heartbeat_at = NONE; }; UPDATE type::record('job_continuation', $id) SET status = IF $child != NONE { 'materialized' } ELSE { 'discarded' }, child_job_id = $child_id, parent_output_json = $detail, claim_token = NONE; }; RETURN IF $valid { 'committed' } ELSE { 'stale' }; COMMIT TRANSACTION;")
            .bind(("table", self.table.clone())).bind(("receipt_table",self.receipt_table())).bind(("parent_state",JobRecord::from(parent.clone()).state)).bind(("parent_id", parent.id.clone())).bind(("id", record.id))
            .bind(("cycle", parent.terminal_cycle)).bind(("floor", record.parent_version_floor)).bind(("version", parent.version)).bind(("epoch", parent.execution_epoch)).bind(("incarnation",parent.incarnation)).bind(("generation_table",format!("{}_execution_generation",self.table))).bind(("child_id", child_id))
            .bind(("child", child.map(JobRecord::from))).bind(("detail", detail)).await.map_err(|e| Self::port_err("atomic continuation", e))?;
        Ok(Self::decision(response)? == CommitResult::Committed)
    }
    async fn poll_wait(
        &self,
        grant: &ExecutionGrant,
        wait: crate::domain::runtime::durable_wait::DurableWaitRecord,
    ) -> Result<crate::domain::runtime::durable_wait::DurableWaitRecord> {
        if wait.job_id != grant.job_id {
            return Err(Self::port_err("wait", "wrong job"));
        }
        let response = self.db.query("BEGIN TRANSACTION; LET $job = SELECT * FROM ONLY type::record($table, $id); IF $job = NONE OR $job.lease_owner != $owner OR $job.execution_epoch != $epoch OR $job.version != $version OR $job.state NOT IN ['leased','running'] OR $job.lease_expires_at <= time::now() OR $job.incarnation != $data.incarnation OR $data.incarnation = 0 { THROW 'stale execution wait'; }; LET $legacy = SELECT * FROM durable_wait WHERE job_id = $id AND (incarnation ?? 0) = 0; IF array::len($legacy) > 0 { THROW 'legacy wait identity requires reconciliation'; }; LET $old = SELECT * FROM ONLY type::record('durable_wait',$wait_id); IF $old = NONE { CREATE type::record('durable_wait',$wait_id) CONTENT $data; }; LET $wait = SELECT * FROM ONLY type::record('durable_wait',$wait_id); IF $wait.job_id != $id OR $wait.incarnation != $job.incarnation { THROW 'wait identity conflict'; }; IF $wait.status = 'pending' { IF $old != NONE AND $wait.deadline_at != NONE AND $wait.deadline_at <= time::now() { UPDATE type::record('durable_wait',$wait_id) SET status = 'timed_out', updated_at = time::now(); } ELSE { LET $retired = SELECT * FROM durable_wait WHERE job_id = $id AND incarnation != $wait.incarnation; LET $consumed = array::flatten($retired.consumed_signal_ids); LET $signal = SELECT * FROM durable_signal WHERE signal_type = $wait.signal_type AND correlation_key = $wait.correlation_key AND signal_id NOT IN $wait.consumed_signal_ids AND signal_id NOT IN $consumed AND (target_incarnations = NONE OR $target IN target_incarnations) ORDER BY created_at ASC LIMIT 1; IF array::len($signal) > 0 { UPDATE type::record('durable_wait',$wait_id) SET status = 'signaled', signal_payload = $signal[0].payload_json, consumed_signal_ids += $signal[0].signal_id, updated_at = time::now(); }; }; }; RETURN SELECT * FROM type::record('durable_wait',$wait_id); COMMIT TRANSACTION;")
            .bind(("table", self.table.clone())).bind(("id",grant.job_id.clone())).bind(("owner",grant.owner.clone()))
            .bind(("epoch",grant.epoch)).bind(("version",grant.version)).bind(("wait_id",wait.wait_id.clone())).bind(("target",wait.target_identity())).bind(("data",WaitRow::from(wait)))
            .await.map_err(|e| Self::port_err("atomic wait",e))?;
        let mut response = response
            .check()
            .map_err(|e| Self::port_err("atomic wait", e))?;
        for index in (0..response.num_statements()).rev() {
            if let Ok(Some(row)) = response.take::<Option<WaitRow>>(index) {
                return row.try_into();
            }
        }
        Err(Self::port_err("atomic wait", "missing wait response"))
    }
    async fn fail_execution(&self, job_id: &str) -> Result<Option<Job>> {
        self.initialize_execution_schema().await?;
        let Some(current) = self
            .get(job_id)
            .await?
            .filter(|j| !crate::application::runtime::runtime_job_dx::is_terminal(&j.state))
        else {
            return Ok(None);
        };
        let p =
            crate::application::runtime::execution_commit::operator_failure(&current, Utc::now());
        let job = p.job.clone();
        let key = p.outbox.event_id.clone();
        let cleanup = ExecutionGrant::from_job(&current).map(|grant| CleanupDbRecord {
            owner: grant.owner,
            epoch: grant.epoch,
            version: grant.version,
            evidence_json: None,
            return_observations: Vec::new(),
            acknowledged_at: None,
            retained_at: Some(Utc::now()),
        });
        let response = self
            .db
            .query(execution_transactions::CANCEL_EXECUTION)
            .bind(("table", self.table.clone()))
            .bind(("receipt_table", self.receipt_table()))
            .bind(("cleanup_table", self.cleanup_table()))
            .bind(("staged_table", format!("{}_execution_staged", self.table)))
            .bind(("job_id", job_id.to_string()))
            .bind(("epoch", current.execution_epoch))
            .bind(("version", current.version))
            .bind((
                "cleanup_key",
                format!("{}:execution:{}", job_id, current.execution_epoch),
            ))
            .bind(("cleanup_data", cleanup))
            .bind(("key", key.clone()))
            .bind(("job_data", JobRecord::from(p.job)))
            .bind(("outbox_data", OutboxRecord::from(p.outbox)))
            .bind(("attempt_data", Some(JobAttemptRecord::from(p.attempt))))
            .bind((
                "receipt_data",
                ReceiptRecord::from_receipt(&key, &p.receipt)?,
            ))
            .await
            .map_err(|e| Self::port_err("atomic operator failure", e))?;
        Ok(if Self::decision(response)? == CommitResult::Committed {
            Some(job)
        } else {
            None
        })
    }
    async fn execution_receipt(
        &self,
        job_id: &str,
        epoch: u64,
    ) -> Result<Option<ExecutionReceipt>> {
        let mut response = self
            .db
            .query("SELECT * FROM type::record($table,$id)")
            .bind(("table", self.receipt_table()))
            .bind(("id", format!("{}:execution:{}", job_id, epoch)))
            .await
            .map_err(|e| Self::port_err("receipt inspection", e))?
            .check()
            .map_err(|e| Self::port_err("receipt inspection", e))?;
        let row: Option<ReceiptRecord> = response
            .take(0)
            .map_err(|e| Self::port_err("receipt decoding", e))?;
        row.map(|r| r.decode()).transpose()
    }
    async fn cleanup_receipt(&self, job_id: &str, epoch: u64) -> Result<Option<CleanupRecord>> {
        let mut response = self
            .db
            .query("SELECT * FROM type::record($table,$id)")
            .bind(("table", self.cleanup_table()))
            .bind(("id", format!("{}:execution:{}", job_id, epoch)))
            .await
            .map_err(|e| Self::port_err("cleanup inspection", e))?
            .check()
            .map_err(|e| Self::port_err("cleanup inspection", e))?;
        let row: Option<CleanupDbRecord> = response
            .take(0)
            .map_err(|e| Self::port_err("cleanup decoding", e))?;
        Ok(row.map(|r| CleanupRecord {
            grant: ExecutionGrant {
                job_id: job_id.into(),
                owner: r.owner,
                epoch: r.epoch,
                version: r.version,
            },
            evidence_json: r.evidence_json,
            return_observations: r.return_observations,
            acknowledged_at: r.acknowledged_at,
            retained_at: r.retained_at,
        }))
    }
    async fn observe_cleanup(
        &self,
        grant: &ExecutionGrant,
        observation: String,
    ) -> Result<CommitResult> {
        if observation.len() > 65536 {
            return Ok(CommitResult::Conflict);
        }
        let response=self.db.query("BEGIN TRANSACTION; LET $old = SELECT * FROM ONLY type::record($table,$id); LET $observations = $old.return_observations ?? []; LET $decision = IF $old = NONE OR $old.owner != $owner OR $old.epoch != $epoch OR $old.version != $version { 'stale' } ELSE IF $observation IN $observations { 'same' } ELSE IF array::len($observations) >= 8 { 'conflict' } ELSE { 'committed' }; IF $decision = 'committed' { UPDATE type::record($table,$id) SET return_observations = array::append($observations,$observation); }; RETURN $decision; COMMIT TRANSACTION;")
            .bind(("table",self.cleanup_table())).bind(("id",format!("{}:execution:{}",grant.job_id,grant.epoch))).bind(("owner",grant.owner.clone())).bind(("epoch",grant.epoch)).bind(("version",grant.version)).bind(("observation",observation))
            .await.map_err(|e|Self::port_err("cleanup observation",e))?;
        Self::decision(response)
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
        worker: &WorkerCapabilities,
    ) -> Result<Option<Job>> {
        // Retry candidate selection a few times in case another worker wins the CAS update.
        self.initialize_execution_schema().await?;
        for _ in 0..5 {
            let mut candidate_response = self
                .db
                .query(
                    "SELECT job_id, scheduled_at, priority FROM type::table($table) \
                     WHERE queue = $queue \
                       AND state = 'enqueued' \
                       AND scheduled_at <= $now \
                       AND (lease_expires_at = NONE OR lease_expires_at <= $now) \
                     ORDER BY scheduled_at ASC, priority ASC \
                     LIMIT 16",
                )
                .bind(("table", self.table.clone()))
                .bind(("queue", queue.to_string()))
                .bind(("now", now))
                .await
                .map_err(|e| Self::port_err("select lease candidate", e))?;

            let candidates: Vec<LeaseCandidateRecord> = candidate_response
                .take(0)
                .map_err(|e| Self::port_err("decode lease candidate", e))?;

            if candidates.is_empty() {
                return Ok(None);
            }

            for candidate in candidates {
                let Some(existing) = self.get(&candidate.job_id).await? else {
                    continue;
                };
                if !existing.placement.matches(worker) {
                    continue;
                }
                let response = self
                    .db
                    .query(execution_transactions::ACQUIRE_EXECUTION)
                    .bind(("table", self.table.clone()))
                    .bind((
                        "generation_table",
                        format!("{}_execution_generation", self.table),
                    ))
                    .bind(("job_id", candidate.job_id))
                    .bind(("queue", queue.to_string()))
                    .bind(("owner", worker_id.to_string()))
                    .bind(("version", existing.version))
                    .bind(("now", now))
                    .bind((
                        "ttl",
                        std::time::Duration::from_secs(lease_seconds.max(0) as u64),
                    ))
                    .await
                    .map_err(|e| Self::port_err("acquire execution", e))?;
                let mut response = response
                    .check()
                    .map_err(|e| Self::port_err("acquire execution", e))?;
                let mut row = None;
                for index in 0..response.num_statements() {
                    if let Ok(records) = response.take::<Vec<JobRecord>>(index) {
                        if let Some(record) = records.into_iter().next() {
                            row = Some(record);
                        }
                    }
                }
                if let Some(record) = row {
                    let job = Job::try_from(record)?;
                    return Ok(Some(job));
                }
            }
        }

        Ok(None)
    }

    async fn heartbeat(
        &self,
        _job_id: &str,
        _worker_id: &str,
        _now: DateTime<Utc>,
        _lease_seconds: i64,
    ) -> Result<()> {
        Err(Self::port_err(
            "heartbeat",
            "owner-only heartbeat retired; use ExecutionGrant",
        ))
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

    async fn list_expired_leases(&self, now: DateTime<Utc>) -> Result<Vec<Job>> {
        let mut response = match self
            .db
            .query("SELECT * FROM type::table($table)")
            .bind(("table", self.table.clone()))
            .await
        {
            Ok(response) => response,
            Err(err) => {
                let message = err.to_string();
                if message.contains("does not exist") && message.contains(&self.table) {
                    return Ok(Vec::new());
                }
                return Err(Self::port_err("list expired leases", err));
            }
        };
        let rows: Vec<JobRecord> = match response.take(0) {
            Ok(rows) => rows,
            Err(err) => {
                let message = err.to_string();
                if message.contains("does not exist") && message.contains(&self.table) {
                    return Ok(Vec::new());
                }
                return Err(Self::port_err("decode expired leases", err));
            }
        };
        let mut jobs = Vec::new();
        for row in rows {
            let job = Job::try_from(row)?;
            if matches!(job.state, JobState::Leased | JobState::Running)
                && job
                    .lease_expires_at
                    .map(|expiry| expiry <= now)
                    .unwrap_or(false)
            {
                jobs.push(job);
            }
        }
        Ok(jobs)
    }

    async fn delete(&self, id: &str) -> Result<bool> {
        let response=self.db.query("BEGIN TRANSACTION; LET $deleted = DELETE type::record($table,$id) WHERE state IN ['succeeded','failed','dead_letter','canceled'] RETURN BEFORE; IF array::len($deleted) > 0 { DELETE type::table($staged_table) WHERE parent_id = $id; }; RETURN IF array::len($deleted) > 0 { 'committed' } ELSE { 'stale' }; COMMIT TRANSACTION;")
            .bind(("table",self.table.clone())).bind(("staged_table",format!("{}_execution_staged",self.table))).bind(("id",id.to_string()))
            .await.map_err(|e|Self::port_err("atomic delete",e))?;
        Ok(Self::decision(response)? == CommitResult::Committed)
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
                let response=self.db.query("BEGIN TRANSACTION; LET $deleted = DELETE type::record($table,$id) WHERE version = $version AND execution_epoch = $epoch AND (incarnation ?? 0) = $incarnation AND state IN ['succeeded','failed','dead_letter','canceled'] AND finished_at <= $cutoff RETURN BEFORE; IF array::len($deleted) > 0 { DELETE type::table($staged_table) WHERE parent_id = $id; }; RETURN IF array::len($deleted) > 0 { 'committed' } ELSE { 'stale' }; COMMIT TRANSACTION;")
                    .bind(("table",self.table.clone())).bind(("staged_table",format!("{}_execution_staged",self.table))).bind(("id",row.job_id)).bind(("version",job.version)).bind(("epoch",job.execution_epoch)).bind(("incarnation",job.incarnation)).bind(("cutoff",cutoff))
                    .await.map_err(|e|Self::port_err("atomic retention prune",e))?;
                if Self::decision(response)? != CommitResult::Committed {
                    continue;
                }
                removed += 1;
            }
        }

        Ok(removed)
    }

    async fn prune_retained_before(
        &self,
        receipt_cutoff: DateTime<Utc>,
        cleanup_cutoff: DateTime<Utc>,
    ) -> Result<(usize, usize)> {
        let deleted_receipts = self
            .delete_retained(&self.receipt_table(), receipt_cutoff)
            .await?;
        let deleted_cleanups = self
            .delete_retained(&self.cleanup_table(), cleanup_cutoff)
            .await?;
        Ok((deleted_receipts, deleted_cleanups))
    }
}
}

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
struct ReceiptRecord {
    receipt_id: String,
    job_id: String,
    owner: String,
    epoch: u64,
    version: u64,
    digest: String,
    receipt_json: String,
    #[serde(default)]
    job_snapshot: Option<JobRecord>,
    #[serde(default)]
    effective_scheduled_at: Option<DateTime<Utc>>,
    delivered: bool,
    #[serde(default)]
    retained_at: Option<DateTime<Utc>>,
}
impl ReceiptRecord {
    fn from_receipt(key: &str, receipt: &ExecutionReceipt) -> Result<Self> {
        Ok(Self {
            receipt_id: key.into(),
            job_id: receipt.grant.job_id.clone(),
            owner: receipt.grant.owner.clone(),
            epoch: receipt.grant.epoch,
            version: receipt.grant.version,
            digest: receipt.digest.clone(),
            receipt_json: serde_json::to_string(receipt)
                .map_err(|e| SurrealJobStore::port_err("encode receipt", e))?,
            job_snapshot: Some(JobRecord::from(
                serde_json::from_str::<Job>(&receipt.job_json)
                    .map_err(|e| SurrealJobStore::port_err("receipt snapshot", e))?,
            )),
            effective_scheduled_at: None,
            delivered: false,
            retained_at: receipt.retained_at,
        })
    }
    fn decode(&self) -> Result<ExecutionReceipt> {
        let mut receipt: ExecutionReceipt = serde_json::from_str(&self.receipt_json)
            .map_err(|e| SurrealJobStore::port_err("receipt json", e))?;
        receipt.delivered = self.delivered;
        if let Some(snapshot) = &self.job_snapshot {
            let job: Job = snapshot.clone().try_into()?;
            receipt.job_json = serde_json::to_string(&job)
                .map_err(|e| SurrealJobStore::port_err("effective receipt snapshot", e))?;
        }
        if let Some(scheduled_at) = self.effective_scheduled_at {
            let mut job: Job = serde_json::from_str(&receipt.job_json)
                .map_err(|e| SurrealJobStore::port_err("receipt job", e))?;
            job.scheduled_at = scheduled_at;
            job.wakeup_pending = false;
            receipt.job_json = serde_json::to_string(&job)
                .map_err(|e| SurrealJobStore::port_err("receipt job", e))?;
            if let crate::domain::runtime::execution::JobLifecycleEvent::Deferred {
                scheduled_at: ref mut actual,
                ..
            } = receipt.lifecycle
            {
                *actual = scheduled_at;
            }
        }
        Ok(receipt)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
struct CleanupDbRecord {
    owner: String,
    epoch: u64,
    version: u64,
    evidence_json: Option<String>,
    #[serde(default)]
    return_observations: Vec<String>,
    acknowledged_at: Option<DateTime<Utc>>,
    #[serde(default)]
    retained_at: Option<DateTime<Utc>>,
}
impl CleanupDbRecord {
    fn from_cleanup(record: CleanupRecord) -> Result<Self> {
        Ok(Self {
            owner: record.grant.owner,
            epoch: record.grant.epoch,
            version: record.grant.version,
            evidence_json: None,
            return_observations: Vec::new(),
            acknowledged_at: None,
            retained_at: record.retained_at,
        })
    }
}
impl SurrealJobStore {
    pub async fn initialize_execution_schema(&self) -> Result<()> {
        let names = [
            self.table.clone(),
            self.receipt_table(),
            self.cleanup_table(),
            format!("{}_execution_generation", self.table),
            format!("{}_execution_staged", self.table),
            "job_attempt".into(),
            "outbox_event".into(),
            "job_continuation".into(),
            "durable_wait".into(),
            "durable_signal".into(),
        ];
        let mut sql = String::new();
        for table in names {
            if !table.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                return Err(Self::port_err("schema", "invalid native table identifier"));
            }
            sql.push_str(&format!("DEFINE TABLE IF NOT EXISTS {table} SCHEMALESS; "));
        }
        self.db
            .query(sql)
            .await
            .map_err(|e| Self::port_err("native schema", e))?
            .check()
            .map_err(|e| Self::port_err("native schema", e))?;
        // Upgrade pre-incarnation rows atomically, including never-leased jobs.
        // Retained generation counters and old receipts are never deleted or reused.
        self.db.query("BEGIN TRANSACTION; LET $legacy = SELECT * FROM type::table($table) WHERE (incarnation ?? 0) = 0; FOR $job IN $legacy { LET $generation = UPSERT type::record($generation_table,$job.job_id) SET epoch = math::max([epoch ?? 0,$job.execution_epoch ?? 0]) + 1 RETURN AFTER; UPDATE type::record($table,$job.job_id) SET incarnation = $generation[0].epoch, wakeup_pending = false WHERE (incarnation ?? 0) = 0; }; COMMIT TRANSACTION;")
            .bind(("table",self.table.clone())).bind(("generation_table",format!("{}_execution_generation",self.table)))
            .await.map_err(|e|Self::port_err("native incarnation migration",e))?.check().map_err(|e|Self::port_err("native incarnation migration",e))?;
        self.reconcile_legacy_identity().await?;
        Ok(())
    }

    /// Quiescent upgrade for pre-incarnation Surreal rows.
    ///
    /// Epoch-zero leases are re-enqueued and are not trusted. A missing terminal
    /// cycle becomes 1 only when no execution receipt exists. Waits and
    /// continuations bind to that single job row. Signals stay unbound.
    /// Conflicting history is an error and is not rewritten.
    pub async fn reconcile_legacy_identity(&self) -> Result<()> {
        let sql = r#"
BEGIN TRANSACTION;
LET $bad = SELECT job_id FROM type::table($table) WHERE (terminal_cycle ?? 0) = 0 AND job_id IN (SELECT VALUE job_id FROM type::table($receipt) WHERE job_id != NONE);
IF array::len($bad) > 0 { THROW 'legacy job has execution receipts but terminal cycle 0; refusing to invent a cycle'; };
UPDATE type::table($table) SET state = 'enqueued', lease_owner = NONE, lease_expires_at = NONE, heartbeat_at = NONE WHERE (execution_epoch ?? 0) = 0 AND state IN ['leased', 'running'];
UPDATE type::table($table) SET terminal_cycle = 1 WHERE (terminal_cycle ?? 0) = 0;
LET $waits = SELECT * FROM durable_wait WHERE (incarnation ?? 0) = 0;
FOR $wait IN $waits {
    LET $job = SELECT * FROM ONLY type::record($table, $wait.job_id);
    IF $job = NONE OR ($job.incarnation ?? 0) = 0 { THROW 'legacy wait has no reconcilable job: ' + $wait.wait_id; };
    LET $foreign = SELECT job_id FROM type::table($receipt) WHERE job_id = $wait.job_id AND (job_snapshot.incarnation ?? 0) != 0 AND (job_snapshot.incarnation ?? 0) != $job.incarnation;
    IF array::len($foreign) > 0 { THROW 'legacy wait conflicts with another incarnation receipt: ' + $wait.wait_id; };
    UPDATE type::record('durable_wait', $wait.wait_id) SET incarnation = $job.incarnation WHERE (incarnation ?? 0) = 0;
};
LET $edges = SELECT * FROM job_continuation WHERE (parent_incarnation ?? 0) = 0 OR (parent_terminal_cycle ?? 0) = 0 OR parent_version_floor = NONE;
FOR $edge IN $edges {
    LET $job = SELECT * FROM ONLY type::record($table, $edge.parent_job_id);
    IF $job = NONE OR ($job.incarnation ?? 0) = 0 OR ($job.terminal_cycle ?? 0) = 0 { THROW 'legacy continuation has no reconcilable parent: ' + $edge.continuation_id; };
    IF ($edge.parent_incarnation ?? 0) != 0 AND $edge.parent_incarnation != $job.incarnation { THROW 'legacy continuation incarnation disagrees with parent: ' + $edge.continuation_id; };
    IF ($edge.parent_terminal_cycle ?? 0) != 0 AND $edge.parent_terminal_cycle != $job.terminal_cycle { THROW 'legacy continuation cycle disagrees with parent: ' + $edge.continuation_id; };
    UPDATE type::record('job_continuation', $edge.continuation_id) MERGE { parent_incarnation: $job.incarnation, parent_terminal_cycle: $job.terminal_cycle, parent_version_floor: $edge.parent_version_floor ?? $job.version };
};
COMMIT TRANSACTION;
"#;
        let mut response = self
            .db
            .query(sql)
            .bind(("table", self.table.clone()))
            .bind(("receipt", self.receipt_table()))
            .await
            .map_err(|e| Self::port_err("legacy identity reconcile", e))?;
        let errors = response.take_errors();
        if !errors.is_empty() {
            return Err(Self::port_err(
                "legacy identity reconcile",
                format!("{errors:?}"),
            ));
        }
        response
            .check()
            .map_err(|e| Self::port_err("legacy identity reconcile", e))?;
        Ok(())
    }
    async fn commit_once(
        &self,
        snapshot: &Job,
        outcome: JobExecutionOutcome,
        recovery: bool,
    ) -> Result<CommitResult> {
        let Some(grant) = ExecutionGrant::from_job(snapshot) else {
            return Ok(CommitResult::Stale);
        };
        // A local snapshot is only a grant carrier, not a publication specification.
        // The transaction revalidates this canonical base's version/epoch and owner.
        // Keep the receipt-first transaction even when the base is gone/stale so
        // genuine duplicate delivery remains idempotent after deletion or replay.
        let current = self.get(&grant.job_id).await?;
        let canonical_base_valid = current.as_ref().is_some_and(|job| grant.matches(job));
        let base = current
            .as_ref()
            .filter(|_| canonical_base_valid)
            .unwrap_or(snapshot);
        let p = publication(base, outcome.clone(), Utc::now());
        let key = format!("{}:execution:{}", snapshot.id, grant.epoch);
        let receipt = ReceiptRecord::from_receipt(&key, &p.receipt)?;
        let response = self
            .db
            .query(execution_transactions::COMMIT_EXECUTION)
            .bind(("table", self.table.clone()))
            .bind(("receipt_table", self.receipt_table()))
            .bind(("key", key))
            .bind(("job_id", grant.job_id))
            .bind(("owner", grant.owner))
            .bind(("epoch", grant.epoch))
            .bind(("version", grant.version))
            .bind(("recovery", recovery))
            .bind(("canonical_base_valid", canonical_base_valid))
            .bind((
                "deferred",
                matches!(outcome, JobExecutionOutcome::Deferred { .. }),
            ))
            .bind((
                "generation_table",
                format!("{}_execution_generation", self.table),
            ))
            .bind(("staged_table", format!("{}_execution_staged", self.table)))
            .bind(("digest", outcome_digest(&outcome)))
            .bind(("job_data", JobRecord::from(p.job)))
            .bind(("attempt_data", JobAttemptRecord::from(p.attempt)))
            .bind(("outbox_data", OutboxRecord::from(p.outbox)))
            .bind(("receipt_data", receipt))
            .await
            .map_err(|e| Self::port_err("atomic execution commit", e))?;
        Self::decision(response)
    }
    async fn commit_native(
        &self,
        snapshot: &Job,
        outcome: JobExecutionOutcome,
        recovery: bool,
    ) -> Result<CommitResult> {
        // Retry only explicit native transaction conflicts, with the identical outcome digest.
        // Transport/unknown errors are returned; an explicit replay remains idempotent.
        for retry in 0..4 {
            let result = self.commit_once(snapshot, outcome.clone(), recovery).await;
            if let Err(error) = &result {
                let message = error.to_string().to_lowercase();
                if retry < 3
                    && (message.contains("transaction can be retried")
                        || message.contains("read or write conflict")
                        || message.contains("transaction conflict"))
                {
                    continue;
                }
            }
            return result;
        }
        unreachable!()
    }
    fn receipt_table(&self) -> String {
        format!("{}_execution_receipt", self.table)
    }
    async fn delete_retained(&self, table: &str, cutoff: DateTime<Utc>) -> Result<usize> {
        let mut response = self
            .db
            .query("LET $rows = DELETE type::table($table) WHERE retained_at != NONE AND retained_at <= $cutoff RETURN BEFORE; RETURN array::len($rows);")
            .bind(("table", table.to_string()))
            .bind(("cutoff", cutoff))
            .await
            .map_err(|e| Self::port_err("prune retained rows", e))?
            .check()
            .map_err(|e| Self::port_err("prune retained rows", e))?;
        let mut count = 0usize;
        for index in 0..response.num_statements() {
            if let Ok(Some(value)) = response.take::<Option<i64>>(index) {
                count = usize::try_from(value).unwrap_or(0);
            }
        }
        Ok(count)
    }
    fn cleanup_table(&self) -> String {
        format!("{}_execution_cleanup", self.table)
    }
    fn decision(response: surrealdb::IndexedResults) -> Result<CommitResult> {
        let mut response = response;
        let errors = response.take_errors();
        if !errors.is_empty() {
            return Err(Self::port_err(
                "execution transaction",
                format!("{errors:?}"),
            ));
        }
        for index in 0..response.num_statements() {
            if let Ok(Some(decision)) = response.take::<Option<String>>(index) {
                return Ok(match decision.as_str() {
                    "committed" => CommitResult::Committed,
                    "same" => CommitResult::AlreadyCommittedSameDigest,
                    "conflict" => CommitResult::Conflict,
                    "stale" => CommitResult::Stale,
                    _ => continue,
                });
            }
        }
        Err(Self::port_err("execution transaction", "missing decision"))
    }
    pub async fn pending_lifecycle(&self) -> Result<Vec<(String, ExecutionReceipt)>> {
        let mut response = self
            .db
            .query("SELECT * FROM type::table($table) WHERE delivered = false")
            .bind(("table", self.receipt_table()))
            .await
            .map_err(|e| Self::port_err("pending lifecycle", e))?
            .check()
            .map_err(|e| Self::port_err("pending lifecycle", e))?;
        let records: Vec<ReceiptRecord> = response
            .take(0)
            .map_err(|e| Self::port_err("decode lifecycle", e))?;
        records
            .into_iter()
            .map(|r| {
                let receipt = r.decode()?;
                Ok((r.receipt_id, receipt))
            })
            .collect()
    }
    pub async fn mark_lifecycle_delivered(&self, key: &str, digest: &str) -> Result<()> {
        self.db
            .query("UPDATE type::record($table, $id) SET delivered = true WHERE digest = $digest")
            .bind(("table", self.receipt_table()))
            .bind(("id", key.to_string()))
            .bind(("digest", digest.to_string()))
            .await
            .map_err(|e| Self::port_err("lifecycle delivered", e))?
            .check()
            .map_err(|e| Self::port_err("lifecycle delivered", e))?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
struct StagedRecord {
    parent_id: String,
    epoch: u64,
    version: u64,
    kind: String,
    canonical_id: String,
    child_data: Option<JobRecord>,
    continuation_data: Option<ContinuationRow>,
    event_data: Option<OutboxRecord>,
}
impl SurrealJobStore {
    async fn stage(
        &self,
        grant: &ExecutionGrant,
        canonical_id: String,
        kind: &str,
        child_data: Option<JobRecord>,
        continuation_data: Option<ContinuationRow>,
        event_data: Option<OutboxRecord>,
    ) -> Result<()> {
        let staged_table = format!("{}_execution_staged", self.table);
        let data = StagedRecord {
            parent_id: grant.job_id.clone(),
            epoch: grant.epoch,
            version: grant.version,
            kind: kind.into(),
            canonical_id: canonical_id.clone(),
            child_data,
            continuation_data,
            event_data,
        };
        let response = self.db.query("BEGIN TRANSACTION; LET $job = SELECT * FROM ONLY type::record($table, $id); LET $pending = SELECT count() AS n FROM type::table($staged_table) WHERE parent_id = $id AND epoch = $epoch GROUP ALL; LET $valid = $job != NONE AND $job.lease_owner = $owner AND $job.execution_epoch = $epoch AND $job.version = $version AND $job.state IN ['leased','running'] AND $job.lease_expires_at > time::now() AND ($pending[0].n ?? 0) < 128 AND ($data.kind != 'continuation' OR ($data.continuation_data.parent_incarnation = $job.incarnation AND $data.continuation_data.parent_version_floor = $job.version AND $job.terminal_cycle > 0 AND $data.continuation_data.parent_terminal_cycle = $job.terminal_cycle)); IF $valid { CREATE type::record($staged_table, $key) CONTENT $data; }; RETURN IF $valid { 'committed' } ELSE { 'stale' }; COMMIT TRANSACTION;")
            .bind(("table", self.table.clone())).bind(("staged_table", staged_table)).bind(("id", grant.job_id.clone()))
            .bind(("owner", grant.owner.clone())).bind(("epoch", grant.epoch)).bind(("version", grant.version))
            .bind(("key", format!("{}:{}:{}", grant.job_id, grant.epoch, canonical_id))).bind(("data", data))
            .await.map_err(|e| Self::port_err("atomic stage", e))?;
        if Self::decision(response)? != CommitResult::Committed {
            return Err(Self::port_err(
                "atomic stage",
                "stale grant or staging limit",
            ));
        }
        Ok(())
    }
}
