use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::runtime::placement::PlacementConstraints;
use crate::domain::runtime::provenance::{ProvenanceRef, SttpProvenanceAdapter};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JobState {
    Enqueued,
    Leased,
    Running,
    Succeeded,
    Failed,
    DeadLetter,
    Canceled,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackoffPolicy {
    pub base_delay_seconds: i64,
    pub max_delay_seconds: i64,
}

impl Default for BackoffPolicy {
    fn default() -> Self {
        Self {
            base_delay_seconds: 5,
            max_delay_seconds: 300,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Job {
    pub id: String,
    pub queue: String,
    pub job_type: String,
    pub payload_ref: String,
    pub state: JobState,
    pub priority: i32,
    pub attempts: u32,
    pub max_attempts: u32,
    pub backoff_policy: BackoffPolicy,
    pub idempotency_key: String,
    pub correlation_id: String,
    pub causation_id: String,
    pub trace_id: String,
    /// Optional runtime-neutral input lineage (STTP retained via [`SttpProvenanceAdapter`]).
    pub input_provenance: Option<ProvenanceRef>,
    /// Optional runtime-neutral output lineage.
    pub output_provenance: Option<ProvenanceRef>,
    pub placement: PlacementConstraints,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub heartbeat_at: Option<DateTime<Utc>>,
    pub scheduled_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub progress_json: Option<String>,
}

impl Job {
    /// Compatibility view of STTP input node id when input provenance uses the STTP scheme.
    pub fn sttp_input_node_id(&self) -> String {
        SttpProvenanceAdapter::to_compat_string(self.input_provenance.as_ref())
    }

    /// Compatibility view of STTP output node id when output provenance uses the STTP scheme.
    pub fn sttp_output_node_id(&self) -> Option<String> {
        SttpProvenanceAdapter::from_optional(self.output_provenance.as_ref())
    }

    pub fn set_sttp_output_node_id(&mut self, node_id: impl AsRef<str>) {
        self.output_provenance = Some(SttpProvenanceAdapter::to_provenance(node_id));
    }
}

#[derive(Clone, Debug)]
pub struct NewJob {
    pub id: String,
    pub queue: String,
    pub job_type: String,
    pub payload_ref: String,
    pub priority: i32,
    pub max_attempts: u32,
    pub idempotency_key: String,
    pub correlation_id: String,
    pub causation_id: String,
    pub trace_id: String,
    pub input_provenance: Option<ProvenanceRef>,
    pub placement: PlacementConstraints,
    pub scheduled_at: DateTime<Utc>,
    pub backoff_policy: BackoffPolicy,
}

impl NewJob {
    pub fn into_job(self) -> Job {
        Job {
            id: self.id,
            queue: self.queue,
            job_type: self.job_type,
            payload_ref: self.payload_ref,
            state: JobState::Enqueued,
            priority: self.priority,
            attempts: 0,
            max_attempts: self.max_attempts,
            backoff_policy: self.backoff_policy,
            idempotency_key: self.idempotency_key,
            correlation_id: self.correlation_id,
            causation_id: self.causation_id,
            trace_id: self.trace_id,
            input_provenance: self.input_provenance,
            output_provenance: None,
            placement: self.placement,
            lease_owner: None,
            lease_expires_at: None,
            heartbeat_at: None,
            scheduled_at: self.scheduled_at,
            started_at: None,
            finished_at: None,
            last_error: None,
            progress_json: None,
        }
    }

    /// Convenience for callers still constructing STTP lineage explicitly.
    pub fn with_sttp_input_node_id(mut self, node_id: impl AsRef<str>) -> Self {
        self.input_provenance = Some(SttpProvenanceAdapter::to_provenance(node_id));
        self
    }
}
