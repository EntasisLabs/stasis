use chrono::{DateTime, Utc};

use crate::domain::runtime::provenance::{ProvenanceRef, SttpProvenanceAdapter};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JobAttemptOutcome {
    Succeeded,
    RetryableFailure,
    FatalFailure,
    /// Job released without consuming an attempt (durable wait / park).
    Deferred,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobAttempt {
    pub attempt_id: String,
    pub job_id: String,
    pub attempt_number: u32,
    pub worker_id: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub outcome: JobAttemptOutcome,
    pub error_message: Option<String>,
    pub output_provenance: Option<ProvenanceRef>,
    pub execution_id: Option<String>,
    pub guardrail_code: Option<String>,
    pub policy_reason: Option<String>,
    pub duration_ms: Option<u64>,
    pub diagnostics: Option<String>,
}

impl JobAttempt {
    pub fn sttp_output_node_id(&self) -> Option<String> {
        SttpProvenanceAdapter::from_optional(self.output_provenance.as_ref())
    }
}
