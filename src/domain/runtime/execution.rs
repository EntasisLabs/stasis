//! Native execution authority and immutable publication receipts.
//! Resource leases, caller attempts, and scheduling timestamps do not issue this authority.
use super::job::{Job, JobState};
use super::provenance::ProvenanceRef;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionGrant {
    pub job_id: String,
    pub owner: String,
    pub epoch: u64,
    pub version: u64,
}
impl ExecutionGrant {
    pub fn from_job(job: &Job) -> Option<Self> {
        Some(Self {
            job_id: job.id.clone(),
            owner: job.lease_owner.clone()?,
            epoch: job.execution_epoch,
            version: job.version,
        })
    }
    pub fn matches(&self, job: &Job) -> bool {
        self.job_id == job.id
            && job.lease_owner.as_deref() == Some(&self.owner)
            && self.epoch != 0
            && self.epoch == job.execution_epoch
            && self.version == job.version
    }
    pub fn permits(&self, job: &Job, now: DateTime<Utc>) -> bool {
        self.matches(job)
            && matches!(job.state, JobState::Leased | JobState::Running)
            && job.lease_expires_at.is_some_and(|expiry| expiry > now)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommitResult {
    Committed,
    AlreadyCommittedSameDigest,
    Stale,
    Conflict,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum JobExecutionOutcome {
    Success {
        output_provenance: Option<ProvenanceRef>,
        execution_id: Option<String>,
        diagnostics: Option<String>,
    },
    RetryableFailure {
        message: String,
        execution_id: Option<String>,
        diagnostics: Option<String>,
    },
    FatalFailure {
        message: String,
        execution_id: Option<String>,
        diagnostics: Option<String>,
    },
    /// Re-enqueue without consuming an attempt (durable external wait).
    Deferred {
        scheduled_at: DateTime<Utc>,
        message: String,
        execution_id: Option<String>,
        diagnostics: Option<String>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum JobLifecycleEvent {
    Succeeded,
    Deferred {
        scheduled_at: DateTime<Utc>,
        message: String,
    },
    RetryScheduled {
        attempt: u32,
        message: String,
    },
    DeadLettered {
        message: String,
    },
    Canceled {
        reason: String,
    },
}

/// Retained independently of deletable jobs/attempts/outbox rows. The stable grant/digest
/// are immutable; delivery acknowledgement does not grant authority to resave the job.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionReceipt {
    pub grant: ExecutionGrant,
    pub digest: String,
    pub job_json: String,
    pub lifecycle: JobLifecycleEvent,
    pub output_json: Option<String>,
    pub delivered: bool,
    /// When this receipt entered retention. `None` is kept until a timestamp exists.
    #[serde(default)]
    pub retained_at: Option<DateTime<Utc>>,
}

/// Native cancellation/failure creates this cleanup-only capability for the retired grant.
/// Acknowledgement can retain finalized evidence, but cannot publish success or modify a job.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CleanupRecord {
    pub grant: ExecutionGrant,
    pub evidence_json: Option<String>,
    /// Bounded worker-return/log observations are NOT cleanup acknowledgement or success.
    #[serde(default)]
    pub return_observations: Vec<String>,
    pub acknowledged_at: Option<DateTime<Utc>>,
    /// When this cleanup record entered retention. `None` is kept.
    #[serde(default)]
    pub retained_at: Option<DateTime<Utc>>,
}
