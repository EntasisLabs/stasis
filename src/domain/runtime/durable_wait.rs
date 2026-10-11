use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurableWaitStatus {
    Pending,
    Signaled,
    TimedOut,
    Cancelled,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DurableWaitRecord {
    pub wait_id: String,
    pub job_id: String,
    /// Stable identity across deferral/reacquisition, not execution epoch.
    #[serde(default)]
    pub incarnation: u64,
    pub signal_type: String,
    pub correlation_key: String,
    pub status: DurableWaitStatus,
    pub deadline_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub signal_payload: Option<String>,
    pub consumed_signal_ids: Vec<String>,
}

impl DurableWaitRecord {
    pub fn target_identity(&self) -> String {
        Self::identity(&self.job_id, self.incarnation)
    }
    pub fn identity(job_id: &str, incarnation: u64) -> String {
        format!("{}:{job_id}:{incarnation}", job_id.len())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DurableSignalRecord {
    pub signal_id: String,
    /// None buffers an unbound event; Some captures stable recipients at dispatch.
    #[serde(default)]
    pub target_incarnations: Option<Vec<String>>,
    pub signal_type: String,
    pub correlation_key: String,
    pub payload_json: String,
    pub created_at: DateTime<Utc>,
}
