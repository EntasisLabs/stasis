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
    pub signal_type: String,
    pub correlation_key: String,
    pub status: DurableWaitStatus,
    pub deadline_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub signal_payload: Option<String>,
    pub consumed_signal_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DurableSignalRecord {
    pub signal_id: String,
    pub signal_type: String,
    pub correlation_key: String,
    pub payload_json: String,
    pub created_at: DateTime<Utc>,
}
