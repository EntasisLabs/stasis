use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnWaitStatus {
    Pending,
    Completed,
    Failed,
    Cancelled,
    TimedOut,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TurnWaitRecord {
    pub turn_id: String,
    pub job_id: String,
    pub session_id: String,
    pub correlation_id: String,
    pub participant_id: String,
    pub status: TurnWaitStatus,
    pub deadline_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub result_payload: Option<Value>,
    pub error_message: Option<String>,
}
