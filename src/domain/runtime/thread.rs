use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewThread {
    pub thread_id: String,
    pub parent_thread_id: Option<String>,
    pub branch_label: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ThreadSnapshot {
    pub thread_id: String,
    pub parent_thread_id: Option<String>,
    pub branch_label: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[deprecated(note = "Use ThreadSnapshot instead")]
pub type ThreadRecord = ThreadSnapshot;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewThreadEvent {
    pub event_id: String,
    pub thread_id: String,
    pub event_kind: String,
    pub payload_ref: String,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ThreadEvent {
    pub event_id: String,
    pub thread_id: String,
    pub event_kind: String,
    pub payload_ref: String,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ThreadMergeMetadata {
    pub parent_thread_id: String,
    pub branch_thread_ids: Vec<String>,
    pub merge_strategy: String,
    pub merged_at: DateTime<Utc>,
}
