use chrono::{DateTime, Utc};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxPublishPolicy {
    pub max_attempts: u32,
    pub base_delay_seconds: i64,
    pub max_delay_seconds: i64,
}

impl Default for OutboxPublishPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 8,
            base_delay_seconds: 2,
            max_delay_seconds: 300,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeEventType {
    JobSucceeded,
    JobRetryScheduled,
    JobDeadLettered,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeEvent {
    pub event_type: RuntimeEventType,
    pub job_id: String,
    pub correlation_id: String,
    pub causation_id: String,
    pub trace_id: String,
    pub sttp_input_node_id: String,
    pub sttp_output_node_id: Option<String>,
    pub execution_id: Option<String>,
    pub occurred_at: DateTime<Utc>,
    pub message: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutboxStatus {
    Pending,
    Published,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxEvent {
    pub event_id: String,
    pub status: OutboxStatus,
    pub publish_attempts: u32,
    pub published_at: Option<DateTime<Utc>>,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub last_publish_error: Option<String>,
    pub event: RuntimeEvent,
}
