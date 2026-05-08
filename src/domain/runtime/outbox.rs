use chrono::{DateTime, Utc};

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
    pub event: RuntimeEvent,
}
