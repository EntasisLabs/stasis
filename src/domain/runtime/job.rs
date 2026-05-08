use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb_types::SurrealValue;

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

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
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
    pub sttp_input_node_id: String,
    pub sttp_output_node_id: Option<String>,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub heartbeat_at: Option<DateTime<Utc>>,
    pub scheduled_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
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
    pub sttp_input_node_id: String,
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
            sttp_input_node_id: self.sttp_input_node_id,
            sttp_output_node_id: None,
            lease_owner: None,
            lease_expires_at: None,
            heartbeat_at: None,
            scheduled_at: self.scheduled_at,
            started_at: None,
            finished_at: None,
            last_error: None,
        }
    }
}
