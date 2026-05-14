use chrono::{DateTime, Utc};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JobAttemptOutcome {
    Succeeded,
    RetryableFailure,
    FatalFailure,
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
    pub sttp_output_node_id: Option<String>,
    pub execution_id: Option<String>,
    pub diagnostics: Option<String>,
}
