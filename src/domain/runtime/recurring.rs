use chrono::{DateTime, Utc};

#[derive(Clone, Debug)]
pub struct RecurringDefinition {
    pub id: String,
    pub queue: String,
    pub job_type: String,
    pub payload_template_ref: String,
    pub interval_seconds: i64,
    pub jitter_seconds: i64,
    pub enabled: bool,
    pub max_attempts: u32,
    pub next_run_at: DateTime<Utc>,
    pub last_run_at: Option<DateTime<Utc>>,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
}
