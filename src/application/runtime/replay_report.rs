use crate::domain::runtime::job_attempt::JobAttempt;
use crate::domain::runtime::outbox::OutboxEvent;

#[derive(Clone, Debug)]
pub struct ReplayReport {
    pub job_id: String,
    pub attempts: Vec<JobAttempt>,
    pub lineage_events: Vec<OutboxEvent>,
}
