use chrono::{DateTime, Duration, Utc};

use crate::domain::runtime::job::Job;

pub const DEFAULT_JOB_LEASE_SECONDS: i64 = 30;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JobLifecycleEvent {
    Succeeded,
    Deferred {
        scheduled_at: DateTime<Utc>,
        message: String,
    },
    RetryScheduled {
        attempt: u32,
        message: String,
    },
    DeadLettered {
        message: String,
    },
    Canceled {
        reason: String,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StaleRecoverReport {
    pub recovered: usize,
    pub dead_lettered: usize,
}

/// Applies retryable-failure accounting. Returns `true` when the job dead-lettered.
pub fn apply_retryable_failure(
    job: &mut Job,
    now: DateTime<Utc>,
    message: impl Into<String>,
) -> bool {
    let message = message.into();
    job.attempts = job.attempts.saturating_add(1);
    job.last_error = Some(message);
    job.lease_owner = None;
    job.lease_expires_at = None;
    job.heartbeat_at = None;
    if job.attempts >= job.max_attempts {
        job.state = crate::domain::runtime::job::JobState::DeadLetter;
        job.finished_at = Some(now);
        true
    } else {
        job.state = crate::domain::runtime::job::JobState::Enqueued;
        job.finished_at = None;
        job.scheduled_at = next_backoff_at(job, now);
        false
    }
}

pub fn next_backoff_at(job: &Job, now: DateTime<Utc>) -> DateTime<Utc> {
    let exponent = job.attempts.saturating_sub(1);
    let mut delay = job
        .backoff_policy
        .base_delay_seconds
        .saturating_mul(2_i64.saturating_pow(exponent));
    delay = delay.min(job.backoff_policy.max_delay_seconds);
    now + Duration::seconds(delay.max(0))
}

pub const STALE_LEASE_MESSAGE: &str = "lease expired; recovered as retryable failure";
