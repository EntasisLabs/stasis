use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::domain::runtime::job::BackoffPolicy;
use crate::domain::runtime::placement::PlacementConstraints;

/// Durable defaults for a typed job. Call-site builders override individual fields.
#[derive(Clone, Debug, PartialEq)]
pub struct JobDeclaration {
    pub queue: String,
    pub priority: i32,
    pub retry: RetryPolicy,
    pub placement: PlacementConstraints,
}

impl JobDeclaration {
    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = queue.into();
        self
    }

    pub fn priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    pub fn placement(mut self, placement: PlacementConstraints) -> Self {
        self.placement = placement;
        self
    }
}

impl Default for JobDeclaration {
    fn default() -> Self {
        Self {
            queue: "default".into(),
            priority: 100,
            retry: RetryPolicy::default(),
            placement: PlacementConstraints::default(),
        }
    }
}

/// Typed durable job payload. `NAME` is the runtime `job_type`.
pub trait StasisJob: Serialize + DeserializeOwned + Send + Sync + 'static {
    const NAME: &'static str;
    const VERSION: u32;
    type Output: Serialize + DeserializeOwned + Send;

    /// Queue, retry, priority, and placement used when a caller does not override them.
    fn declaration() -> JobDeclaration {
        JobDeclaration::default()
    }
}

/// Typed durable signal/event used with [`crate::application::runtime::job_context::JobContext::wait_for`].
pub trait StasisEvent: Serialize + DeserializeOwned + Send + Sync + 'static {
    const NAME: &'static str;
    const VERSION: u32;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub backoff: BackoffPolicy,
}

impl RetryPolicy {
    pub fn exponential(max_attempts: u32) -> Self {
        Self {
            max_attempts: max_attempts.max(1),
            backoff: BackoffPolicy::default(),
        }
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::exponential(3)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TypedJobEnvelope<T> {
    pub version: u32,
    pub payload: T,
}
