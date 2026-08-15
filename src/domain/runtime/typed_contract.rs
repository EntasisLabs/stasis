use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::domain::runtime::job::BackoffPolicy;

/// Typed durable job payload. `NAME` is the runtime `job_type`.
pub trait StasisJob: Serialize + DeserializeOwned + Send + Sync + 'static {
    const NAME: &'static str;
    const VERSION: u32;
    type Output: Serialize + DeserializeOwned + Send;
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
