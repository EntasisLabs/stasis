use std::time::{Duration, Instant};

use dashmap::DashMap;

use crate::error::ApiError;

/// In-memory sliding window. Used for mint-by-IP and mint-by-subject.
#[derive(Debug)]
pub struct SlidingWindowLimiter {
    limit: u32,
    window: Duration,
    hits: DashMap<String, Vec<Instant>>,
}

impl SlidingWindowLimiter {
    pub fn new(limit: u32, window: Duration) -> Self {
        Self {
            limit: limit.max(1),
            window,
            hits: DashMap::new(),
        }
    }

    /// Returns `Err` if `key` has already used `limit` slots in the window.
    pub fn try_acquire(&self, key: &str) -> Result<(), ApiError> {
        let now = Instant::now();
        let cutoff = now.checked_sub(self.window).unwrap_or(now);
        let mut hits = self.hits.entry(key.to_string()).or_default();
        hits.retain(|at| *at > cutoff);
        if hits.len() >= self.limit as usize {
            return Err(ApiError::too_many(
                "Too many mint requests. Try again later.",
            ));
        }
        hits.push(now);
        Ok(())
    }

    pub fn len(&self, key: &str) -> usize {
        self.hits.get(key).map(|hits| hits.len()).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;

    #[test]
    fn trips_after_limit() {
        let limiter = SlidingWindowLimiter::new(2, Duration::from_secs(60));
        limiter.try_acquire("ip").unwrap();
        limiter.try_acquire("ip").unwrap();
        let err = limiter.try_acquire("ip").unwrap_err();
        assert_eq!(err.status, StatusCode::TOO_MANY_REQUESTS);
        limiter.try_acquire("other").unwrap();
    }
}
