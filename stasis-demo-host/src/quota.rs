use std::sync::Arc;

use dashmap::DashMap;

use crate::error::ApiError;

#[derive(Debug, Clone, Copy, Default)]
pub struct QuotaLimits {
    pub daily_completions: Option<u64>,
    pub daily_tokens: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default)]
struct Usage {
    completions: u64,
    tokens: u64,
}

/// In-memory per-subject daily counters. A process restart resets counts.
#[derive(Debug, Clone)]
pub struct QuotaStore {
    limits: QuotaLimits,
    usage: Arc<DashMap<String, Usage>>,
}

impl QuotaStore {
    pub fn new(limits: QuotaLimits) -> Self {
        Self {
            limits,
            usage: Arc::new(DashMap::new()),
        }
    }

    pub fn key(subject_hex: &str, utc_date: &str) -> String {
        format!("{subject_hex}:{utc_date}")
    }

    /// Reject if this subject is already at or over a configured cap.
    pub fn check(&self, subject_hex: &str, utc_date: &str) -> Result<(), ApiError> {
        let usage = self
            .usage
            .get(&Self::key(subject_hex, utc_date))
            .map(|entry| *entry)
            .unwrap_or_default();
        self.exceeded(&usage)
    }

    /// Record a successful completion. `tokens` is prompt+completion when known.
    pub fn record_success(&self, subject_hex: &str, utc_date: &str, tokens: u64) {
        self.usage
            .entry(Self::key(subject_hex, utc_date))
            .and_modify(|usage| {
                usage.completions = usage.completions.saturating_add(1);
                usage.tokens = usage.tokens.saturating_add(tokens);
            })
            .or_insert(Usage {
                completions: 1,
                tokens,
            });
    }

    pub fn snapshot(&self, subject_hex: &str, utc_date: &str) -> (u64, u64) {
        self.usage
            .get(&Self::key(subject_hex, utc_date))
            .map(|entry| (entry.completions, entry.tokens))
            .unwrap_or((0, 0))
    }

    fn exceeded(&self, usage: &Usage) -> Result<(), ApiError> {
        if let Some(limit) = self.limits.daily_completions
            && usage.completions >= limit
        {
            return Err(quota_error(format!(
                "Daily demo limit reached ({limit} completions). Try again tomorrow (UTC)."
            )));
        }
        if let Some(limit) = self.limits.daily_tokens
            && usage.tokens >= limit
        {
            return Err(quota_error(format!(
                "Daily demo token limit reached ({limit} prompt+completion tokens). Try again tomorrow (UTC)."
            )));
        }
        Ok(())
    }
}

fn quota_error(message: String) -> ApiError {
    ApiError::too_many(message).with_code("daily_limit_exceeded")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;

    #[test]
    fn trips_after_n_completions() {
        let store = QuotaStore::new(QuotaLimits {
            daily_completions: Some(2),
            daily_tokens: None,
        });
        let subject = "aa".repeat(32);
        store.check(&subject, "2026-09-19").unwrap();
        store.record_success(&subject, "2026-09-19", 10);
        store.check(&subject, "2026-09-19").unwrap();
        store.record_success(&subject, "2026-09-19", 10);
        let err = store.check(&subject, "2026-09-19").unwrap_err();
        assert_eq!(err.status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(err.code, Some("daily_limit_exceeded"));
        assert_eq!(store.snapshot(&subject, "2026-09-19"), (2, 20));
    }

    #[test]
    fn other_subject_or_day_is_independent() {
        let store = QuotaStore::new(QuotaLimits {
            daily_completions: Some(1),
            daily_tokens: None,
        });
        let a = "aa".repeat(32);
        let b = "bb".repeat(32);
        store.record_success(&a, "2026-09-19", 1);
        store.check(&b, "2026-09-19").unwrap();
        store.check(&a, "2026-09-20").unwrap();
        assert!(store.check(&a, "2026-09-19").is_err());
    }

    #[test]
    fn token_cap_trips_independently() {
        let store = QuotaStore::new(QuotaLimits {
            daily_completions: None,
            daily_tokens: Some(8),
        });
        store.record_success("s", "2026-09-19", 8);
        let err = store.check("s", "2026-09-19").unwrap_err();
        assert_eq!(err.status, StatusCode::TOO_MANY_REQUESTS);
    }

    #[test]
    fn unlimited_when_no_caps() {
        let store = QuotaStore::new(QuotaLimits::default());
        for _ in 0..20 {
            store.check("s", "2026-09-19").unwrap();
            store.record_success("s", "2026-09-19", 100);
        }
    }
}
