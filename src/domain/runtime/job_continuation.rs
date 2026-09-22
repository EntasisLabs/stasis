use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::runtime::job::BackoffPolicy;
use crate::domain::runtime::placement::PlacementConstraints;

/// When a parent job may materialize a child. `AnyTerminal` matches success, failure, and cancel.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuationTrigger {
    Succeeded,
    Failed,
    Canceled,
    AnyTerminal,
}

impl ContinuationTrigger {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
            Self::AnyTerminal => "any_terminal",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "succeeded" => Some(Self::Succeeded),
            "failed" => Some(Self::Failed),
            "canceled" => Some(Self::Canceled),
            "any_terminal" => Some(Self::AnyTerminal),
            _ => None,
        }
    }

    /// `actual` is the parent outcome. `AnyTerminal` is only valid as the declared trigger.
    pub fn matches_actual(self, actual: Self) -> bool {
        match self {
            Self::AnyTerminal => !matches!(actual, Self::AnyTerminal),
            declared => declared == actual,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuationStatus {
    Pending,
    Settling,
    Materialized,
    Discarded,
}

impl ContinuationStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Settling => "settling",
            Self::Materialized => "materialized",
            Self::Discarded => "discarded",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "pending" => Some(Self::Pending),
            "settling" => Some(Self::Settling),
            "materialized" => Some(Self::Materialized),
            "discarded" => Some(Self::Discarded),
            _ => None,
        }
    }
}

/// Child job specification captured when a continuation is registered.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ChildJobSpec {
    pub job_type: String,
    pub payload_ref: String,
    pub queue: String,
    pub priority: i32,
    pub max_attempts: u32,
    pub backoff: BackoffPolicy,
    pub idempotency_key: Option<String>,
    pub correlation_id: Option<String>,
    pub placement: PlacementConstraints,
}

/// Durable parent → child edge. The child job is inserted only when the trigger matches.
#[derive(Clone, Debug, PartialEq)]
pub struct JobContinuation {
    pub id: String,
    pub parent_job_id: String,
    pub trigger: ContinuationTrigger,
    pub status: ContinuationStatus,
    pub child: ChildJobSpec,
    pub child_job_id: Option<String>,
    pub parent_output_json: Option<String>,
    pub claim_token: Option<String>,
    pub created_at: DateTime<Utc>,
}
