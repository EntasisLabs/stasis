use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::domain::agent::turn_wait::{TurnWaitRecord, TurnWaitStatus};
use crate::domain::errors::Result;

#[async_trait]
pub trait TurnWaitStore: Send + Sync {
    async fn insert(&self, record: TurnWaitRecord) -> Result<()>;
    async fn get(&self, turn_id: &str) -> Result<Option<TurnWaitRecord>>;
    async fn get_by_job_id(&self, job_id: &str) -> Result<Option<TurnWaitRecord>>;
    async fn complete(
        &self,
        turn_id: &str,
        status: TurnWaitStatus,
        result_payload: Option<Value>,
        error_message: Option<String>,
        updated_at: DateTime<Utc>,
    ) -> Result<bool>;
}
