use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::errors::Result;
use crate::domain::runtime::recurring::RecurringDefinition;

#[async_trait]
pub trait RecurringStore: Send + Sync {
    async fn insert(&self, definition: RecurringDefinition) -> Result<()>;
    async fn save(&self, definition: RecurringDefinition) -> Result<()>;
    async fn lease_due(
        &self,
        now: DateTime<Utc>,
        scheduler_id: &str,
        lease_seconds: i64,
    ) -> Result<Vec<RecurringDefinition>>;
    async fn list(&self) -> Result<Vec<RecurringDefinition>>;
}
