use chrono::{DateTime, Utc};

use crate::domain::errors::Result;
use crate::domain::runtime::recurring::RecurringDefinition;
use crate::ports::outbound::runtime::store_bounds::StoreBound;

crate::async_store! {
pub trait RecurringStore: StoreBound {
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
}
