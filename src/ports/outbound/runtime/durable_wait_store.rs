use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::errors::Result;
use crate::domain::runtime::durable_wait::{
    DurableSignalRecord, DurableWaitRecord, DurableWaitStatus,
};

#[async_trait]
pub trait DurableWaitStore: Send + Sync {
    async fn insert_wait(&self, record: DurableWaitRecord) -> Result<()>;
    async fn get_wait(&self, wait_id: &str) -> Result<Option<DurableWaitRecord>>;
    async fn get_pending_wait(
        &self,
        job_id: &str,
        signal_type: &str,
        correlation_key: &str,
    ) -> Result<Option<DurableWaitRecord>>;
    async fn list_pending_by_signal(
        &self,
        signal_type: &str,
        correlation_key: &str,
    ) -> Result<Vec<DurableWaitRecord>>;
    async fn list_pending_by_job(&self, job_id: &str) -> Result<Vec<DurableWaitRecord>>;
    async fn save_wait(&self, record: DurableWaitRecord) -> Result<()>;
    async fn insert_signal(&self, record: DurableSignalRecord) -> Result<bool>;
    async fn get_signal(&self, signal_id: &str) -> Result<Option<DurableSignalRecord>>;
    async fn take_unconsumed_signal(
        &self,
        signal_type: &str,
        correlation_key: &str,
        consumed_ids: &[String],
    ) -> Result<Option<DurableSignalRecord>>;
    async fn complete_wait(
        &self,
        wait_id: &str,
        status: DurableWaitStatus,
        signal_payload: Option<String>,
        signal_id: Option<String>,
        updated_at: DateTime<Utc>,
    ) -> Result<bool>;
}
