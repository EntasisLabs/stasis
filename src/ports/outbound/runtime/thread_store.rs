use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::errors::Result;
use crate::domain::runtime::thread::{NewThread, NewThreadEvent, ThreadEvent, ThreadRecord};

#[async_trait]
pub trait ThreadStore: Send + Sync {
    async fn create_thread(&self, thread: NewThread) -> Result<ThreadRecord>;
    async fn get_thread(&self, thread_id: &str) -> Result<Option<ThreadRecord>>;
    async fn append_event(&self, event: NewThreadEvent) -> Result<ThreadEvent>;
    async fn list_events(&self, thread_id: &str) -> Result<Vec<ThreadEvent>>;
    async fn fork_thread(
        &self,
        parent_thread_id: &str,
        child_thread_id: &str,
        branch_label: Option<String>,
        created_at: DateTime<Utc>,
    ) -> Result<ThreadRecord>;
    async fn list_lineage(&self, thread_id: &str) -> Result<Vec<ThreadRecord>>;
}
