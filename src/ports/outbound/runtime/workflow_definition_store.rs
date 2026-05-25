use async_trait::async_trait;

use crate::domain::errors::Result;
use crate::domain::runtime::workflow_definition::{WorkflowDefinition, WorkflowRevision};

#[async_trait]
pub trait WorkflowDefinitionStore: Send + Sync {
    async fn upsert_definition(&self, definition: WorkflowDefinition) -> Result<()>;
    async fn get_definition(&self, workflow_id: &str) -> Result<Option<WorkflowDefinition>>;
    async fn insert_revision(&self, revision: WorkflowRevision) -> Result<()>;
    async fn list_revisions(&self, workflow_id: &str) -> Result<Vec<WorkflowRevision>>;
}
