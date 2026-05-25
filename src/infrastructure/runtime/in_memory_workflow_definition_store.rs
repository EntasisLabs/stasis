use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::workflow_definition::{WorkflowDefinition, WorkflowRevision};
use crate::ports::outbound::runtime::workflow_definition_store::WorkflowDefinitionStore;

#[derive(Clone, Default)]
pub struct InMemoryWorkflowDefinitionStore {
    definitions: Arc<RwLock<HashMap<String, WorkflowDefinition>>>,
    revisions: Arc<RwLock<HashMap<String, Vec<WorkflowRevision>>>>,
}

#[async_trait]
impl WorkflowDefinitionStore for InMemoryWorkflowDefinitionStore {
    async fn upsert_definition(&self, definition: WorkflowDefinition) -> Result<()> {
        let mut definitions = self.definitions.write().map_err(|_| {
            StasisError::PortFailure("workflow definition store lock poisoned".to_string())
        })?;
        definitions.insert(definition.workflow_id.clone(), definition);
        Ok(())
    }

    async fn get_definition(&self, workflow_id: &str) -> Result<Option<WorkflowDefinition>> {
        let definitions = self.definitions.read().map_err(|_| {
            StasisError::PortFailure("workflow definition store lock poisoned".to_string())
        })?;
        Ok(definitions.get(workflow_id).cloned())
    }

    async fn insert_revision(&self, revision: WorkflowRevision) -> Result<()> {
        let mut revisions = self.revisions.write().map_err(|_| {
            StasisError::PortFailure("workflow revision store lock poisoned".to_string())
        })?;
        revisions
            .entry(revision.workflow_id.clone())
            .or_default()
            .push(revision);
        Ok(())
    }

    async fn list_revisions(&self, workflow_id: &str) -> Result<Vec<WorkflowRevision>> {
        let revisions = self.revisions.read().map_err(|_| {
            StasisError::PortFailure("workflow revision store lock poisoned".to_string())
        })?;

        let mut out = revisions.get(workflow_id).cloned().unwrap_or_default();
        out.sort_by(|left, right| right.reflected_at_utc.cmp(&left.reflected_at_utc));
        Ok(out)
    }
}
