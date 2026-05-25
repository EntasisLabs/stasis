use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::{Surreal, engine::any::Any};
use surrealdb_types::SurrealValue;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::workflow_definition::{WorkflowDefinition, WorkflowRevision};
use crate::ports::outbound::runtime::workflow_definition_store::WorkflowDefinitionStore;

#[derive(Clone)]
pub struct SurrealWorkflowDefinitionStore {
    db: Surreal<Any>,
    definition_table: String,
    revision_table: String,
}

impl SurrealWorkflowDefinitionStore {
    pub fn new(db: Surreal<Any>) -> Self {
        Self {
            db,
            definition_table: "workflow_definition".to_string(),
            revision_table: "workflow_revision".to_string(),
        }
    }

    fn port_err(prefix: &str, err: impl std::fmt::Display) -> StasisError {
        StasisError::PortFailure(format!("{prefix}: {err}"))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
struct WorkflowDefinitionRecord {
    workflow_id: String,
    queue: String,
    latest_revision_id: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
struct WorkflowRevisionRecord {
    workflow_id: String,
    revision_id: String,
    source: String,
    graph_modules_csv: String,
    graph_function_steps_csv: String,
    graph_function_inputs_json: String,
    reflected_at_utc: DateTime<Utc>,
    executable_count: usize,
    reflection_receipt_json: String,
}

impl From<WorkflowDefinition> for WorkflowDefinitionRecord {
    fn from(value: WorkflowDefinition) -> Self {
        Self {
            workflow_id: value.workflow_id,
            queue: value.queue,
            latest_revision_id: value.latest_revision_id,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}

impl From<WorkflowDefinitionRecord> for WorkflowDefinition {
    fn from(value: WorkflowDefinitionRecord) -> Self {
        Self {
            workflow_id: value.workflow_id,
            queue: value.queue,
            latest_revision_id: value.latest_revision_id,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}

impl From<WorkflowRevision> for WorkflowRevisionRecord {
    fn from(value: WorkflowRevision) -> Self {
        Self {
            workflow_id: value.workflow_id,
            revision_id: value.revision_id,
            source: value.source,
            graph_modules_csv: value.graph_modules_csv,
            graph_function_steps_csv: value.graph_function_steps_csv,
            graph_function_inputs_json: value.graph_function_inputs_json,
            reflected_at_utc: value.reflected_at_utc,
            executable_count: value.executable_count,
            reflection_receipt_json: value.reflection_receipt_json,
        }
    }
}

impl From<WorkflowRevisionRecord> for WorkflowRevision {
    fn from(value: WorkflowRevisionRecord) -> Self {
        Self {
            workflow_id: value.workflow_id,
            revision_id: value.revision_id,
            source: value.source,
            graph_modules_csv: value.graph_modules_csv,
            graph_function_steps_csv: value.graph_function_steps_csv,
            graph_function_inputs_json: value.graph_function_inputs_json,
            reflected_at_utc: value.reflected_at_utc,
            executable_count: value.executable_count,
            reflection_receipt_json: value.reflection_receipt_json,
        }
    }
}

#[async_trait]
impl WorkflowDefinitionStore for SurrealWorkflowDefinitionStore {
    async fn upsert_definition(&self, definition: WorkflowDefinition) -> Result<()> {
        let record: WorkflowDefinitionRecord = definition.into();
        self.db
            .query("UPSERT type::record($table, $id) CONTENT $data")
            .bind(("table", self.definition_table.clone()))
            .bind(("id", record.workflow_id.clone()))
            .bind(("data", record))
            .await
            .map_err(|e| Self::port_err("upsert workflow definition", e))?;
        Ok(())
    }

    async fn get_definition(&self, workflow_id: &str) -> Result<Option<WorkflowDefinition>> {
        let mut response = self
            .db
            .query("SELECT * FROM type::record($table, $id)")
            .bind(("table", self.definition_table.clone()))
            .bind(("id", workflow_id.to_string()))
            .await
            .map_err(|e| Self::port_err("load workflow definition", e))?;

        let row: Option<WorkflowDefinitionRecord> = response
            .take(0)
            .map_err(|e| Self::port_err("decode workflow definition", e))?;

        Ok(row.map(WorkflowDefinition::from))
    }

    async fn insert_revision(&self, revision: WorkflowRevision) -> Result<()> {
        let record: WorkflowRevisionRecord = revision.into();
        let record_id = format!("{}::{}", record.workflow_id, record.revision_id);

        self.db
            .query("CREATE type::record($table, $id) CONTENT $data")
            .bind(("table", self.revision_table.clone()))
            .bind(("id", record_id))
            .bind(("data", record))
            .await
            .map_err(|e| Self::port_err("insert workflow revision", e))?;

        Ok(())
    }

    async fn list_revisions(&self, workflow_id: &str) -> Result<Vec<WorkflowRevision>> {
        let mut response = self
            .db
            .query(
                "SELECT * FROM type::table($table) WHERE workflow_id = $workflow_id ORDER BY reflected_at_utc DESC",
            )
            .bind(("table", self.revision_table.clone()))
            .bind(("workflow_id", workflow_id.to_string()))
            .await
            .map_err(|e| Self::port_err("list workflow revisions", e))?;

        let rows: Vec<WorkflowRevisionRecord> = response
            .take(0)
            .map_err(|e| Self::port_err("decode workflow revisions", e))?;

        Ok(rows.into_iter().map(WorkflowRevision::from).collect())
    }
}
