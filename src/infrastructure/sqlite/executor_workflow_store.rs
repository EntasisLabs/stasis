use std::sync::Arc;

use async_trait::async_trait;

use crate::domain::errors::Result;
use crate::domain::runtime::workflow_definition::{WorkflowDefinition, WorkflowRevision};
use crate::ports::outbound::runtime::workflow_definition_store::WorkflowDefinitionStore;

use super::codec::{decode_time, encode_time};
use super::executor::{SqlExecutor, SqlRow, SqlValue, query_one};

fn map_definition(row: &SqlRow) -> Result<WorkflowDefinition> {
    Ok(WorkflowDefinition {
        workflow_id: row.text(0)?,
        queue: row.text(1)?,
        latest_revision_id: row.text(2)?,
        created_at: decode_time(row.i64(3)?)?,
        updated_at: decode_time(row.i64(4)?)?,
    })
}

fn map_revision(row: &SqlRow) -> Result<WorkflowRevision> {
    Ok(WorkflowRevision {
        workflow_id: row.text(0)?,
        revision_id: row.text(1)?,
        source: row.text(2)?,
        graph_state_json: row.text_opt(3)?.unwrap_or_else(|| "{}".to_string()),
        compiler_metadata_json: row.text_opt(4)?.unwrap_or_else(|| "{}".to_string()),
        graph_modules_csv: row.text(5)?,
        graph_function_steps_csv: row.text(6)?,
        graph_function_inputs_json: row.text(7)?,
        reflected_at_utc: decode_time(row.i64(8)?)?,
        executable_count: usize::try_from(row.i64(9)?).unwrap_or(0),
        reflection_receipt_json: row.text(10)?,
    })
}

#[derive(Clone)]
pub struct ExecutorWorkflowDefinitionStore {
    executor: Arc<dyn SqlExecutor>,
}

impl ExecutorWorkflowDefinitionStore {
    pub fn new(executor: Arc<dyn SqlExecutor>) -> Self {
        Self { executor }
    }

    pub fn latest_graph_state(&self, workflow_id: &str) -> Result<Option<String>> {
        let row = query_one(
            self.executor.as_ref(),
            "SELECT graph_state_json FROM workflow_revision
             WHERE workflow_id = ?1
             ORDER BY reflected_at_utc DESC
             LIMIT 1",
            &[SqlValue::text(workflow_id)],
        )?;
        match row {
            Some(row) => Ok(Some(row.text_opt(0)?.unwrap_or_else(|| "{}".to_string()))),
            None => Ok(None),
        }
    }
}

#[async_trait]
impl WorkflowDefinitionStore for ExecutorWorkflowDefinitionStore {
    async fn upsert_definition(&self, definition: WorkflowDefinition) -> Result<()> {
        self.executor.execute(
            "INSERT INTO workflow_definition (
                workflow_id, queue, latest_revision_id, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(workflow_id) DO UPDATE SET
                queue = excluded.queue,
                latest_revision_id = excluded.latest_revision_id,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at",
            &[
                SqlValue::from(definition.workflow_id),
                SqlValue::from(definition.queue),
                SqlValue::from(definition.latest_revision_id),
                SqlValue::from(encode_time(definition.created_at)),
                SqlValue::from(encode_time(definition.updated_at)),
            ],
        )?;
        Ok(())
    }

    async fn get_definition(&self, workflow_id: &str) -> Result<Option<WorkflowDefinition>> {
        let row = query_one(
            self.executor.as_ref(),
            "SELECT workflow_id, queue, latest_revision_id, CAST(created_at AS TEXT), \
             CAST(updated_at AS TEXT)
             FROM workflow_definition WHERE workflow_id = ?1",
            &[SqlValue::text(workflow_id)],
        )?;
        row.as_ref().map(map_definition).transpose()
    }

    async fn insert_revision(&self, revision: WorkflowRevision) -> Result<()> {
        let revision_key = format!("{}::{}", revision.workflow_id, revision.revision_id);
        self.executor.execute(
            "INSERT INTO workflow_revision (
                revision_key, workflow_id, revision_id, source, graph_state_json,
                compiler_metadata_json, graph_modules_csv, graph_function_steps_csv,
                graph_function_inputs_json, reflected_at_utc, executable_count,
                reflection_receipt_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            &[
                SqlValue::from(revision_key),
                SqlValue::from(revision.workflow_id),
                SqlValue::from(revision.revision_id),
                SqlValue::from(revision.source),
                SqlValue::from(revision.graph_state_json),
                SqlValue::from(revision.compiler_metadata_json),
                SqlValue::from(revision.graph_modules_csv),
                SqlValue::from(revision.graph_function_steps_csv),
                SqlValue::from(revision.graph_function_inputs_json),
                SqlValue::from(encode_time(revision.reflected_at_utc)),
                SqlValue::from(i64::try_from(revision.executable_count).unwrap_or(i64::MAX)),
                SqlValue::from(revision.reflection_receipt_json),
            ],
        )?;
        Ok(())
    }

    async fn list_revisions(&self, workflow_id: &str) -> Result<Vec<WorkflowRevision>> {
        let rows = self.executor.query(
            "SELECT workflow_id, revision_id, source, graph_state_json, compiler_metadata_json,
                    graph_modules_csv, graph_function_steps_csv, graph_function_inputs_json,
                    CAST(reflected_at_utc AS TEXT), CAST(executable_count AS TEXT),
                    reflection_receipt_json
             FROM workflow_revision WHERE workflow_id = ?1
             ORDER BY reflected_at_utc DESC",
            &[SqlValue::text(workflow_id)],
        )?;
        rows.iter().map(map_revision).collect()
    }
}
