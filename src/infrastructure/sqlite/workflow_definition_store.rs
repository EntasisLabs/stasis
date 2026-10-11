use async_trait::async_trait;

use crate::domain::errors::Result;
use crate::domain::runtime::workflow_definition::{WorkflowDefinition, WorkflowRevision};
use crate::ports::outbound::runtime::workflow_definition_store::WorkflowDefinitionStore;

use super::codec::{decode_time, encode_time};
use super::session::SqliteSession;

#[derive(Clone)]
pub struct SqliteWorkflowDefinitionStore {
    session: SqliteSession,
}

impl SqliteWorkflowDefinitionStore {
    pub fn new(session: SqliteSession) -> Self {
        Self { session }
    }

    fn map_definition(row: &rusqlite::Row<'_>) -> rusqlite::Result<WorkflowDefinition> {
        let conv = |err: crate::domain::errors::StasisError| {
            rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(err.to_string())))
        };
        Ok(WorkflowDefinition {
            workflow_id: row.get(0)?,
            queue: row.get(1)?,
            latest_revision_id: row.get(2)?,
            created_at: decode_time(row.get(3)?).map_err(conv)?,
            updated_at: decode_time(row.get(4)?).map_err(conv)?,
        })
    }

    fn map_revision(row: &rusqlite::Row<'_>) -> rusqlite::Result<WorkflowRevision> {
        let conv = |err: crate::domain::errors::StasisError| {
            rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(err.to_string())))
        };
        let graph_state: Option<String> = row.get(3)?;
        let compiler: Option<String> = row.get(4)?;
        Ok(WorkflowRevision {
            workflow_id: row.get(0)?,
            revision_id: row.get(1)?,
            source: row.get(2)?,
            graph_state_json: graph_state.unwrap_or_else(|| "{}".to_string()),
            compiler_metadata_json: compiler.unwrap_or_else(|| "{}".to_string()),
            graph_modules_csv: row.get(5)?,
            graph_function_steps_csv: row.get(6)?,
            graph_function_inputs_json: row.get(7)?,
            reflected_at_utc: decode_time(row.get(8)?).map_err(conv)?,
            executable_count: usize::try_from(row.get::<_, i64>(9)?).unwrap_or(0),
            reflection_receipt_json: row.get(10)?,
        })
    }
}

#[async_trait]
impl WorkflowDefinitionStore for SqliteWorkflowDefinitionStore {
    async fn upsert_definition(&self, definition: WorkflowDefinition) -> Result<()> {
        self.session.execute(
            "INSERT INTO workflow_definition (
                workflow_id, queue, latest_revision_id, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(workflow_id) DO UPDATE SET
                queue = excluded.queue,
                latest_revision_id = excluded.latest_revision_id,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at",
            rusqlite::params![
                definition.workflow_id,
                definition.queue,
                definition.latest_revision_id,
                encode_time(definition.created_at),
                encode_time(definition.updated_at),
            ],
        )?;
        Ok(())
    }

    async fn get_definition(&self, workflow_id: &str) -> Result<Option<WorkflowDefinition>> {
        self.session.query_opt(
            "SELECT workflow_id, queue, latest_revision_id, created_at, updated_at
             FROM workflow_definition WHERE workflow_id = ?1",
            rusqlite::params![workflow_id],
            Self::map_definition,
        )
    }

    async fn insert_revision(&self, revision: WorkflowRevision) -> Result<()> {
        let revision_key = format!("{}::{}", revision.workflow_id, revision.revision_id);
        self.session.execute(
            "INSERT INTO workflow_revision (
                revision_key, workflow_id, revision_id, source, graph_state_json,
                compiler_metadata_json, graph_modules_csv, graph_function_steps_csv,
                graph_function_inputs_json, reflected_at_utc, executable_count,
                reflection_receipt_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            rusqlite::params![
                revision_key,
                revision.workflow_id,
                revision.revision_id,
                revision.source,
                revision.graph_state_json,
                revision.compiler_metadata_json,
                revision.graph_modules_csv,
                revision.graph_function_steps_csv,
                revision.graph_function_inputs_json,
                encode_time(revision.reflected_at_utc),
                i64::try_from(revision.executable_count).unwrap_or(i64::MAX),
                revision.reflection_receipt_json,
            ],
        )?;
        Ok(())
    }

    async fn list_revisions(&self, workflow_id: &str) -> Result<Vec<WorkflowRevision>> {
        self.session.query_map(
            "SELECT workflow_id, revision_id, source, graph_state_json, compiler_metadata_json,
                    graph_modules_csv, graph_function_steps_csv, graph_function_inputs_json,
                    reflected_at_utc, executable_count, reflection_receipt_json
             FROM workflow_revision WHERE workflow_id = ?1
             ORDER BY reflected_at_utc DESC",
            rusqlite::params![workflow_id],
            Self::map_revision,
        )
    }
}
