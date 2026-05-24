use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::{Surreal, engine::local::Db};
use surrealdb_types::SurrealValue;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::thread::{
    NewThread, NewThreadEvent, ThreadEvent, ThreadSnapshot,
};
use crate::ports::outbound::runtime::thread_store::ThreadStore;

#[derive(Clone)]
pub struct SurrealThreadStore {
    db: Surreal<Db>,
    thread_table: String,
    event_table: String,
}

impl SurrealThreadStore {
    pub fn new(db: Surreal<Db>) -> Self {
        Self {
            db,
            thread_table: "thread".to_string(),
            event_table: "thread_event".to_string(),
        }
    }

    fn port_err(prefix: &str, err: impl std::fmt::Display) -> StasisError {
        StasisError::PortFailure(format!("{prefix}: {err}"))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
struct ThreadRecordRow {
    thread_id: String,
    parent_thread_id: Option<String>,
    branch_label: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
struct ThreadEventRow {
    event_id: String,
    thread_id: String,
    event_kind: String,
    payload_ref: String,
    occurred_at: DateTime<Utc>,
}

impl From<ThreadRecordRow> for ThreadSnapshot {
    fn from(row: ThreadRecordRow) -> Self {
        Self {
            thread_id: row.thread_id,
            parent_thread_id: row.parent_thread_id,
            branch_label: row.branch_label,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

impl From<ThreadEventRow> for ThreadEvent {
    fn from(row: ThreadEventRow) -> Self {
        Self {
            event_id: row.event_id,
            thread_id: row.thread_id,
            event_kind: row.event_kind,
            payload_ref: row.payload_ref,
            occurred_at: row.occurred_at,
        }
    }
}

#[async_trait]
impl ThreadStore for SurrealThreadStore {
    async fn create_thread(&self, thread: NewThread) -> Result<ThreadSnapshot> {
        if let Some(parent_thread_id) = &thread.parent_thread_id {
            let parent = self.get_thread(parent_thread_id).await?;
            if parent.is_none() {
                return Err(StasisError::PortFailure(format!(
                    "parent thread not found: {}",
                    parent_thread_id
                )));
            }
        }

        let row = ThreadRecordRow {
            thread_id: thread.thread_id,
            parent_thread_id: thread.parent_thread_id,
            branch_label: thread.branch_label,
            created_at: thread.created_at,
            updated_at: thread.created_at,
        };

        self.db
            .query("CREATE type::record($table, $id) CONTENT $data")
            .bind(("table", self.thread_table.clone()))
            .bind(("id", row.thread_id.clone()))
            .bind(("data", row.clone()))
            .await
            .map_err(|e| Self::port_err("create thread", e))?;

        Ok(row.into())
    }

    async fn get_thread(&self, thread_id: &str) -> Result<Option<ThreadSnapshot>> {
        let mut response = self
            .db
            .query("SELECT * FROM type::record($table, $id)")
            .bind(("table", self.thread_table.clone()))
            .bind(("id", thread_id.to_string()))
            .await
            .map_err(|e| Self::port_err("get thread", e))?;

        let row: Option<ThreadRecordRow> = response
            .take(0)
            .map_err(|e| Self::port_err("decode thread", e))?;

        Ok(row.map(ThreadSnapshot::from))
    }

    async fn append_event(&self, event: NewThreadEvent) -> Result<ThreadEvent> {
        let Some(mut thread) = self.get_thread(&event.thread_id).await? else {
            return Err(StasisError::PortFailure(format!(
                "thread not found: {}",
                event.thread_id
            )));
        };

        let event_row = ThreadEventRow {
            event_id: event.event_id,
            thread_id: event.thread_id,
            event_kind: event.event_kind,
            payload_ref: event.payload_ref,
            occurred_at: event.occurred_at,
        };

        self.db
            .query("CREATE type::record($table, $id) CONTENT $data")
            .bind(("table", self.event_table.clone()))
            .bind(("id", event_row.event_id.clone()))
            .bind(("data", event_row.clone()))
            .await
            .map_err(|e| Self::port_err("append thread event", e))?;

        thread.updated_at = event_row.occurred_at;
        let thread_row = ThreadRecordRow {
            thread_id: thread.thread_id.clone(),
            parent_thread_id: thread.parent_thread_id.clone(),
            branch_label: thread.branch_label.clone(),
            created_at: thread.created_at,
            updated_at: thread.updated_at,
        };

        self.db
            .query("UPSERT type::record($table, $id) CONTENT $data")
            .bind(("table", self.thread_table.clone()))
            .bind(("id", thread_row.thread_id.clone()))
            .bind(("data", thread_row))
            .await
            .map_err(|e| Self::port_err("update thread metadata", e))?;

        Ok(event_row.into())
    }

    async fn list_events(&self, thread_id: &str) -> Result<Vec<ThreadEvent>> {
        let mut response = self
            .db
            .query("SELECT * FROM type::table($table) WHERE thread_id = $thread_id ORDER BY occurred_at ASC")
            .bind(("table", self.event_table.clone()))
            .bind(("thread_id", thread_id.to_string()))
            .await
            .map_err(|e| Self::port_err("list thread events", e))?;

        let rows: Vec<ThreadEventRow> = response
            .take(0)
            .map_err(|e| Self::port_err("decode thread events", e))?;

        Ok(rows.into_iter().map(ThreadEvent::from).collect())
    }

    async fn fork_thread(
        &self,
        parent_thread_id: &str,
        child_thread_id: &str,
        branch_label: Option<String>,
        created_at: DateTime<Utc>,
    ) -> Result<ThreadSnapshot> {
        self.create_thread(NewThread {
            thread_id: child_thread_id.to_string(),
            parent_thread_id: Some(parent_thread_id.to_string()),
            branch_label,
            created_at,
        })
        .await
    }

    async fn list_lineage(&self, thread_id: &str) -> Result<Vec<ThreadSnapshot>> {
        let mut lineage = Vec::new();
        let mut cursor = self.get_thread(thread_id).await?;

        while let Some(node) = cursor {
            cursor = if let Some(parent_thread_id) = &node.parent_thread_id {
                self.get_thread(parent_thread_id).await?
            } else {
                None
            };
            lineage.push(node);
        }

        lineage.reverse();
        Ok(lineage)
    }
}
