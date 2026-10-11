use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::thread::{NewThread, NewThreadEvent, ThreadEvent, ThreadSnapshot};
use crate::ports::outbound::runtime::thread_store::ThreadStore;

use super::codec::{decode_time, encode_time, sql_err};
use super::session::SqliteSession;

#[derive(Clone)]
pub struct SqliteThreadStore {
    session: SqliteSession,
}

impl SqliteThreadStore {
    pub fn new(session: SqliteSession) -> Self {
        Self { session }
    }

    fn map_thread(row: &rusqlite::Row<'_>) -> rusqlite::Result<ThreadSnapshot> {
        Ok(ThreadSnapshot {
            thread_id: row.get(0)?,
            parent_thread_id: row.get(1)?,
            branch_label: row.get(2)?,
            created_at: decode_time(row.get(3)?).map_err(|err| {
                rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(
                    err.to_string(),
                )))
            })?,
            updated_at: decode_time(row.get(4)?).map_err(|err| {
                rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(
                    err.to_string(),
                )))
            })?,
        })
    }
}

#[async_trait]
impl ThreadStore for SqliteThreadStore {
    async fn create_thread(&self, thread: NewThread) -> Result<ThreadSnapshot> {
        if let Some(parent_thread_id) = &thread.parent_thread_id
            && self.get_thread(parent_thread_id).await?.is_none()
        {
            return Err(StasisError::PortFailure(format!(
                "parent thread not found: {parent_thread_id}"
            )));
        }

        let created_at = encode_time(thread.created_at);
        self.session.execute(
            "INSERT INTO thread (thread_id, parent_thread_id, branch_label, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                thread.thread_id,
                thread.parent_thread_id,
                thread.branch_label,
                created_at,
                created_at
            ],
        )?;

        self.get_thread(&thread.thread_id)
            .await?
            .ok_or_else(|| sql_err("create thread", "row missing after insert"))
    }

    async fn get_thread(&self, thread_id: &str) -> Result<Option<ThreadSnapshot>> {
        self.session.query_opt(
            "SELECT thread_id, parent_thread_id, branch_label, created_at, updated_at
             FROM thread WHERE thread_id = ?1",
            rusqlite::params![thread_id],
            Self::map_thread,
        )
    }

    async fn append_event(&self, event: NewThreadEvent) -> Result<ThreadEvent> {
        let Some(thread) = self.get_thread(&event.thread_id).await? else {
            return Err(StasisError::PortFailure(format!(
                "thread not found: {}",
                event.thread_id
            )));
        };

        let occurred_at = encode_time(event.occurred_at);
        self.session.with_tx(|tx| {
            tx.execute(
                "INSERT INTO thread_event (event_id, thread_id, event_kind, payload_ref, occurred_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    event.event_id,
                    event.thread_id,
                    event.event_kind,
                    event.payload_ref,
                    occurred_at
                ],
            )
            .map_err(|err| sql_err("append thread event", err))?;
            tx.execute(
                "UPDATE thread SET updated_at = ?1 WHERE thread_id = ?2",
                rusqlite::params![occurred_at, thread.thread_id],
            )
            .map_err(|err| sql_err("update thread metadata", err))?;
            Ok(())
        })?;

        Ok(ThreadEvent {
            event_id: event.event_id,
            thread_id: event.thread_id,
            event_kind: event.event_kind,
            payload_ref: event.payload_ref,
            occurred_at: event.occurred_at,
        })
    }

    async fn list_events(&self, thread_id: &str) -> Result<Vec<ThreadEvent>> {
        self.session.query_map(
            "SELECT event_id, thread_id, event_kind, payload_ref, occurred_at
             FROM thread_event WHERE thread_id = ?1 ORDER BY occurred_at ASC",
            rusqlite::params![thread_id],
            |row| {
                Ok(ThreadEvent {
                    event_id: row.get(0)?,
                    thread_id: row.get(1)?,
                    event_kind: row.get(2)?,
                    payload_ref: row.get(3)?,
                    occurred_at: decode_time(row.get(4)?).map_err(|err| {
                        rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(
                            err.to_string(),
                        )))
                    })?,
                })
            },
        )
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
