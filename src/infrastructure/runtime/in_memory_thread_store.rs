use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::thread::{
    NewThread, NewThreadEvent, ThreadEvent, ThreadSnapshot,
};
use crate::ports::outbound::runtime::thread_store::ThreadStore;

#[derive(Clone, Default)]
pub struct InMemoryThreadStore {
    threads: Arc<RwLock<HashMap<String, ThreadSnapshot>>>,
    events: Arc<RwLock<HashMap<String, Vec<ThreadEvent>>>>,
}

#[async_trait]
impl ThreadStore for InMemoryThreadStore {
    async fn create_thread(&self, thread: NewThread) -> Result<ThreadSnapshot> {
        let mut threads = self
            .threads
            .write()
            .map_err(|_| StasisError::PortFailure("thread store lock poisoned".to_string()))?;

        if threads.contains_key(&thread.thread_id) {
            return Err(StasisError::PortFailure(format!(
                "thread already exists: {}",
                thread.thread_id
            )));
        }

        if let Some(parent_thread_id) = &thread.parent_thread_id
            && !threads.contains_key(parent_thread_id)
        {
            return Err(StasisError::PortFailure(format!(
                "parent thread not found: {}",
                parent_thread_id
            )));
        }

        let record = ThreadSnapshot {
            thread_id: thread.thread_id,
            parent_thread_id: thread.parent_thread_id,
            branch_label: thread.branch_label,
            created_at: thread.created_at,
            updated_at: thread.created_at,
        };
        threads.insert(record.thread_id.clone(), record.clone());
        Ok(record)
    }

    async fn get_thread(&self, thread_id: &str) -> Result<Option<ThreadSnapshot>> {
        let threads = self
            .threads
            .read()
            .map_err(|_| StasisError::PortFailure("thread store lock poisoned".to_string()))?;

        Ok(threads.get(thread_id).cloned())
    }

    async fn append_event(&self, event: NewThreadEvent) -> Result<ThreadEvent> {
        {
            let mut threads = self
                .threads
                .write()
                .map_err(|_| StasisError::PortFailure("thread store lock poisoned".to_string()))?;
            let Some(thread) = threads.get_mut(&event.thread_id) else {
                return Err(StasisError::PortFailure(format!(
                    "thread not found: {}",
                    event.thread_id
                )));
            };
            thread.updated_at = event.occurred_at;
        }

        let mut events = self.events.write().map_err(|_| {
            StasisError::PortFailure("thread event store lock poisoned".to_string())
        })?;

        let record = ThreadEvent {
            event_id: event.event_id,
            thread_id: event.thread_id,
            event_kind: event.event_kind,
            payload_ref: event.payload_ref,
            occurred_at: event.occurred_at,
        };
        events
            .entry(record.thread_id.clone())
            .or_insert_with(Vec::new)
            .push(record.clone());

        Ok(record)
    }

    async fn list_events(&self, thread_id: &str) -> Result<Vec<ThreadEvent>> {
        let events = self.events.read().map_err(|_| {
            StasisError::PortFailure("thread event store lock poisoned".to_string())
        })?;

        let mut result = events.get(thread_id).cloned().unwrap_or_default();
        result.sort_by(|a, b| a.occurred_at.cmp(&b.occurred_at));
        Ok(result)
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
        let threads = self
            .threads
            .read()
            .map_err(|_| StasisError::PortFailure("thread store lock poisoned".to_string()))?;

        let mut lineage = Vec::new();
        let mut cursor = threads.get(thread_id).cloned();
        while let Some(node) = cursor {
            cursor = node
                .parent_thread_id
                .as_ref()
                .and_then(|parent| threads.get(parent).cloned());
            lineage.push(node);
        }

        lineage.reverse();
        Ok(lineage)
    }
}
