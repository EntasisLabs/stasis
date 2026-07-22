use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::domain::agent::turn_wait::{TurnWaitRecord, TurnWaitStatus};
use crate::domain::errors::{Result, StasisError};
use crate::ports::outbound::agent::turn_wait_store::TurnWaitStore;

#[derive(Clone, Debug, Default)]
pub struct InMemoryTurnWaitStore {
    by_turn: Arc<RwLock<HashMap<String, TurnWaitRecord>>>,
    job_index: Arc<RwLock<HashMap<String, String>>>,
}

impl InMemoryTurnWaitStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl TurnWaitStore for InMemoryTurnWaitStore {
    async fn insert(&self, record: TurnWaitRecord) -> Result<()> {
        let mut by_turn = self
            .by_turn
            .write()
            .map_err(|_| StasisError::PortFailure("turn wait store lock poisoned".into()))?;
        let mut job_index = self
            .job_index
            .write()
            .map_err(|_| StasisError::PortFailure("turn wait store lock poisoned".into()))?;

        if by_turn.contains_key(&record.turn_id) {
            return Err(StasisError::PortFailure(format!(
                "turn wait already exists: {}",
                record.turn_id
            )));
        }
        job_index.insert(record.job_id.clone(), record.turn_id.clone());
        by_turn.insert(record.turn_id.clone(), record);
        Ok(())
    }

    async fn get(&self, turn_id: &str) -> Result<Option<TurnWaitRecord>> {
        let by_turn = self
            .by_turn
            .read()
            .map_err(|_| StasisError::PortFailure("turn wait store lock poisoned".into()))?;
        Ok(by_turn.get(turn_id).cloned())
    }

    async fn get_by_job_id(&self, job_id: &str) -> Result<Option<TurnWaitRecord>> {
        let turn_id = {
            let job_index = self
                .job_index
                .read()
                .map_err(|_| StasisError::PortFailure("turn wait store lock poisoned".into()))?;
            job_index.get(job_id).cloned()
        };
        let Some(turn_id) = turn_id else {
            return Ok(None);
        };
        self.get(&turn_id).await
    }

    async fn complete(
        &self,
        turn_id: &str,
        status: TurnWaitStatus,
        result_payload: Option<Value>,
        error_message: Option<String>,
        updated_at: DateTime<Utc>,
    ) -> Result<bool> {
        if matches!(status, TurnWaitStatus::Pending) {
            return Err(StasisError::PortFailure(
                "cannot complete turn wait as pending".into(),
            ));
        }

        let mut by_turn = self
            .by_turn
            .write()
            .map_err(|_| StasisError::PortFailure("turn wait store lock poisoned".into()))?;
        let Some(record) = by_turn.get_mut(turn_id) else {
            return Ok(false);
        };
        if record.status != TurnWaitStatus::Pending {
            // Idempotent: already terminal.
            return Ok(true);
        }
        record.status = status;
        record.result_payload = result_payload;
        record.error_message = error_message;
        record.updated_at = updated_at;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn pending(turn_id: &str) -> TurnWaitRecord {
        let now = Utc::now();
        TurnWaitRecord {
            turn_id: turn_id.into(),
            job_id: "job-1".into(),
            session_id: "sess-1".into(),
            correlation_id: "corr-1".into(),
            participant_id: "agent-a".into(),
            status: TurnWaitStatus::Pending,
            deadline_at: now + chrono::Duration::seconds(30),
            created_at: now,
            updated_at: now,
            result_payload: None,
            error_message: None,
        }
    }

    #[tokio::test]
    async fn insert_get_and_complete() {
        let store = InMemoryTurnWaitStore::new();
        store.insert(pending("turn-1")).await.unwrap();
        assert!(store.get("turn-1").await.unwrap().is_some());
        assert!(store.get_by_job_id("job-1").await.unwrap().is_some());

        let ok = store
            .complete(
                "turn-1",
                TurnWaitStatus::Completed,
                Some(json!({"text": "done"})),
                None,
                Utc::now(),
            )
            .await
            .unwrap();
        assert!(ok);
        let record = store.get("turn-1").await.unwrap().unwrap();
        assert_eq!(record.status, TurnWaitStatus::Completed);

        // Idempotent complete
        assert!(
            store
                .complete(
                    "turn-1",
                    TurnWaitStatus::Failed,
                    None,
                    Some("ignored".into()),
                    Utc::now(),
                )
                .await
                .unwrap()
        );
        assert_eq!(
            store.get("turn-1").await.unwrap().unwrap().status,
            TurnWaitStatus::Completed
        );
    }

    #[tokio::test]
    async fn rejects_duplicate_insert_and_missing_complete() {
        let store = InMemoryTurnWaitStore::new();
        store.insert(pending("turn-1")).await.unwrap();
        assert!(store.insert(pending("turn-1")).await.is_err());
        assert!(!store
            .complete(
                "missing",
                TurnWaitStatus::Completed,
                None,
                None,
                Utc::now(),
            )
            .await
            .unwrap());
    }
}
