use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::durable_wait::{
    DurableSignalRecord, DurableWaitRecord, DurableWaitStatus,
};
use crate::ports::outbound::runtime::durable_wait_store::DurableWaitStore;

#[derive(Clone, Default)]
pub struct InMemoryDurableWaitStore {
    waits: Arc<RwLock<HashMap<String, DurableWaitRecord>>>,
    signals: Arc<RwLock<HashMap<String, DurableSignalRecord>>>,
}

fn lock_err() -> StasisError {
    StasisError::PortFailure("durable wait store lock poisoned".into())
}

#[async_trait]
impl DurableWaitStore for InMemoryDurableWaitStore {
    async fn insert_wait(&self, record: DurableWaitRecord) -> Result<()> {
        let mut waits = self.waits.write().map_err(|_| lock_err())?;
        if waits.contains_key(&record.wait_id) {
            return Err(StasisError::PortFailure(format!(
                "durable wait already exists: {}",
                record.wait_id
            )));
        }
        waits.insert(record.wait_id.clone(), record);
        Ok(())
    }

    async fn get_wait(&self, wait_id: &str) -> Result<Option<DurableWaitRecord>> {
        let waits = self.waits.read().map_err(|_| lock_err())?;
        Ok(waits.get(wait_id).cloned())
    }

    async fn get_pending_wait(
        &self,
        job_id: &str,
        signal_type: &str,
        correlation_key: &str,
    ) -> Result<Option<DurableWaitRecord>> {
        let waits = self.waits.read().map_err(|_| lock_err())?;
        Ok(waits
            .values()
            .find(|wait| {
                wait.job_id == job_id
                    && wait.signal_type == signal_type
                    && wait.correlation_key == correlation_key
                    && wait.status == DurableWaitStatus::Pending
            })
            .cloned())
    }

    async fn list_pending_by_signal(
        &self,
        signal_type: &str,
        correlation_key: &str,
    ) -> Result<Vec<DurableWaitRecord>> {
        let waits = self.waits.read().map_err(|_| lock_err())?;
        Ok(waits
            .values()
            .filter(|wait| {
                wait.signal_type == signal_type
                    && wait.correlation_key == correlation_key
                    && wait.status == DurableWaitStatus::Pending
            })
            .cloned()
            .collect())
    }

    async fn list_pending_by_job(&self, job_id: &str) -> Result<Vec<DurableWaitRecord>> {
        let waits = self.waits.read().map_err(|_| lock_err())?;
        Ok(waits
            .values()
            .filter(|wait| wait.job_id == job_id && wait.status == DurableWaitStatus::Pending)
            .cloned()
            .collect())
    }

    async fn save_wait(&self, record: DurableWaitRecord) -> Result<()> {
        let mut waits = self.waits.write().map_err(|_| lock_err())?;
        waits.insert(record.wait_id.clone(), record);
        Ok(())
    }

    async fn insert_signal(&self, record: DurableSignalRecord) -> Result<bool> {
        let mut signals = self.signals.write().map_err(|_| lock_err())?;
        if signals.contains_key(&record.signal_id) {
            return Ok(false);
        }
        signals.insert(record.signal_id.clone(), record);
        Ok(true)
    }

    async fn get_signal(&self, signal_id: &str) -> Result<Option<DurableSignalRecord>> {
        let signals = self.signals.read().map_err(|_| lock_err())?;
        Ok(signals.get(signal_id).cloned())
    }

    async fn take_unconsumed_signal(
        &self,
        signal_type: &str,
        correlation_key: &str,
        consumed_ids: &[String],
    ) -> Result<Option<DurableSignalRecord>> {
        let signals = self.signals.read().map_err(|_| lock_err())?;
        Ok(signals
            .values()
            .filter(|signal| {
                signal.signal_type == signal_type
                    && signal.correlation_key == correlation_key
                    && !consumed_ids.contains(&signal.signal_id)
            })
            .min_by_key(|signal| signal.created_at)
            .cloned())
    }

    async fn complete_wait(
        &self,
        wait_id: &str,
        status: DurableWaitStatus,
        signal_payload: Option<String>,
        signal_id: Option<String>,
        updated_at: DateTime<Utc>,
    ) -> Result<bool> {
        if matches!(status, DurableWaitStatus::Pending) {
            return Err(StasisError::PortFailure(
                "cannot complete durable wait as pending".into(),
            ));
        }
        let mut waits = self.waits.write().map_err(|_| lock_err())?;
        let Some(wait) = waits.get_mut(wait_id) else {
            return Ok(false);
        };
        if wait.status != DurableWaitStatus::Pending {
            return Ok(false);
        }
        wait.status = status;
        wait.signal_payload = signal_payload;
        if let Some(signal_id) = signal_id {
            wait.consumed_signal_ids.push(signal_id);
        }
        wait.updated_at = updated_at;
        Ok(true)
    }
}
