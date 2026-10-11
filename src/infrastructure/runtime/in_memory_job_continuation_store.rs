use std::sync::{Arc, RwLock};

use async_trait::async_trait;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::job_continuation::{ContinuationStatus, JobContinuation};
use crate::ports::outbound::runtime::job_continuation_store::JobContinuationStore;

#[derive(Clone, Default)]
pub struct InMemoryJobContinuationStore {
    records: Arc<RwLock<crate::application::runtime::in_memory_runtime::NativeState>>,
}

impl InMemoryJobContinuationStore {
    pub(crate) fn from_native(
        records: Arc<RwLock<crate::application::runtime::in_memory_runtime::NativeState>>,
    ) -> Self {
        Self { records }
    }
}

fn lock_err() -> StasisError {
    StasisError::PortFailure("job continuation store lock poisoned".into())
}

#[async_trait]
impl JobContinuationStore for InMemoryJobContinuationStore {
    async fn insert(&self, record: JobContinuation) -> Result<()> {
        let mut state = self.records.write().map_err(|_| lock_err())?;
        let records = &mut state.continuations;
        if records.contains_key(&record.id) {
            return Err(StasisError::PortFailure(format!(
                "job continuation already exists: {}",
                record.id
            )));
        }
        records.insert(record.id.clone(), record);
        Ok(())
    }

    async fn get(&self, id: &str) -> Result<Option<JobContinuation>> {
        let state = self.records.read().map_err(|_| lock_err())?;
        let records = &state.continuations;
        Ok(records.get(id).cloned())
    }

    async fn save(&self, record: JobContinuation) -> Result<()> {
        let mut state = self.records.write().map_err(|_| lock_err())?;
        let records = &mut state.continuations;
        if !records.contains_key(&record.id) {
            return Err(StasisError::PortFailure(format!(
                "job continuation not found: {}",
                record.id
            )));
        }
        records.insert(record.id.clone(), record);
        Ok(())
    }

    async fn list_pending_by_parent(&self, parent_job_id: &str) -> Result<Vec<JobContinuation>> {
        let state = self.records.read().map_err(|_| lock_err())?;
        let records = &state.continuations;
        Ok(records
            .values()
            .filter(|record| {
                record.parent_job_id == parent_job_id
                    && record.status == ContinuationStatus::Pending
            })
            .cloned()
            .collect())
    }

    async fn try_claim(&self, id: &str, claim_token: &str) -> Result<bool> {
        let mut state = self.records.write().map_err(|_| lock_err())?;
        let records = &mut state.continuations;
        let Some(record) = records.get_mut(id) else {
            return Ok(false);
        };
        if record.status != ContinuationStatus::Pending {
            return Ok(false);
        }
        record.status = ContinuationStatus::Settling;
        record.claim_token = Some(claim_token.to_string());
        Ok(true)
    }

    async fn get_by_child(&self, child_job_id: &str) -> Result<Option<JobContinuation>> {
        let state = self.records.read().map_err(|_| lock_err())?;
        let records = &state.continuations;
        Ok(records
            .values()
            .find(|record| record.child_job_id.as_deref() == Some(child_job_id))
            .cloned())
    }
}
