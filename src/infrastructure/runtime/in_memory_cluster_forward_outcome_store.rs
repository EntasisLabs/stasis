use std::sync::{Arc, RwLock};

use async_trait::async_trait;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::cluster_node::ClusterForwardOutcome;
use crate::ports::outbound::runtime::cluster_forward_outcome_store::ClusterForwardOutcomeStore;

#[derive(Clone, Default)]
pub struct InMemoryClusterForwardOutcomeStore {
    outcomes: Arc<RwLock<Vec<ClusterForwardOutcome>>>,
}

#[async_trait]
impl ClusterForwardOutcomeStore for InMemoryClusterForwardOutcomeStore {
    async fn record(&self, outcome: ClusterForwardOutcome) -> Result<()> {
        let mut outcomes = self
            .outcomes
            .write()
            .map_err(|_| StasisError::PortFailure("cluster forward outcome lock poisoned".to_string()))?;
        outcomes.push(outcome);
        Ok(())
    }

    async fn list_recent(&self, limit: usize) -> Result<Vec<ClusterForwardOutcome>> {
        let outcomes = self
            .outcomes
            .read()
            .map_err(|_| StasisError::PortFailure("cluster forward outcome lock poisoned".to_string()))?;

        let take = limit.min(outcomes.len());
        Ok(outcomes
            .iter()
            .rev()
            .take(take)
            .cloned()
            .collect::<Vec<_>>())
    }
}
