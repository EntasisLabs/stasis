use async_trait::async_trait;
use std::sync::Arc;

use crate::domain::errors::Result;
use crate::domain::runtime::cluster_node::ClusterForwardOutcome;

#[async_trait]
pub trait ClusterForwardOutcomeStore: Send + Sync {
    async fn record(&self, outcome: ClusterForwardOutcome) -> Result<()>;
    async fn list_recent(&self, limit: usize) -> Result<Vec<ClusterForwardOutcome>>;
}

#[async_trait]
impl<T> ClusterForwardOutcomeStore for Arc<T>
where
    T: ClusterForwardOutcomeStore + ?Sized,
{
    async fn record(&self, outcome: ClusterForwardOutcome) -> Result<()> {
        self.as_ref().record(outcome).await
    }

    async fn list_recent(&self, limit: usize) -> Result<Vec<ClusterForwardOutcome>> {
        self.as_ref().list_recent(limit).await
    }
}
