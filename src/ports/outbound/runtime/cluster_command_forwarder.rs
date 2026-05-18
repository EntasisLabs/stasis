use async_trait::async_trait;
use std::sync::Arc;

use crate::domain::errors::Result;
use crate::domain::runtime::cluster_node::ClusterForwardCommand;

#[async_trait]
pub trait ClusterCommandForwarder: Send + Sync {
    async fn forward(&self, command: ClusterForwardCommand) -> Result<bool>;
}

#[async_trait]
impl<T> ClusterCommandForwarder for Arc<T>
where
    T: ClusterCommandForwarder + ?Sized,
{
    async fn forward(&self, command: ClusterForwardCommand) -> Result<bool> {
        self.as_ref().forward(command).await
    }
}
