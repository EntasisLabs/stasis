use async_trait::async_trait;
use std::sync::Arc;

use crate::domain::errors::Result;
use crate::domain::runtime::cluster_node::ClusterControlEvent;

#[async_trait]
pub trait ClusterControlEventSink: Send + Sync {
    async fn emit(&self, event: ClusterControlEvent) -> Result<()>;
}

#[async_trait]
impl<T> ClusterControlEventSink for Arc<T>
where
    T: ClusterControlEventSink + ?Sized,
{
    async fn emit(&self, event: ClusterControlEvent) -> Result<()> {
        self.as_ref().emit(event).await
    }
}
