use async_trait::async_trait;

use crate::domain::errors::Result;
use crate::domain::runtime::cluster_node::ClusterControlEvent;
use crate::ports::outbound::runtime::cluster_control_event_sink::ClusterControlEventSink;

#[derive(Clone, Default)]
pub struct NoopClusterControlEventSink;

#[async_trait]
impl ClusterControlEventSink for NoopClusterControlEventSink {
    async fn emit(&self, _event: ClusterControlEvent) -> Result<()> {
        Ok(())
    }
}
