use async_trait::async_trait;

use crate::domain::errors::Result;
use crate::domain::runtime::cluster_node::ClusterForwardCommand;
use crate::ports::outbound::runtime::cluster_command_forwarder::ClusterCommandForwarder;

#[derive(Clone, Default)]
pub struct NoopClusterCommandForwarder;

#[async_trait]
impl ClusterCommandForwarder for NoopClusterCommandForwarder {
    async fn forward(&self, _command: ClusterForwardCommand) -> Result<bool> {
        Ok(true)
    }
}
