use std::sync::{Arc, RwLock};

use async_trait::async_trait;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::cluster_node::ClusterForwardCommand;
use crate::ports::outbound::runtime::cluster_command_forwarder::ClusterCommandForwarder;

#[derive(Clone, Default)]
pub struct InMemoryClusterCommandForwarder {
    commands: Arc<RwLock<Vec<ClusterForwardCommand>>>,
}

impl InMemoryClusterCommandForwarder {
    pub fn forwarded_commands(&self) -> Result<Vec<ClusterForwardCommand>> {
        let commands = self
            .commands
            .read()
            .map_err(|_| StasisError::PortFailure("cluster command forwarder lock poisoned".to_string()))?;
        Ok(commands.clone())
    }
}

#[async_trait]
impl ClusterCommandForwarder for InMemoryClusterCommandForwarder {
    async fn forward(&self, command: ClusterForwardCommand) -> Result<bool> {
        let mut commands = self
            .commands
            .write()
            .map_err(|_| StasisError::PortFailure("cluster command forwarder lock poisoned".to_string()))?;
        commands.push(command);
        Ok(true)
    }
}
