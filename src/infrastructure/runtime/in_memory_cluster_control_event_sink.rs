use std::sync::{Arc, RwLock};

use async_trait::async_trait;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::cluster_node::ClusterControlEvent;
use crate::ports::outbound::runtime::cluster_control_event_sink::ClusterControlEventSink;

#[derive(Clone, Default)]
pub struct InMemoryClusterControlEventSink {
    events: Arc<RwLock<Vec<ClusterControlEvent>>>,
}

impl InMemoryClusterControlEventSink {
    pub fn events(&self) -> Result<Vec<ClusterControlEvent>> {
        let events = self
            .events
            .read()
            .map_err(|_| StasisError::PortFailure("cluster event sink lock poisoned".to_string()))?;
        Ok(events.clone())
    }
}

#[async_trait]
impl ClusterControlEventSink for InMemoryClusterControlEventSink {
    async fn emit(&self, event: ClusterControlEvent) -> Result<()> {
        let mut events = self
            .events
            .write()
            .map_err(|_| StasisError::PortFailure("cluster event sink lock poisoned".to_string()))?;
        events.push(event);
        Ok(())
    }
}
