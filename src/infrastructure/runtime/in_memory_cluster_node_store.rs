use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::cluster_node::{ClusterNode, ClusterNodeHeartbeat, NewClusterNode};
use crate::ports::outbound::runtime::cluster_node_store::ClusterNodeStore;

#[derive(Clone, Default)]
pub struct InMemoryClusterNodeStore {
    nodes: Arc<RwLock<HashMap<String, ClusterNode>>>,
}

#[async_trait]
impl ClusterNodeStore for InMemoryClusterNodeStore {
    async fn register(&self, node: NewClusterNode) -> Result<ClusterNode> {
        let record = node.into_record();
        let mut nodes = self
            .nodes
            .write()
            .map_err(|_| StasisError::PortFailure("cluster node store lock poisoned".to_string()))?;

        if nodes.contains_key(&record.node_id) {
            return Err(StasisError::PortFailure(format!(
                "cluster node already exists: {}",
                record.node_id
            )));
        }

        nodes.insert(record.node_id.clone(), record.clone());
        Ok(record)
    }

    async fn heartbeat(&self, heartbeat: ClusterNodeHeartbeat) -> Result<Option<ClusterNode>> {
        let mut nodes = self
            .nodes
            .write()
            .map_err(|_| StasisError::PortFailure("cluster node store lock poisoned".to_string()))?;

        let Some(node) = nodes.get_mut(&heartbeat.node_id) else {
            return Ok(None);
        };

        node.heartbeat_at = heartbeat.heartbeat_at;
        node.lease_expires_at = heartbeat.heartbeat_at + Duration::seconds(heartbeat.lease_ttl_seconds.max(1));
        if let Some(queue_ownership) = heartbeat.queue_ownership {
            node.queue_ownership = queue_ownership;
        }
        if let Some(capability_tags) = heartbeat.capability_tags {
            node.capability_tags = capability_tags;
        }
        if heartbeat.metadata.is_some() {
            node.metadata = heartbeat.metadata;
        }
        node.updated_at = heartbeat.heartbeat_at;

        Ok(Some(node.clone()))
    }

    async fn get(&self, node_id: &str) -> Result<Option<ClusterNode>> {
        let nodes = self
            .nodes
            .read()
            .map_err(|_| StasisError::PortFailure("cluster node store lock poisoned".to_string()))?;
        Ok(nodes.get(node_id).cloned())
    }

    async fn list(&self) -> Result<Vec<ClusterNode>> {
        let nodes = self
            .nodes
            .read()
            .map_err(|_| StasisError::PortFailure("cluster node store lock poisoned".to_string()))?;
        let mut out = nodes.values().cloned().collect::<Vec<_>>();
        out.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        Ok(out)
    }

    async fn remove(&self, node_id: &str) -> Result<bool> {
        let mut nodes = self
            .nodes
            .write()
            .map_err(|_| StasisError::PortFailure("cluster node store lock poisoned".to_string()))?;
        Ok(nodes.remove(node_id).is_some())
    }

    async fn prune_expired(&self, now: DateTime<Utc>) -> Result<u64> {
        let mut nodes = self
            .nodes
            .write()
            .map_err(|_| StasisError::PortFailure("cluster node store lock poisoned".to_string()))?;

        let before = nodes.len();
        nodes.retain(|_, node| node.lease_expires_at >= now);
        Ok((before - nodes.len()) as u64)
    }
}
