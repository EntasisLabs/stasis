use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;

use crate::domain::errors::Result;
use crate::domain::runtime::cluster_node::{ClusterNode, ClusterNodeHeartbeat, NewClusterNode};

#[async_trait]
pub trait ClusterNodeStore: Send + Sync {
    async fn register(&self, node: NewClusterNode) -> Result<ClusterNode>;
    async fn heartbeat(&self, heartbeat: ClusterNodeHeartbeat) -> Result<Option<ClusterNode>>;
    async fn get(&self, node_id: &str) -> Result<Option<ClusterNode>>;
    async fn list(&self) -> Result<Vec<ClusterNode>>;
    async fn remove(&self, node_id: &str) -> Result<bool>;
    async fn prune_expired(&self, now: DateTime<Utc>) -> Result<u64>;
}
