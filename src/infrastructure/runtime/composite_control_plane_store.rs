use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::errors::Result;
use crate::domain::runtime::cluster_node::{ClusterNode, ClusterNodeHeartbeat, NewClusterNode};
use crate::domain::runtime::delivery_endpoint::{DeliveryEndpoint, NewDeliveryEndpoint};
use crate::ports::outbound::runtime::cluster_node_store::ClusterNodeStore;
use crate::ports::outbound::runtime::delivery_endpoint_store::DeliveryEndpointStore;

#[derive(Clone)]
pub struct CompositeControlPlaneStore<E, C>
where
    E: DeliveryEndpointStore,
    C: ClusterNodeStore,
{
    endpoint_store: E,
    cluster_store: C,
}

impl<E, C> CompositeControlPlaneStore<E, C>
where
    E: DeliveryEndpointStore,
    C: ClusterNodeStore,
{
    pub fn new(endpoint_store: E, cluster_store: C) -> Self {
        Self {
            endpoint_store,
            cluster_store,
        }
    }
}

#[async_trait]
impl<E, C> DeliveryEndpointStore for CompositeControlPlaneStore<E, C>
where
    E: DeliveryEndpointStore + Send + Sync,
    C: ClusterNodeStore + Send + Sync,
{
    async fn insert(&self, endpoint: NewDeliveryEndpoint) -> Result<DeliveryEndpoint> {
        self.endpoint_store.insert(endpoint).await
    }

    async fn get(&self, endpoint_id: &str) -> Result<Option<DeliveryEndpoint>> {
        self.endpoint_store.get(endpoint_id).await
    }

    async fn list(&self) -> Result<Vec<DeliveryEndpoint>> {
        self.endpoint_store.list().await
    }

    async fn set_enabled(&self, endpoint_id: &str, enabled: bool) -> Result<bool> {
        self.endpoint_store.set_enabled(endpoint_id, enabled).await
    }
}

#[async_trait]
impl<E, C> ClusterNodeStore for CompositeControlPlaneStore<E, C>
where
    E: DeliveryEndpointStore + Send + Sync,
    C: ClusterNodeStore + Send + Sync,
{
    async fn register(&self, node: NewClusterNode) -> Result<ClusterNode> {
        self.cluster_store.register(node).await
    }

    async fn heartbeat(&self, heartbeat: ClusterNodeHeartbeat) -> Result<Option<ClusterNode>> {
        self.cluster_store.heartbeat(heartbeat).await
    }

    async fn get(&self, node_id: &str) -> Result<Option<ClusterNode>> {
        self.cluster_store.get(node_id).await
    }

    async fn list(&self) -> Result<Vec<ClusterNode>> {
        self.cluster_store.list().await
    }

    async fn remove(&self, node_id: &str) -> Result<bool> {
        self.cluster_store.remove(node_id).await
    }

    async fn prune_expired(&self, now: DateTime<Utc>) -> Result<u64> {
        self.cluster_store.prune_expired(now).await
    }
}
