use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::sync::Arc;

use crate::domain::errors::Result;
use crate::domain::runtime::endpoint_delivery_status::EndpointDeliveryStatus;

#[async_trait]
pub trait EndpointDeliveryStatusStore: Send + Sync {
    async fn record_success(
        &self,
        endpoint_id: &str,
        event_id: &str,
        at: DateTime<Utc>,
    ) -> Result<()>;
    async fn record_failure(
        &self,
        endpoint_id: &str,
        event_id: &str,
        error: &str,
        at: DateTime<Utc>,
    ) -> Result<()>;
    async fn get(&self, endpoint_id: &str) -> Result<Option<EndpointDeliveryStatus>>;
    async fn list(&self) -> Result<Vec<EndpointDeliveryStatus>>;
    async fn prune_updated_before(&self, cutoff: DateTime<Utc>) -> Result<u64>;
}

#[async_trait]
impl<T> EndpointDeliveryStatusStore for Arc<T>
where
    T: EndpointDeliveryStatusStore + ?Sized,
{
    async fn record_success(
        &self,
        endpoint_id: &str,
        event_id: &str,
        at: DateTime<Utc>,
    ) -> Result<()> {
        self.as_ref()
            .record_success(endpoint_id, event_id, at)
            .await
    }

    async fn record_failure(
        &self,
        endpoint_id: &str,
        event_id: &str,
        error: &str,
        at: DateTime<Utc>,
    ) -> Result<()> {
        self.as_ref()
            .record_failure(endpoint_id, event_id, error, at)
            .await
    }

    async fn get(&self, endpoint_id: &str) -> Result<Option<EndpointDeliveryStatus>> {
        self.as_ref().get(endpoint_id).await
    }

    async fn list(&self) -> Result<Vec<EndpointDeliveryStatus>> {
        self.as_ref().list().await
    }

    async fn prune_updated_before(&self, cutoff: DateTime<Utc>) -> Result<u64> {
        self.as_ref().prune_updated_before(cutoff).await
    }
}
