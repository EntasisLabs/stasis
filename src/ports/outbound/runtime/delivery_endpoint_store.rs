use async_trait::async_trait;

use crate::domain::errors::Result;
use crate::domain::runtime::delivery_endpoint::{DeliveryEndpoint, NewDeliveryEndpoint};

#[async_trait]
pub trait DeliveryEndpointStore: Send + Sync {
    async fn insert(&self, endpoint: NewDeliveryEndpoint) -> Result<DeliveryEndpoint>;
    async fn get(&self, endpoint_id: &str) -> Result<Option<DeliveryEndpoint>>;
    async fn list(&self) -> Result<Vec<DeliveryEndpoint>>;
    async fn set_enabled(&self, endpoint_id: &str, enabled: bool) -> Result<bool>;
}
