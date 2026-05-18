use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use chrono::Utc;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::delivery_endpoint::{DeliveryEndpoint, NewDeliveryEndpoint};
use crate::ports::outbound::runtime::delivery_endpoint_store::DeliveryEndpointStore;

#[derive(Clone, Default)]
pub struct InMemoryDeliveryEndpointStore {
    endpoints: Arc<RwLock<HashMap<String, DeliveryEndpoint>>>,
}

#[async_trait]
impl DeliveryEndpointStore for InMemoryDeliveryEndpointStore {
    async fn insert(&self, endpoint: NewDeliveryEndpoint) -> Result<DeliveryEndpoint> {
        let mut endpoints = self
            .endpoints
            .write()
            .map_err(|_| StasisError::PortFailure("delivery endpoint store lock poisoned".to_string()))?;

        if endpoints.contains_key(&endpoint.endpoint_id) {
            return Err(StasisError::PortFailure(format!(
                "delivery endpoint already exists: {}",
                endpoint.endpoint_id
            )));
        }

        let record = endpoint.into_record();
        endpoints.insert(record.endpoint_id.clone(), record.clone());
        Ok(record)
    }

    async fn get(&self, endpoint_id: &str) -> Result<Option<DeliveryEndpoint>> {
        let endpoints = self
            .endpoints
            .read()
            .map_err(|_| StasisError::PortFailure("delivery endpoint store lock poisoned".to_string()))?;

        Ok(endpoints.get(endpoint_id).cloned())
    }

    async fn list(&self) -> Result<Vec<DeliveryEndpoint>> {
        let endpoints = self
            .endpoints
            .read()
            .map_err(|_| StasisError::PortFailure("delivery endpoint store lock poisoned".to_string()))?;

        let mut out = endpoints.values().cloned().collect::<Vec<_>>();
        out.sort_by(|left, right| left.endpoint_id.cmp(&right.endpoint_id));
        Ok(out)
    }

    async fn set_enabled(&self, endpoint_id: &str, enabled: bool) -> Result<bool> {
        let mut endpoints = self
            .endpoints
            .write()
            .map_err(|_| StasisError::PortFailure("delivery endpoint store lock poisoned".to_string()))?;

        let Some(endpoint) = endpoints.get_mut(endpoint_id) else {
            return Ok(false);
        };

        endpoint.enabled = enabled;
        endpoint.updated_at = Utc::now();
        Ok(true)
    }
}
