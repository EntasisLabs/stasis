use chrono::Utc;

use crate::application::dto::{
    RegisterDeliveryEndpointRequest, RegisterDeliveryEndpointResponse,
    SetDeliveryEndpointEnabledRequest,
};
use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::delivery_endpoint::{DeliveryEndpoint, NewDeliveryEndpoint};
use crate::ports::outbound::runtime::delivery_endpoint_store::DeliveryEndpointStore;

#[derive(Clone)]
pub struct RegisterDeliveryEndpoint<S>
where
    S: DeliveryEndpointStore,
{
    store: S,
}

impl<S> RegisterDeliveryEndpoint<S>
where
    S: DeliveryEndpointStore,
{
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub async fn execute(
        &self,
        request: RegisterDeliveryEndpointRequest,
    ) -> Result<RegisterDeliveryEndpointResponse> {
        if request.endpoint_id.trim().is_empty() {
            return Err(StasisError::PortFailure(
                "endpoint_id must not be empty".to_string(),
            ));
        }
        if request.name.trim().is_empty() {
            return Err(StasisError::PortFailure("name must not be empty".to_string()));
        }
        if request.target.trim().is_empty() {
            return Err(StasisError::PortFailure("target must not be empty".to_string()));
        }

        let record = self
            .store
            .insert(NewDeliveryEndpoint {
                endpoint_id: request.endpoint_id,
                name: request.name,
                protocol: request.protocol,
                target: request.target,
                metadata: request.metadata,
                created_at: Utc::now(),
            })
            .await?;

        Ok(RegisterDeliveryEndpointResponse {
            endpoint_id: record.endpoint_id,
            enabled: record.enabled,
        })
    }
}

#[derive(Clone)]
pub struct SetDeliveryEndpointEnabled<S>
where
    S: DeliveryEndpointStore,
{
    store: S,
}

impl<S> SetDeliveryEndpointEnabled<S>
where
    S: DeliveryEndpointStore,
{
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub async fn execute(&self, request: SetDeliveryEndpointEnabledRequest) -> Result<()> {
        if request.endpoint_id.trim().is_empty() {
            return Err(StasisError::PortFailure(
                "endpoint_id must not be empty".to_string(),
            ));
        }

        let updated = self
            .store
            .set_enabled(&request.endpoint_id, request.enabled)
            .await?;

        if !updated {
            return Err(StasisError::PortFailure(format!(
                "delivery endpoint not found: {}",
                request.endpoint_id
            )));
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct ListDeliveryEndpoints<S>
where
    S: DeliveryEndpointStore,
{
    store: S,
}

impl<S> ListDeliveryEndpoints<S>
where
    S: DeliveryEndpointStore,
{
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub async fn execute(&self) -> Result<Vec<DeliveryEndpoint>> {
        self.store.list().await
    }
}

#[cfg(test)]
mod tests {
    use super::{ListDeliveryEndpoints, RegisterDeliveryEndpoint, SetDeliveryEndpointEnabled};
    use crate::application::dto::{
        RegisterDeliveryEndpointRequest, SetDeliveryEndpointEnabledRequest,
    };
    use crate::domain::runtime::delivery_endpoint::DeliveryProtocol;
    use crate::infrastructure::runtime::in_memory_delivery_endpoint_store::InMemoryDeliveryEndpointStore;

    #[tokio::test]
    async fn register_list_and_toggle_delivery_endpoint_flow_works() {
        let store = InMemoryDeliveryEndpointStore::default();
        let register = RegisterDeliveryEndpoint::new(store.clone());
        let list = ListDeliveryEndpoints::new(store.clone());
        let set_enabled = SetDeliveryEndpointEnabled::new(store);

        let registered = register
            .execute(RegisterDeliveryEndpointRequest {
                endpoint_id: "endpoint.ops.webhook".to_string(),
                name: "Ops webhook".to_string(),
                protocol: DeliveryProtocol::HttpWebhook,
                target: "https://ops.example.com/hooks/stasis".to_string(),
                metadata: Some("priority=high".to_string()),
            })
            .await
            .expect("endpoint should register");

        assert_eq!(registered.endpoint_id, "endpoint.ops.webhook");
        assert!(registered.enabled);

        set_enabled
            .execute(SetDeliveryEndpointEnabledRequest {
                endpoint_id: "endpoint.ops.webhook".to_string(),
                enabled: false,
            })
            .await
            .expect("endpoint should toggle");

        let endpoints = list.execute().await.expect("list should succeed");
        assert_eq!(endpoints.len(), 1);
        assert!(!endpoints[0].enabled);
    }
}