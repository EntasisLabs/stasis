use crate::domain::runtime::delivery_endpoint::DeliveryEndpoint;
use crate::domain::runtime::outbox::OutboxEvent;

pub trait EndpointRoutingPolicy: Send + Sync {
    fn should_route(&self, endpoint: &DeliveryEndpoint, event: &OutboxEvent) -> bool;
}