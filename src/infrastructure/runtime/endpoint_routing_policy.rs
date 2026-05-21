use crate::domain::runtime::delivery_endpoint::DeliveryEndpoint;
use crate::domain::runtime::outbox::{OutboxEvent, RuntimeEventType};
use crate::ports::outbound::runtime::endpoint_routing_policy::EndpointRoutingPolicy;

#[derive(Clone, Default)]
pub struct AllowAllEndpointRoutingPolicy;

impl EndpointRoutingPolicy for AllowAllEndpointRoutingPolicy {
    fn should_route(&self, _endpoint: &DeliveryEndpoint, _event: &OutboxEvent) -> bool {
        true
    }
}

#[derive(Clone, Debug, Default)]
pub struct EndpointRouteRule {
    pub endpoint_ids: Option<Vec<String>>,
    pub event_types: Option<Vec<RuntimeEventType>>,
    pub correlation_id_prefix: Option<String>,
    pub trace_id_prefix: Option<String>,
}

#[derive(Clone, Debug)]
pub struct RuleBasedEndpointRoutingPolicy {
    rules: Vec<EndpointRouteRule>,
    default_route: bool,
}

impl RuleBasedEndpointRoutingPolicy {
    pub fn new(rules: Vec<EndpointRouteRule>) -> Self {
        Self {
            rules,
            default_route: false,
        }
    }

    pub fn with_default_route(mut self, default_route: bool) -> Self {
        self.default_route = default_route;
        self
    }

    fn rule_matches(
        rule: &EndpointRouteRule,
        endpoint: &DeliveryEndpoint,
        event: &OutboxEvent,
    ) -> bool {
        let endpoint_match = rule
            .endpoint_ids
            .as_ref()
            .map(|ids| ids.iter().any(|id| id == &endpoint.endpoint_id))
            .unwrap_or(true);

        let event_type_match = rule
            .event_types
            .as_ref()
            .map(|types| types.iter().any(|t| t == &event.event.event_type))
            .unwrap_or(true);

        let correlation_match = rule
            .correlation_id_prefix
            .as_deref()
            .map(|prefix| event.event.correlation_id.starts_with(prefix))
            .unwrap_or(true);

        let trace_match = rule
            .trace_id_prefix
            .as_deref()
            .map(|prefix| event.event.trace_id.starts_with(prefix))
            .unwrap_or(true);

        endpoint_match && event_type_match && correlation_match && trace_match
    }
}

impl EndpointRoutingPolicy for RuleBasedEndpointRoutingPolicy {
    fn should_route(&self, endpoint: &DeliveryEndpoint, event: &OutboxEvent) -> bool {
        if self.rules.is_empty() {
            return self.default_route;
        }

        self.rules
            .iter()
            .any(|rule| Self::rule_matches(rule, endpoint, event))
    }
}
