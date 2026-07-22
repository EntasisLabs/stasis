//! Agent platform infrastructure adapters (ADR-0007).

pub mod in_memory_agent_event_ingress;
pub mod in_memory_agent_transport;
pub mod in_memory_turn_wait_store;
pub mod json_agent_message_codec;
pub mod wait_correlating_agent_event_ingress;

pub use in_memory_agent_event_ingress::InMemoryAgentEventIngress;
pub use in_memory_agent_transport::InMemoryAgentTransport;
pub use in_memory_turn_wait_store::InMemoryTurnWaitStore;
pub use json_agent_message_codec::{
    JsonAgentMessageCodec, JSON_AGENT_CONTENT_TYPE, JSON_AGENT_SCHEMA_NAME,
};
pub use wait_correlating_agent_event_ingress::WaitCorrelatingAgentEventIngress;
