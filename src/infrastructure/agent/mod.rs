//! Agent platform infrastructure adapters (ADR-0007).

pub mod in_memory_agent_event_ingress;
pub mod json_agent_message_codec;

pub use in_memory_agent_event_ingress::InMemoryAgentEventIngress;
pub use json_agent_message_codec::{
    JsonAgentMessageCodec, JSON_AGENT_CONTENT_TYPE, JSON_AGENT_SCHEMA_NAME,
};
