//! Vendor-neutral agent platform domain types (ADR-0007).

pub mod envelope;
pub mod mcp;
pub mod turn_wait;

pub use envelope::{
    AgentEnvelope, AgentEnvelopeKind, EncodedAgentMessage, AGENT_ENVELOPE_SCHEMA_VERSION_V1,
};
pub use mcp::McpToolDescriptor;
pub use turn_wait::{TurnWaitRecord, TurnWaitStatus};
