//! Agent platform outbound ports (ADR-0007).
//!
//! Comms, translation, and MCP bridge contracts stay separate:
//! transport ≠ codec ≠ tool bridge.

pub mod event_ingress;
pub mod mcp_tool_exporter;
pub mod mcp_tool_provider;
pub mod message_codec;
pub mod transport;

pub use event_ingress::{AgentEventIngress, IngressAck, IngressDisposition};
pub use mcp_tool_exporter::McpToolExporter;
pub use mcp_tool_provider::McpToolProvider;
pub use message_codec::AgentMessageCodec;
pub use transport::AgentTransport;
