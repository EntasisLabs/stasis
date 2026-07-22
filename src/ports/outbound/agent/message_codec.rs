use crate::domain::agent::envelope::{AgentEnvelope, EncodedAgentMessage};
use crate::domain::errors::Result;

/// Pure translation edge: canonical envelopes ↔ wire formats (ADR-0007).
///
/// Implementations must be side-effect free aside from encode/decode work.
pub trait AgentMessageCodec: Send + Sync {
    fn content_type(&self) -> &'static str;
    fn schema_name(&self) -> &'static str;
    fn encode(&self, envelope: &AgentEnvelope) -> Result<EncodedAgentMessage>;
    fn decode(&self, message: &EncodedAgentMessage) -> Result<AgentEnvelope>;
}
