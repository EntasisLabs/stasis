use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::domain::agent::envelope::AgentEnvelope;
use crate::domain::errors::Result;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IngressDisposition {
    Accepted,
    Duplicate,
    Rejected,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngressAck {
    pub disposition: IngressDisposition,
    pub message: Option<String>,
}

/// Bidirectional comms ingress for agent envelopes (ADR-0007).
#[async_trait]
pub trait AgentEventIngress: Send + Sync {
    async fn accept(&self, envelope: AgentEnvelope) -> Result<IngressAck>;
}
