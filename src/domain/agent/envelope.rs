use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Schema version for the canonical agent envelope (ADR-0007).
pub const AGENT_ENVELOPE_SCHEMA_VERSION_V1: u32 = 1;

/// Canonical agent coordination event kinds.
///
/// Runtime orchestration speaks this model; codecs translate at comms edges.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentEnvelopeKind {
    SessionStarted,
    TurnGranted,
    TurnAccepted,
    MessageAppended,
    ToolCallRequested,
    ToolCallCompleted,
    TurnCompleted,
    Progress,
    Heartbeat,
    CancelRequested,
    Cancelled,
    Failed,
    SessionTerminated,
}

/// Stasis-canonical agent message used across comms and translation ports.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentEnvelope {
    pub schema_version: u32,
    pub kind: AgentEnvelopeKind,
    pub envelope_id: String,
    pub session_id: String,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub job_id: Option<String>,
    pub correlation_id: String,
    pub causation_id: String,
    pub participant_id: Option<String>,
    pub occurred_at: DateTime<Utc>,
    pub payload: Value,
}

impl AgentEnvelope {
    pub fn validate_schema_version(&self) -> Result<(), String> {
        if self.schema_version == AGENT_ENVELOPE_SCHEMA_VERSION_V1 {
            Ok(())
        } else {
            Err(format!(
                "unsupported agent envelope schema_version={} (supported={AGENT_ENVELOPE_SCHEMA_VERSION_V1})",
                self.schema_version
            ))
        }
    }
}

/// Wire bytes produced/consumed by an [`crate::ports::outbound::agent::AgentMessageCodec`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncodedAgentMessage {
    pub content_type: String,
    pub schema_name: String,
    pub body: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn accepts_v1_schema_version() {
        let envelope = AgentEnvelope {
            schema_version: AGENT_ENVELOPE_SCHEMA_VERSION_V1,
            kind: AgentEnvelopeKind::TurnGranted,
            envelope_id: "env-1".into(),
            session_id: "sess-1".into(),
            thread_id: None,
            turn_id: Some("turn-1".into()),
            job_id: None,
            correlation_id: "corr-1".into(),
            causation_id: "cause-1".into(),
            participant_id: Some("agent-a".into()),
            occurred_at: Utc::now(),
            payload: json!({}),
        };
        assert!(envelope.validate_schema_version().is_ok());
    }

    #[test]
    fn rejects_unknown_schema_version() {
        let envelope = AgentEnvelope {
            schema_version: 99,
            kind: AgentEnvelopeKind::Failed,
            envelope_id: "env-2".into(),
            session_id: "sess-1".into(),
            thread_id: None,
            turn_id: None,
            job_id: None,
            correlation_id: "corr-1".into(),
            causation_id: "cause-1".into(),
            participant_id: None,
            occurred_at: Utc::now(),
            payload: json!({}),
        };
        assert!(envelope.validate_schema_version().is_err());
    }
}
