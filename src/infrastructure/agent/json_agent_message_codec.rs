use crate::domain::agent::envelope::{
    AgentEnvelope, EncodedAgentMessage, AGENT_ENVELOPE_SCHEMA_VERSION_V1,
};
use crate::domain::errors::{Result, StasisError};
use crate::ports::outbound::agent::message_codec::AgentMessageCodec;

pub const JSON_AGENT_CONTENT_TYPE: &str = "application/json";
pub const JSON_AGENT_SCHEMA_NAME: &str = "stasis.agent.envelope.v1";

/// Reference JSON codec for canonical agent envelopes (ADR-0007 Phase 1).
#[derive(Clone, Debug, Default)]
pub struct JsonAgentMessageCodec;

impl JsonAgentMessageCodec {
    pub fn v1() -> Self {
        Self
    }
}

impl AgentMessageCodec for JsonAgentMessageCodec {
    fn content_type(&self) -> &'static str {
        JSON_AGENT_CONTENT_TYPE
    }

    fn schema_name(&self) -> &'static str {
        JSON_AGENT_SCHEMA_NAME
    }

    fn encode(&self, envelope: &AgentEnvelope) -> Result<EncodedAgentMessage> {
        envelope
            .validate_schema_version()
            .map_err(StasisError::PortFailure)?;
        validate_required_ids(envelope)?;

        let body = serde_json::to_vec(envelope).map_err(|err| {
            StasisError::PortFailure(format!("failed to encode agent envelope json: {err}"))
        })?;

        Ok(EncodedAgentMessage {
            content_type: self.content_type().to_string(),
            schema_name: self.schema_name().to_string(),
            body,
        })
    }

    fn decode(&self, message: &EncodedAgentMessage) -> Result<AgentEnvelope> {
        if message.content_type != JSON_AGENT_CONTENT_TYPE {
            return Err(StasisError::PortFailure(format!(
                "unsupported content_type='{}' (expected '{JSON_AGENT_CONTENT_TYPE}')",
                message.content_type
            )));
        }
        if message.schema_name != JSON_AGENT_SCHEMA_NAME {
            return Err(StasisError::PortFailure(format!(
                "unsupported schema_name='{}' (expected '{JSON_AGENT_SCHEMA_NAME}')",
                message.schema_name
            )));
        }
        if message.body.is_empty() {
            return Err(StasisError::PortFailure(
                "encoded agent message body is empty".to_string(),
            ));
        }

        let envelope: AgentEnvelope = serde_json::from_slice(&message.body).map_err(|err| {
            StasisError::PortFailure(format!("failed to decode agent envelope json: {err}"))
        })?;

        envelope
            .validate_schema_version()
            .map_err(StasisError::PortFailure)?;
        validate_required_ids(&envelope)?;
        Ok(envelope)
    }
}

fn validate_required_ids(envelope: &AgentEnvelope) -> Result<()> {
    if envelope.envelope_id.trim().is_empty() {
        return Err(StasisError::PortFailure(
            "agent envelope_id must not be empty".to_string(),
        ));
    }
    if envelope.session_id.trim().is_empty() {
        return Err(StasisError::PortFailure(
            "agent session_id must not be empty".to_string(),
        ));
    }
    if envelope.correlation_id.trim().is_empty() {
        return Err(StasisError::PortFailure(
            "agent correlation_id must not be empty".to_string(),
        ));
    }
    if envelope.causation_id.trim().is_empty() {
        return Err(StasisError::PortFailure(
            "agent causation_id must not be empty".to_string(),
        ));
    }
    let _ = AGENT_ENVELOPE_SCHEMA_VERSION_V1;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::agent::envelope::AgentEnvelopeKind;
    use chrono::{TimeZone, Utc};
    use serde_json::json;

    fn sample_envelope() -> AgentEnvelope {
        AgentEnvelope {
            schema_version: AGENT_ENVELOPE_SCHEMA_VERSION_V1,
            kind: AgentEnvelopeKind::TurnGranted,
            envelope_id: "env-1".into(),
            session_id: "sess-1".into(),
            thread_id: Some("thread-1".into()),
            turn_id: Some("turn-1".into()),
            job_id: Some("job-1".into()),
            correlation_id: "corr-1".into(),
            causation_id: "cause-1".into(),
            participant_id: Some("agent-a".into()),
            occurred_at: Utc.with_ymd_and_hms(2026, 7, 22, 12, 0, 0).unwrap(),
            payload: json!({"prompt": "continue"}),
        }
    }

    #[test]
    fn round_trip_preserves_envelope() {
        let codec = JsonAgentMessageCodec::v1();
        let encoded = codec.encode(&sample_envelope()).unwrap();
        assert_eq!(encoded.content_type, JSON_AGENT_CONTENT_TYPE);
        assert_eq!(encoded.schema_name, JSON_AGENT_SCHEMA_NAME);
        let decoded = codec.decode(&encoded).unwrap();
        assert_eq!(decoded, sample_envelope());
    }

    #[test]
    fn encode_rejects_unknown_schema_version() {
        let codec = JsonAgentMessageCodec::v1();
        let mut envelope = sample_envelope();
        envelope.schema_version = 9;
        let err = codec.encode(&envelope).unwrap_err();
        assert!(err.to_string().contains("schema_version"));
    }

    #[test]
    fn encode_rejects_empty_ids() {
        let codec = JsonAgentMessageCodec::v1();
        let mut envelope = sample_envelope();
        envelope.envelope_id = "  ".into();
        assert!(codec.encode(&envelope).is_err());
        envelope = sample_envelope();
        envelope.session_id.clear();
        assert!(codec.encode(&envelope).is_err());
        envelope = sample_envelope();
        envelope.correlation_id.clear();
        assert!(codec.encode(&envelope).is_err());
        envelope = sample_envelope();
        envelope.causation_id.clear();
        assert!(codec.encode(&envelope).is_err());
    }

    #[test]
    fn decode_rejects_wrong_content_type() {
        let codec = JsonAgentMessageCodec::v1();
        let mut encoded = codec.encode(&sample_envelope()).unwrap();
        encoded.content_type = "text/plain".into();
        let err = codec.decode(&encoded).unwrap_err();
        assert!(err.to_string().contains("content_type"));
    }

    #[test]
    fn decode_rejects_wrong_schema_name() {
        let codec = JsonAgentMessageCodec::v1();
        let mut encoded = codec.encode(&sample_envelope()).unwrap();
        encoded.schema_name = "other.v1".into();
        let err = codec.decode(&encoded).unwrap_err();
        assert!(err.to_string().contains("schema_name"));
    }

    #[test]
    fn decode_rejects_empty_body() {
        let codec = JsonAgentMessageCodec::v1();
        let err = codec
            .decode(&EncodedAgentMessage {
                content_type: JSON_AGENT_CONTENT_TYPE.into(),
                schema_name: JSON_AGENT_SCHEMA_NAME.into(),
                body: Vec::new(),
            })
            .unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn decode_rejects_malformed_json() {
        let codec = JsonAgentMessageCodec::v1();
        let err = codec
            .decode(&EncodedAgentMessage {
                content_type: JSON_AGENT_CONTENT_TYPE.into(),
                schema_name: JSON_AGENT_SCHEMA_NAME.into(),
                body: b"{not-json".to_vec(),
            })
            .unwrap_err();
        assert!(err.to_string().contains("decode"));
    }

    #[test]
    fn decode_rejects_unknown_kind() {
        let codec = JsonAgentMessageCodec::v1();
        let mut value = serde_json::to_value(sample_envelope()).unwrap();
        value["kind"] = json!("not_a_real_kind");
        let err = codec
            .decode(&EncodedAgentMessage {
                content_type: JSON_AGENT_CONTENT_TYPE.into(),
                schema_name: JSON_AGENT_SCHEMA_NAME.into(),
                body: serde_json::to_vec(&value).unwrap(),
            })
            .unwrap_err();
        assert!(err.to_string().contains("decode"));
    }

    #[test]
    fn decode_rejects_unsupported_schema_version_in_body() {
        let codec = JsonAgentMessageCodec::v1();
        let mut value = serde_json::to_value(sample_envelope()).unwrap();
        value["schema_version"] = json!(2);
        let err = codec
            .decode(&EncodedAgentMessage {
                content_type: JSON_AGENT_CONTENT_TYPE.into(),
                schema_name: JSON_AGENT_SCHEMA_NAME.into(),
                body: serde_json::to_vec(&value).unwrap(),
            })
            .unwrap_err();
        assert!(err.to_string().contains("schema_version"));
    }

    #[test]
    fn golden_json_contains_snake_case_kind() {
        let codec = JsonAgentMessageCodec::v1();
        let encoded = codec.encode(&sample_envelope()).unwrap();
        let text = String::from_utf8(encoded.body).unwrap();
        assert!(text.contains("\"kind\":\"turn_granted\""));
        assert!(text.contains("\"schema_version\":1"));
        assert!(text.contains("\"envelope_id\":\"env-1\""));
    }
}
