use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;

use crate::domain::agent::envelope::AgentEnvelope;
use crate::domain::errors::{Result, StasisError};
use crate::ports::outbound::agent::event_ingress::{
    AgentEventIngress, IngressAck, IngressDisposition,
};

#[derive(Clone, Debug, Default)]
pub struct InMemoryAgentEventIngress {
    accepted_keys: Arc<RwLock<HashSet<(String, String)>>>,
    accepted: Arc<RwLock<HashMap<String, AgentEnvelope>>>,
}

impl InMemoryAgentEventIngress {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn accepted_count(&self) -> Result<usize> {
        let state = self
            .accepted
            .read()
            .map_err(|_| StasisError::PortFailure("agent ingress lock poisoned".to_string()))?;
        Ok(state.len())
    }

    pub fn get_accepted(&self, envelope_id: &str) -> Result<Option<AgentEnvelope>> {
        let state = self
            .accepted
            .read()
            .map_err(|_| StasisError::PortFailure("agent ingress lock poisoned".to_string()))?;
        Ok(state.get(envelope_id).cloned())
    }
}

#[async_trait]
impl AgentEventIngress for InMemoryAgentEventIngress {
    async fn accept(&self, envelope: AgentEnvelope) -> Result<IngressAck> {
        envelope
            .validate_schema_version()
            .map_err(StasisError::PortFailure)?;

        if envelope.envelope_id.trim().is_empty() {
            return Ok(IngressAck {
                disposition: IngressDisposition::Rejected,
                message: Some("envelope_id must not be empty".into()),
            });
        }
        if envelope.session_id.trim().is_empty() {
            return Ok(IngressAck {
                disposition: IngressDisposition::Rejected,
                message: Some("session_id must not be empty".into()),
            });
        }
        if envelope.correlation_id.trim().is_empty() {
            return Ok(IngressAck {
                disposition: IngressDisposition::Rejected,
                message: Some("correlation_id must not be empty".into()),
            });
        }

        let key = (
            envelope.correlation_id.clone(),
            envelope.envelope_id.clone(),
        );

        let mut keys = self
            .accepted_keys
            .write()
            .map_err(|_| StasisError::PortFailure("agent ingress lock poisoned".to_string()))?;
        if keys.contains(&key) {
            return Ok(IngressAck {
                disposition: IngressDisposition::Duplicate,
                message: Some(format!(
                    "duplicate envelope_id='{}' for correlation_id='{}'",
                    envelope.envelope_id, envelope.correlation_id
                )),
            });
        }

        keys.insert(key);
        drop(keys);

        let mut accepted = self
            .accepted
            .write()
            .map_err(|_| StasisError::PortFailure("agent ingress lock poisoned".to_string()))?;
        accepted.insert(envelope.envelope_id.clone(), envelope);

        Ok(IngressAck {
            disposition: IngressDisposition::Accepted,
            message: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::agent::envelope::{AgentEnvelopeKind, AGENT_ENVELOPE_SCHEMA_VERSION_V1};
    use crate::ports::outbound::agent::AgentEventIngress;
    use chrono::Utc;
    use serde_json::json;

    fn sample(envelope_id: &str) -> AgentEnvelope {
        AgentEnvelope {
            schema_version: AGENT_ENVELOPE_SCHEMA_VERSION_V1,
            kind: AgentEnvelopeKind::TurnCompleted,
            envelope_id: envelope_id.into(),
            session_id: "sess-1".into(),
            thread_id: None,
            turn_id: Some("turn-1".into()),
            job_id: Some("job-1".into()),
            correlation_id: "corr-1".into(),
            causation_id: "cause-1".into(),
            participant_id: Some("agent-a".into()),
            occurred_at: Utc::now(),
            payload: json!({"text": "done"}),
        }
    }

    #[tokio::test]
    async fn accepts_valid_envelope() {
        let ingress = InMemoryAgentEventIngress::new();
        let ack = ingress.accept(sample("env-1")).await.unwrap();
        assert_eq!(ack.disposition, IngressDisposition::Accepted);
        assert_eq!(ingress.accepted_count().unwrap(), 1);
        assert!(ingress.get_accepted("env-1").unwrap().is_some());
    }

    #[tokio::test]
    async fn duplicate_is_idempotent() {
        let ingress = InMemoryAgentEventIngress::new();
        let first = ingress.accept(sample("env-1")).await.unwrap();
        let second = ingress.accept(sample("env-1")).await.unwrap();
        assert_eq!(first.disposition, IngressDisposition::Accepted);
        assert_eq!(second.disposition, IngressDisposition::Duplicate);
        assert_eq!(ingress.accepted_count().unwrap(), 1);
    }

    #[tokio::test]
    async fn same_envelope_id_different_correlation_is_accepted() {
        let ingress = InMemoryAgentEventIngress::new();
        let mut first = sample("env-1");
        first.correlation_id = "corr-a".into();
        let mut second = sample("env-1");
        second.correlation_id = "corr-b".into();
        assert_eq!(
            ingress.accept(first).await.unwrap().disposition,
            IngressDisposition::Accepted
        );
        assert_eq!(
            ingress.accept(second).await.unwrap().disposition,
            IngressDisposition::Accepted
        );
        // second write overwrites by envelope_id in accepted map; key set has 2
        assert_eq!(ingress.accepted_count().unwrap(), 1);
    }

    #[tokio::test]
    async fn rejects_empty_ids_without_storing() {
        let ingress = InMemoryAgentEventIngress::new();
        let mut envelope = sample("env-1");
        envelope.envelope_id.clear();
        let ack = ingress.accept(envelope).await.unwrap();
        assert_eq!(ack.disposition, IngressDisposition::Rejected);
        assert_eq!(ingress.accepted_count().unwrap(), 0);

        let mut envelope = sample("env-2");
        envelope.session_id = " ".into();
        let ack = ingress.accept(envelope).await.unwrap();
        assert_eq!(ack.disposition, IngressDisposition::Rejected);

        let mut envelope = sample("env-3");
        envelope.correlation_id.clear();
        let ack = ingress.accept(envelope).await.unwrap();
        assert_eq!(ack.disposition, IngressDisposition::Rejected);
        assert_eq!(ingress.accepted_count().unwrap(), 0);
    }

    #[tokio::test]
    async fn rejects_unsupported_schema_version() {
        let ingress = InMemoryAgentEventIngress::new();
        let mut envelope = sample("env-1");
        envelope.schema_version = 99;
        let err = ingress.accept(envelope).await.unwrap_err();
        assert!(err.to_string().contains("schema_version"));
        assert_eq!(ingress.accepted_count().unwrap(), 0);
    }

    #[tokio::test]
    async fn grant_then_complete_round_trip_shape() {
        let ingress = InMemoryAgentEventIngress::new();
        let mut granted = sample("env-grant");
        granted.kind = AgentEnvelopeKind::TurnGranted;
        let mut completed = sample("env-complete");
        completed.kind = AgentEnvelopeKind::TurnCompleted;
        completed.causation_id = granted.envelope_id.clone();

        assert_eq!(
            ingress.accept(granted).await.unwrap().disposition,
            IngressDisposition::Accepted
        );
        assert_eq!(
            ingress.accept(completed.clone()).await.unwrap().disposition,
            IngressDisposition::Accepted
        );
        let stored = ingress.get_accepted("env-complete").unwrap().unwrap();
        assert_eq!(stored.kind, AgentEnvelopeKind::TurnCompleted);
        assert_eq!(stored.causation_id, "env-grant");
    }
}
