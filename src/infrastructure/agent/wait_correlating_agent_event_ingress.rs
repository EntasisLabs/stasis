use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;

use crate::domain::agent::envelope::{AgentEnvelope, AgentEnvelopeKind};
use crate::domain::agent::turn_wait::TurnWaitStatus;
use crate::domain::errors::Result;
use crate::ports::outbound::agent::event_ingress::{
    AgentEventIngress, IngressAck, IngressDisposition,
};
use crate::ports::outbound::agent::turn_wait_store::TurnWaitStore;

/// Ingress adapter that completes durable turn waits on terminal envelopes.
pub struct WaitCorrelatingAgentEventIngress {
    inner: Arc<dyn AgentEventIngress>,
    wait_store: Arc<dyn TurnWaitStore>,
}

impl WaitCorrelatingAgentEventIngress {
    pub fn new(inner: Arc<dyn AgentEventIngress>, wait_store: Arc<dyn TurnWaitStore>) -> Self {
        Self { inner, wait_store }
    }
}

#[async_trait]
impl AgentEventIngress for WaitCorrelatingAgentEventIngress {
    async fn accept(&self, envelope: AgentEnvelope) -> Result<IngressAck> {
        let ack = self.inner.accept(envelope.clone()).await?;
        if ack.disposition != IngressDisposition::Accepted {
            return Ok(ack);
        }

        let Some(turn_id) = envelope.turn_id.as_deref() else {
            return Ok(ack);
        };

        let status = match envelope.kind {
            AgentEnvelopeKind::TurnCompleted => TurnWaitStatus::Completed,
            AgentEnvelopeKind::Failed => TurnWaitStatus::Failed,
            AgentEnvelopeKind::Cancelled => TurnWaitStatus::Cancelled,
            _ => return Ok(ack),
        };

        let error_message = match status {
            TurnWaitStatus::Failed | TurnWaitStatus::Cancelled => envelope
                .payload
                .get("error")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .or_else(|| Some(format!("external turn ended with {:?}", envelope.kind))),
            _ => None,
        };

        let updated = self
            .wait_store
            .complete(
                turn_id,
                status,
                Some(envelope.payload.clone()),
                error_message,
                Utc::now(),
            )
            .await?;

        if !updated {
            return Ok(IngressAck {
                disposition: IngressDisposition::Rejected,
                message: Some(format!(
                    "no pending turn wait found for turn_id='{turn_id}'"
                )),
            });
        }

        Ok(ack)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::agent::envelope::{AgentEnvelopeKind, AGENT_ENVELOPE_SCHEMA_VERSION_V1};
    use crate::domain::agent::turn_wait::TurnWaitRecord;
    use crate::infrastructure::agent::in_memory_agent_event_ingress::InMemoryAgentEventIngress;
    use crate::infrastructure::agent::in_memory_turn_wait_store::InMemoryTurnWaitStore;
    use crate::ports::outbound::agent::AgentEventIngress;
    use serde_json::json;

    fn envelope(kind: AgentEnvelopeKind, turn_id: &str) -> AgentEnvelope {
        AgentEnvelope {
            schema_version: AGENT_ENVELOPE_SCHEMA_VERSION_V1,
            kind,
            envelope_id: format!("env-{turn_id}"),
            session_id: "sess-1".into(),
            thread_id: None,
            turn_id: Some(turn_id.into()),
            job_id: Some("job-1".into()),
            correlation_id: "corr-1".into(),
            causation_id: "cause-1".into(),
            participant_id: Some("agent-a".into()),
            occurred_at: Utc::now(),
            payload: json!({"text": "ok"}),
        }
    }

    #[tokio::test]
    async fn completes_wait_on_turn_completed() {
        let wait_store = Arc::new(InMemoryTurnWaitStore::new());
        let now = Utc::now();
        wait_store
            .insert(TurnWaitRecord {
                turn_id: "turn-1".into(),
                job_id: "job-1".into(),
                session_id: "sess-1".into(),
                correlation_id: "corr-1".into(),
                participant_id: "agent-a".into(),
                status: TurnWaitStatus::Pending,
                deadline_at: now + chrono::Duration::seconds(30),
                created_at: now,
                updated_at: now,
                result_payload: None,
                error_message: None,
            })
            .await
            .unwrap();

        let ingress = WaitCorrelatingAgentEventIngress::new(
            Arc::new(InMemoryAgentEventIngress::new()),
            wait_store.clone(),
        );
        let ack = ingress
            .accept(envelope(AgentEnvelopeKind::TurnCompleted, "turn-1"))
            .await
            .unwrap();
        assert_eq!(ack.disposition, IngressDisposition::Accepted);
        assert_eq!(
            wait_store.get("turn-1").await.unwrap().unwrap().status,
            TurnWaitStatus::Completed
        );
    }

    #[tokio::test]
    async fn rejects_when_wait_missing() {
        let ingress = WaitCorrelatingAgentEventIngress::new(
            Arc::new(InMemoryAgentEventIngress::new()),
            Arc::new(InMemoryTurnWaitStore::new()),
        );
        let ack = ingress
            .accept(envelope(AgentEnvelopeKind::TurnCompleted, "missing"))
            .await
            .unwrap();
        assert_eq!(ack.disposition, IngressDisposition::Rejected);
    }

    #[tokio::test]
    async fn ignores_non_terminal_kinds() {
        let wait_store = Arc::new(InMemoryTurnWaitStore::new());
        let now = Utc::now();
        wait_store
            .insert(TurnWaitRecord {
                turn_id: "turn-1".into(),
                job_id: "job-1".into(),
                session_id: "sess-1".into(),
                correlation_id: "corr-1".into(),
                participant_id: "agent-a".into(),
                status: TurnWaitStatus::Pending,
                deadline_at: now + chrono::Duration::seconds(30),
                created_at: now,
                updated_at: now,
                result_payload: None,
                error_message: None,
            })
            .await
            .unwrap();
        let ingress = WaitCorrelatingAgentEventIngress::new(
            Arc::new(InMemoryAgentEventIngress::new()),
            wait_store.clone(),
        );
        let ack = ingress
            .accept(envelope(AgentEnvelopeKind::Progress, "turn-1"))
            .await
            .unwrap();
        assert_eq!(ack.disposition, IngressDisposition::Accepted);
        assert_eq!(
            wait_store.get("turn-1").await.unwrap().unwrap().status,
            TurnWaitStatus::Pending
        );
    }
}
