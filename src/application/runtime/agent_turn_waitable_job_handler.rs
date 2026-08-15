use std::sync::Arc;

use async_trait::async_trait;
use chrono::{Duration, Utc};
use serde_json::json;

use crate::application::orchestration::runtime_job_payloads::AgentTurnWaitableJobPayload;
use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::domain::agent::envelope::{
    AGENT_ENVELOPE_SCHEMA_VERSION_V1, AgentEnvelope, AgentEnvelopeKind,
};
use crate::domain::agent::turn_wait::{TurnWaitRecord, TurnWaitStatus};
use crate::domain::errors::Result;
use crate::domain::runtime::job::Job;
use crate::ports::outbound::agent::transport::AgentTransport;
use crate::ports::outbound::agent::turn_wait_store::TurnWaitStore;
use crate::ports::outbound::agent::{AgentEventIngress, AgentMessageCodec};
use crate::ports::outbound::runtime::delivery_endpoint_store::DeliveryEndpointStore;

pub struct AgentTurnWaitableJobHandler {
    wait_store: Arc<dyn TurnWaitStore>,
    /// Optional ingress used to record the outbound `TurnGranted` locally for tests/fakes.
    grant_ingress: Option<Arc<dyn AgentEventIngress>>,
    codec: Arc<dyn AgentMessageCodec>,
    endpoint_store: Option<Arc<dyn DeliveryEndpointStore>>,
    transport: Option<Arc<dyn AgentTransport>>,
}

impl AgentTurnWaitableJobHandler {
    pub fn new(
        wait_store: Arc<dyn TurnWaitStore>,
        codec: Arc<dyn AgentMessageCodec>,
        grant_ingress: Option<Arc<dyn AgentEventIngress>>,
    ) -> Self {
        Self {
            wait_store,
            grant_ingress,
            codec,
            endpoint_store: None,
            transport: None,
        }
    }

    pub fn with_endpoint_publish(
        mut self,
        endpoint_store: Arc<dyn DeliveryEndpointStore>,
        transport: Arc<dyn AgentTransport>,
    ) -> Self {
        self.endpoint_store = Some(endpoint_store);
        self.transport = Some(transport);
        self
    }

    fn parse_payload(raw: &str) -> std::result::Result<AgentTurnWaitableJobPayload, String> {
        let payload: AgentTurnWaitableJobPayload = serde_json::from_str(raw).map_err(|err| {
            format!("policy violation: invalid agent-turn-waitable payload json: {err}")
        })?;
        if payload.agent_id.trim().is_empty() {
            return Err(
                "policy violation: agent-turn-waitable payload.agent_id must be non-empty".into(),
            );
        }
        if payload.session_id.trim().is_empty() {
            return Err(
                "policy violation: agent-turn-waitable payload.session_id must be non-empty".into(),
            );
        }
        if payload.turn_id.trim().is_empty() {
            return Err(
                "policy violation: agent-turn-waitable payload.turn_id must be non-empty".into(),
            );
        }
        if payload.user_prompt.trim().is_empty() {
            return Err(
                "policy violation: agent-turn-waitable payload.user_prompt must be non-empty"
                    .into(),
            );
        }
        if payload.timeout_seconds == 0 {
            return Err(
                "policy violation: agent-turn-waitable payload.timeout_seconds must be >= 1".into(),
            );
        }
        if payload.poll_interval_seconds == 0 {
            return Err(
                "policy violation: agent-turn-waitable payload.poll_interval_seconds must be >= 1"
                    .into(),
            );
        }
        Ok(payload)
    }

    fn policy_failure(message: String) -> JobExecutionOutcome {
        JobExecutionOutcome::FatalFailure {
            message: message.clone(),
            execution_id: None,
            diagnostics: Some(
                json!({
                    "provider": "stasis-agent-turn-waitable",
                    "status": "failure",
                    "guardrail_code": "POLICY_VIOLATION",
                    "policy_reason": message,
                })
                .to_string(),
            ),
        }
    }
}

#[async_trait]
impl JobHandler for AgentTurnWaitableJobHandler {
    fn job_type(&self) -> &'static str {
        "workflow.stasis.agent_turn.waitable"
    }

    async fn execute(&self, job: &Job) -> Result<JobExecutionOutcome> {
        let payload = match Self::parse_payload(&job.payload_ref) {
            Ok(payload) => payload,
            Err(message) => return Ok(Self::policy_failure(message)),
        };

        let now = Utc::now();
        let existing = self.wait_store.get(&payload.turn_id).await?;

        if let Some(record) = existing {
            return Ok(match record.status {
                TurnWaitStatus::Pending => {
                    if now >= record.deadline_at {
                        let _ = self
                            .wait_store
                            .complete(
                                &payload.turn_id,
                                TurnWaitStatus::TimedOut,
                                None,
                                Some("external turn wait timed out".into()),
                                now,
                            )
                            .await?;
                        JobExecutionOutcome::FatalFailure {
                            message: "external turn wait timed out".into(),
                            execution_id: None,
                            diagnostics: Some(
                                json!({
                                    "provider": "stasis-agent-turn-waitable",
                                    "status": "failure",
                                    "wait_status": "timed_out",
                                    "turn_id": payload.turn_id,
                                })
                                .to_string(),
                            ),
                        }
                    } else {
                        let poll = Duration::seconds(payload.poll_interval_seconds as i64);
                        JobExecutionOutcome::Deferred {
                            scheduled_at: now + poll,
                            message: "waiting for external turn completion".into(),
                            execution_id: None,
                            diagnostics: Some(
                                json!({
                                    "provider": "stasis-agent-turn-waitable",
                                    "status": "deferred",
                                    "wait_status": "pending",
                                    "turn_id": payload.turn_id,
                                })
                                .to_string(),
                            ),
                        }
                    }
                }
                TurnWaitStatus::Completed => JobExecutionOutcome::Success {
                    sttp_output_node_id: format!("sttp:agent-turn-waitable:{}", job.id),
                    execution_id: Some(payload.turn_id.clone()),
                    diagnostics: Some(
                        json!({
                            "provider": "stasis-agent-turn-waitable",
                            "status": "success",
                            "wait_status": "completed",
                            "turn_id": payload.turn_id,
                            "result": record.result_payload,
                        })
                        .to_string(),
                    ),
                },
                TurnWaitStatus::Failed | TurnWaitStatus::Cancelled | TurnWaitStatus::TimedOut => {
                    JobExecutionOutcome::FatalFailure {
                        message: record.error_message.unwrap_or_else(|| {
                            format!("external turn ended as {:?}", record.status)
                        }),
                        execution_id: Some(payload.turn_id.clone()),
                        diagnostics: Some(
                            json!({
                                "provider": "stasis-agent-turn-waitable",
                                "status": "failure",
                                "wait_status": format!("{:?}", record.status).to_ascii_lowercase(),
                                "turn_id": payload.turn_id,
                            })
                            .to_string(),
                        ),
                    }
                }
            });
        }

        // First observation: create wait + grant envelope.
        let deadline_at = now + Duration::seconds(payload.timeout_seconds as i64);
        let record = TurnWaitRecord {
            turn_id: payload.turn_id.clone(),
            job_id: job.id.clone(),
            session_id: payload.session_id.clone(),
            correlation_id: job.correlation_id.clone(),
            participant_id: payload.agent_id.clone(),
            status: TurnWaitStatus::Pending,
            deadline_at,
            created_at: now,
            updated_at: now,
            result_payload: None,
            error_message: None,
        };
        self.wait_store.insert(record).await?;

        let grant = AgentEnvelope {
            schema_version: AGENT_ENVELOPE_SCHEMA_VERSION_V1,
            kind: AgentEnvelopeKind::TurnGranted,
            envelope_id: format!("grant-{}", payload.turn_id),
            session_id: payload.session_id.clone(),
            thread_id: payload.thread_id.clone(),
            turn_id: Some(payload.turn_id.clone()),
            job_id: Some(job.id.clone()),
            correlation_id: job.correlation_id.clone(),
            causation_id: job.causation_id.clone(),
            participant_id: Some(payload.agent_id.clone()),
            occurred_at: now,
            payload: json!({
                "user_prompt": payload.user_prompt,
                "system_prompt": payload.system_prompt,
            }),
        };

        // Validate grant encodes cleanly (codec contract).
        let encoded = self.codec.encode(&grant)?;
        if let Some(ref endpoint_ref) = payload.endpoint_ref {
            let (Some(store), Some(transport)) = (&self.endpoint_store, &self.transport) else {
                return Ok(Self::policy_failure(
                    "policy violation: endpoint_ref set but agent transport/endpoint store not configured"
                        .into(),
                ));
            };
            let endpoint_id = if endpoint_ref.starts_with("stasisd:endpoint:") {
                endpoint_ref.clone()
            } else {
                format!("stasisd:endpoint:{endpoint_ref}")
            };
            let Some(endpoint) = store.get(&endpoint_id).await? else {
                return Ok(Self::policy_failure(format!(
                    "policy violation: delivery endpoint not found: {endpoint_id}"
                )));
            };
            if !endpoint.enabled {
                return Ok(Self::policy_failure(format!(
                    "policy violation: delivery endpoint disabled: {endpoint_id}"
                )));
            }
            if !transport.supports(&endpoint.protocol) {
                return Ok(Self::policy_failure(format!(
                    "policy violation: agent transport does not support {:?}",
                    endpoint.protocol
                )));
            }
            transport.publish(&endpoint, &encoded).await?;
        }
        if let Some(ingress) = &self.grant_ingress {
            let _ = ingress.accept(grant).await?;
        }

        let poll = Duration::seconds(payload.poll_interval_seconds as i64);
        Ok(JobExecutionOutcome::Deferred {
            scheduled_at: now + poll,
            message: "turn granted; waiting for external completion".into(),
            execution_id: Some(payload.turn_id),
            diagnostics: Some(
                json!({
                    "provider": "stasis-agent-turn-waitable",
                    "status": "deferred",
                    "wait_status": "pending",
                    "turn_granted": true,
                    "endpoint_ref": payload.endpoint_ref,
                })
                .to_string(),
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::runtime::job::{BackoffPolicy, Job, JobState};
    use crate::infrastructure::agent::in_memory_agent_event_ingress::InMemoryAgentEventIngress;
    use crate::infrastructure::agent::in_memory_turn_wait_store::InMemoryTurnWaitStore;
    use crate::infrastructure::agent::json_agent_message_codec::JsonAgentMessageCodec;
    use crate::infrastructure::agent::wait_correlating_agent_event_ingress::WaitCorrelatingAgentEventIngress;
    use crate::ports::outbound::agent::AgentEventIngress;
    use chrono::Utc;

    fn job(payload: &AgentTurnWaitableJobPayload) -> Job {
        Job {
            id: "job-wait-1".into(),
            queue: "default".into(),
            job_type: "workflow.stasis.agent_turn.waitable".into(),
            payload_ref: payload.to_payload_ref().unwrap(),
            priority: 100,
            state: JobState::Running,
            attempts: 0,
            max_attempts: 3,
            idempotency_key: "idem-1".into(),
            correlation_id: "corr-1".into(),
            causation_id: "cause-1".into(),
            trace_id: "trace-1".into(),
            sttp_input_node_id: "sttp:in".into(),
            sttp_output_node_id: None,
            scheduled_at: Utc::now(),
            lease_owner: None,
            lease_expires_at: None,
            heartbeat_at: None,
            started_at: Some(Utc::now()),
            finished_at: None,
            last_error: None,
            backoff_policy: BackoffPolicy::default(),
            progress_json: None,
        }
    }

    fn payload() -> AgentTurnWaitableJobPayload {
        AgentTurnWaitableJobPayload {
            agent_id: "external-coder".into(),
            session_id: "sess-1".into(),
            turn_id: "turn-1".into(),
            thread_id: Some("thread-1".into()),
            user_prompt: "implement the feature".into(),
            system_prompt: None,
            timeout_seconds: 30,
            poll_interval_seconds: 1,
            endpoint_ref: None,
            mcp_gateway_ref: None,
        }
    }

    #[tokio::test]
    async fn grants_then_completes_via_ingress() {
        let wait_store = Arc::new(InMemoryTurnWaitStore::new());
        let base_ingress = Arc::new(InMemoryAgentEventIngress::new());
        let ingress: Arc<dyn AgentEventIngress> = Arc::new(WaitCorrelatingAgentEventIngress::new(
            base_ingress.clone(),
            wait_store.clone(),
        ));
        let handler = AgentTurnWaitableJobHandler::new(
            wait_store.clone(),
            Arc::new(JsonAgentMessageCodec::v1()),
            Some(base_ingress),
        );

        let first = handler.execute(&job(&payload())).await.unwrap();
        assert!(matches!(first, JobExecutionOutcome::Deferred { .. }));

        let complete = AgentEnvelope {
            schema_version: AGENT_ENVELOPE_SCHEMA_VERSION_V1,
            kind: AgentEnvelopeKind::TurnCompleted,
            envelope_id: "env-complete".into(),
            session_id: "sess-1".into(),
            thread_id: Some("thread-1".into()),
            turn_id: Some("turn-1".into()),
            job_id: Some("job-wait-1".into()),
            correlation_id: "corr-1".into(),
            causation_id: "grant-turn-1".into(),
            participant_id: Some("external-coder".into()),
            occurred_at: Utc::now(),
            payload: json!({"text": "done"}),
        };
        assert_eq!(
            ingress.accept(complete).await.unwrap().disposition,
            crate::ports::outbound::agent::IngressDisposition::Accepted
        );

        let second = handler.execute(&job(&payload())).await.unwrap();
        assert!(matches!(second, JobExecutionOutcome::Success { .. }));
    }

    #[tokio::test]
    async fn times_out_when_deadline_passes() {
        let wait_store = Arc::new(InMemoryTurnWaitStore::new());
        let handler = AgentTurnWaitableJobHandler::new(
            wait_store.clone(),
            Arc::new(JsonAgentMessageCodec::v1()),
            None,
        );
        let mut p = payload();
        p.timeout_seconds = 1;
        let _ = handler.execute(&job(&p)).await.unwrap();

        // Force deadline into the past.
        let mut record = wait_store.get("turn-1").await.unwrap().unwrap();
        record.deadline_at = Utc::now() - Duration::seconds(1);
        // recreate by completing then... we need to mutate store; use complete TimedOut path via execute
        // Overwrite by inserting is blocked; use complete then re-insert isn't possible.
        // Instead call complete to pending is invalid. Directly poke via complete TimedOut from handler path:
        // Re-get and use store.complete is for terminal. Let's insert fresh with past deadline via new turn.
        let wait_store = Arc::new(InMemoryTurnWaitStore::new());
        let handler = AgentTurnWaitableJobHandler::new(
            wait_store.clone(),
            Arc::new(JsonAgentMessageCodec::v1()),
            None,
        );
        let now = Utc::now();
        wait_store
            .insert(TurnWaitRecord {
                turn_id: "turn-late".into(),
                job_id: "job-wait-1".into(),
                session_id: "sess-1".into(),
                correlation_id: "corr-1".into(),
                participant_id: "external-coder".into(),
                status: TurnWaitStatus::Pending,
                deadline_at: now - Duration::seconds(1),
                created_at: now - Duration::seconds(10),
                updated_at: now - Duration::seconds(10),
                result_payload: None,
                error_message: None,
            })
            .await
            .unwrap();
        let mut p = payload();
        p.turn_id = "turn-late".into();
        let outcome = handler.execute(&job(&p)).await.unwrap();
        assert!(matches!(outcome, JobExecutionOutcome::FatalFailure { .. }));
        assert_eq!(
            wait_store.get("turn-late").await.unwrap().unwrap().status,
            TurnWaitStatus::TimedOut
        );
    }

    #[tokio::test]
    async fn rejects_invalid_payload() {
        let handler = AgentTurnWaitableJobHandler::new(
            Arc::new(InMemoryTurnWaitStore::new()),
            Arc::new(JsonAgentMessageCodec::v1()),
            None,
        );
        let mut j = job(&payload());
        j.payload_ref = "{}".into();
        let outcome = handler.execute(&j).await.unwrap();
        assert!(matches!(outcome, JobExecutionOutcome::FatalFailure { .. }));
    }

    #[tokio::test]
    async fn rejects_empty_required_fields_and_zero_timeouts() {
        let handler = AgentTurnWaitableJobHandler::new(
            Arc::new(InMemoryTurnWaitStore::new()),
            Arc::new(JsonAgentMessageCodec::v1()),
            None,
        );
        let cases = [
            {
                let mut p = payload();
                p.agent_id = "  ".into();
                p
            },
            {
                let mut p = payload();
                p.session_id = "".into();
                p
            },
            {
                let mut p = payload();
                p.turn_id = "".into();
                p
            },
            {
                let mut p = payload();
                p.user_prompt = "".into();
                p
            },
            {
                let mut p = payload();
                p.timeout_seconds = 0;
                p
            },
            {
                let mut p = payload();
                p.poll_interval_seconds = 0;
                p
            },
        ];
        for p in cases {
            let outcome = handler.execute(&job(&p)).await.unwrap();
            assert!(
                matches!(outcome, JobExecutionOutcome::FatalFailure { .. }),
                "expected fatal for {:?}",
                p
            );
        }
    }

    #[tokio::test]
    async fn fails_when_ingress_reports_failed() {
        let wait_store = Arc::new(InMemoryTurnWaitStore::new());
        let ingress = WaitCorrelatingAgentEventIngress::new(
            Arc::new(InMemoryAgentEventIngress::new()),
            wait_store.clone(),
        );
        let handler = AgentTurnWaitableJobHandler::new(
            wait_store.clone(),
            Arc::new(JsonAgentMessageCodec::v1()),
            None,
        );
        assert!(matches!(
            handler.execute(&job(&payload())).await.unwrap(),
            JobExecutionOutcome::Deferred { .. }
        ));

        let failed = AgentEnvelope {
            schema_version: AGENT_ENVELOPE_SCHEMA_VERSION_V1,
            kind: AgentEnvelopeKind::Failed,
            envelope_id: "env-fail".into(),
            session_id: "sess-1".into(),
            thread_id: Some("thread-1".into()),
            turn_id: Some("turn-1".into()),
            job_id: Some("job-wait-1".into()),
            correlation_id: "corr-1".into(),
            causation_id: "grant-turn-1".into(),
            participant_id: Some("external-coder".into()),
            occurred_at: Utc::now(),
            payload: json!({"error": "gateway exploded"}),
        };
        assert_eq!(
            ingress.accept(failed).await.unwrap().disposition,
            crate::ports::outbound::agent::IngressDisposition::Accepted
        );

        let outcome = handler.execute(&job(&payload())).await.unwrap();
        match outcome {
            JobExecutionOutcome::FatalFailure { message, .. } => {
                assert!(message.contains("gateway exploded"));
            }
            other => panic!("expected fatal failure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn process_once_defers_without_attempts_then_completes_via_fake_gateway() {
        use crate::application::composition::runtime_composition::RuntimeComposition;
        use crate::application::orchestration::runtime_workflow_job_builder::RuntimeWorkflowJobBuilder;
        use crate::application::runtime::stasis_runtime_builder::StasisRuntimeBuilder;
        use crate::domain::runtime::job::JobState;
        use crate::ports::outbound::runtime::job_store::JobStore;
        use crate::prelude::RuntimeBackend;
        use crate::sdk::runtime_sdk::RuntimeSdk;

        let wait_store = Arc::new(InMemoryTurnWaitStore::new());
        let base_ingress = Arc::new(InMemoryAgentEventIngress::new());
        let ingress: Arc<dyn AgentEventIngress> = Arc::new(WaitCorrelatingAgentEventIngress::new(
            base_ingress.clone(),
            wait_store.clone(),
        ));

        let runtime = RuntimeSdk::from_builder(
            StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
                .without_prompt_handler()
                .without_tool_loop_handler()
                .without_agent_handlers()
                .without_grapheme_handlers()
                .without_memory_operation_handlers()
                .without_orchestration_pattern_handlers()
                .with_extra_handler(AgentTurnWaitableJobHandler::new(
                    wait_store.clone(),
                    Arc::new(JsonAgentMessageCodec::v1()),
                    Some(base_ingress),
                ))
                .with_extra_handler(LocalFakeHandler),
        )
        .await
        .unwrap();

        // Mixed session: local fake participant first, then external waitable.
        let local = crate::domain::runtime::job::NewJob {
            id: "job-local-1".into(),
            queue: "agents".into(),
            job_type: "workflow.stasis.fake_local".into(),
            payload_ref: "{}".into(),
            priority: 100,
            max_attempts: 3,
            idempotency_key: "idem-local".into(),
            correlation_id: "corr-session".into(),
            causation_id: "cause-local".into(),
            trace_id: "trace-local".into(),
            sttp_input_node_id: "sttp:in".into(),
            scheduled_at: Utc::now(),
            backoff_policy: BackoffPolicy::default(),
        };
        runtime.enqueue(local).await.unwrap();
        assert_eq!(
            runtime.process_once("agents", "worker-1").await.unwrap(),
            Some("job-local-1".into())
        );

        let mut wait_payload = payload();
        wait_payload.turn_id = "turn-rt".into();
        let waitable =
            RuntimeWorkflowJobBuilder::for_agent_turn_waitable("job-wait-rt", &wait_payload)
                .unwrap()
                .with_queue("agents")
                .with_max_attempts(3)
                .with_correlation_id("corr-1")
                .build();
        runtime.enqueue(waitable).await.unwrap();

        assert_eq!(
            runtime.process_once("agents", "worker-1").await.unwrap(),
            Some("job-wait-rt".into())
        );

        let RuntimeComposition::InMemory(rt) = runtime.runtime() else {
            panic!("expected in-memory");
        };
        let mut parked = rt.job_store.get("job-wait-rt").await.unwrap().unwrap();
        assert_eq!(parked.state, JobState::Enqueued);
        assert_eq!(parked.attempts, 0, "Deferred must not consume attempts");
        parked.scheduled_at = Utc::now() - Duration::seconds(1);
        rt.job_store.save(parked).await.unwrap();

        let complete = AgentEnvelope {
            schema_version: AGENT_ENVELOPE_SCHEMA_VERSION_V1,
            kind: AgentEnvelopeKind::TurnCompleted,
            envelope_id: "env-complete-rt".into(),
            session_id: "sess-1".into(),
            thread_id: Some("thread-1".into()),
            turn_id: Some("turn-rt".into()),
            job_id: Some("job-wait-rt".into()),
            correlation_id: "corr-1".into(),
            causation_id: "grant-turn-rt".into(),
            participant_id: Some("external-coder".into()),
            occurred_at: Utc::now(),
            payload: json!({"text": "external done"}),
        };
        assert_eq!(
            ingress.accept(complete).await.unwrap().disposition,
            crate::ports::outbound::agent::IngressDisposition::Accepted
        );

        assert_eq!(
            runtime.process_once("agents", "worker-1").await.unwrap(),
            Some("job-wait-rt".into())
        );
        let stats = runtime.stats_snapshot(10).await.unwrap();
        assert!(stats.succeeded_jobs >= 2);
        assert_eq!(stats.dead_letter_jobs, 0);
    }

    struct LocalFakeHandler;

    #[async_trait]
    impl JobHandler for LocalFakeHandler {
        fn job_type(&self) -> &'static str {
            "workflow.stasis.fake_local"
        }

        async fn execute(&self, job: &Job) -> Result<JobExecutionOutcome> {
            Ok(JobExecutionOutcome::Success {
                sttp_output_node_id: format!("sttp:fake-local:{}", job.id),
                execution_id: None,
                diagnostics: Some(r#"{"provider":"fake-local","status":"success"}"#.into()),
            })
        }
    }
}
