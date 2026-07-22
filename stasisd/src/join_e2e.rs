//! Phase 4 join-tracks e2e: TOML → local + fake external via contracts.

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use chrono::{Duration, Utc};
    use serde_json::json;
    use stasis::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
    use stasis::application::runtime::stasis_runtime_builder::StasisRuntimeBuilder;
    use stasis::domain::agent::envelope::{
        AgentEnvelope, AgentEnvelopeKind, AGENT_ENVELOPE_SCHEMA_VERSION_V1,
    };
    use stasis::domain::errors::Result;
    use stasis::domain::runtime::job::{Job, JobState};
    use stasis::infrastructure::agent::{
        InMemoryAgentEventIngress, InMemoryAgentTransport, InMemoryTurnWaitStore,
        JsonAgentMessageCodec, WaitCorrelatingAgentEventIngress,
    };
    use stasis::infrastructure::runtime::in_memory_delivery_endpoint_store::InMemoryDeliveryEndpointStore;
    use stasis::ports::outbound::agent::AgentEventIngress;
    use stasis::ports::outbound::runtime::delivery_endpoint_store::DeliveryEndpointStore;
    use stasis::ports::outbound::runtime::job_store::JobStore;
    use stasis::prelude::RuntimeBackend;
    use stasis::sdk::runtime_sdk::RuntimeSdk;
    use async_trait::async_trait;

    use crate::config::load_desired_state;
    use crate::host::reconcile_from_path;
    use crate::provenance::managed_endpoint_id;
    use crate::tick::{tick_once, TickOptions};

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("stasisd-join-{label}-{nanos}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    struct FakeLocalHandler;

    #[async_trait]
    impl JobHandler for FakeLocalHandler {
        fn job_type(&self) -> &'static str {
            "workflow.stasis.prompt"
        }

        async fn execute(&self, job: &Job) -> Result<JobExecutionOutcome> {
            Ok(JobExecutionOutcome::Success {
                sttp_output_node_id: format!("sttp:local:{}", job.id),
                execution_id: None,
                diagnostics: Some(r#"{"provider":"fake-local","status":"success"}"#.into()),
            })
        }
    }

    #[tokio::test]
    async fn toml_mixed_local_and_fake_external_completes() {
        let dir = temp_dir("mixed");
        fs::write(
            dir.join("mixed.toml"),
            r#"
api_version = "stasisd/v1"

[[endpoint]]
id = "fake-external"
name = "Fake external participant"
protocol = "http_webhook"
target = "http://127.0.0.1:39001/agent"

[[mcp_gateway]]
id = "local-mcp"
transport = "command"
command = "fake-mcp-gateway"
args = ["--stdio"]

[[schedule]]
id = "local-step"
queue = "agents"
job_type = "workflow.stasis.prompt"
cron = "0/1 * * * * * *"
payload = { user_prompt = "local work" }

[[schedule]]
id = "external-step"
queue = "agents"
job_type = "workflow.stasis.agent_turn.waitable"
cron = "0/1 * * * * * *"
payload = { agent_id = "external-reviewer", session_id = "sess-join", turn_id = "turn-join", user_prompt = "review the plan", endpoint_ref = "fake-external", timeout_seconds = 30, poll_interval_seconds = 1 }
"#,
        )
        .unwrap();

        let desired = load_desired_state(&dir).unwrap();
        assert!(desired.diagnostics.is_empty(), "{:?}", desired.diagnostics);
        assert_eq!(desired.endpoints.len(), 1);
        assert_eq!(desired.mcp_gateways.len(), 1);
        assert_eq!(desired.schedules.len(), 2);

        let endpoint_store: Arc<dyn DeliveryEndpointStore> =
            Arc::new(InMemoryDeliveryEndpointStore::default());
        let wait_store = Arc::new(InMemoryTurnWaitStore::new());
        let base_ingress = Arc::new(InMemoryAgentEventIngress::new());
        let ingress: Arc<dyn AgentEventIngress> = Arc::new(WaitCorrelatingAgentEventIngress::new(
            base_ingress,
            wait_store.clone(),
        ));
        let transport = Arc::new(InMemoryAgentTransport::new());

        let runtime = RuntimeSdk::from_builder(
            StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
                .without_prompt_handler()
                .with_extra_handler(FakeLocalHandler)
                .with_delivery_endpoint_store(endpoint_store.clone())
                .with_turn_wait_store(wait_store)
                .with_agent_message_codec(Arc::new(JsonAgentMessageCodec::v1()))
                .with_agent_event_ingress(ingress.clone())
                .with_agent_transport(transport.clone()),
        )
        .await
        .unwrap();

        let report = reconcile_from_path(&runtime, &dir, true, Some(endpoint_store.clone()))
            .await
            .unwrap();
        assert!(report
            .endpoint_created
            .contains(&managed_endpoint_id("fake-external")));
        assert_eq!(report.created.len(), 2);

        // Force both schedules due.
        let mut defs = runtime.list_recurring().await.unwrap();
        for mut def in defs.drain(..) {
            def.next_run_at = Utc::now() - Duration::hours(1);
            runtime.save_recurring(def).await.unwrap();
        }

        let tick = tick_once(
            &runtime,
            &TickOptions {
                queues: vec!["agents".into()],
                process_limit: 10,
                ..TickOptions::default()
            },
        )
        .await
        .unwrap();
        assert!(tick.materialized >= 2);
        assert!(tick.processed >= 1);

        // Fake external gateway: read published TurnGranted and complete via ingress.
        let published = transport.published().unwrap();
        assert!(
            !published.is_empty(),
            "expected TurnGranted publish to endpoint"
        );
        assert_eq!(published[0].0, managed_endpoint_id("fake-external"));

        let complete = AgentEnvelope {
            schema_version: AGENT_ENVELOPE_SCHEMA_VERSION_V1,
            kind: AgentEnvelopeKind::TurnCompleted,
            envelope_id: "env-join-complete".into(),
            session_id: "sess-join".into(),
            thread_id: None,
            turn_id: Some("turn-join".into()),
            job_id: None,
            correlation_id: "corr-join".into(),
            causation_id: "grant-turn-join".into(),
            participant_id: Some("external-reviewer".into()),
            occurred_at: Utc::now(),
            payload: json!({"text": "looks good"}),
        };
        assert_eq!(
            ingress.accept(complete).await.unwrap().disposition,
            stasis::ports::outbound::agent::IngressDisposition::Accepted
        );

        // Make deferred waitable runnable again.
        use stasis::application::composition::runtime_composition::RuntimeComposition;
        let RuntimeComposition::InMemory(rt) = runtime.runtime() else {
            panic!("expected in-memory");
        };
        let jobs = rt.job_store.list_by_state(JobState::Enqueued).await.unwrap();
        for mut job in jobs {
            if job.job_type.contains("waitable") {
                job.scheduled_at = Utc::now() - Duration::seconds(1);
                rt.job_store.save(job).await.unwrap();
            }
        }

        let _ = tick_once(
            &runtime,
            &TickOptions {
                queues: vec!["agents".into()],
                process_limit: 10,
                ..TickOptions::default()
            },
        )
        .await
        .unwrap();

        let stats = runtime.stats_snapshot(20).await.unwrap();
        assert!(stats.succeeded_jobs >= 2, "stats={stats:?}");
        assert_eq!(stats.dead_letter_jobs, 0);

        let _ = fs::remove_dir_all(dir);
    }
}
