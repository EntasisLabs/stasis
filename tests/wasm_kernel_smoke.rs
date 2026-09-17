//! In-memory kernel smoke for native and `wasm32-unknown-unknown`.
//!
//! W2 gate: `StasisSdk` register/invoke with `MockLlmGateway`, and one typed
//! job completion through `RuntimeSdk`. Same assertions on both targets so
//! lineage/diagnostics cannot drift.
//!
//! WASM CI:
//! `cargo test -p stasis-rs --target wasm32-unknown-unknown --no-default-features --test wasm_kernel_smoke`

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use stasis::application::dto::{InvokeAgentRequest, RegisterAgentRequest};
use stasis::application::runtime::job_context::{JobContext, JobResult};
use stasis::application::runtime::runtime_factory::{RuntimeBackend, RuntimeComposition};
use stasis::application::runtime::stasis_runtime_builder::StasisRuntimeBuilder;
use stasis::application::runtime::typed_job::JobConsumer;
use stasis::domain::runtime::job::JobState;
use stasis::domain::runtime::job_attempt::JobAttemptOutcome;
use stasis::domain::runtime::provenance::{ProvenanceRef, ProvenanceScheme};
use stasis::domain::runtime::typed_contract::StasisJob;
use stasis::infrastructure::llm::mock_gateway::MockLlmGateway;
use stasis::infrastructure::persistence::in_memory_agent_repository::InMemoryAgentRepository;
use stasis::ports::outbound::runtime::job_store::JobStore;
use stasis::sdk::runtime_sdk::RuntimeSdk;
use stasis::sdk::stasis_sdk::StasisSdk;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NoopPing {
    n: u32,
}

impl StasisJob for NoopPing {
    const NAME: &'static str = "wasm.noop_ping";
    const VERSION: u32 = 1;
    type Output = NoopPing;
}

struct NoopPingConsumer;

#[async_trait]
impl JobConsumer<NoopPing> for NoopPingConsumer {
    async fn consume(&self, job: NoopPing, _ctx: JobContext) -> JobResult<NoopPing> {
        Ok(job)
    }
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn stasis_sdk_register_and_invoke_with_mock_gateway() {
    let repository = InMemoryAgentRepository::default();
    let llm = MockLlmGateway::new("wasm mock completion");
    let sdk = StasisSdk::new(repository, llm);

    sdk.register_agent(RegisterAgentRequest {
        id: "planner".into(),
        name: "Planner".into(),
        system_prompt: "Break tasks into steps".into(),
    })
    .await
    .expect("agent should register");

    let response = sdk
        .invoke_agent(InvokeAgentRequest {
            agent_id: "planner".into(),
            user_prompt: "Plan a sprint kickoff".into(),
        })
        .await
        .expect("agent should invoke");

    assert_eq!(response.completion, "wasm mock completion");
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn runtime_sdk_completes_typed_in_memory_job() {
    let runtime = RuntimeSdk::from_builder(
        StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
            .without_prompt_handler()
            .without_tool_loop_handler()
            .without_agent_handlers()
            .without_grapheme_handlers()
            .without_memory_operation_handlers()
            .without_orchestration_pattern_handlers()
            .without_cluster_control_handlers(),
    )
    .await
    .expect("in-memory runtime should build");

    runtime
        .register_consumer(NoopPingConsumer)
        .expect("typed consumer should register");

    let input = ProvenanceRef::sttp("sttp:in:wasm-smoke");
    let job_id = runtime
        .enqueue_job(NoopPing { n: 7 })
        .queue("wasm-smoke")
        .idempotency_key("idem-wasm-smoke")
        .correlation_id("corr-wasm-smoke")
        .input_provenance(input.clone())
        .send()
        .await
        .expect("typed job should enqueue");

    let processed = runtime
        .process_once("wasm-smoke", "wasm-worker")
        .await
        .expect("process_once should succeed");
    assert_eq!(processed.as_deref(), Some(job_id.as_str()));

    let rt = match runtime.runtime() {
        RuntimeComposition::InMemory(rt) => rt,
        #[cfg(feature = "surreal")]
        RuntimeComposition::Surreal(_) => panic!("expected in-memory runtime"),
    };
    let job = rt
        .job_store
        .get(&job_id)
        .await
        .expect("job store get")
        .expect("job should exist");

    assert_eq!(job.state, JobState::Succeeded);
    assert_eq!(job.job_type, NoopPing::NAME);
    assert_eq!(
        job.attempts, 0,
        "successful first execution does not increment Job.attempts (native contract)"
    );
    assert!(job.last_error.is_none());
    assert!(job.finished_at.is_some());
    assert_eq!(job.input_provenance.as_ref(), Some(&input));
    let output = job
        .output_provenance
        .as_ref()
        .expect("typed success writes CAS output provenance");
    assert_eq!(output.scheme, ProvenanceScheme::Cas);
    assert!(
        output
            .digest
            .as_ref()
            .is_some_and(|digest| digest.algorithm == "sha256" && digest.hex.len() == 64),
        "output provenance should be sha256 CAS, got {output:?}"
    );

    let attempts = rt
        .list_job_attempts(&job_id)
        .await
        .expect("attempts should list");
    assert_eq!(attempts.len(), 1);
    let attempt = &attempts[0];
    assert_eq!(attempt.outcome, JobAttemptOutcome::Succeeded);
    assert_eq!(attempt.attempt_number, 1);
    assert_eq!(attempt.execution_id.as_deref(), Some(job_id.as_str()));
    assert_eq!(attempt.output_provenance.as_ref(), Some(output));
    let diagnostics = attempt
        .diagnostics
        .as_deref()
        .expect("typed success writes diagnostics JSON");
    assert!(
        diagnostics.contains("\"status\":\"success\""),
        "diagnostics should be a success envelope, got {diagnostics}"
    );
    assert!(
        diagnostics.contains("\"n\":7"),
        "diagnostics should include typed output, got {diagnostics}"
    );

    let stats = runtime
        .stats_snapshot(10)
        .await
        .expect("stats snapshot should succeed");
    assert_eq!(stats.succeeded_jobs, 1);
    assert_eq!(stats.enqueued_jobs, 0);
    assert_eq!(stats.dead_letter_jobs, 0);
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn locus_memory_store_then_recall_job() {
    use std::sync::Arc;

    use stasis::application::orchestration::runtime_job_payloads::{
        MemoryPolicyPayload, MemoryRecallJobPayload,
    };
    use stasis::application::orchestration::runtime_workflow_job_builder::RuntimeWorkflowJobBuilder;
    use stasis::application::runtime::memory_persistence_helpers::{
        SttpPromptNodeFormat, render_prompt_response_sttp_node,
    };
    use stasis::infrastructure::memory::locus_context_reader::LocusContextReader;
    use stasis::infrastructure::memory::locus_context_writer::LocusContextWriter;
    use stasis::infrastructure::memory::locus_memory_operations::LocusMemoryOperations;
    use stasis::infrastructure::memory::locus_node_store_factory::LocusNodeStoreFactory;
    use stasis::ports::outbound::memory::memory_context_writer::MemoryContextWriter;
    use stasis::ports::outbound::memory::memory_models::MemoryStoreRequest;

    let session_id = "corr-wasm-memory";
    let memory = LocusNodeStoreFactory::in_memory()
        .await
        .expect("in-memory locus store should initialize");
    let writer = LocusContextWriter::new(memory.clone());
    let raw_node = render_prompt_response_sttp_node(
        session_id,
        "what did we decide?",
        "ship the wasm kernel profile",
        SttpPromptNodeFormat::TaggedSchema,
    );
    let stored = writer
        .store_context(&MemoryStoreRequest {
            session_id: session_id.to_string(),
            raw_node,
        })
        .await
        .expect("locus store should accept STTP node");
    assert!(stored.valid);

    let runtime = RuntimeSdk::from_builder(
        StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
            .with_memory_context_reader(Arc::new(LocusContextReader::new(memory.clone())))
            .with_memory_context_writer(Arc::new(writer))
            .with_memory_operations(Arc::new(LocusMemoryOperations::new(memory, None)))
            .without_prompt_handler()
            .without_tool_loop_handler()
            .without_agent_handlers()
            .without_grapheme_handlers()
            .without_orchestration_pattern_handlers()
            .without_cluster_control_handlers(),
    )
    .await
    .expect("memory runtime should build");

    let recall_job = RuntimeWorkflowJobBuilder::for_memory_recall(
        "job-wasm-memory-recall",
        &MemoryRecallJobPayload {
            memory_policy: Some(MemoryPolicyPayload {
                tenant_id: None,
                session_ids: Some(vec![session_id.to_string()]),
                tiers: None,
                from_utc: None,
                to_utc: None,
                limit: None,
                alpha: None,
                beta: None,
                gamma: None,
                fallback_policy: None,
                strictness: None,
                query_text: Some("wasm kernel".into()),
                include_explain: None,
                store_mode: None,
                filter: Default::default(),
            }),
        },
    )
    .expect("recall payload should encode")
    .with_queue("wasm-memory")
    .with_correlation_id(session_id)
    .with_idempotency_key("idem-wasm-memory-recall")
    .build();

    runtime
        .enqueue(recall_job)
        .await
        .expect("memory recall job should enqueue");

    let processed = runtime
        .process_once("wasm-memory", "wasm-memory-worker")
        .await
        .expect("process_once should succeed");
    assert_eq!(processed.as_deref(), Some("job-wasm-memory-recall"));

    let rt = match runtime.runtime() {
        RuntimeComposition::InMemory(rt) => rt,
        #[cfg(feature = "surreal")]
        RuntimeComposition::Surreal(_) => panic!("expected in-memory runtime"),
    };
    let job = rt
        .job_store
        .get("job-wasm-memory-recall")
        .await
        .expect("job store get")
        .expect("recall job should exist");
    assert_eq!(job.state, JobState::Succeeded);

    let attempts = rt
        .list_job_attempts("job-wasm-memory-recall")
        .await
        .expect("attempts should list");
    assert_eq!(attempts.len(), 1);
    let diagnostics = attempts[0]
        .diagnostics
        .as_deref()
        .expect("recall writes diagnostics JSON");
    assert!(
        diagnostics.contains("\"status\":\"success\""),
        "recall diagnostics should succeed, got {diagnostics}"
    );
    assert!(
        diagnostics.contains("\"retrieved\":1") || diagnostics.contains("\"retrieved\": 1"),
        "recall should retrieve the stored node, got {diagnostics}"
    );
}
