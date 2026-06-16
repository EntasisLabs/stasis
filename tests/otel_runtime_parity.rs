use std::sync::{Arc, Mutex};

use chrono::Utc;
use stasis::application::orchestration::runtime_job_payloads::MemoryRecallJobPayload;
use stasis::application::orchestration::runtime_workflow_job_builder::RuntimeWorkflowJobBuilder;
use stasis::application::runtime::in_memory_runtime::{InMemoryRuntime, JobExecutionOutcome, JobHandler};
use stasis::application::runtime::stasis_runtime_builder::StasisRuntimeBuilder;
use stasis::application::telemetry::keys::{
    JOB_SUCCEEDED_TOTAL, MEMORY_RECALL_TOTAL, WORKER_PROCESS_ONCE_TOTAL,
};
use stasis::application::telemetry::spans;
use stasis::domain::errors::Result;
use stasis::domain::runtime::job::{BackoffPolicy, NewJob};
use stasis::infrastructure::memory::locus_context_reader::LocusContextReader;
use stasis::infrastructure::memory::locus_node_store_factory::LocusNodeStoreFactory;
use stasis::infrastructure::runtime::in_memory_runtime_metrics::InMemoryRuntimeMetrics;
use stasis::infrastructure::telemetry::{NoopRuntimeTelemetry, NoopRuntimeTracing};
use stasis::ports::outbound::memory::memory_context_reader::MemoryContextReader;
use stasis::ports::outbound::runtime::runtime_metrics::RuntimeMetrics;
use stasis::ports::outbound::runtime::runtime_tracing::{
    OtelAttribute, RuntimeTracing, SpanGuard, TraceContext,
};

#[derive(Clone, Default)]
struct RecordingTracing {
    spans: Arc<Mutex<Vec<String>>>,
}

impl RecordingTracing {
    fn new() -> (Self, Arc<Mutex<Vec<String>>>) {
        let spans = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                spans: Arc::clone(&spans),
            },
            spans,
        )
    }
}

impl RuntimeTracing for RecordingTracing {
    fn start_span(&self, name: &'static str, _attributes: &[OtelAttribute]) -> SpanGuard {
        self.spans
            .lock()
            .expect("recording spans lock")
            .push(name.to_string());
        SpanGuard::noop()
    }

    fn start_span_with_trace_context(
        &self,
        name: &'static str,
        attributes: &[OtelAttribute],
        parent: Option<&TraceContext>,
    ) -> SpanGuard {
        let _ = parent;
        self.start_span(name, attributes)
    }

    fn active_trace_context(&self) -> Option<TraceContext> {
        None
    }
}

struct RecordingTelemetry {
    metrics: InMemoryRuntimeMetrics,
    tracing: RecordingTracing,
}

impl RuntimeMetrics for RecordingTelemetry {
    fn incr_counter(&self, name: &str, value: u64) {
        self.metrics.incr_counter(name, value);
    }

    fn observe_duration_ms(&self, name: &str, duration_ms: u64) {
        self.metrics.observe_duration_ms(name, duration_ms);
    }
}

impl RuntimeTracing for RecordingTelemetry {
    fn start_span(&self, name: &'static str, attributes: &[OtelAttribute]) -> SpanGuard {
        self.tracing.start_span(name, attributes)
    }

    fn start_span_with_trace_context(
        &self,
        name: &'static str,
        attributes: &[OtelAttribute],
        parent: Option<&TraceContext>,
    ) -> SpanGuard {
        self.tracing
            .start_span_with_trace_context(name, attributes, parent)
    }

    fn active_trace_context(&self) -> Option<TraceContext> {
        None
    }
}

#[derive(Clone)]
struct SuccessHandler;

#[async_trait::async_trait]
impl JobHandler for SuccessHandler {
    fn job_type(&self) -> &'static str {
        "test.otel.success"
    }

    async fn execute(
        &self,
        _job: &stasis::domain::runtime::job::Job,
    ) -> Result<JobExecutionOutcome> {
        Ok(JobExecutionOutcome::Success {
            sttp_output_node_id: "sttp:out:otel".to_string(),
            execution_id: Some("exec:otel".to_string()),
            diagnostics: None,
        })
    }
}

#[tokio::test]
async fn runtime_workflow_job_builder_with_traceparent_sets_w3c_trace_id() {
    let job = RuntimeWorkflowJobBuilder::for_prompt(
        "job-traceparent",
        &stasis::application::orchestration::runtime_job_payloads::PromptJobPayload {
            user_prompt: "hello".to_string(),
            system_prompt: None,
            policy_profile: None,
            model_hint: None,
            reasoning_effort: None,
            memory_policy: None,
        },
    )
    .expect("prompt payload should build")
    .with_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
    .expect("traceparent should parse")
    .build();

    assert_eq!(job.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
}

#[tokio::test]
async fn wired_runtime_emits_worker_and_job_spans_and_metrics() {
    let metrics = Arc::new(InMemoryRuntimeMetrics::default());
    let (tracing, spans) = RecordingTracing::new();
    let telemetry = Arc::new(RecordingTelemetry {
        metrics: (*metrics).clone(),
        tracing,
    });

    let mut runtime = InMemoryRuntime::with_dependencies_and_telemetry(
        Arc::new(stasis::infrastructure::runtime::system_clock::SystemClock),
        Arc::new(stasis::infrastructure::runtime::atomic_id_generator::AtomicIdGenerator::new(1)),
        metrics.clone(),
        telemetry.clone(),
    );

    runtime
        .register_handler(SuccessHandler)
        .expect("handler should register");

    let now = Utc::now();
    runtime
        .enqueue(NewJob {
            id: "job-otel-metrics".to_string(),
            queue: "default".to_string(),
            job_type: "test.otel.success".to_string(),
            payload_ref: "payload".to_string(),
            priority: 100,
            max_attempts: 1,
            idempotency_key: "idem-otel".to_string(),
            correlation_id: "corr-otel".to_string(),
            causation_id: "cause-otel".to_string(),
            trace_id: "4bf92f3577b34da6a3ce929d0e0e4736".to_string(),
            sttp_input_node_id: "sttp:in:otel".to_string(),
            scheduled_at: now,
            backoff_policy: BackoffPolicy::default(),
        })
        .await
        .expect("job should enqueue");

    runtime
        .process_once("default", "worker-otel", now)
        .await
        .expect("process_once should succeed");

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.counters.get(WORKER_PROCESS_ONCE_TOTAL), Some(&1));
    assert_eq!(snapshot.counters.get(JOB_SUCCEEDED_TOTAL), Some(&1));

    let recorded_spans = spans.lock().expect("spans lock");
    assert!(recorded_spans.iter().any(|name| name == spans::WORKER_PROCESS_ONCE));
    assert!(recorded_spans.iter().any(|name| name == spans::JOB_EXECUTE));
}

#[tokio::test]
async fn builder_wires_memory_recall_telemetry() {
    let store = LocusNodeStoreFactory::in_memory()
        .await
        .expect("memory store should initialize");
    let reader: Arc<dyn MemoryContextReader> = Arc::new(LocusContextReader::new(store));
    let metrics = Arc::new(InMemoryRuntimeMetrics::default());
    let (tracing, spans) = RecordingTracing::new();
    let telemetry = Arc::new(RecordingTelemetry {
        metrics: (*metrics).clone(),
        tracing,
    });

    let runtime = StasisRuntimeBuilder::new(stasis::application::runtime::runtime_factory::RuntimeBackend::InMemory)
        .with_runtime_telemetry(telemetry)
        .with_memory_context_reader(reader)
        .without_grapheme_handlers()
        .without_prompt_handler()
        .without_tool_loop_handler()
        .without_agent_handlers()
        .without_orchestration_pattern_handlers()
        .without_cluster_control_handlers()
        .build()
        .await
        .expect("runtime should build");

    let stasis::application::runtime::runtime_factory::RuntimeComposition::InMemory(rt) = runtime
    else {
        panic!("expected in-memory runtime");
    };

    let payload = MemoryRecallJobPayload {
        memory_policy: None,
    };
    let job = RuntimeWorkflowJobBuilder::for_memory_recall("job-memory-recall", &payload)
        .expect("recall payload should build")
        .build();

    rt.enqueue(job).await.expect("job should enqueue");
    rt.process_once("default", "worker-memory", Utc::now())
        .await
        .expect("process_once should succeed");

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.counters.get(MEMORY_RECALL_TOTAL), Some(&1));

    let recorded_spans = spans.lock().expect("spans lock");
    assert!(recorded_spans.iter().any(|name| name == spans::MEMORY_RECALL));
}

#[test]
fn default_build_without_otel_feature_still_compiles_and_uses_noop_tracing() {
    let _ = NoopRuntimeTracing;
    let _ = NoopRuntimeTelemetry;
}
