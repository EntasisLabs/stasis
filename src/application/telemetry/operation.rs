use std::sync::Arc;
use std::time::Instant;

use crate::application::telemetry::keys::{
    MEMORY_RECALL_DURATION_MS, MEMORY_RECALL_ERRORS_TOTAL, MEMORY_RECALL_TOTAL,
    MEMORY_STORE_DURATION_MS, MEMORY_STORE_ERRORS_TOTAL, MEMORY_STORE_TOTAL,
};
use crate::application::telemetry::spans;
use crate::ports::outbound::runtime::runtime_metrics::RuntimeMetrics;
use crate::ports::outbound::runtime::runtime_tracing::{OtelAttribute, RuntimeTracing, SpanGuard};

/// Shared metrics + tracing helper for handler-level observability.
#[derive(Clone)]
pub struct OperationTelemetry {
    metrics: Arc<dyn RuntimeMetrics>,
    tracing: Arc<dyn RuntimeTracing>,
}

impl OperationTelemetry {
    pub fn new(metrics: Arc<dyn RuntimeMetrics>, tracing: Arc<dyn RuntimeTracing>) -> Self {
        Self { metrics, tracing }
    }

    pub fn start_span(&self, name: &'static str, attributes: &[OtelAttribute]) -> SpanGuard {
        self.tracing.start_span(name, attributes)
    }

    pub fn recall_span(&self, correlation_id: &str) -> SpanGuard {
        self.start_span(
            spans::MEMORY_RECALL,
            &[OtelAttribute::string(
                "stasis.memory.correlation_id",
                correlation_id.to_string(),
            )],
        )
    }

    pub fn store_span(&self, correlation_id: &str) -> SpanGuard {
        self.start_span(
            spans::MEMORY_STORE,
            &[OtelAttribute::string(
                "stasis.memory.correlation_id",
                correlation_id.to_string(),
            )],
        )
    }

    pub fn grapheme_span(&self, job_id: &str) -> SpanGuard {
        self.start_span(
            spans::GRAPHEME_EXECUTE,
            &[OtelAttribute::string("stasis.job.id", job_id.to_string())],
        )
    }

    pub fn outbox_publish_span(&self, event_type: &str, job_id: &str) -> SpanGuard {
        self.start_span(
            spans::OUTBOX_PUBLISH,
            &[
                OtelAttribute::string("stasis.outbox.event_type", event_type.to_string()),
                OtelAttribute::string("stasis.outbox.job_id", job_id.to_string()),
            ],
        )
    }

    pub fn record_recall_started(&self) {
        self.metrics.incr_counter(MEMORY_RECALL_TOTAL, 1);
    }

    pub fn record_recall_success(&self, started: Instant) {
        self.metrics.observe_duration_ms(
            MEMORY_RECALL_DURATION_MS,
            started.elapsed().as_millis() as u64,
        );
    }

    pub fn record_recall_error(&self, started: Instant) {
        self.metrics.incr_counter(MEMORY_RECALL_ERRORS_TOTAL, 1);
        self.metrics.observe_duration_ms(
            MEMORY_RECALL_DURATION_MS,
            started.elapsed().as_millis() as u64,
        );
    }

    pub fn record_store_started(&self) {
        self.metrics.incr_counter(MEMORY_STORE_TOTAL, 1);
    }

    pub fn record_store_success(&self, started: Instant) {
        self.metrics.observe_duration_ms(
            MEMORY_STORE_DURATION_MS,
            started.elapsed().as_millis() as u64,
        );
    }

    pub fn record_store_error(&self, started: Instant) {
        self.metrics.incr_counter(MEMORY_STORE_ERRORS_TOTAL, 1);
        self.metrics.observe_duration_ms(
            MEMORY_STORE_DURATION_MS,
            started.elapsed().as_millis() as u64,
        );
    }
}

pub fn runtime_event_type_name(event_type: &crate::domain::runtime::outbox::RuntimeEventType) -> &'static str {
    match event_type {
        crate::domain::runtime::outbox::RuntimeEventType::JobSucceeded => "job.succeeded",
        crate::domain::runtime::outbox::RuntimeEventType::JobRetryScheduled => "job.retry_scheduled",
        crate::domain::runtime::outbox::RuntimeEventType::JobDeadLettered => "job.dead_lettered",
    }
}
