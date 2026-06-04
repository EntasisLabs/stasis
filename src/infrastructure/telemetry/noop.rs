use crate::infrastructure::runtime::noop_runtime_metrics::NoopRuntimeMetrics;
use crate::ports::outbound::runtime::runtime_metrics::RuntimeMetrics;
use crate::ports::outbound::runtime::runtime_tracing::{
    OtelAttribute, RuntimeTracing, SpanGuard, TraceContext,
};

#[derive(Clone, Copy, Debug, Default)]
pub struct NoopRuntimeTracing;

impl RuntimeTracing for NoopRuntimeTracing {
    fn start_span(&self, _name: &'static str, _attributes: &[OtelAttribute]) -> SpanGuard {
        SpanGuard::noop()
    }

    fn active_trace_context(&self) -> Option<TraceContext> {
        None
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoopRuntimeTelemetry;

impl RuntimeMetrics for NoopRuntimeTelemetry {
    fn incr_counter(&self, name: &str, value: u64) {
        NoopRuntimeMetrics.incr_counter(name, value);
    }

    fn observe_duration_ms(&self, name: &str, duration_ms: u64) {
        NoopRuntimeMetrics.observe_duration_ms(name, duration_ms);
    }
}

impl RuntimeTracing for NoopRuntimeTelemetry {
    fn start_span(&self, name: &'static str, attributes: &[OtelAttribute]) -> SpanGuard {
        NoopRuntimeTracing.start_span(name, attributes)
    }

    fn active_trace_context(&self) -> Option<TraceContext> {
        None
    }
}
