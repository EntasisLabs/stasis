use std::future::Future;

use crate::application::telemetry::propagation::{trace_propagation_mode, TracePropagationMode};
use crate::ports::outbound::runtime::runtime_tracing::TraceContext;

tokio::task_local! {
    static INBOUND_TRACE: TraceContext;
}

pub async fn scope_inbound_trace<F, R>(trace: TraceContext, f: F) -> R
where
    F: Future<Output = R>,
{
    INBOUND_TRACE.scope(trace, f).await
}

pub fn inbound_trace_context() -> Option<TraceContext> {
    INBOUND_TRACE.try_with(|trace| trace.clone()).ok()
}

pub fn inbound_trace_context_for_propagation() -> Option<TraceContext> {
    match trace_propagation_mode() {
        TracePropagationMode::Legacy => None,
        TracePropagationMode::W3c | TracePropagationMode::Both => inbound_trace_context(),
    }
}

pub fn inbound_trace_id_for_propagation() -> Option<String> {
    inbound_trace_context_for_propagation().map(|trace| trace.trace_id)
}

pub fn trace_id_for_enqueue(default: impl FnOnce() -> String) -> String {
    inbound_trace_id_for_propagation().unwrap_or_else(default)
}
