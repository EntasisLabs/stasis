use crate::ports::outbound::runtime::runtime_metrics::RuntimeMetrics;
use crate::ports::outbound::runtime::runtime_tracing::RuntimeTracing;

/// Combined metrics + tracing handle wired through the runtime builder.
pub trait RuntimeTelemetry: RuntimeMetrics + RuntimeTracing {}

impl<T> RuntimeTelemetry for T where T: RuntimeMetrics + RuntimeTracing {}
