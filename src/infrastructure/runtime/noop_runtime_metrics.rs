use crate::ports::outbound::runtime::runtime_metrics::RuntimeMetrics;

#[derive(Clone, Default)]
pub struct NoopRuntimeMetrics;

impl RuntimeMetrics for NoopRuntimeMetrics {
    fn incr_counter(&self, _name: &str, _value: u64) {}

    fn observe_duration_ms(&self, _name: &str, _duration_ms: u64) {}
}
