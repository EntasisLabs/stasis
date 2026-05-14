pub trait RuntimeMetrics: Send + Sync {
    fn incr_counter(&self, name: &str, value: u64);
    fn observe_duration_ms(&self, name: &str, duration_ms: u64);
}
