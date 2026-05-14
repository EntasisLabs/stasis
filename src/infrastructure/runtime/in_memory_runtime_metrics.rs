use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::ports::outbound::runtime::runtime_metrics::RuntimeMetrics;

#[derive(Clone, Default)]
pub struct InMemoryRuntimeMetrics {
    counters: Arc<RwLock<HashMap<String, u64>>>,
    durations_ms: Arc<RwLock<HashMap<String, Vec<u64>>>>,
}

#[derive(Clone, Debug, Default)]
pub struct RuntimeMetricsSnapshot {
    pub counters: HashMap<String, u64>,
    pub durations_ms: HashMap<String, Vec<u64>>,
}

impl InMemoryRuntimeMetrics {
    pub fn snapshot(&self) -> RuntimeMetricsSnapshot {
        let counters = self
            .counters
            .read()
            .map(|state| state.clone())
            .unwrap_or_default();
        let durations_ms = self
            .durations_ms
            .read()
            .map(|state| state.clone())
            .unwrap_or_default();

        RuntimeMetricsSnapshot {
            counters,
            durations_ms,
        }
    }
}

impl RuntimeMetrics for InMemoryRuntimeMetrics {
    fn incr_counter(&self, name: &str, value: u64) {
        if let Ok(mut state) = self.counters.write() {
            *state.entry(name.to_string()).or_insert(0) += value;
        }
    }

    fn observe_duration_ms(&self, name: &str, duration_ms: u64) {
        if let Ok(mut state) = self.durations_ms.write() {
            state
                .entry(name.to_string())
                .or_insert_with(Vec::new)
                .push(duration_ms);
        }
    }
}
