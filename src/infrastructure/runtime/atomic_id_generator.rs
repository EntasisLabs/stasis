use std::sync::atomic::{AtomicU64, Ordering};

use crate::ports::outbound::runtime::id_generator::IdGenerator;

#[derive(Default)]
pub struct AtomicIdGenerator {
    counter: AtomicU64,
}

impl AtomicIdGenerator {
    pub fn new(start: u64) -> Self {
        Self {
            counter: AtomicU64::new(start),
        }
    }
}

impl IdGenerator for AtomicIdGenerator {
    fn next_id(&self, prefix: &str) -> String {
        let n = self.counter.fetch_add(1, Ordering::SeqCst);
        format!("{}-{}", prefix, n)
    }
}
