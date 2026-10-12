//! Durable, collision-safe allocation. Persistence failures are errors at runtime
//! call sites, not process-local or fixed-string fallback identifiers.
use super::{atomic_state::AtomicState, executor::SqlExecutor};
use crate::domain::errors::Result;
use crate::ports::outbound::runtime::id_generator::IdGenerator;
use std::sync::Arc;
#[derive(Clone)]
pub struct DurableIds {
    state: AtomicState,
}
impl DurableIds {
    pub fn executor(executor: Arc<dyn SqlExecutor>) -> Self {
        Self {
            state: AtomicState::executor(executor),
        }
    }
    #[cfg(feature = "sqlite")]
    pub fn native(session: super::session::SqliteSession) -> Self {
        Self {
            state: AtomicState::native(session),
        }
    }
}
impl IdGenerator for DurableIds {
    fn next_id(&self, prefix: &str) -> String {
        self.try_next_id(prefix)
            .expect("durable identifier allocation failed; no volatile fallback")
    }
    fn try_next_id(&self, prefix: &str) -> Result<String> {
        self.state.next_id(prefix)
    }
}
