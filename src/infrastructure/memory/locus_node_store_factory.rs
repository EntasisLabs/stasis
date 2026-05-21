use std::sync::Arc;

use locus_core_rs::{InMemoryNodeStore, NodeStore, NodeStoreInitializer};

use crate::domain::errors::{Result, StasisError};

pub struct LocusNodeStoreFactory;

impl LocusNodeStoreFactory {
    pub async fn in_memory() -> Result<Arc<dyn NodeStore>> {
        let store = Arc::new(InMemoryNodeStore::new());
        let initializer: Arc<dyn NodeStoreInitializer> = store.clone();
        initializer.initialize_async().await.map_err(|e| {
            StasisError::PortFailure(format!("initialize locus in-memory store: {e}"))
        })?;
        Ok(store)
    }
}
