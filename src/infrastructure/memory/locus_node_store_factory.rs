use std::sync::Arc;

use locus_core_rs::{
    InMemoryNodeStore, InMemorySemanticIndexStore, NodeStore, NodeStoreInitializer,
    SemanticIndexStore, SemanticIndexStoreInitializer,
};

use crate::domain::errors::{Result, StasisError};

pub struct LocusMemoryStore {
    pub node_store: Arc<dyn NodeStore>,
    pub semantic_index: Arc<dyn SemanticIndexStore>,
}

impl LocusMemoryStore {
    pub async fn in_memory() -> Result<Arc<Self>> {
        let node_store = Arc::new(InMemoryNodeStore::new());
        let node_initializer: Arc<dyn NodeStoreInitializer> = node_store.clone();
        node_initializer.initialize_async().await.map_err(|e| {
            StasisError::PortFailure(format!("initialize locus in-memory node store: {e}"))
        })?;

        let semantic_index = Arc::new(InMemorySemanticIndexStore::new());
        let index_initializer: Arc<dyn SemanticIndexStoreInitializer> = semantic_index.clone();
        index_initializer.initialize_async().await.map_err(|e| {
            StasisError::PortFailure(format!("initialize locus semantic index store: {e}"))
        })?;

        Ok(Arc::new(Self {
            node_store,
            semantic_index,
        }))
    }
}

pub struct LocusNodeStoreFactory;

impl LocusNodeStoreFactory {
    pub async fn in_memory() -> Result<Arc<LocusMemoryStore>> {
        LocusMemoryStore::in_memory().await
    }
}
