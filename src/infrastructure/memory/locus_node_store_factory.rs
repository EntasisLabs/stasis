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

    /// Persist Locus memory through `locus-surreal-adapter`.
    ///
    /// Browser/edge endpoints:
    /// - `indxdb://<name>` IndexedDB
    /// - `mem://` embedded in-memory Surreal
    /// - `ws://` / `wss://` remote Surreal
    ///
    /// Stasis does not reimplement STTP; this wires Locus stores only.
    #[cfg(feature = "locus-persist")]
    pub async fn from_surreal_endpoint(
        endpoint: impl Into<String>,
        namespace: impl Into<String>,
        database: impl Into<String>,
    ) -> Result<Arc<Self>> {
        use locus_core_rs::storage::surrealdb::{
            SurrealDbNodeStore, SurrealDbRuntimeOptions, SurrealDbSemanticIndexStore,
        };
        use locus_surreal_adapter::{RuntimeSurrealDbClient, is_remote_endpoint};

        let endpoint = endpoint.into();
        let namespace = namespace.into();
        let database = database.into();
        let options = SurrealDbRuntimeOptions {
            root_dir: String::new(),
            use_remote: is_remote_endpoint(&endpoint),
            endpoint: endpoint.clone(),
            namespace,
            database,
        };
        let client = RuntimeSurrealDbClient::connect(&options, None, None)
            .await
            .map_err(|err| {
                StasisError::PortFailure(format!(
                    "connect locus surreal adapter ({endpoint}): {err}"
                ))
            })?;
        let client = Arc::new(client);

        let node_store = Arc::new(SurrealDbNodeStore::new(client.clone()));
        let node_initializer: Arc<dyn NodeStoreInitializer> = node_store.clone();
        node_initializer.initialize_async().await.map_err(|err| {
            StasisError::PortFailure(format!("initialize locus surreal node store: {err}"))
        })?;

        let semantic_index = Arc::new(SurrealDbSemanticIndexStore::new(client));
        let index_initializer: Arc<dyn SemanticIndexStoreInitializer> = semantic_index.clone();
        index_initializer.initialize_async().await.map_err(|err| {
            StasisError::PortFailure(format!("initialize locus surreal semantic index: {err}"))
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

    #[cfg(feature = "locus-persist")]
    pub async fn from_surreal_endpoint(
        endpoint: impl Into<String>,
        namespace: impl Into<String>,
        database: impl Into<String>,
    ) -> Result<Arc<LocusMemoryStore>> {
        LocusMemoryStore::from_surreal_endpoint(endpoint, namespace, database).await
    }
}
