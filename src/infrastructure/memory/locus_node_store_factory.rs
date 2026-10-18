use std::sync::Arc;

use crate::infrastructure::sqlite::SqlExecutor;
#[cfg(feature = "sqlite")]
use crate::infrastructure::sqlite::session::SqliteSession;
use locus_core_rs::{
    InMemoryNodeStore, InMemorySemanticIndexStore, NodeStore, NodeStoreInitializer,
    SemanticIndexStore, SemanticIndexStoreInitializer,
};
#[cfg(feature = "surreal")]
use locus_core_rs::{SurrealDbNodeStore, SurrealDbSemanticIndexStore};

use crate::domain::errors::{Result, StasisError};
use crate::infrastructure::memory::executor_locus_store::{
    ExecutorLocusNodeStore, ExecutorSemanticIndexStore,
};

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

    /// Persist Locus nodes and the semantic index through a [`SqlExecutor`].
    ///
    /// Durable Objects pass `JsSqlExecutor`. Native tests use this to prove the
    /// executor schema matches `locus-sqlite-adapter`. The tables are migration
    /// v4 (`temporal_node`, `calibration`, `sync_checkpoint`, `semantic_tag_index`).
    pub fn from_executor(executor: Arc<dyn SqlExecutor>) -> Result<Arc<Self>> {
        let node_store = Arc::new(ExecutorLocusNodeStore::new(executor.clone()));
        node_store.initialize().map_err(|err| {
            StasisError::PortFailure(format!("initialize locus sqlite node store: {err}"))
        })?;
        let semantic_index = Arc::new(ExecutorSemanticIndexStore::new(executor));
        semantic_index.initialize().map_err(|err| {
            StasisError::PortFailure(format!("initialize locus sqlite semantic index: {err}"))
        })?;
        Ok(Arc::new(Self {
            node_store,
            semantic_index,
        }))
    }

    /// Native Locus memory on `locus-sqlite-adapter`.
    ///
    /// The adapter opens its own connection to [`SqliteSession::path`]. File
    /// databases share the file. In-memory sessions use a shared-cache URI so
    /// this connection and the session see one database. Identity and Stasis
    /// migrations stay on the session: `SqliteDatabase` has no constructor that
    /// accepts an existing `rusqlite` connection, and the two connections do
    /// not share a transaction.
    #[cfg(feature = "sqlite")]
    pub async fn from_sqlite_session(session: SqliteSession) -> Result<Arc<Self>> {
        use locus_sqlite_adapter::{SqliteDatabase, SqliteNodeStore, SqliteSemanticIndexStore};

        let db = SqliteDatabase::open(session.path())
            .map_err(|err| StasisError::PortFailure(format!("open locus sqlite adapter: {err}")))?;
        let db = Arc::new(db);
        let node_store = Arc::new(SqliteNodeStore::new(db.clone()));
        let node_initializer: Arc<dyn NodeStoreInitializer> = node_store.clone();
        node_initializer.initialize_async().await.map_err(|err| {
            StasisError::PortFailure(format!("initialize locus sqlite adapter: {err}"))
        })?;
        let semantic_index = Arc::new(SqliteSemanticIndexStore::new(db));
        let index_initializer: Arc<dyn SemanticIndexStoreInitializer> = semantic_index.clone();
        index_initializer.initialize_async().await.map_err(|err| {
            StasisError::PortFailure(format!("initialize locus sqlite semantic index: {err}"))
        })?;
        Ok(Arc::new(Self {
            node_store,
            semantic_index,
        }))
    }

    /// Persist Locus memory in the Surreal database the runtime is already using.
    #[cfg(feature = "surreal")]
    pub async fn from_surreal_db(
        db: surrealdb::Surreal<surrealdb::engine::any::Any>,
    ) -> Result<Arc<Self>> {
        let client = Arc::new(StasisSurrealLocusClient { db });
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

    pub fn from_executor(executor: Arc<dyn SqlExecutor>) -> Result<Arc<LocusMemoryStore>> {
        LocusMemoryStore::from_executor(executor)
    }

    #[cfg(feature = "sqlite")]
    pub async fn from_sqlite_session(session: SqliteSession) -> Result<Arc<LocusMemoryStore>> {
        LocusMemoryStore::from_sqlite_session(session).await
    }

    #[cfg(feature = "surreal")]
    pub async fn from_surreal_db(
        db: surrealdb::Surreal<surrealdb::engine::any::Any>,
    ) -> Result<Arc<LocusMemoryStore>> {
        LocusMemoryStore::from_surreal_db(db).await
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

/// Locus `SurrealDbClient` over the Surreal handle the runtime already opened.
#[cfg(feature = "surreal")]
struct StasisSurrealLocusClient {
    db: surrealdb::Surreal<surrealdb::engine::any::Any>,
}

#[cfg(feature = "surreal")]
impl StasisSurrealLocusClient {
    fn is_read_query(query: &str) -> bool {
        query
            .trim_start()
            .to_ascii_uppercase()
            .starts_with("SELECT")
    }
}

#[cfg(feature = "surreal")]
#[async_trait::async_trait]
impl locus_core_rs::SurrealDbClient for StasisSurrealLocusClient {
    async fn raw_query(
        &self,
        query: &str,
        parameters: locus_core_rs::storage::surrealdb::QueryParams,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        let is_read_query = Self::is_read_query(query);
        let response = if parameters.is_empty() {
            self.db.query(query).await?
        } else {
            self.db.query(query).bind(parameters).await?
        };
        let mut response = response.check()?;
        if !is_read_query {
            return Ok(Vec::new());
        }
        if let Ok(rows) = response.take::<Vec<serde_json::Value>>(0) {
            return Ok(rows);
        }
        if let Ok(Some(row)) = response.take::<Option<serde_json::Value>>(0) {
            return Ok(vec![row]);
        }
        Ok(Vec::new())
    }
}
