use surrealdb::Surreal;
use surrealdb::engine::local::{Db, Mem};

use crate::application::runtime::in_memory_runtime::InMemoryRuntime;
use crate::application::runtime::surreal_runtime::SurrealRuntime;
use crate::domain::errors::{Result, StasisError};

#[derive(Clone, Debug)]
pub enum RuntimeBackend {
    InMemory,
    SurrealMem { namespace: String, database: String },
}

#[derive(Clone)]
pub enum RuntimeComposition {
    InMemory(InMemoryRuntime),
    Surreal(SurrealRuntime),
}

pub struct RuntimeFactory;

impl RuntimeFactory {
    pub async fn build(config: RuntimeBackend) -> Result<RuntimeComposition> {
        match config {
            RuntimeBackend::InMemory => Ok(RuntimeComposition::InMemory(InMemoryRuntime::new())),
            RuntimeBackend::SurrealMem {
                namespace,
                database,
            } => {
                let db = Surreal::new::<Mem>(())
                    .await
                    .map_err(|e| StasisError::PortFailure(format!("create surreal mem db: {e}")))?;

                db.use_ns(namespace).use_db(database).await.map_err(|e| {
                    StasisError::PortFailure(format!("select surreal namespace/database: {e}"))
                })?;

                Ok(RuntimeComposition::Surreal(SurrealRuntime::new(db)))
            }
        }
    }

    pub fn from_db(db: Surreal<Db>) -> RuntimeComposition {
        RuntimeComposition::Surreal(SurrealRuntime::new(db))
    }
}
