use std::sync::Arc;

use async_trait::async_trait;
use locus_core_rs::{NodeStore, StoreContextService, TreeSitterValidator};

use crate::domain::errors::{Result, StasisError};
use crate::ports::outbound::memory::memory_context_writer::MemoryContextWriter;
use crate::ports::outbound::memory::memory_models::{MemoryStoreRequest, MemoryStoreResponse};

#[derive(Clone)]
pub struct LocusContextWriter {
    service: Arc<StoreContextService>,
}

impl LocusContextWriter {
    pub fn new(store: Arc<dyn NodeStore>) -> Self {
        let validator = Arc::new(TreeSitterValidator::new());
        let service = StoreContextService::new(store, validator);
        Self {
            service: Arc::new(service),
        }
    }
}

#[async_trait]
impl MemoryContextWriter for LocusContextWriter {
    async fn store_context(&self, request: &MemoryStoreRequest) -> Result<MemoryStoreResponse> {
        let result = self
            .service
            .store_async(&request.raw_node, &request.session_id)
            .await;

        if !result.valid {
            return Err(StasisError::PortFailure(
                result
                    .validation_error
                    .unwrap_or_else(|| "locus store rejected context".to_string()),
            ));
        }

        Ok(MemoryStoreResponse {
            node_id: result.node_id,
            psi: result.psi,
            valid: result.valid,
            validation_error: result.validation_error,
        })
    }
}
