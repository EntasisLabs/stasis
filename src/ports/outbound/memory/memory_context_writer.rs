use async_trait::async_trait;

use crate::domain::errors::Result;
use crate::ports::outbound::memory::memory_models::{MemoryStoreRequest, MemoryStoreResponse};

#[async_trait]
pub trait MemoryContextWriter: Send + Sync {
    async fn store_context(&self, request: &MemoryStoreRequest) -> Result<MemoryStoreResponse>;
}
