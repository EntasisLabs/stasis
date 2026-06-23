use async_trait::async_trait;

use crate::domain::errors::Result;
use crate::ports::outbound::memory::memory_models::{
    MemoryFindRequest, MemoryFindResponse, MemoryGraphRequest, MemoryGraphResponse,
    MemoryRecallRequest, MemoryRecallResponse,
};

#[async_trait]
pub trait MemoryContextReader: Send + Sync {
    async fn recall(&self, request: &MemoryRecallRequest) -> Result<MemoryRecallResponse>;

    async fn find(&self, request: &MemoryFindRequest) -> Result<MemoryFindResponse>;

    async fn graph(&self, request: &MemoryGraphRequest) -> Result<MemoryGraphResponse>;
}
