use async_trait::async_trait;

use crate::domain::errors::Result;
use crate::ports::outbound::memory::memory_models::{
    MemoryAggregateRequest, MemoryAggregateResponse, MemoryRollupRequest, MemoryRollupResponse,
    MemorySchemaResponse, MemoryTransformRequest, MemoryTransformResponse,
};

#[async_trait]
pub trait MemoryOperations: Send + Sync {
    async fn aggregate(&self, request: &MemoryAggregateRequest) -> Result<MemoryAggregateResponse>;

    async fn transform(&self, request: &MemoryTransformRequest) -> Result<MemoryTransformResponse>;

    async fn rollup(&self, request: &MemoryRollupRequest) -> Result<MemoryRollupResponse>;

    async fn schema(&self) -> Result<MemorySchemaResponse>;
}
