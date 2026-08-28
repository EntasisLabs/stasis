use async_trait::async_trait;

use crate::domain::errors::Result;
use crate::domain::runtime::blob_descriptor::BlobDescriptor;

/// Pluggable port for transferring content-addressed blobs (payloads/outputs).
///
/// Job records and federated envelopes carry [`BlobDescriptor`]s only; bytes move through this port.
#[async_trait]
pub trait BlobTransferPort: Send + Sync {
    async fn put(&self, bytes: &[u8], media_type: Option<&str>) -> Result<BlobDescriptor>;
    async fn get(&self, descriptor: &BlobDescriptor) -> Result<Vec<u8>>;
    async fn exists(&self, descriptor: &BlobDescriptor) -> Result<bool>;
    async fn delete(&self, descriptor: &BlobDescriptor) -> Result<bool>;
}
