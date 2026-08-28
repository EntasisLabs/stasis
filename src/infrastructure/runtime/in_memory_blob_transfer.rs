use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::blob_descriptor::BlobDescriptor;
use crate::ports::outbound::runtime::blob_transfer::BlobTransferPort;

fn lock_err() -> StasisError {
    StasisError::PortFailure("blob transfer store lock poisoned".into())
}

/// In-memory content-addressed blob store for tests and single-process federation.
#[derive(Clone, Default)]
pub struct InMemoryBlobTransfer {
    blobs: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl InMemoryBlobTransfer {
    pub fn new() -> Self {
        Self::default()
    }

    fn key(descriptor: &BlobDescriptor) -> String {
        format!("{}:{}", descriptor.digest.algorithm, descriptor.digest.hex)
    }
}

#[async_trait]
impl BlobTransferPort for InMemoryBlobTransfer {
    async fn put(&self, bytes: &[u8], media_type: Option<&str>) -> Result<BlobDescriptor> {
        let mut descriptor = BlobDescriptor::from_bytes(bytes);
        if let Some(media_type) = media_type {
            descriptor = descriptor.with_media_type(media_type);
        }
        let hint = format!("mem://{}", descriptor.digest.hex);
        descriptor = descriptor.with_transfer_hint(hint);
        let mut blobs = self.blobs.write().map_err(|_| lock_err())?;
        blobs.insert(Self::key(&descriptor), bytes.to_vec());
        Ok(descriptor)
    }

    async fn get(&self, descriptor: &BlobDescriptor) -> Result<Vec<u8>> {
        let blobs = self.blobs.read().map_err(|_| lock_err())?;
        let Some(bytes) = blobs.get(&Self::key(descriptor)).cloned() else {
            return Err(StasisError::PortFailure(format!(
                "blob not found: {}:{}",
                descriptor.digest.algorithm, descriptor.digest.hex
            )));
        };
        if !descriptor.verify(&bytes) {
            return Err(StasisError::PortFailure(
                "blob digest/size mismatch".into(),
            ));
        }
        Ok(bytes)
    }

    async fn exists(&self, descriptor: &BlobDescriptor) -> Result<bool> {
        let blobs = self.blobs.read().map_err(|_| lock_err())?;
        Ok(blobs.contains_key(&Self::key(descriptor)))
    }

    async fn delete(&self, descriptor: &BlobDescriptor) -> Result<bool> {
        let mut blobs = self.blobs.write().map_err(|_| lock_err())?;
        Ok(blobs.remove(&Self::key(descriptor)).is_some())
    }
}
