use serde::{Deserialize, Serialize};

use crate::domain::runtime::provenance::ContentDigest;

/// Content-addressed payload/output descriptor. Large artifacts stay out of job records.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BlobDescriptor {
    pub digest: ContentDigest,
    pub size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    /// Opaque transfer hint for pluggable blob ports (URI, bucket key, local path, …).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfer_hint: Option<String>,
}

impl BlobDescriptor {
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            digest: ContentDigest::sha256_bytes(bytes),
            size_bytes: bytes.len() as u64,
            media_type: None,
            transfer_hint: None,
        }
    }

    pub fn with_media_type(mut self, media_type: impl Into<String>) -> Self {
        self.media_type = Some(media_type.into());
        self
    }

    pub fn with_transfer_hint(mut self, hint: impl Into<String>) -> Self {
        self.transfer_hint = Some(hint.into());
        self
    }

    pub fn verify(&self, bytes: &[u8]) -> bool {
        self.size_bytes == bytes.len() as u64 && self.digest.matches(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_verifies_content() {
        let bytes = b"artifact-bytes";
        let descriptor = BlobDescriptor::from_bytes(bytes)
            .with_media_type("application/octet-stream")
            .with_transfer_hint("mem://artifact-1");
        assert!(descriptor.verify(bytes));
        assert!(!descriptor.verify(b"tampered"));
    }
}
