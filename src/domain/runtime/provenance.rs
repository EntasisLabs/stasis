use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Content digest for content-addressed provenance and blob descriptors.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct ContentDigest {
    pub algorithm: String,
    pub hex: String,
}

impl ContentDigest {
    pub const SHA256: &'static str = "sha256";

    pub fn sha256_bytes(bytes: &[u8]) -> Self {
        let digest = Sha256::digest(bytes);
        Self {
            algorithm: Self::SHA256.to_string(),
            hex: hex::encode(digest),
        }
    }

    pub fn matches(&self, bytes: &[u8]) -> bool {
        if self.algorithm != Self::SHA256 {
            return false;
        }
        Self::sha256_bytes(bytes).hex == self.hex
    }
}

/// Scheme for a provenance locator. STTP remains first-class via adapter; other schemes are runtime-neutral.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProvenanceScheme {
    Sttp,
    Cas,
    Uri,
    Opaque,
}

impl ProvenanceScheme {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Sttp => "sttp",
            Self::Cas => "cas",
            Self::Uri => "uri",
            Self::Opaque => "opaque",
        }
    }

    pub fn parse(raw: &str) -> Self {
        match raw {
            "sttp" => Self::Sttp,
            "cas" => Self::Cas,
            "uri" => Self::Uri,
            _ => Self::Opaque,
        }
    }
}

/// Runtime-neutral input/output lineage reference. Replaces mandatory STTP node IDs.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProvenanceRef {
    pub scheme: ProvenanceScheme,
    pub locator: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<ContentDigest>,
}

impl ProvenanceRef {
    pub fn new(scheme: ProvenanceScheme, locator: impl Into<String>) -> Self {
        Self {
            scheme,
            locator: locator.into(),
            media_type: None,
            digest: None,
        }
    }

    pub fn sttp(node_id: impl AsRef<str>) -> Self {
        Self::new(ProvenanceScheme::Sttp, node_id.as_ref())
    }

    pub fn cas(digest: ContentDigest) -> Self {
        let locator = format!("{}:{}", digest.algorithm, digest.hex);
        Self {
            scheme: ProvenanceScheme::Cas,
            locator,
            media_type: None,
            digest: Some(digest),
        }
    }

    pub fn uri(uri: impl AsRef<str>) -> Self {
        Self::new(ProvenanceScheme::Uri, uri.as_ref())
    }

    pub fn with_media_type(mut self, media_type: impl Into<String>) -> Self {
        self.media_type = Some(media_type.into());
        self
    }

    pub fn with_digest(mut self, digest: ContentDigest) -> Self {
        self.digest = Some(digest);
        self
    }

    pub fn compact(&self) -> String {
        format!("{}:{}", self.scheme.as_str(), self.locator)
    }

    pub fn parse_compact(raw: &str) -> Option<Self> {
        let (scheme_raw, locator) = raw.split_once(':')?;
        if locator.is_empty() {
            return None;
        }
        Some(Self::new(ProvenanceScheme::parse(scheme_raw), locator))
    }
}

/// Maps between STTP node IDs and generic [`ProvenanceRef`] values.
#[derive(Clone, Debug, Default)]
pub struct SttpProvenanceAdapter;

impl SttpProvenanceAdapter {
    pub fn to_provenance(node_id: impl AsRef<str>) -> ProvenanceRef {
        ProvenanceRef::sttp(node_id)
    }

    pub fn from_provenance(reference: &ProvenanceRef) -> Option<&str> {
        match reference.scheme {
            ProvenanceScheme::Sttp => Some(reference.locator.as_str()),
            _ => None,
        }
    }

    pub fn from_optional(reference: Option<&ProvenanceRef>) -> Option<String> {
        reference.and_then(|r| Self::from_provenance(r).map(str::to_string))
    }

    /// Persist-compat helper: empty string means absent STTP lineage.
    pub fn to_compat_string(reference: Option<&ProvenanceRef>) -> String {
        Self::from_optional(reference).unwrap_or_default()
    }

    /// Restore provenance from legacy STTP string columns (empty ⇒ None).
    pub fn from_compat_string(raw: &str) -> Option<ProvenanceRef> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(Self::to_provenance(trimmed))
        }
    }

    pub fn merge_legacy(
        structured: Option<ProvenanceRef>,
        legacy_sttp: &str,
    ) -> Option<ProvenanceRef> {
        structured.or_else(|| Self::from_compat_string(legacy_sttp))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sttp_adapter_round_trips() {
        let prov = SttpProvenanceAdapter::to_provenance("sttp:in:1");
        assert_eq!(
            SttpProvenanceAdapter::from_provenance(&prov),
            Some("sttp:in:1")
        );
        assert_eq!(
            SttpProvenanceAdapter::to_compat_string(Some(&prov)),
            "sttp:in:1"
        );
        assert!(SttpProvenanceAdapter::from_compat_string("").is_none());
    }

    #[test]
    fn cas_digest_matches_bytes() {
        let digest = ContentDigest::sha256_bytes(b"hello");
        let prov = ProvenanceRef::cas(digest.clone());
        assert_eq!(prov.scheme, ProvenanceScheme::Cas);
        assert!(digest.matches(b"hello"));
        assert!(!digest.matches(b"other"));
    }
}
