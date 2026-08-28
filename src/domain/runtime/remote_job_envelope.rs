use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::domain::runtime::blob_descriptor::BlobDescriptor;
use crate::domain::runtime::placement::PlacementConstraints;

pub const REMOTE_JOB_ENVELOPE_SCHEMA_VERSION_V1: u32 = 1;

/// Authority that originated a remote/federated job.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OriginAuthority {
    pub runtime_id: String,
    pub authority_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub realm: Option<String>,
}

/// Where terminal outcomes must be delivered after execution.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TerminalDeliveryEndpoint {
    pub endpoint_id: String,
    pub protocol: String,
    pub address: String,
}

/// Opaque signature over the canonical envelope bytes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EnvelopeSignature {
    pub algorithm: String,
    pub key_id: String,
    pub signature_hex: String,
}

impl EnvelopeSignature {
    pub const HMAC_SHA256: &'static str = "hmac-sha256";
}

/// Signed, versioned remote job envelope for cross-runtime federation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RemoteJobEnvelope {
    pub schema_version: u32,
    pub envelope_id: String,
    pub job_type: String,
    pub payload: BlobDescriptor,
    pub idempotency_key: String,
    pub correlation_id: String,
    pub causation_id: String,
    pub deadline: DateTime<Utc>,
    pub origin_authority: OriginAuthority,
    pub terminal_delivery: TerminalDeliveryEndpoint,
    pub placement: PlacementConstraints,
    pub signature: EnvelopeSignature,
}

impl RemoteJobEnvelope {
    pub fn validate_schema_version(&self) -> Result<(), String> {
        if self.schema_version == REMOTE_JOB_ENVELOPE_SCHEMA_VERSION_V1 {
            Ok(())
        } else {
            Err(format!(
                "unsupported remote job envelope schema_version={} (supported={REMOTE_JOB_ENVELOPE_SCHEMA_VERSION_V1})",
                self.schema_version
            ))
        }
    }

    /// Canonical bytes used for signing/verification (signature field excluded).
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, String> {
        #[derive(Serialize)]
        struct Canonical<'a> {
            schema_version: u32,
            envelope_id: &'a str,
            job_type: &'a str,
            payload: &'a BlobDescriptor,
            idempotency_key: &'a str,
            correlation_id: &'a str,
            causation_id: &'a str,
            deadline: DateTime<Utc>,
            origin_authority: &'a OriginAuthority,
            terminal_delivery: &'a TerminalDeliveryEndpoint,
            placement: &'a PlacementConstraints,
        }

        serde_json::to_vec(&Canonical {
            schema_version: self.schema_version,
            envelope_id: &self.envelope_id,
            job_type: &self.job_type,
            payload: &self.payload,
            idempotency_key: &self.idempotency_key,
            correlation_id: &self.correlation_id,
            causation_id: &self.causation_id,
            deadline: self.deadline,
            origin_authority: &self.origin_authority,
            terminal_delivery: &self.terminal_delivery,
            placement: &self.placement,
        })
        .map_err(|err| format!("canonicalize remote job envelope: {err}"))
    }
}

/// HMAC-SHA256 helper for reference signing without extra crypto crates.
pub fn hmac_sha256_hex(key: &[u8], message: &[u8]) -> String {
    // Simplified HMAC-SHA256 (RFC 2104) using sha2 only.
    const BLOCK: usize = 64;
    let mut key_block = [0u8; BLOCK];
    if key.len() > BLOCK {
        let hashed = Sha256::digest(key);
        key_block[..hashed.len()].copy_from_slice(&hashed);
    } else {
        key_block[..key.len()].copy_from_slice(key);
    }

    let mut ipad = [0x36u8; BLOCK];
    let mut opad = [0x5cu8; BLOCK];
    for i in 0..BLOCK {
        ipad[i] ^= key_block[i];
        opad[i] ^= key_block[i];
    }

    let mut inner = Sha256::new();
    inner.update(ipad);
    inner.update(message);
    let inner_hash = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(opad);
    outer.update(inner_hash);
    hex::encode(outer.finalize())
}

pub fn sign_remote_job_envelope(
    envelope: &mut RemoteJobEnvelope,
    key_id: impl Into<String>,
    key: &[u8],
) -> Result<(), String> {
    let bytes = envelope.canonical_bytes()?;
    envelope.signature = EnvelopeSignature {
        algorithm: EnvelopeSignature::HMAC_SHA256.to_string(),
        key_id: key_id.into(),
        signature_hex: hmac_sha256_hex(key, &bytes),
    };
    Ok(())
}

pub fn verify_remote_job_envelope(envelope: &RemoteJobEnvelope, key: &[u8]) -> Result<(), String> {
    envelope.validate_schema_version()?;
    if envelope.signature.algorithm != EnvelopeSignature::HMAC_SHA256 {
        return Err(format!(
            "unsupported signature algorithm: {}",
            envelope.signature.algorithm
        ));
    }
    let bytes = envelope.canonical_bytes()?;
    let expected = hmac_sha256_hex(key, &bytes);
    if expected != envelope.signature.signature_hex {
        return Err("remote job envelope signature mismatch".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::runtime::blob_descriptor::BlobDescriptor;
    use chrono::Duration;

    fn sample_envelope() -> RemoteJobEnvelope {
        RemoteJobEnvelope {
            schema_version: REMOTE_JOB_ENVELOPE_SCHEMA_VERSION_V1,
            envelope_id: "env-1".into(),
            job_type: "federated.echo".into(),
            payload: BlobDescriptor::from_bytes(b"{\"n\":1}"),
            idempotency_key: "idem-1".into(),
            correlation_id: "corr-1".into(),
            causation_id: "cause-1".into(),
            deadline: Utc::now() + Duration::minutes(5),
            origin_authority: OriginAuthority {
                runtime_id: "rt-a".into(),
                authority_id: "auth-a".into(),
                realm: Some("prod".into()),
            },
            terminal_delivery: TerminalDeliveryEndpoint {
                endpoint_id: "ep-1".into(),
                protocol: "https".into(),
                address: "https://rt-a.example/terminal".into(),
            },
            placement: PlacementConstraints::unrestricted().require_capability("cpu"),
            signature: EnvelopeSignature {
                algorithm: EnvelopeSignature::HMAC_SHA256.into(),
                key_id: String::new(),
                signature_hex: String::new(),
            },
        }
    }

    #[test]
    fn signed_envelope_verifies() {
        let mut envelope = sample_envelope();
        let key = b"test-federation-key";
        sign_remote_job_envelope(&mut envelope, "key-1", key).expect("sign");
        verify_remote_job_envelope(&envelope, key).expect("verify");
    }

    #[test]
    fn tampered_envelope_fails_verify() {
        let mut envelope = sample_envelope();
        let key = b"test-federation-key";
        sign_remote_job_envelope(&mut envelope, "key-1", key).expect("sign");
        envelope.job_type = "federated.tampered".into();
        assert!(verify_remote_job_envelope(&envelope, key).is_err());
    }
}
