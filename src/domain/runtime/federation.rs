use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::runtime::blob_descriptor::BlobDescriptor;
use crate::domain::runtime::provenance::ProvenanceRef;
use crate::domain::runtime::remote_job_envelope::{
    EnvelopeSignature, OriginAuthority, TerminalDeliveryEndpoint, hmac_sha256_hex,
    verify_hmac_sha256_signature,
};

pub const FEDERATED_SIGNAL_SCHEMA_VERSION_V1: u32 = 1;
pub const FEDERATED_TERMINAL_RESULT_SCHEMA_VERSION_V1: u32 = 1;

/// Durable signal that can cross independent Stasis runtimes without a shared database.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FederatedSignalEnvelope {
    pub schema_version: u32,
    pub signal_id: String,
    pub signal_type: String,
    pub correlation_key: String,
    pub payload: BlobDescriptor,
    pub origin_authority: OriginAuthority,
    pub destination_runtime_id: String,
    pub causation_id: String,
    pub correlation_id: String,
    pub occurred_at: DateTime<Utc>,
    pub signature: EnvelopeSignature,
}

/// Terminal job result delivered back to an origin runtime/endpoint.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FederatedTerminalResult {
    pub schema_version: u32,
    pub result_id: String,
    pub envelope_id: String,
    pub job_id: String,
    pub job_type: String,
    pub succeeded: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<BlobDescriptor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_provenance: Option<ProvenanceRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    pub origin_authority: OriginAuthority,
    pub terminal_delivery: TerminalDeliveryEndpoint,
    pub correlation_id: String,
    pub causation_id: String,
    pub occurred_at: DateTime<Utc>,
    pub signature: EnvelopeSignature,
}

impl FederatedSignalEnvelope {
    pub fn validate_schema_version(&self) -> Result<(), String> {
        if self.schema_version == FEDERATED_SIGNAL_SCHEMA_VERSION_V1 {
            Ok(())
        } else {
            Err(format!(
                "unsupported federated signal schema_version={} (supported={FEDERATED_SIGNAL_SCHEMA_VERSION_V1})",
                self.schema_version
            ))
        }
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>, String> {
        #[derive(Serialize)]
        struct Canonical<'a> {
            schema_version: u32,
            signal_id: &'a str,
            signal_type: &'a str,
            correlation_key: &'a str,
            payload: &'a BlobDescriptor,
            origin_authority: &'a OriginAuthority,
            destination_runtime_id: &'a str,
            causation_id: &'a str,
            correlation_id: &'a str,
            occurred_at: DateTime<Utc>,
        }

        serde_json::to_vec(&Canonical {
            schema_version: self.schema_version,
            signal_id: &self.signal_id,
            signal_type: &self.signal_type,
            correlation_key: &self.correlation_key,
            payload: &self.payload,
            origin_authority: &self.origin_authority,
            destination_runtime_id: &self.destination_runtime_id,
            causation_id: &self.causation_id,
            correlation_id: &self.correlation_id,
            occurred_at: self.occurred_at,
        })
        .map_err(|error| format!("canonicalize federated signal envelope: {error}"))
    }
}

impl FederatedTerminalResult {
    pub fn validate_schema_version(&self) -> Result<(), String> {
        if self.schema_version == FEDERATED_TERMINAL_RESULT_SCHEMA_VERSION_V1 {
            Ok(())
        } else {
            Err(format!(
                "unsupported federated terminal result schema_version={} (supported={FEDERATED_TERMINAL_RESULT_SCHEMA_VERSION_V1})",
                self.schema_version
            ))
        }
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>, String> {
        #[derive(Serialize)]
        struct Canonical<'a> {
            schema_version: u32,
            result_id: &'a str,
            envelope_id: &'a str,
            job_id: &'a str,
            job_type: &'a str,
            succeeded: bool,
            output: &'a Option<BlobDescriptor>,
            output_provenance: &'a Option<ProvenanceRef>,
            error_message: &'a Option<String>,
            origin_authority: &'a OriginAuthority,
            terminal_delivery: &'a TerminalDeliveryEndpoint,
            correlation_id: &'a str,
            causation_id: &'a str,
            occurred_at: DateTime<Utc>,
        }

        serde_json::to_vec(&Canonical {
            schema_version: self.schema_version,
            result_id: &self.result_id,
            envelope_id: &self.envelope_id,
            job_id: &self.job_id,
            job_type: &self.job_type,
            succeeded: self.succeeded,
            output: &self.output,
            output_provenance: &self.output_provenance,
            error_message: &self.error_message,
            origin_authority: &self.origin_authority,
            terminal_delivery: &self.terminal_delivery,
            correlation_id: &self.correlation_id,
            causation_id: &self.causation_id,
            occurred_at: self.occurred_at,
        })
        .map_err(|error| format!("canonicalize federated terminal result: {error}"))
    }
}

pub fn sign_federated_signal(
    envelope: &mut FederatedSignalEnvelope,
    key_id: impl Into<String>,
    key: &[u8],
) -> Result<(), String> {
    envelope.validate_schema_version()?;
    let canonical_bytes = envelope.canonical_bytes()?;
    envelope.signature = EnvelopeSignature {
        algorithm: EnvelopeSignature::HMAC_SHA256.to_string(),
        key_id: key_id.into(),
        signature_hex: hmac_sha256_hex(key, &canonical_bytes),
    };
    Ok(())
}

pub fn verify_federated_signal(
    envelope: &FederatedSignalEnvelope,
    key: &[u8],
) -> Result<(), String> {
    envelope.validate_schema_version()?;
    verify_hmac_sha256_signature(&envelope.signature, &envelope.canonical_bytes()?, key)
        .map_err(|_| "federated signal signature mismatch".to_string())
}

pub fn sign_federated_terminal_result(
    result: &mut FederatedTerminalResult,
    key_id: impl Into<String>,
    key: &[u8],
) -> Result<(), String> {
    result.validate_schema_version()?;
    let canonical_bytes = result.canonical_bytes()?;
    result.signature = EnvelopeSignature {
        algorithm: EnvelopeSignature::HMAC_SHA256.to_string(),
        key_id: key_id.into(),
        signature_hex: hmac_sha256_hex(key, &canonical_bytes),
    };
    Ok(())
}

pub fn verify_federated_terminal_result(
    result: &FederatedTerminalResult,
    key: &[u8],
) -> Result<(), String> {
    result.validate_schema_version()?;
    verify_hmac_sha256_signature(&result.signature, &result.canonical_bytes()?, key)
        .map_err(|_| "federated terminal result signature mismatch".to_string())
}
