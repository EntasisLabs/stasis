use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::runtime::blob_descriptor::BlobDescriptor;
use crate::domain::runtime::provenance::ProvenanceRef;
use crate::domain::runtime::remote_job_envelope::{
    EnvelopeSignature, OriginAuthority, TerminalDeliveryEndpoint,
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
}
