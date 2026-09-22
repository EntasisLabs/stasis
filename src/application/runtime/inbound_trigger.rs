use serde::Deserialize;
use serde_json::Value;

use crate::application::runtime::typed_job::encode_typed_payload;
use crate::application::telemetry::propagation::generate_w3c_trace_id;
use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::inbound_trigger::{
    INBOUND_TRIGGER_SCHEMA_VERSION_V1, InboundAccept, InboundDisposition, InboundJobTrigger,
    InboundProtocol, InboundTriggerReceipt,
};
use crate::domain::runtime::job::{BackoffPolicy, NewJob};
use crate::domain::runtime::placement::PlacementConstraints;
use crate::domain::runtime::typed_contract::StasisJob;
use crate::ports::outbound::runtime::clock::Clock;
use crate::ports::outbound::runtime::id_generator::IdGenerator;
use crate::ports::outbound::runtime::inbound_trigger_store::{InboundTriggerStore, ReceiptInsert};
use crate::ports::outbound::runtime::job_store::JobStore;

#[derive(Debug, Deserialize)]
struct InboundDocument {
    schema_version: u32,
    idempotency_key: String,
    job_type: String,
    #[serde(default = "default_payload_version")]
    payload_version: u32,
    payload: Value,
    #[serde(default)]
    queue: Option<String>,
    #[serde(default)]
    correlation_id: Option<String>,
    #[serde(default)]
    max_attempts: Option<u32>,
    #[serde(default)]
    priority: Option<i32>,
    #[serde(default)]
    payload_encoding: Option<String>,
}

fn default_payload_version() -> u32 {
    1
}

/// Decode the canonical inbound document. `protocol` comes from the listener, not the body.
pub fn decode_inbound_json(protocol: InboundProtocol, body: &[u8]) -> Result<InboundJobTrigger> {
    let document: InboundDocument = serde_json::from_slice(body)
        .map_err(|err| StasisError::PortFailure(format!("decode inbound job trigger: {err}")))?;
    if document.schema_version != INBOUND_TRIGGER_SCHEMA_VERSION_V1 {
        return Err(StasisError::PortFailure(format!(
            "unsupported inbound trigger schema_version={} (supported={INBOUND_TRIGGER_SCHEMA_VERSION_V1})",
            document.schema_version
        )));
    }
    let idempotency_key = document.idempotency_key.trim().to_string();
    let job_type = document.job_type.trim().to_string();
    if idempotency_key.is_empty() {
        return Err(StasisError::PortFailure(
            "inbound trigger idempotency_key must not be empty".into(),
        ));
    }
    if job_type.is_empty() {
        return Err(StasisError::PortFailure(
            "inbound trigger job_type must not be empty".into(),
        ));
    }
    let encoding = document
        .payload_encoding
        .as_deref()
        .unwrap_or("typed")
        .trim();
    let payload_ref = match encoding {
        "typed" => serde_json::to_string(&serde_json::json!({
            "version": document.payload_version,
            "payload": document.payload,
        }))
        .map_err(|err| StasisError::PortFailure(format!("encode typed inbound payload: {err}")))?,
        "raw" => serde_json::to_string(&document.payload).map_err(|err| {
            StasisError::PortFailure(format!("encode raw inbound payload: {err}"))
        })?,
        other => {
            return Err(StasisError::PortFailure(format!(
                "unsupported inbound payload_encoding '{other}'"
            )));
        }
    };
    let queue = document
        .queue
        .map(|queue| queue.trim().to_string())
        .filter(|queue| !queue.is_empty())
        .unwrap_or_else(|| "default".into());
    Ok(InboundJobTrigger {
        protocol,
        idempotency_key,
        job_type,
        payload_ref,
        queue,
        priority: document.priority.unwrap_or(100),
        max_attempts: document.max_attempts.unwrap_or(3).max(1),
        backoff: BackoffPolicy::default(),
        correlation_id: document
            .correlation_id
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty()),
        placement: PlacementConstraints::default(),
    })
}

pub fn trigger_from_job<T: StasisJob>(
    protocol: InboundProtocol,
    idempotency_key: impl Into<String>,
    payload: &T,
) -> Result<InboundJobTrigger> {
    let idempotency_key = idempotency_key.into().trim().to_string();
    if idempotency_key.is_empty() {
        return Err(StasisError::PortFailure(
            "inbound trigger idempotency_key must not be empty".into(),
        ));
    }
    let declared = T::declaration();
    Ok(InboundJobTrigger {
        protocol,
        idempotency_key,
        job_type: T::NAME.to_string(),
        payload_ref: encode_typed_payload(payload)?,
        queue: declared.queue,
        priority: declared.priority,
        max_attempts: declared.retry.max_attempts,
        backoff: declared.retry.backoff,
        correlation_id: None,
        placement: declared.placement,
    })
}

pub async fn accept_inbound_trigger(
    jobs: &dyn JobStore,
    receipts: &dyn InboundTriggerStore,
    clock: &dyn Clock,
    ids: &dyn IdGenerator,
    trigger: InboundJobTrigger,
) -> Result<InboundAccept> {
    let job_id = ids.next_id("job");
    let now = clock.now();
    let receipt = InboundTriggerReceipt {
        idempotency_key: trigger.idempotency_key.clone(),
        protocol: trigger.protocol.as_str().to_string(),
        job_id: job_id.clone(),
        job_type: trigger.job_type.clone(),
        accepted_at: now,
    };
    match receipts.insert_if_absent(receipt).await? {
        ReceiptInsert::Exists(existing) => {
            return Ok(InboundAccept {
                disposition: InboundDisposition::Duplicate,
                job_id: existing.job_id,
            });
        }
        ReceiptInsert::Created => {}
    }

    let correlation_id = trigger
        .correlation_id
        .clone()
        .unwrap_or_else(|| trigger.idempotency_key.clone());
    let job = NewJob {
        id: job_id.clone(),
        queue: trigger.queue,
        job_type: trigger.job_type,
        payload_ref: trigger.payload_ref,
        priority: trigger.priority,
        max_attempts: trigger.max_attempts,
        idempotency_key: trigger.idempotency_key.clone(),
        correlation_id,
        causation_id: format!("inbound:{}", trigger.protocol.as_str()),
        trace_id: generate_w3c_trace_id(),
        input_provenance: None,
        placement: trigger.placement,
        scheduled_at: now,
        backoff_policy: trigger.backoff,
    }
    .into_job();

    if let Err(err) = jobs.insert(job).await {
        if let Err(rollback) = receipts.delete(&trigger.idempotency_key).await {
            return Err(StasisError::PortFailure(format!(
                "inbound job insert failed ({err}); receipt rollback failed ({rollback})"
            )));
        }
        return Err(err);
    }

    Ok(InboundAccept {
        disposition: InboundDisposition::Accepted,
        job_id,
    })
}
