use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::federation::{FederatedSignalEnvelope, FederatedTerminalResult};
use crate::domain::runtime::federation::{
    verify_federated_signal, verify_federated_terminal_result,
};
use crate::domain::runtime::remote_job_envelope::{
    EnvelopeSignature, RemoteJobEnvelope, verify_remote_job_envelope,
};
use crate::ports::outbound::runtime::federated_delivery::{
    FederatedDeliveryPort, FederatedIngressPort,
};

fn lock_err() -> StasisError {
    StasisError::PortFailure("federated delivery bus lock poisoned".into())
}

#[derive(Clone, Debug, Default)]
pub struct FederatedInbox {
    pub remote_jobs: Vec<RemoteJobEnvelope>,
    pub signals: Vec<FederatedSignalEnvelope>,
    pub terminal_results: Vec<FederatedTerminalResult>,
}

/// In-process federation bus connecting independent runtime inboxes without a shared DB.
#[derive(Clone, Default)]
pub struct InMemoryFederatedBus {
    inboxes: Arc<RwLock<HashMap<String, FederatedInbox>>>,
    verification_keys: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl InMemoryFederatedBus {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn ensure_runtime(&self, runtime_id: impl Into<String>) -> Result<()> {
        let mut inboxes = self.inboxes.write().map_err(|_| lock_err())?;
        inboxes
            .entry(runtime_id.into())
            .or_insert_with(FederatedInbox::default);
        Ok(())
    }

    pub fn register_verification_key(
        &self,
        key_id: impl Into<String>,
        key: impl Into<Vec<u8>>,
    ) -> Result<()> {
        let mut keys = self.verification_keys.write().map_err(|_| lock_err())?;
        keys.insert(key_id.into(), key.into());
        Ok(())
    }

    fn verification_key(&self, signature: &EnvelopeSignature) -> Result<Vec<u8>> {
        let keys = self.verification_keys.read().map_err(|_| lock_err())?;
        keys.get(&signature.key_id).cloned().ok_or_else(|| {
            StasisError::PortFailure(format!(
                "federated verification key not found: {}",
                signature.key_id
            ))
        })
    }

    fn verify_remote_job(&self, envelope: &RemoteJobEnvelope) -> Result<()> {
        envelope
            .validate_for_acceptance(chrono::Utc::now())
            .map_err(StasisError::PortFailure)?;
        let key = self.verification_key(&envelope.signature)?;
        verify_remote_job_envelope(envelope, &key).map_err(StasisError::PortFailure)
    }

    fn verify_signal(&self, envelope: &FederatedSignalEnvelope) -> Result<()> {
        let key = self.verification_key(&envelope.signature)?;
        verify_federated_signal(envelope, &key).map_err(StasisError::PortFailure)
    }

    fn verify_terminal_result(&self, result: &FederatedTerminalResult) -> Result<()> {
        let key = self.verification_key(&result.signature)?;
        verify_federated_terminal_result(result, &key).map_err(StasisError::PortFailure)
    }

    pub fn inbox(&self, runtime_id: &str) -> Result<FederatedInbox> {
        let inboxes = self.inboxes.read().map_err(|_| lock_err())?;
        Ok(inboxes.get(runtime_id).cloned().unwrap_or_default())
    }

    pub fn delivery_port(
        &self,
        destination_runtime_id: impl Into<String>,
    ) -> InMemoryFederatedDelivery {
        InMemoryFederatedDelivery {
            bus: self.clone(),
            destination_runtime_id: destination_runtime_id.into(),
        }
    }

    pub fn ingress_port(&self, local_runtime_id: impl Into<String>) -> InMemoryFederatedIngress {
        InMemoryFederatedIngress {
            bus: self.clone(),
            local_runtime_id: local_runtime_id.into(),
        }
    }
}

#[derive(Clone)]
pub struct InMemoryFederatedDelivery {
    bus: InMemoryFederatedBus,
    destination_runtime_id: String,
}

#[async_trait]
impl FederatedDeliveryPort for InMemoryFederatedDelivery {
    async fn submit_remote_job(&self, envelope: RemoteJobEnvelope) -> Result<()> {
        self.bus.verify_remote_job(&envelope)?;
        let mut inboxes = self.bus.inboxes.write().map_err(|_| lock_err())?;
        let inbox = inboxes
            .entry(self.destination_runtime_id.clone())
            .or_insert_with(FederatedInbox::default);
        if !inbox.remote_jobs.iter().any(|existing| {
            existing.envelope_id == envelope.envelope_id
                || existing.idempotency_key == envelope.idempotency_key
        }) {
            inbox.remote_jobs.push(envelope);
        }
        Ok(())
    }

    async fn deliver_signal(&self, envelope: FederatedSignalEnvelope) -> Result<()> {
        self.bus.verify_signal(&envelope)?;
        let destination = envelope.destination_runtime_id.clone();
        let mut inboxes = self.bus.inboxes.write().map_err(|_| lock_err())?;
        let inbox = inboxes
            .entry(destination)
            .or_insert_with(FederatedInbox::default);
        if !inbox
            .signals
            .iter()
            .any(|existing| existing.signal_id == envelope.signal_id)
        {
            inbox.signals.push(envelope);
        }
        Ok(())
    }

    async fn deliver_terminal_result(&self, result: FederatedTerminalResult) -> Result<()> {
        self.bus.verify_terminal_result(&result)?;
        let destination = result.origin_authority.runtime_id.clone();
        let mut inboxes = self.bus.inboxes.write().map_err(|_| lock_err())?;
        let inbox = inboxes
            .entry(destination)
            .or_insert_with(FederatedInbox::default);
        if !inbox
            .terminal_results
            .iter()
            .any(|existing| existing.result_id == result.result_id)
        {
            inbox.terminal_results.push(result);
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct InMemoryFederatedIngress {
    bus: InMemoryFederatedBus,
    local_runtime_id: String,
}

#[async_trait]
impl FederatedIngressPort for InMemoryFederatedIngress {
    async fn accept_remote_job(&self, envelope: RemoteJobEnvelope) -> Result<String> {
        self.bus.verify_remote_job(&envelope)?;
        let id = envelope.envelope_id.clone();
        let mut inboxes = self.bus.inboxes.write().map_err(|_| lock_err())?;
        let inbox = inboxes
            .entry(self.local_runtime_id.clone())
            .or_insert_with(FederatedInbox::default);
        if !inbox.remote_jobs.iter().any(|existing| {
            existing.envelope_id == envelope.envelope_id
                || existing.idempotency_key == envelope.idempotency_key
        }) {
            inbox.remote_jobs.push(envelope);
        }
        Ok(id)
    }

    async fn accept_signal(&self, envelope: FederatedSignalEnvelope) -> Result<()> {
        if envelope.destination_runtime_id != self.local_runtime_id {
            return Err(StasisError::PortFailure(format!(
                "signal destination {} does not match local runtime {}",
                envelope.destination_runtime_id, self.local_runtime_id
            )));
        }
        self.bus.verify_signal(&envelope)?;
        let mut inboxes = self.bus.inboxes.write().map_err(|_| lock_err())?;
        let inbox = inboxes
            .entry(self.local_runtime_id.clone())
            .or_insert_with(FederatedInbox::default);
        if !inbox
            .signals
            .iter()
            .any(|existing| existing.signal_id == envelope.signal_id)
        {
            inbox.signals.push(envelope);
        }
        Ok(())
    }

    async fn accept_terminal_result(&self, result: FederatedTerminalResult) -> Result<()> {
        if result.origin_authority.runtime_id != self.local_runtime_id {
            return Err(StasisError::PortFailure(format!(
                "terminal result origin {} does not match local runtime {}",
                result.origin_authority.runtime_id, self.local_runtime_id
            )));
        }
        self.bus.verify_terminal_result(&result)?;
        let mut inboxes = self.bus.inboxes.write().map_err(|_| lock_err())?;
        let inbox = inboxes
            .entry(self.local_runtime_id.clone())
            .or_insert_with(FederatedInbox::default);
        if !inbox
            .terminal_results
            .iter()
            .any(|existing| existing.result_id == result.result_id)
        {
            inbox.terminal_results.push(result);
        }
        Ok(())
    }
}
