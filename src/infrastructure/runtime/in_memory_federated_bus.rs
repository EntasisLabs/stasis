use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::federation::{FederatedSignalEnvelope, FederatedTerminalResult};
use crate::domain::runtime::remote_job_envelope::RemoteJobEnvelope;
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

    pub fn inbox(&self, runtime_id: &str) -> Result<FederatedInbox> {
        let inboxes = self.inboxes.read().map_err(|_| lock_err())?;
        Ok(inboxes.get(runtime_id).cloned().unwrap_or_default())
    }

    pub fn delivery_port(&self, destination_runtime_id: impl Into<String>) -> InMemoryFederatedDelivery {
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
        envelope
            .validate_schema_version()
            .map_err(StasisError::PortFailure)?;
        let mut inboxes = self.bus.inboxes.write().map_err(|_| lock_err())?;
        let inbox = inboxes
            .entry(self.destination_runtime_id.clone())
            .or_insert_with(FederatedInbox::default);
        inbox.remote_jobs.push(envelope);
        Ok(())
    }

    async fn deliver_signal(&self, envelope: FederatedSignalEnvelope) -> Result<()> {
        envelope
            .validate_schema_version()
            .map_err(StasisError::PortFailure)?;
        let destination = envelope.destination_runtime_id.clone();
        let mut inboxes = self.bus.inboxes.write().map_err(|_| lock_err())?;
        let inbox = inboxes
            .entry(destination)
            .or_insert_with(FederatedInbox::default);
        inbox.signals.push(envelope);
        Ok(())
    }

    async fn deliver_terminal_result(&self, result: FederatedTerminalResult) -> Result<()> {
        result
            .validate_schema_version()
            .map_err(StasisError::PortFailure)?;
        let destination = result.origin_authority.runtime_id.clone();
        let mut inboxes = self.bus.inboxes.write().map_err(|_| lock_err())?;
        let inbox = inboxes
            .entry(destination)
            .or_insert_with(FederatedInbox::default);
        inbox.terminal_results.push(result);
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
        envelope
            .validate_schema_version()
            .map_err(StasisError::PortFailure)?;
        let id = envelope.envelope_id.clone();
        let mut inboxes = self.bus.inboxes.write().map_err(|_| lock_err())?;
        let inbox = inboxes
            .entry(self.local_runtime_id.clone())
            .or_insert_with(FederatedInbox::default);
        inbox.remote_jobs.push(envelope);
        Ok(id)
    }

    async fn accept_signal(&self, envelope: FederatedSignalEnvelope) -> Result<()> {
        if envelope.destination_runtime_id != self.local_runtime_id {
            return Err(StasisError::PortFailure(format!(
                "signal destination {} does not match local runtime {}",
                envelope.destination_runtime_id, self.local_runtime_id
            )));
        }
        envelope
            .validate_schema_version()
            .map_err(StasisError::PortFailure)?;
        let mut inboxes = self.bus.inboxes.write().map_err(|_| lock_err())?;
        let inbox = inboxes
            .entry(self.local_runtime_id.clone())
            .or_insert_with(FederatedInbox::default);
        inbox.signals.push(envelope);
        Ok(())
    }

    async fn accept_terminal_result(&self, result: FederatedTerminalResult) -> Result<()> {
        if result.origin_authority.runtime_id != self.local_runtime_id {
            return Err(StasisError::PortFailure(format!(
                "terminal result origin {} does not match local runtime {}",
                result.origin_authority.runtime_id, self.local_runtime_id
            )));
        }
        result
            .validate_schema_version()
            .map_err(StasisError::PortFailure)?;
        let mut inboxes = self.bus.inboxes.write().map_err(|_| lock_err())?;
        let inbox = inboxes
            .entry(self.local_runtime_id.clone())
            .or_insert_with(FederatedInbox::default);
        inbox.terminal_results.push(result);
        Ok(())
    }
}
