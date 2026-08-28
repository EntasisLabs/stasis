use async_trait::async_trait;

use crate::domain::errors::Result;
use crate::domain::runtime::federation::{FederatedSignalEnvelope, FederatedTerminalResult};
use crate::domain::runtime::remote_job_envelope::RemoteJobEnvelope;

/// Cross-runtime delivery without requiring a shared database.
///
/// Independent Stasis runtimes exchange signed envelopes over this port (HTTP, bus, etc.).
#[async_trait]
pub trait FederatedDeliveryPort: Send + Sync {
    async fn submit_remote_job(&self, envelope: RemoteJobEnvelope) -> Result<()>;
    async fn deliver_signal(&self, envelope: FederatedSignalEnvelope) -> Result<()>;
    async fn deliver_terminal_result(&self, result: FederatedTerminalResult) -> Result<()>;
}

/// Accepts inbound federated envelopes into a local runtime.
#[async_trait]
pub trait FederatedIngressPort: Send + Sync {
    async fn accept_remote_job(&self, envelope: RemoteJobEnvelope) -> Result<String>;
    async fn accept_signal(&self, envelope: FederatedSignalEnvelope) -> Result<()>;
    async fn accept_terminal_result(&self, result: FederatedTerminalResult) -> Result<()>;
}
