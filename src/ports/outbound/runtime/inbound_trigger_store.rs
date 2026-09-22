use async_trait::async_trait;

use crate::domain::errors::Result;
use crate::domain::runtime::inbound_trigger::InboundTriggerReceipt;

pub enum ReceiptInsert {
    Created,
    Exists(InboundTriggerReceipt),
}

#[async_trait]
pub trait InboundTriggerStore: Send + Sync {
    async fn insert_if_absent(&self, receipt: InboundTriggerReceipt) -> Result<ReceiptInsert>;
    async fn delete(&self, idempotency_key: &str) -> Result<()>;
}
