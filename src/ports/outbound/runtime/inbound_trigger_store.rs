use crate::domain::errors::Result;
use crate::domain::runtime::inbound_trigger::InboundTriggerReceipt;
use crate::ports::outbound::runtime::store_bounds::StoreBound;

pub enum ReceiptInsert {
    Created,
    Exists(InboundTriggerReceipt),
}

crate::async_store! {
pub trait InboundTriggerStore: StoreBound {
    async fn insert_if_absent(&self, receipt: InboundTriggerReceipt) -> Result<ReceiptInsert>;
    async fn delete(&self, idempotency_key: &str) -> Result<()>;
}
}
