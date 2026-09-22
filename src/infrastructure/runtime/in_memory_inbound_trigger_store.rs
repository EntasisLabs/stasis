use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::inbound_trigger::InboundTriggerReceipt;
use crate::ports::outbound::runtime::inbound_trigger_store::{InboundTriggerStore, ReceiptInsert};

#[derive(Clone, Default)]
pub struct InMemoryInboundTriggerStore {
    receipts: Arc<RwLock<HashMap<String, InboundTriggerReceipt>>>,
}

fn lock_err() -> StasisError {
    StasisError::PortFailure("inbound trigger store lock poisoned".into())
}

#[async_trait]
impl InboundTriggerStore for InMemoryInboundTriggerStore {
    async fn insert_if_absent(&self, receipt: InboundTriggerReceipt) -> Result<ReceiptInsert> {
        let mut receipts = self.receipts.write().map_err(|_| lock_err())?;
        if let Some(existing) = receipts.get(&receipt.idempotency_key) {
            return Ok(ReceiptInsert::Exists(existing.clone()));
        }
        receipts.insert(receipt.idempotency_key.clone(), receipt);
        Ok(ReceiptInsert::Created)
    }

    async fn delete(&self, idempotency_key: &str) -> Result<()> {
        let mut receipts = self.receipts.write().map_err(|_| lock_err())?;
        receipts.remove(idempotency_key);
        Ok(())
    }
}
