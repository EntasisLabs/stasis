use std::sync::Arc;

use crate::domain::errors::Result;
use crate::domain::runtime::inbound_trigger::InboundTriggerReceipt;
use crate::ports::outbound::runtime::inbound_trigger_store::{InboundTriggerStore, ReceiptInsert};

use super::codec::{decode_time, encode_time};
use super::executor::{SqlExecutor, SqlRow, SqlValue, query_one};

fn map_receipt(row: &SqlRow) -> Result<InboundTriggerReceipt> {
    Ok(InboundTriggerReceipt {
        idempotency_key: row.text(0)?,
        protocol: row.text(1)?,
        job_id: row.text(2)?,
        job_type: row.text(3)?,
        accepted_at: decode_time(row.i64(4)?)?,
    })
}

#[derive(Clone)]
pub struct ExecutorInboundTriggerStore {
    executor: Arc<dyn SqlExecutor>,
}

impl ExecutorInboundTriggerStore {
    pub fn new(executor: Arc<dyn SqlExecutor>) -> Self {
        Self { executor }
    }
}

crate::async_store! {
impl InboundTriggerStore for ExecutorInboundTriggerStore {
    async fn insert_if_absent(&self, receipt: InboundTriggerReceipt) -> Result<ReceiptInsert> {
        let inserted = query_one(
            self.executor.as_ref(),
            "INSERT INTO inbound_trigger (idempotency_key, protocol, job_id, job_type, accepted_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(idempotency_key) DO NOTHING
             RETURNING idempotency_key",
            &[
                SqlValue::from(receipt.idempotency_key.clone()),
                SqlValue::from(receipt.protocol.clone()),
                SqlValue::from(receipt.job_id.clone()),
                SqlValue::from(receipt.job_type.clone()),
                SqlValue::from(encode_time(receipt.accepted_at)),
            ],
        )?;
        if inserted.is_some() {
            return Ok(ReceiptInsert::Created);
        }
        let existing = query_one(
            self.executor.as_ref(),
            "SELECT idempotency_key, protocol, job_id, job_type, CAST(accepted_at AS TEXT)
             FROM inbound_trigger WHERE idempotency_key = ?1",
            &[SqlValue::text(&receipt.idempotency_key)],
        )?;
        let Some(row) = existing else {
            return Ok(ReceiptInsert::Exists(receipt));
        };
        Ok(ReceiptInsert::Exists(map_receipt(&row)?))
    }

    async fn delete(&self, idempotency_key: &str) -> Result<()> {
        self.executor.execute(
            "DELETE FROM inbound_trigger WHERE idempotency_key = ?1",
            &[SqlValue::text(idempotency_key)],
        )?;
        Ok(())
    }
}
}
