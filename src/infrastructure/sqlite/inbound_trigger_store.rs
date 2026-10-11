use async_trait::async_trait;

use crate::domain::errors::Result;
use crate::domain::runtime::inbound_trigger::InboundTriggerReceipt;
use crate::ports::outbound::runtime::inbound_trigger_store::{InboundTriggerStore, ReceiptInsert};

use super::codec::{decode_time, encode_time};
use super::session::SqliteSession;

#[derive(Clone)]
pub struct SqliteInboundTriggerStore {
    session: SqliteSession,
}

impl SqliteInboundTriggerStore {
    pub fn new(session: SqliteSession) -> Self {
        Self { session }
    }
}

fn map_receipt(row: &rusqlite::Row<'_>) -> rusqlite::Result<InboundTriggerReceipt> {
    let accepted_at: i64 = row.get(4)?;
    Ok(InboundTriggerReceipt {
        idempotency_key: row.get(0)?,
        protocol: row.get(1)?,
        job_id: row.get(2)?,
        job_type: row.get(3)?,
        accepted_at: decode_time(accepted_at).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                4,
                rusqlite::types::Type::Integer,
                Box::new(std::io::Error::other(err.to_string())),
            )
        })?,
    })
}

#[async_trait]
impl InboundTriggerStore for SqliteInboundTriggerStore {
    async fn insert_if_absent(&self, receipt: InboundTriggerReceipt) -> Result<ReceiptInsert> {
        let changed = self.session.execute(
            "INSERT INTO inbound_trigger (idempotency_key, protocol, job_id, job_type, accepted_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(idempotency_key) DO NOTHING",
            rusqlite::params![
                receipt.idempotency_key,
                receipt.protocol,
                receipt.job_id,
                receipt.job_type,
                encode_time(receipt.accepted_at),
            ],
        )?;
        if changed == 1 {
            return Ok(ReceiptInsert::Created);
        }
        let existing = self
            .session
            .query_opt(
                "SELECT idempotency_key, protocol, job_id, job_type, accepted_at
                 FROM inbound_trigger WHERE idempotency_key = ?1",
                [&receipt.idempotency_key],
                map_receipt,
            )?
            .unwrap_or(receipt);
        Ok(ReceiptInsert::Exists(existing))
    }

    async fn delete(&self, idempotency_key: &str) -> Result<()> {
        self.session.execute(
            "DELETE FROM inbound_trigger WHERE idempotency_key = ?1",
            [idempotency_key],
        )?;
        Ok(())
    }
}
