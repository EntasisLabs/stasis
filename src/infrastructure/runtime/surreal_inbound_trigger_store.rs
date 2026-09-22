use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::{Surreal, engine::any::Any};
use surrealdb_types::SurrealValue;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::inbound_trigger::InboundTriggerReceipt;
use crate::ports::outbound::runtime::inbound_trigger_store::{InboundTriggerStore, ReceiptInsert};

#[derive(Clone)]
pub struct SurrealInboundTriggerStore {
    db: Surreal<Any>,
    table: String,
}

impl SurrealInboundTriggerStore {
    pub fn new(db: Surreal<Any>) -> Self {
        Self {
            db,
            table: "inbound_trigger".to_string(),
        }
    }

    fn port_err(prefix: &str, err: impl std::fmt::Display) -> StasisError {
        StasisError::PortFailure(format!("{prefix}: {err}"))
    }

    fn already_exists(err: &impl std::fmt::Display) -> bool {
        let message = err.to_string().to_ascii_lowercase();
        message.contains("already") && message.contains("exist")
    }

    fn missing_table(err: &impl std::fmt::Display) -> bool {
        let message = err.to_string();
        message.contains("does not exist") && message.contains("inbound_trigger")
    }
}

pub(crate) fn receipt_record_id(idempotency_key: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut id = String::from("k");
    for byte in idempotency_key.as_bytes() {
        id.push(HEX[(byte >> 4) as usize] as char);
        id.push(HEX[(byte & 0x0f) as usize] as char);
    }
    id
}

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
struct ReceiptRow {
    idempotency_key: String,
    protocol: String,
    job_id: String,
    job_type: String,
    accepted_at: DateTime<Utc>,
}

impl From<InboundTriggerReceipt> for ReceiptRow {
    fn from(value: InboundTriggerReceipt) -> Self {
        Self {
            idempotency_key: value.idempotency_key,
            protocol: value.protocol,
            job_id: value.job_id,
            job_type: value.job_type,
            accepted_at: value.accepted_at,
        }
    }
}

impl From<ReceiptRow> for InboundTriggerReceipt {
    fn from(value: ReceiptRow) -> Self {
        Self {
            idempotency_key: value.idempotency_key,
            protocol: value.protocol,
            job_id: value.job_id,
            job_type: value.job_type,
            accepted_at: value.accepted_at,
        }
    }
}

#[async_trait]
impl InboundTriggerStore for SurrealInboundTriggerStore {
    async fn insert_if_absent(&self, receipt: InboundTriggerReceipt) -> Result<ReceiptInsert> {
        let id = receipt_record_id(&receipt.idempotency_key);
        if let Some(existing) = self.get_by_id(&id).await? {
            return Ok(ReceiptInsert::Exists(existing));
        }
        let row = ReceiptRow::from(receipt);
        let mut response = self
            .db
            .query("CREATE type::record($table, $id) CONTENT $data")
            .bind(("table", self.table.clone()))
            .bind(("id", id.clone()))
            .bind(("data", row))
            .await
            .map_err(|err| {
                if Self::already_exists(&err) {
                    Self::port_err("insert inbound trigger raced", err)
                } else {
                    Self::port_err("insert inbound trigger", err)
                }
            })?;
        match response.take::<Vec<ReceiptRow>>(0) {
            Ok(_) => Ok(ReceiptInsert::Created),
            Err(err) if Self::already_exists(&err) => {
                let existing = self.get_by_id(&id).await?;
                let existing = existing.ok_or_else(|| {
                    Self::port_err("inbound trigger exists but could not be read", &err)
                })?;
                Ok(ReceiptInsert::Exists(existing))
            }
            Err(err) => Err(Self::port_err("insert inbound trigger", err)),
        }
    }

    async fn delete(&self, idempotency_key: &str) -> Result<()> {
        let id = receipt_record_id(idempotency_key);
        self.db
            .query("DELETE type::record($table, $id)")
            .bind(("table", self.table.clone()))
            .bind(("id", id))
            .await
            .map_err(|err| Self::port_err("delete inbound trigger", err))?;
        Ok(())
    }
}

impl SurrealInboundTriggerStore {
    async fn get_by_id(&self, id: &str) -> Result<Option<InboundTriggerReceipt>> {
        let mut response = match self
            .db
            .query("SELECT * FROM type::record($table, $id)")
            .bind(("table", self.table.clone()))
            .bind(("id", id.to_string()))
            .await
        {
            Ok(response) => response,
            Err(err) if Self::missing_table(&err) => return Ok(None),
            Err(err) => return Err(Self::port_err("get inbound trigger", err)),
        };
        let rows: Vec<ReceiptRow> = match response.take(0) {
            Ok(rows) => rows,
            Err(err) if Self::missing_table(&err) => return Ok(None),
            Err(err) => return Err(Self::port_err("decode inbound trigger", err)),
        };
        Ok(rows.into_iter().next().map(InboundTriggerReceipt::from))
    }
}
