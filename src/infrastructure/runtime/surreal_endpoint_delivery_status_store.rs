use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::{engine::any::Any, Surreal};
use surrealdb_types::SurrealValue;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::endpoint_delivery_status::EndpointDeliveryStatus;
use crate::ports::outbound::runtime::endpoint_delivery_status_store::EndpointDeliveryStatusStore;

#[derive(Clone)]
pub struct SurrealEndpointDeliveryStatusStore {
    db: Surreal<Any>,
    table: String,
}

impl SurrealEndpointDeliveryStatusStore {
    pub fn new(db: Surreal<Any>) -> Self {
        Self {
            db,
            table: "endpoint_delivery_status".to_string(),
        }
    }

    fn port_err(prefix: &str, err: impl std::fmt::Display) -> StasisError {
        StasisError::PortFailure(format!("{prefix}: {err}"))
    }

    async fn load_record(&self, endpoint_id: &str) -> Result<Option<EndpointDeliveryStatusRecord>> {
        let mut response = match self
            .db
            .query("SELECT * FROM type::record($table, $id)")
            .bind(("table", self.table.clone()))
            .bind(("id", endpoint_id.to_string()))
            .await
        {
            Ok(response) => response,
            Err(err) => {
                let message = err.to_string();
                if message.contains("does not exist") && message.contains(&self.table) {
                    return Ok(None);
                }
                return Err(Self::port_err("load endpoint delivery status", err));
            }
        };

        let row: Option<EndpointDeliveryStatusRecord> = match response.take(0) {
            Ok(row) => row,
            Err(err) => {
                let message = err.to_string();
                if message.contains("does not exist") && message.contains(&self.table) {
                    return Ok(None);
                }
                return Err(Self::port_err("decode endpoint delivery status", err));
            }
        };
        Ok(row)
    }

    async fn save_record(&self, record: EndpointDeliveryStatusRecord) -> Result<()> {
        self.db
            .query("UPDATE type::record($table, $id) CONTENT $data")
            .bind(("table", self.table.clone()))
            .bind(("id", record.endpoint_id.clone()))
            .bind(("data", record.clone()))
            .await
            .map_err(|e| Self::port_err("save endpoint delivery status", e))?;

        if self.load_record(&record.endpoint_id).await?.is_none() {
            let endpoint_id = record.endpoint_id.clone();
            self.db
                .query("CREATE type::record($table, $id) CONTENT $data")
                .bind(("table", self.table.clone()))
                .bind(("id", endpoint_id))
                .bind(("data", record))
                .await
                .map_err(|e| Self::port_err("create endpoint delivery status", e))?;
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, SurrealValue)]
struct EndpointDeliveryStatusRecord {
    endpoint_id: String,
    success_count: u64,
    failure_count: u64,
    last_event_id: Option<String>,
    last_error: Option<String>,
    last_success_at: Option<DateTime<Utc>>,
    last_failure_at: Option<DateTime<Utc>>,
    updated_at: DateTime<Utc>,
}

impl From<EndpointDeliveryStatusRecord> for EndpointDeliveryStatus {
    fn from(value: EndpointDeliveryStatusRecord) -> Self {
        Self {
            endpoint_id: value.endpoint_id,
            success_count: value.success_count,
            failure_count: value.failure_count,
            last_event_id: value.last_event_id,
            last_error: value.last_error,
            last_success_at: value.last_success_at,
            last_failure_at: value.last_failure_at,
            updated_at: value.updated_at,
        }
    }
}

impl From<EndpointDeliveryStatus> for EndpointDeliveryStatusRecord {
    fn from(value: EndpointDeliveryStatus) -> Self {
        Self {
            endpoint_id: value.endpoint_id,
            success_count: value.success_count,
            failure_count: value.failure_count,
            last_event_id: value.last_event_id,
            last_error: value.last_error,
            last_success_at: value.last_success_at,
            last_failure_at: value.last_failure_at,
            updated_at: value.updated_at,
        }
    }
}

#[async_trait]
impl EndpointDeliveryStatusStore for SurrealEndpointDeliveryStatusStore {
    async fn record_success(
        &self,
        endpoint_id: &str,
        event_id: &str,
        at: DateTime<Utc>,
    ) -> Result<()> {
        let mut record = self.load_record(endpoint_id).await?.unwrap_or_else(|| {
            EndpointDeliveryStatusRecord::from(EndpointDeliveryStatus::new(endpoint_id, at))
        });

        record.success_count = record.success_count.saturating_add(1);
        record.last_event_id = Some(event_id.to_string());
        record.last_error = None;
        record.last_success_at = Some(at);
        record.updated_at = at;

        self.save_record(record).await
    }

    async fn record_failure(
        &self,
        endpoint_id: &str,
        event_id: &str,
        error: &str,
        at: DateTime<Utc>,
    ) -> Result<()> {
        let mut record = self.load_record(endpoint_id).await?.unwrap_or_else(|| {
            EndpointDeliveryStatusRecord::from(EndpointDeliveryStatus::new(endpoint_id, at))
        });

        record.failure_count = record.failure_count.saturating_add(1);
        record.last_event_id = Some(event_id.to_string());
        record.last_error = Some(error.to_string());
        record.last_failure_at = Some(at);
        record.updated_at = at;

        self.save_record(record).await
    }

    async fn get(&self, endpoint_id: &str) -> Result<Option<EndpointDeliveryStatus>> {
        Ok(self
            .load_record(endpoint_id)
            .await?
            .map(EndpointDeliveryStatus::from))
    }

    async fn list(&self) -> Result<Vec<EndpointDeliveryStatus>> {
        let mut response = match self
            .db
            .query("SELECT * FROM type::table($table)")
            .bind(("table", self.table.clone()))
            .await
        {
            Ok(response) => response,
            Err(err) => {
                let message = err.to_string();
                if message.contains("does not exist") && message.contains(&self.table) {
                    return Ok(Vec::new());
                }
                return Err(Self::port_err("list endpoint delivery statuses", err));
            }
        };

        let rows: Vec<EndpointDeliveryStatusRecord> = match response.take(0) {
            Ok(rows) => rows,
            Err(err) => {
                let message = err.to_string();
                if message.contains("does not exist") && message.contains(&self.table) {
                    return Ok(Vec::new());
                }
                return Err(Self::port_err("decode endpoint delivery statuses", err));
            }
        };

        let mut statuses = rows
            .into_iter()
            .map(EndpointDeliveryStatus::from)
            .collect::<Vec<_>>();
        statuses.sort_by(|left, right| left.endpoint_id.cmp(&right.endpoint_id));
        Ok(statuses)
    }

    async fn prune_updated_before(&self, cutoff: DateTime<Utc>) -> Result<u64> {
        let mut response = match self
            .db
            .query("DELETE type::table($table) WHERE updated_at < $cutoff RETURN BEFORE")
            .bind(("table", self.table.clone()))
            .bind(("cutoff", cutoff))
            .await
        {
            Ok(response) => response,
            Err(err) => {
                let message = err.to_string();
                if message.contains("does not exist") && message.contains(&self.table) {
                    return Ok(0);
                }
                return Err(Self::port_err("prune endpoint delivery statuses", err));
            }
        };

        let deleted: Vec<EndpointDeliveryStatusRecord> = match response.take(0) {
            Ok(rows) => rows,
            Err(err) => {
                let message = err.to_string();
                if message.contains("does not exist") && message.contains(&self.table) {
                    return Ok(0);
                }
                return Err(Self::port_err(
                    "decode pruned endpoint delivery statuses",
                    err,
                ));
            }
        };

        Ok(deleted.len() as u64)
    }
}
