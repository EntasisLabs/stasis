use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::{engine::any::Any, Surreal};
use surrealdb_types::SurrealValue;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::delivery_endpoint::{
    DeliveryEndpoint, DeliveryProtocol, NewDeliveryEndpoint,
};
use crate::ports::outbound::runtime::delivery_endpoint_store::DeliveryEndpointStore;

#[derive(Clone)]
pub struct SurrealDeliveryEndpointStore {
    db: Surreal<Any>,
    table: String,
}

impl SurrealDeliveryEndpointStore {
    pub fn new(db: Surreal<Any>) -> Self {
        Self {
            db,
            table: "delivery_endpoint".to_string(),
        }
    }

    fn port_err(prefix: &str, err: impl std::fmt::Display) -> StasisError {
        StasisError::PortFailure(format!("{prefix}: {err}"))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
struct DeliveryEndpointRecord {
    endpoint_id: String,
    name: String,
    protocol: String,
    target: String,
    metadata: Option<String>,
    enabled: bool,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl TryFrom<DeliveryEndpointRecord> for DeliveryEndpoint {
    type Error = StasisError;

    fn try_from(value: DeliveryEndpointRecord) -> std::result::Result<Self, Self::Error> {
        let protocol = match value.protocol.as_str() {
            "http_webhook" => DeliveryProtocol::HttpWebhook,
            "tcp" => DeliveryProtocol::Tcp,
            "kafka" => DeliveryProtocol::Kafka,
            "rabbitmq" => DeliveryProtocol::RabbitMq,
            other => {
                return Err(StasisError::PortFailure(format!(
                    "invalid delivery protocol: {other}"
                )));
            }
        };

        Ok(Self {
            endpoint_id: value.endpoint_id,
            name: value.name,
            protocol,
            target: value.target,
            metadata: value.metadata,
            enabled: value.enabled,
            created_at: value.created_at,
            updated_at: value.updated_at,
        })
    }
}

impl From<DeliveryEndpoint> for DeliveryEndpointRecord {
    fn from(value: DeliveryEndpoint) -> Self {
        let protocol = match value.protocol {
            DeliveryProtocol::HttpWebhook => "http_webhook".to_string(),
            DeliveryProtocol::Tcp => "tcp".to_string(),
            DeliveryProtocol::Kafka => "kafka".to_string(),
            DeliveryProtocol::RabbitMq => "rabbitmq".to_string(),
        };

        Self {
            endpoint_id: value.endpoint_id,
            name: value.name,
            protocol,
            target: value.target,
            metadata: value.metadata,
            enabled: value.enabled,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}

impl From<NewDeliveryEndpoint> for DeliveryEndpointRecord {
    fn from(value: NewDeliveryEndpoint) -> Self {
        DeliveryEndpointRecord::from(value.into_record())
    }
}

#[async_trait]
impl DeliveryEndpointStore for SurrealDeliveryEndpointStore {
    async fn insert(&self, endpoint: NewDeliveryEndpoint) -> Result<DeliveryEndpoint> {
        let record: DeliveryEndpointRecord = endpoint.into();

        self.db
            .query("CREATE type::record($table, $id) CONTENT $data")
            .bind(("table", self.table.clone()))
            .bind(("id", record.endpoint_id.clone()))
            .bind(("data", record.clone()))
            .await
            .map_err(|e| Self::port_err("insert delivery endpoint", e))?;

        DeliveryEndpoint::try_from(record)
    }

    async fn get(&self, endpoint_id: &str) -> Result<Option<DeliveryEndpoint>> {
        let mut response = self
            .db
            .query("SELECT * FROM type::record($table, $id)")
            .bind(("table", self.table.clone()))
            .bind(("id", endpoint_id.to_string()))
            .await
            .map_err(|e| Self::port_err("load delivery endpoint", e))?;

        let row: Option<DeliveryEndpointRecord> = response
            .take(0)
            .map_err(|e| Self::port_err("decode delivery endpoint", e))?;

        row.map(DeliveryEndpoint::try_from).transpose()
    }

    async fn list(&self) -> Result<Vec<DeliveryEndpoint>> {
        let mut response = self
            .db
            .query("SELECT * FROM type::table($table)")
            .bind(("table", self.table.clone()))
            .await
            .map_err(|e| Self::port_err("list delivery endpoints", e))?;

        let rows: Vec<DeliveryEndpointRecord> = response
            .take(0)
            .map_err(|e| Self::port_err("decode delivery endpoints", e))?;

        let mut endpoints = Vec::with_capacity(rows.len());
        for row in rows {
            endpoints.push(DeliveryEndpoint::try_from(row)?);
        }

        endpoints.sort_by(|left, right| left.endpoint_id.cmp(&right.endpoint_id));
        Ok(endpoints)
    }

    async fn set_enabled(&self, endpoint_id: &str, enabled: bool) -> Result<bool> {
        let now = Utc::now();
        let mut response = self
            .db
            .query(
                "UPDATE type::record($table, $id) \
                 SET enabled = $enabled, updated_at = $updated_at \
                 RETURN AFTER",
            )
            .bind(("table", self.table.clone()))
            .bind(("id", endpoint_id.to_string()))
            .bind(("enabled", enabled))
            .bind(("updated_at", now))
            .await
            .map_err(|e| Self::port_err("set delivery endpoint enabled", e))?;

        let updated: Option<DeliveryEndpointRecord> = response
            .take(0)
            .map_err(|e| Self::port_err("decode updated delivery endpoint", e))?;

        Ok(updated.is_some())
    }
}
