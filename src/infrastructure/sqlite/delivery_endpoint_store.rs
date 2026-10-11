use async_trait::async_trait;
use chrono::Utc;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::delivery_endpoint::{
    DeliveryEndpoint, DeliveryProtocol, NewDeliveryEndpoint,
};
use crate::ports::outbound::runtime::delivery_endpoint_store::DeliveryEndpointStore;

use super::codec::{decode_time, encode_time, sql_err};
use super::session::SqliteSession;

#[derive(Clone)]
pub struct SqliteDeliveryEndpointStore {
    session: SqliteSession,
}

impl SqliteDeliveryEndpointStore {
    pub fn new(session: SqliteSession) -> Self {
        Self { session }
    }

    fn protocol_str(protocol: DeliveryProtocol) -> &'static str {
        match protocol {
            DeliveryProtocol::HttpWebhook => "http_webhook",
            DeliveryProtocol::Tcp => "tcp",
            DeliveryProtocol::Kafka => "kafka",
            DeliveryProtocol::RabbitMq => "rabbitmq",
        }
    }

    fn parse_protocol(raw: &str) -> Result<DeliveryProtocol> {
        match raw {
            "http_webhook" => Ok(DeliveryProtocol::HttpWebhook),
            "tcp" => Ok(DeliveryProtocol::Tcp),
            "kafka" => Ok(DeliveryProtocol::Kafka),
            "rabbitmq" => Ok(DeliveryProtocol::RabbitMq),
            other => Err(sql_err("decode delivery protocol", other)),
        }
    }

    fn map_endpoint(row: &rusqlite::Row<'_>) -> rusqlite::Result<DeliveryEndpoint> {
        let protocol_raw: String = row.get(2)?;
        let enabled: i64 = row.get(5)?;
        let created_at: i64 = row.get(6)?;
        let updated_at: i64 = row.get(7)?;
        let conv = |err: StasisError| {
            rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(err.to_string())))
        };
        Ok(DeliveryEndpoint {
            endpoint_id: row.get(0)?,
            name: row.get(1)?,
            protocol: Self::parse_protocol(&protocol_raw).map_err(conv)?,
            target: row.get(3)?,
            metadata: row.get(4)?,
            enabled: enabled != 0,
            created_at: decode_time(created_at).map_err(conv)?,
            updated_at: decode_time(updated_at).map_err(conv)?,
        })
    }

    fn insert_record(&self, endpoint: &DeliveryEndpoint) -> Result<()> {
        self.session.execute(
            "INSERT INTO delivery_endpoint (
                endpoint_id, name, protocol, target, metadata, enabled, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![
                endpoint.endpoint_id,
                endpoint.name,
                Self::protocol_str(endpoint.protocol.clone()),
                endpoint.target,
                endpoint.metadata,
                i64::from(endpoint.enabled),
                encode_time(endpoint.created_at),
                encode_time(endpoint.updated_at),
            ],
        )?;
        Ok(())
    }
}

#[async_trait]
impl DeliveryEndpointStore for SqliteDeliveryEndpointStore {
    async fn insert(&self, endpoint: NewDeliveryEndpoint) -> Result<DeliveryEndpoint> {
        let record = endpoint.into_record();
        self.insert_record(&record)?;
        Ok(record)
    }

    async fn get(&self, endpoint_id: &str) -> Result<Option<DeliveryEndpoint>> {
        self.session.query_opt(
            "SELECT endpoint_id, name, protocol, target, metadata, enabled, created_at, updated_at
             FROM delivery_endpoint WHERE endpoint_id = ?1",
            rusqlite::params![endpoint_id],
            Self::map_endpoint,
        )
    }

    async fn list(&self) -> Result<Vec<DeliveryEndpoint>> {
        self.session.query_map(
            "SELECT endpoint_id, name, protocol, target, metadata, enabled, created_at, updated_at
             FROM delivery_endpoint ORDER BY endpoint_id ASC",
            [],
            Self::map_endpoint,
        )
    }

    async fn set_enabled(&self, endpoint_id: &str, enabled: bool) -> Result<bool> {
        Ok(self.session.execute(
            "UPDATE delivery_endpoint SET enabled = ?1, updated_at = ?2 WHERE endpoint_id = ?3",
            rusqlite::params![i64::from(enabled), encode_time(Utc::now()), endpoint_id],
        )? > 0)
    }

    async fn upsert(&self, endpoint: NewDeliveryEndpoint) -> Result<DeliveryEndpoint> {
        if let Some(existing) = self.get(&endpoint.endpoint_id).await? {
            let mut record = endpoint.into_record();
            record.created_at = existing.created_at;
            record.updated_at = Utc::now();
            self.session.execute(
                "UPDATE delivery_endpoint SET
                    name = ?1, protocol = ?2, target = ?3, metadata = ?4, enabled = ?5,
                    created_at = ?6, updated_at = ?7
                 WHERE endpoint_id = ?8",
                rusqlite::params![
                    record.name,
                    Self::protocol_str(record.protocol.clone()),
                    record.target,
                    record.metadata,
                    i64::from(record.enabled),
                    encode_time(record.created_at),
                    encode_time(record.updated_at),
                    record.endpoint_id,
                ],
            )?;
            return Ok(record);
        }
        self.insert(endpoint).await
    }
}
