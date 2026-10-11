use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::errors::Result;
use crate::domain::runtime::endpoint_delivery_status::EndpointDeliveryStatus;
use crate::ports::outbound::runtime::endpoint_delivery_status_store::EndpointDeliveryStatusStore;

use super::codec::{decode_time, encode_time};
use super::session::SqliteSession;

#[derive(Clone)]
pub struct SqliteEndpointDeliveryStatusStore {
    session: SqliteSession,
}

impl SqliteEndpointDeliveryStatusStore {
    pub fn new(session: SqliteSession) -> Self {
        Self { session }
    }

    fn map_status(row: &rusqlite::Row<'_>) -> rusqlite::Result<EndpointDeliveryStatus> {
        let conv = |err: crate::domain::errors::StasisError| {
            rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(err.to_string())))
        };
        let last_success: Option<i64> = row.get(5)?;
        let last_failure: Option<i64> = row.get(6)?;
        let updated_at: i64 = row.get(7)?;
        Ok(EndpointDeliveryStatus {
            endpoint_id: row.get(0)?,
            success_count: u64::try_from(row.get::<_, i64>(1)?).unwrap_or(0),
            failure_count: u64::try_from(row.get::<_, i64>(2)?).unwrap_or(0),
            last_event_id: row.get(3)?,
            last_error: row.get(4)?,
            last_success_at: last_success.map(decode_time).transpose().map_err(conv)?,
            last_failure_at: last_failure.map(decode_time).transpose().map_err(conv)?,
            updated_at: decode_time(updated_at).map_err(conv)?,
        })
    }

    fn save(&self, status: &EndpointDeliveryStatus) -> Result<()> {
        self.session.execute(
            "INSERT INTO endpoint_delivery_status (
                endpoint_id, success_count, failure_count, last_event_id, last_error,
                last_success_at, last_failure_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(endpoint_id) DO UPDATE SET
                success_count = excluded.success_count,
                failure_count = excluded.failure_count,
                last_event_id = excluded.last_event_id,
                last_error = excluded.last_error,
                last_success_at = excluded.last_success_at,
                last_failure_at = excluded.last_failure_at,
                updated_at = excluded.updated_at",
            rusqlite::params![
                status.endpoint_id,
                i64::try_from(status.success_count).unwrap_or(i64::MAX),
                i64::try_from(status.failure_count).unwrap_or(i64::MAX),
                status.last_event_id,
                status.last_error,
                status.last_success_at.map(encode_time),
                status.last_failure_at.map(encode_time),
                encode_time(status.updated_at),
            ],
        )?;
        Ok(())
    }
}

#[async_trait]
impl EndpointDeliveryStatusStore for SqliteEndpointDeliveryStatusStore {
    async fn record_success(&self, endpoint_id: &str, event_id: &str, at: DateTime<Utc>) -> Result<()> {
        let mut record = self
            .get(endpoint_id)
            .await?
            .unwrap_or_else(|| EndpointDeliveryStatus::new(endpoint_id, at));
        record.success_count = record.success_count.saturating_add(1);
        record.last_event_id = Some(event_id.to_string());
        record.last_error = None;
        record.last_success_at = Some(at);
        record.updated_at = at;
        self.save(&record)
    }

    async fn record_failure(
        &self,
        endpoint_id: &str,
        event_id: &str,
        error: &str,
        at: DateTime<Utc>,
    ) -> Result<()> {
        let mut record = self
            .get(endpoint_id)
            .await?
            .unwrap_or_else(|| EndpointDeliveryStatus::new(endpoint_id, at));
        record.failure_count = record.failure_count.saturating_add(1);
        record.last_event_id = Some(event_id.to_string());
        record.last_error = Some(error.to_string());
        record.last_failure_at = Some(at);
        record.updated_at = at;
        self.save(&record)
    }

    async fn get(&self, endpoint_id: &str) -> Result<Option<EndpointDeliveryStatus>> {
        self.session.query_opt(
            "SELECT endpoint_id, success_count, failure_count, last_event_id, last_error,
                    last_success_at, last_failure_at, updated_at
             FROM endpoint_delivery_status WHERE endpoint_id = ?1",
            rusqlite::params![endpoint_id],
            Self::map_status,
        )
    }

    async fn list(&self) -> Result<Vec<EndpointDeliveryStatus>> {
        self.session.query_map(
            "SELECT endpoint_id, success_count, failure_count, last_event_id, last_error,
                    last_success_at, last_failure_at, updated_at
             FROM endpoint_delivery_status ORDER BY endpoint_id ASC",
            [],
            Self::map_status,
        )
    }

    async fn prune_updated_before(&self, cutoff: DateTime<Utc>) -> Result<u64> {
        let deleted = self.session.execute(
            "DELETE FROM endpoint_delivery_status WHERE updated_at < ?1",
            rusqlite::params![encode_time(cutoff)],
        )?;
        Ok(deleted as u64)
    }
}
