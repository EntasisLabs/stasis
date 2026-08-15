use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::{Surreal, engine::any::Any};
use surrealdb_types::SurrealValue;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::durable_wait::{
    DurableSignalRecord, DurableWaitRecord, DurableWaitStatus,
};
use crate::ports::outbound::runtime::durable_wait_store::DurableWaitStore;

#[derive(Clone)]
pub struct SurrealDurableWaitStore {
    db: Surreal<Any>,
    wait_table: String,
    signal_table: String,
}

impl SurrealDurableWaitStore {
    pub fn new(db: Surreal<Any>) -> Self {
        Self {
            db,
            wait_table: "durable_wait".to_string(),
            signal_table: "durable_signal".to_string(),
        }
    }

    fn port_err(prefix: &str, err: impl std::fmt::Display) -> StasisError {
        StasisError::PortFailure(format!("{prefix}: {err}"))
    }

    fn missing_table(err: &impl std::fmt::Display, table: &str) -> bool {
        let message = err.to_string();
        message.contains("does not exist") && message.contains(table)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
struct WaitRow {
    wait_id: String,
    job_id: String,
    signal_type: String,
    correlation_key: String,
    status: String,
    deadline_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    signal_payload: Option<String>,
    consumed_signal_ids: Vec<String>,
}

impl From<DurableWaitRecord> for WaitRow {
    fn from(value: DurableWaitRecord) -> Self {
        Self {
            wait_id: value.wait_id,
            job_id: value.job_id,
            signal_type: value.signal_type,
            correlation_key: value.correlation_key,
            status: match value.status {
                DurableWaitStatus::Pending => "pending".into(),
                DurableWaitStatus::Signaled => "signaled".into(),
                DurableWaitStatus::TimedOut => "timed_out".into(),
                DurableWaitStatus::Cancelled => "cancelled".into(),
            },
            deadline_at: value.deadline_at,
            created_at: value.created_at,
            updated_at: value.updated_at,
            signal_payload: value.signal_payload,
            consumed_signal_ids: value.consumed_signal_ids,
        }
    }
}

impl TryFrom<WaitRow> for DurableWaitRecord {
    type Error = StasisError;

    fn try_from(value: WaitRow) -> std::result::Result<Self, Self::Error> {
        let status = match value.status.as_str() {
            "pending" => DurableWaitStatus::Pending,
            "signaled" => DurableWaitStatus::Signaled,
            "timed_out" => DurableWaitStatus::TimedOut,
            "cancelled" => DurableWaitStatus::Cancelled,
            other => {
                return Err(StasisError::PortFailure(format!(
                    "invalid durable wait status: {other}"
                )));
            }
        };
        Ok(Self {
            wait_id: value.wait_id,
            job_id: value.job_id,
            signal_type: value.signal_type,
            correlation_key: value.correlation_key,
            status,
            deadline_at: value.deadline_at,
            created_at: value.created_at,
            updated_at: value.updated_at,
            signal_payload: value.signal_payload,
            consumed_signal_ids: value.consumed_signal_ids,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
struct SignalRow {
    signal_id: String,
    signal_type: String,
    correlation_key: String,
    payload_json: String,
    created_at: DateTime<Utc>,
}

impl From<DurableSignalRecord> for SignalRow {
    fn from(value: DurableSignalRecord) -> Self {
        Self {
            signal_id: value.signal_id,
            signal_type: value.signal_type,
            correlation_key: value.correlation_key,
            payload_json: value.payload_json,
            created_at: value.created_at,
        }
    }
}

impl From<SignalRow> for DurableSignalRecord {
    fn from(value: SignalRow) -> Self {
        Self {
            signal_id: value.signal_id,
            signal_type: value.signal_type,
            correlation_key: value.correlation_key,
            payload_json: value.payload_json,
            created_at: value.created_at,
        }
    }
}

#[async_trait]
impl DurableWaitStore for SurrealDurableWaitStore {
    async fn insert_wait(&self, record: DurableWaitRecord) -> Result<()> {
        let row: WaitRow = record.into();
        self.db
            .query("CREATE type::record($table, $id) CONTENT $data")
            .bind(("table", self.wait_table.clone()))
            .bind(("id", row.wait_id.clone()))
            .bind(("data", row))
            .await
            .map_err(|e| Self::port_err("insert durable wait", e))?;
        Ok(())
    }

    async fn get_wait(&self, wait_id: &str) -> Result<Option<DurableWaitRecord>> {
        let mut response = match self
            .db
            .query("SELECT * FROM type::record($table, $id)")
            .bind(("table", self.wait_table.clone()))
            .bind(("id", wait_id.to_string()))
            .await
        {
            Ok(response) => response,
            Err(err) if Self::missing_table(&err, &self.wait_table) => return Ok(None),
            Err(err) => return Err(Self::port_err("get durable wait", err)),
        };
        let rows: Vec<WaitRow> = match response.take(0) {
            Ok(rows) => rows,
            Err(err) if Self::missing_table(&err, &self.wait_table) => return Ok(None),
            Err(err) => return Err(Self::port_err("decode durable wait", err)),
        };
        rows.into_iter()
            .next()
            .map(DurableWaitRecord::try_from)
            .transpose()
    }

    async fn get_pending_wait(
        &self,
        job_id: &str,
        signal_type: &str,
        correlation_key: &str,
    ) -> Result<Option<DurableWaitRecord>> {
        let waits = self
            .list_pending_by_signal(signal_type, correlation_key)
            .await?;
        Ok(waits.into_iter().find(|wait| wait.job_id == job_id))
    }

    async fn list_pending_by_signal(
        &self,
        signal_type: &str,
        correlation_key: &str,
    ) -> Result<Vec<DurableWaitRecord>> {
        let mut response = match self
            .db
            .query("SELECT * FROM type::table($table)")
            .bind(("table", self.wait_table.clone()))
            .await
        {
            Ok(response) => response,
            Err(err) if Self::missing_table(&err, &self.wait_table) => return Ok(Vec::new()),
            Err(err) => return Err(Self::port_err("list durable waits", err)),
        };
        let rows: Vec<WaitRow> = match response.take(0) {
            Ok(rows) => rows,
            Err(err) if Self::missing_table(&err, &self.wait_table) => return Ok(Vec::new()),
            Err(err) => return Err(Self::port_err("decode durable waits", err)),
        };
        let mut out = Vec::new();
        for row in rows {
            let wait = DurableWaitRecord::try_from(row)?;
            if wait.signal_type == signal_type
                && wait.correlation_key == correlation_key
                && wait.status == DurableWaitStatus::Pending
            {
                out.push(wait);
            }
        }
        Ok(out)
    }

    async fn list_pending_by_job(&self, job_id: &str) -> Result<Vec<DurableWaitRecord>> {
        let mut response = match self
            .db
            .query("SELECT * FROM type::table($table)")
            .bind(("table", self.wait_table.clone()))
            .await
        {
            Ok(response) => response,
            Err(err) if Self::missing_table(&err, &self.wait_table) => return Ok(Vec::new()),
            Err(err) => return Err(Self::port_err("list durable waits by job", err)),
        };
        let rows: Vec<WaitRow> = match response.take(0) {
            Ok(rows) => rows,
            Err(err) if Self::missing_table(&err, &self.wait_table) => return Ok(Vec::new()),
            Err(err) => return Err(Self::port_err("decode durable waits by job", err)),
        };
        let mut out = Vec::new();
        for row in rows {
            let wait = DurableWaitRecord::try_from(row)?;
            if wait.job_id == job_id && wait.status == DurableWaitStatus::Pending {
                out.push(wait);
            }
        }
        Ok(out)
    }

    async fn save_wait(&self, record: DurableWaitRecord) -> Result<()> {
        let row: WaitRow = record.into();
        self.db
            .query("UPDATE type::record($table, $id) CONTENT $data")
            .bind(("table", self.wait_table.clone()))
            .bind(("id", row.wait_id.clone()))
            .bind(("data", row))
            .await
            .map_err(|e| Self::port_err("save durable wait", e))?;
        Ok(())
    }

    async fn insert_signal(&self, record: DurableSignalRecord) -> Result<bool> {
        if self.get_signal(&record.signal_id).await?.is_some() {
            return Ok(false);
        }
        let row: SignalRow = record.into();
        self.db
            .query("CREATE type::record($table, $id) CONTENT $data")
            .bind(("table", self.signal_table.clone()))
            .bind(("id", row.signal_id.clone()))
            .bind(("data", row))
            .await
            .map_err(|e| Self::port_err("insert durable signal", e))?;
        Ok(true)
    }

    async fn get_signal(&self, signal_id: &str) -> Result<Option<DurableSignalRecord>> {
        let mut response = match self
            .db
            .query("SELECT * FROM type::record($table, $id)")
            .bind(("table", self.signal_table.clone()))
            .bind(("id", signal_id.to_string()))
            .await
        {
            Ok(response) => response,
            Err(err) if Self::missing_table(&err, &self.signal_table) => return Ok(None),
            Err(err) => return Err(Self::port_err("get durable signal", err)),
        };
        let rows: Vec<SignalRow> = match response.take(0) {
            Ok(rows) => rows,
            Err(err) if Self::missing_table(&err, &self.signal_table) => return Ok(None),
            Err(err) => return Err(Self::port_err("decode durable signal", err)),
        };
        Ok(rows.into_iter().next().map(DurableSignalRecord::from))
    }

    async fn take_unconsumed_signal(
        &self,
        signal_type: &str,
        correlation_key: &str,
        consumed_ids: &[String],
    ) -> Result<Option<DurableSignalRecord>> {
        let mut response = match self
            .db
            .query("SELECT * FROM type::table($table)")
            .bind(("table", self.signal_table.clone()))
            .await
        {
            Ok(response) => response,
            Err(err) if Self::missing_table(&err, &self.signal_table) => return Ok(None),
            Err(err) => return Err(Self::port_err("list durable signals", err)),
        };
        let rows: Vec<SignalRow> = match response.take(0) {
            Ok(rows) => rows,
            Err(err) if Self::missing_table(&err, &self.signal_table) => return Ok(None),
            Err(err) => return Err(Self::port_err("decode durable signals", err)),
        };
        Ok(rows
            .into_iter()
            .map(DurableSignalRecord::from)
            .filter(|signal| {
                signal.signal_type == signal_type
                    && signal.correlation_key == correlation_key
                    && !consumed_ids.contains(&signal.signal_id)
            })
            .min_by_key(|signal| signal.created_at))
    }

    async fn complete_wait(
        &self,
        wait_id: &str,
        status: DurableWaitStatus,
        signal_payload: Option<String>,
        signal_id: Option<String>,
        updated_at: DateTime<Utc>,
    ) -> Result<bool> {
        let Some(mut wait) = self.get_wait(wait_id).await? else {
            return Ok(false);
        };
        if wait.status != DurableWaitStatus::Pending {
            return Ok(false);
        }
        wait.status = status;
        wait.signal_payload = signal_payload;
        if let Some(signal_id) = signal_id {
            wait.consumed_signal_ids.push(signal_id);
        }
        wait.updated_at = updated_at;
        self.save_wait(wait).await?;
        Ok(true)
    }
}
