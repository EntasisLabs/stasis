use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::errors::Result;
use crate::domain::runtime::durable_wait::{
    DurableSignalRecord, DurableWaitRecord, DurableWaitStatus,
};
use crate::ports::outbound::runtime::durable_wait_store::DurableWaitStore;

use super::codec::{
    decode_string_list, decode_time, encode_string_list, encode_time,
    sql_err,
};
use super::session::SqliteSession;

#[derive(Clone)]
pub struct SqliteDurableWaitStore {
    session: SqliteSession,
}

impl SqliteDurableWaitStore {
    pub fn new(session: SqliteSession) -> Self {
        Self { session }
    }
}

fn status_str(status: &DurableWaitStatus) -> &'static str {
    match status {
        DurableWaitStatus::Pending => "pending",
        DurableWaitStatus::Signaled => "signaled",
        DurableWaitStatus::TimedOut => "timed_out",
        DurableWaitStatus::Cancelled => "cancelled",
    }
}

fn parse_status(raw: &str) -> Result<DurableWaitStatus> {
    match raw {
        "pending" => Ok(DurableWaitStatus::Pending),
        "signaled" => Ok(DurableWaitStatus::Signaled),
        "timed_out" => Ok(DurableWaitStatus::TimedOut),
        "cancelled" => Ok(DurableWaitStatus::Cancelled),
        other => Err(sql_err("decode durable wait status", other)),
    }
}

fn map_wait(row: &rusqlite::Row<'_>) -> rusqlite::Result<DurableWaitRecord> {
    let convert = |err: crate::domain::errors::StasisError| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::other(err.to_string())),
        )
    };
    let status = parse_status(&row.get::<_, String>(4)?).map_err(convert)?;
    let deadline: Option<i64> = row.get(5)?;
    let created_at: i64 = row.get(6)?;
    let updated_at: i64 = row.get(7)?;
    let consumed: String = row.get(9)?;
    Ok(DurableWaitRecord {
        wait_id: row.get(0)?,
        job_id: row.get(1)?,
        signal_type: row.get(2)?,
        correlation_key: row.get(3)?,
        status,
        deadline_at: deadline.map(decode_time).transpose().map_err(convert)?,
        created_at: decode_time(created_at).map_err(convert)?,
        updated_at: decode_time(updated_at).map_err(convert)?,
        signal_payload: row.get(8)?,
        consumed_signal_ids: decode_string_list(&consumed).map_err(convert)?,
    })
}

fn map_signal(row: &rusqlite::Row<'_>) -> rusqlite::Result<DurableSignalRecord> {
    let created_at: i64 = row.get(4)?;
    Ok(DurableSignalRecord {
        signal_id: row.get(0)?,
        signal_type: row.get(1)?,
        correlation_key: row.get(2)?,
        payload_json: row.get(3)?,
        created_at: decode_time(created_at).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                4,
                rusqlite::types::Type::Integer,
                Box::new(std::io::Error::other(err.to_string())),
            )
        })?,
    })
}

const WAIT_COLS: &str = "wait_id, job_id, signal_type, correlation_key, status, deadline_at, created_at, updated_at, signal_payload, consumed_signal_ids_json";

impl SqliteDurableWaitStore {
    fn save_wait(&self, record: &DurableWaitRecord) -> Result<()> {
        let consumed = encode_string_list(&record.consumed_signal_ids)?;
        self.session.execute(
            &format!(
                "INSERT INTO durable_wait ({WAIT_COLS}) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                 ON CONFLICT(wait_id) DO UPDATE SET
                    job_id = excluded.job_id,
                    signal_type = excluded.signal_type,
                    correlation_key = excluded.correlation_key,
                    status = excluded.status,
                    deadline_at = excluded.deadline_at,
                    created_at = excluded.created_at,
                    updated_at = excluded.updated_at,
                    signal_payload = excluded.signal_payload,
                    consumed_signal_ids_json = excluded.consumed_signal_ids_json"
            ),
            rusqlite::params![
                record.wait_id,
                record.job_id,
                record.signal_type,
                record.correlation_key,
                status_str(&record.status),
                record.deadline_at.map(encode_time),
                encode_time(record.created_at),
                encode_time(record.updated_at),
                record.signal_payload,
                consumed,
            ],
        )?;
        Ok(())
    }
}

#[async_trait]
impl DurableWaitStore for SqliteDurableWaitStore {
    async fn insert_wait(&self, record: DurableWaitRecord) -> Result<()> {
        self.save_wait(&record)
    }

    async fn get_wait(&self, wait_id: &str) -> Result<Option<DurableWaitRecord>> {
        self.session.query_opt(
            &format!("SELECT {WAIT_COLS} FROM durable_wait WHERE wait_id = ?1"),
            [wait_id],
            map_wait,
        )
    }

    async fn get_pending_wait(
        &self,
        job_id: &str,
        signal_type: &str,
        correlation_key: &str,
    ) -> Result<Option<DurableWaitRecord>> {
        self.session.query_opt(
            &format!(
                "SELECT {WAIT_COLS} FROM durable_wait
                 WHERE job_id = ?1 AND signal_type = ?2 AND correlation_key = ?3 AND status = 'pending'
                 LIMIT 1"
            ),
            rusqlite::params![job_id, signal_type, correlation_key],
            map_wait,
        )
    }

    async fn list_pending_by_signal(
        &self,
        signal_type: &str,
        correlation_key: &str,
    ) -> Result<Vec<DurableWaitRecord>> {
        self.session.query_map(
            &format!(
                "SELECT {WAIT_COLS} FROM durable_wait
                 WHERE signal_type = ?1 AND correlation_key = ?2 AND status = 'pending'"
            ),
            rusqlite::params![signal_type, correlation_key],
            map_wait,
        )
    }

    async fn list_pending_by_job(&self, job_id: &str) -> Result<Vec<DurableWaitRecord>> {
        self.session.query_map(
            &format!(
                "SELECT {WAIT_COLS} FROM durable_wait WHERE job_id = ?1 AND status = 'pending'"
            ),
            [job_id],
            map_wait,
        )
    }

    async fn save_wait(&self, record: DurableWaitRecord) -> Result<()> {
        self.save_wait(&record)
    }

    async fn insert_signal(&self, record: DurableSignalRecord) -> Result<bool> {
        let changed = self.session.execute(
            "INSERT INTO durable_signal (signal_id, signal_type, correlation_key, payload_json, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(signal_id) DO NOTHING",
            rusqlite::params![
                record.signal_id,
                record.signal_type,
                record.correlation_key,
                record.payload_json,
                encode_time(record.created_at),
            ],
        )?;
        Ok(changed == 1)
    }

    async fn get_signal(&self, signal_id: &str) -> Result<Option<DurableSignalRecord>> {
        self.session.query_opt(
            "SELECT signal_id, signal_type, correlation_key, payload_json, created_at
             FROM durable_signal WHERE signal_id = ?1",
            [signal_id],
            map_signal,
        )
    }

    async fn take_unconsumed_signal(
        &self,
        signal_type: &str,
        correlation_key: &str,
        consumed_ids: &[String],
    ) -> Result<Option<DurableSignalRecord>> {
        let rows = self.session.query_map(
            "SELECT signal_id, signal_type, correlation_key, payload_json, created_at
             FROM durable_signal
             WHERE signal_type = ?1 AND correlation_key = ?2
             ORDER BY created_at ASC",
            rusqlite::params![signal_type, correlation_key],
            map_signal,
        )?;
        Ok(rows
            .into_iter()
            .find(|signal| !consumed_ids.contains(&signal.signal_id)))
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
        self.save_wait(&wait)?;
        Ok(true)
    }
}
