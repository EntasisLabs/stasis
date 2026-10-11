use async_trait::async_trait;

use crate::domain::errors::Result;
use crate::domain::runtime::job_continuation::{
    ChildJobSpec, ContinuationStatus, ContinuationTrigger, JobContinuation,
};
use crate::ports::outbound::runtime::job_continuation_store::JobContinuationStore;

use super::codec::{decode_json, decode_time, encode_json, encode_time, sql_err};
use super::session::SqliteSession;

#[derive(Clone)]
pub struct SqliteJobContinuationStore {
    session: SqliteSession,
}

impl SqliteJobContinuationStore {
    pub fn new(session: SqliteSession) -> Self {
        Self { session }
    }
}

const COLS: &str = "continuation_id, parent_job_id, trigger, status, child_spec_json, child_job_id, parent_output_json, claim_token, created_at";

fn map_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<JobContinuation> {
    let convert = |err: crate::domain::errors::StasisError| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::other(err.to_string())),
        )
    };
    let trigger_raw: String = row.get(2)?;
    let status_raw: String = row.get(3)?;
    let child_json: String = row.get(4)?;
    let created_at: i64 = row.get(8)?;
    let trigger = ContinuationTrigger::parse(&trigger_raw)
        .ok_or_else(|| sql_err("decode continuation trigger", &trigger_raw))
        .map_err(convert)?;
    let status = ContinuationStatus::parse(&status_raw)
        .ok_or_else(|| sql_err("decode continuation status", &status_raw))
        .map_err(convert)?;
    let child: ChildJobSpec = decode_json(&child_json).map_err(convert)?;
    Ok(JobContinuation {
        id: row.get(0)?,
        parent_job_id: row.get(1)?,
        trigger,
        status,
        child,
        child_job_id: row.get(5)?,
        parent_output_json: row.get(6)?,
        claim_token: row.get(7)?,
        created_at: decode_time(created_at).map_err(convert)?,
    })
}

impl SqliteJobContinuationStore {
    fn save(&self, record: &JobContinuation) -> Result<()> {
        let child = encode_json(&record.child)?;
        self.session.execute(
            &format!(
                "INSERT INTO job_continuation ({COLS}) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                 ON CONFLICT(continuation_id) DO UPDATE SET
                    parent_job_id = excluded.parent_job_id,
                    trigger = excluded.trigger,
                    status = excluded.status,
                    child_spec_json = excluded.child_spec_json,
                    child_job_id = excluded.child_job_id,
                    parent_output_json = excluded.parent_output_json,
                    claim_token = excluded.claim_token,
                    created_at = excluded.created_at"
            ),
            rusqlite::params![
                record.id,
                record.parent_job_id,
                record.trigger.as_str(),
                record.status.as_str(),
                child,
                record.child_job_id,
                record.parent_output_json,
                record.claim_token,
                encode_time(record.created_at),
            ],
        )?;
        Ok(())
    }
}

#[async_trait]
impl JobContinuationStore for SqliteJobContinuationStore {
    async fn insert(&self, record: JobContinuation) -> Result<()> {
        self.save(&record)
    }

    async fn get(&self, id: &str) -> Result<Option<JobContinuation>> {
        self.session.query_opt(
            &format!("SELECT {COLS} FROM job_continuation WHERE continuation_id = ?1"),
            [id],
            map_row,
        )
    }

    async fn save(&self, record: JobContinuation) -> Result<()> {
        self.save(&record)
    }

    async fn list_pending_by_parent(&self, parent_job_id: &str) -> Result<Vec<JobContinuation>> {
        self.session.query_map(
            &format!(
                "SELECT {COLS} FROM job_continuation
                 WHERE parent_job_id = ?1 AND status = 'pending'"
            ),
            [parent_job_id],
            map_row,
        )
    }

    async fn try_claim(&self, id: &str, claim_token: &str) -> Result<bool> {
        self.session.with_tx(|tx| {
            let changed = tx
                .execute(
                    "UPDATE job_continuation
                     SET status = 'settling', claim_token = ?1
                     WHERE continuation_id = ?2 AND status = 'pending'",
                    rusqlite::params![claim_token, id],
                )
                .map_err(|err| sql_err("claim job continuation", err))?;
            if changed != 1 {
                return Ok(false);
            }
            let won: Option<String> = tx
                .query_row(
                    "SELECT claim_token FROM job_continuation WHERE continuation_id = ?1 AND status = 'settling'",
                    [id],
                    |row| row.get(0),
                )
                .optional_compat()
                .map_err(|err| sql_err("read claimed continuation", err))?;
            Ok(won.as_deref() == Some(claim_token))
        })
    }

    async fn get_by_child(&self, child_job_id: &str) -> Result<Option<JobContinuation>> {
        self.session.query_opt(
            &format!(
                "SELECT {COLS} FROM job_continuation WHERE child_job_id = ?1 LIMIT 1"
            ),
            [child_job_id],
            map_row,
        )
    }
}

trait OptionalCompat<T> {
    fn optional_compat(self) -> std::result::Result<Option<T>, rusqlite::Error>;
}

impl<T> OptionalCompat<T> for std::result::Result<T, rusqlite::Error> {
    fn optional_compat(self) -> std::result::Result<Option<T>, rusqlite::Error> {
        match self {
            Ok(value) => Ok(Some(value)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(err) => Err(err),
        }
    }
}
