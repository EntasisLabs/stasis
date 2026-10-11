use chrono::{DateTime, Utc};

use crate::domain::errors::Result;
use crate::domain::runtime::recurring::RecurringDefinition;
use crate::ports::outbound::runtime::recurring_store::RecurringStore;

use super::codec::{decode_time, encode_time, sql_err};
use super::session::SqliteSession;

#[derive(Clone)]
pub struct SqliteRecurringStore {
    session: SqliteSession,
}

impl SqliteRecurringStore {
    pub fn new(session: SqliteSession) -> Self {
        Self { session }
    }
}

const COLS: &str = "recurring_id, queue, job_type, payload_template_ref, cron_expr, timezone, jitter_seconds, \
     enabled, max_attempts, next_run_at, last_run_at, lease_owner, lease_expires_at";

fn map_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RecurringDefinition> {
    let convert = |err: crate::domain::errors::StasisError| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Integer,
            Box::new(std::io::Error::other(err.to_string())),
        )
    };
    let enabled: i64 = row.get(7)?;
    let next_run_at: i64 = row.get(9)?;
    let last_run_at: Option<i64> = row.get(10)?;
    let lease_expires_at: Option<i64> = row.get(12)?;
    Ok(RecurringDefinition {
        id: row.get(0)?,
        queue: row.get(1)?,
        job_type: row.get(2)?,
        payload_template_ref: row.get(3)?,
        cron_expr: row.get(4)?,
        timezone: row.get(5)?,
        jitter_seconds: row.get(6)?,
        enabled: enabled != 0,
        max_attempts: u32::try_from(row.get::<_, i64>(8)?).unwrap_or(0),
        next_run_at: decode_time(next_run_at).map_err(convert)?,
        last_run_at: last_run_at.map(decode_time).transpose().map_err(convert)?,
        lease_owner: row.get(11)?,
        lease_expires_at: lease_expires_at
            .map(decode_time)
            .transpose()
            .map_err(convert)?,
    })
}

impl SqliteRecurringStore {
    fn save_def(&self, definition: &RecurringDefinition) -> Result<()> {
        self.session.execute(
            &format!(
                "INSERT INTO recurring_definition ({COLS}) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
                 ON CONFLICT(recurring_id) DO UPDATE SET
                    queue = excluded.queue,
                    job_type = excluded.job_type,
                    payload_template_ref = excluded.payload_template_ref,
                    cron_expr = excluded.cron_expr,
                    timezone = excluded.timezone,
                    jitter_seconds = excluded.jitter_seconds,
                    enabled = excluded.enabled,
                    max_attempts = excluded.max_attempts,
                    next_run_at = excluded.next_run_at,
                    last_run_at = excluded.last_run_at,
                    lease_owner = excluded.lease_owner,
                    lease_expires_at = excluded.lease_expires_at"
            ),
            rusqlite::params![
                definition.id,
                definition.queue,
                definition.job_type,
                definition.payload_template_ref,
                definition.cron_expr,
                definition.timezone,
                definition.jitter_seconds,
                i64::from(definition.enabled),
                i64::from(definition.max_attempts),
                encode_time(definition.next_run_at),
                definition.last_run_at.map(encode_time),
                definition.lease_owner,
                definition.lease_expires_at.map(encode_time),
            ],
        )?;
        Ok(())
    }
}

crate::async_store! {
impl RecurringStore for SqliteRecurringStore {
    async fn insert(&self, definition: RecurringDefinition) -> Result<()> {
        self.save_def(&definition)
    }

    async fn save(&self, definition: RecurringDefinition) -> Result<()> {
        self.save_def(&definition)
    }

    async fn lease_due(
        &self,
        now: DateTime<Utc>,
        scheduler_id: &str,
        lease_seconds: i64,
    ) -> Result<Vec<RecurringDefinition>> {
        let now_ns = encode_time(now);
        let lease_expires_at = encode_time(now + chrono::Duration::seconds(lease_seconds));
        let ids: Vec<String> = self.session.query_map(
            "SELECT recurring_id FROM recurring_definition
             WHERE enabled != 0
               AND next_run_at <= ?1
               AND (lease_expires_at IS NULL OR lease_expires_at <= ?1)",
            [now_ns],
            |row| row.get(0),
        )?;
        let mut leased = Vec::new();
        for id in ids {
            let won = self.session.with_tx(|tx| {
                let changed = tx
                    .execute(
                        "UPDATE recurring_definition
                         SET lease_owner = ?1, lease_expires_at = ?2
                         WHERE recurring_id = ?3
                           AND enabled != 0
                           AND next_run_at <= ?4
                           AND (lease_expires_at IS NULL OR lease_expires_at <= ?4)",
                        rusqlite::params![scheduler_id, lease_expires_at, id, now_ns],
                    )
                    .map_err(|err| sql_err("lease recurring definition", err))?;
                Ok(changed == 1)
            })?;
            if won {
                if let Some(definition) = self.session.query_opt(
                    &format!("SELECT {COLS} FROM recurring_definition WHERE recurring_id = ?1"),
                    [&id],
                    map_row,
                )? {
                    leased.push(definition);
                }
            }
        }
        Ok(leased)
    }

    async fn list(&self) -> Result<Vec<RecurringDefinition>> {
        self.session.query_map(
            &format!("SELECT {COLS} FROM recurring_definition ORDER BY recurring_id ASC"),
            [],
            map_row,
        )
    }
}
}
