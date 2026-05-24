use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::{engine::any::Any, Surreal};
use surrealdb_types::SurrealValue;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::recurring::RecurringDefinition;
use crate::ports::outbound::runtime::recurring_store::RecurringStore;

#[derive(Clone)]
pub struct SurrealRecurringStore {
    db: Surreal<Any>,
    table: String,
}

impl SurrealRecurringStore {
    pub fn new(db: Surreal<Any>) -> Self {
        Self {
            db,
            table: "recurring_definition".to_string(),
        }
    }

    pub fn with_table(db: Surreal<Any>, table: impl Into<String>) -> Self {
        Self {
            db,
            table: table.into(),
        }
    }

    fn port_err(prefix: &str, err: impl std::fmt::Display) -> StasisError {
        StasisError::PortFailure(format!("{prefix}: {err}"))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
struct RecurringRecord {
    recurring_id: String,
    queue: String,
    job_type: String,
    payload_template_ref: String,
    cron_expr: String,
    timezone: String,
    jitter_seconds: i64,
    enabled: bool,
    max_attempts: u32,
    next_run_at: DateTime<Utc>,
    last_run_at: Option<DateTime<Utc>>,
    lease_owner: Option<String>,
    lease_expires_at: Option<DateTime<Utc>>,
}

impl From<RecurringDefinition> for RecurringRecord {
    fn from(value: RecurringDefinition) -> Self {
        Self {
            recurring_id: value.id,
            queue: value.queue,
            job_type: value.job_type,
            payload_template_ref: value.payload_template_ref,
            cron_expr: value.cron_expr,
            timezone: value.timezone,
            jitter_seconds: value.jitter_seconds,
            enabled: value.enabled,
            max_attempts: value.max_attempts,
            next_run_at: value.next_run_at,
            last_run_at: value.last_run_at,
            lease_owner: value.lease_owner,
            lease_expires_at: value.lease_expires_at,
        }
    }
}

impl From<RecurringRecord> for RecurringDefinition {
    fn from(value: RecurringRecord) -> Self {
        Self {
            id: value.recurring_id,
            queue: value.queue,
            job_type: value.job_type,
            payload_template_ref: value.payload_template_ref,
            cron_expr: value.cron_expr,
            timezone: value.timezone,
            jitter_seconds: value.jitter_seconds,
            enabled: value.enabled,
            max_attempts: value.max_attempts,
            next_run_at: value.next_run_at,
            last_run_at: value.last_run_at,
            lease_owner: value.lease_owner,
            lease_expires_at: value.lease_expires_at,
        }
    }
}

#[async_trait]
impl RecurringStore for SurrealRecurringStore {
    async fn insert(&self, definition: RecurringDefinition) -> Result<()> {
        let record: RecurringRecord = definition.into();
        self.db
            .query("CREATE type::record($table, $id) CONTENT $data")
            .bind(("table", self.table.clone()))
            .bind(("id", record.recurring_id.clone()))
            .bind(("data", record))
            .await
            .map_err(|e| Self::port_err("insert recurring definition", e))?;

        Ok(())
    }

    async fn save(&self, definition: RecurringDefinition) -> Result<()> {
        let record: RecurringRecord = definition.into();
        self.db
            .query("UPSERT type::record($table, $id) CONTENT $data")
            .bind(("table", self.table.clone()))
            .bind(("id", record.recurring_id.clone()))
            .bind(("data", record))
            .await
            .map_err(|e| Self::port_err("save recurring definition", e))?;

        Ok(())
    }

    async fn lease_due(
        &self,
        now: DateTime<Utc>,
        scheduler_id: &str,
        lease_seconds: i64,
    ) -> Result<Vec<RecurringDefinition>> {
        let mut response = self
            .db
            .query("SELECT * FROM type::table($table)")
            .bind(("table", self.table.clone()))
            .await
            .map_err(|e| Self::port_err("list recurring definitions", e))?;

        let rows: Vec<RecurringRecord> = response
            .take(0)
            .map_err(|e| Self::port_err("decode recurring definitions", e))?;

        let mut leased = Vec::new();

        for row in rows {
            let mut definition: RecurringDefinition = row.into();
            let lease_expired = definition
                .lease_expires_at
                .map(|expiry| expiry <= now)
                .unwrap_or(true);

            if definition.enabled && definition.next_run_at <= now && lease_expired {
                definition.lease_owner = Some(scheduler_id.to_string());
                definition.lease_expires_at = Some(now + Duration::seconds(lease_seconds));
                self.save(definition.clone()).await?;
                leased.push(definition);
            }
        }

        Ok(leased)
    }

    async fn list(&self) -> Result<Vec<RecurringDefinition>> {
        let mut response = self
            .db
            .query("SELECT * FROM type::table($table)")
            .bind(("table", self.table.clone()))
            .await
            .map_err(|e| Self::port_err("list recurring definitions", e))?;

        let rows: Vec<RecurringRecord> = response
            .take(0)
            .map_err(|e| Self::port_err("decode recurring definitions", e))?;

        Ok(rows.into_iter().map(RecurringDefinition::from).collect())
    }
}
