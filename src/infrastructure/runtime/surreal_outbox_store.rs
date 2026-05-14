use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::{Surreal, engine::local::Db};
use surrealdb_types::SurrealValue;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::outbox::{OutboxEvent, OutboxStatus, RuntimeEvent, RuntimeEventType};
use crate::ports::outbound::runtime::outbox_store::OutboxStore;

#[derive(Clone)]
pub struct SurrealOutboxStore {
    db: Surreal<Db>,
    table: String,
}

impl SurrealOutboxStore {
    pub fn new(db: Surreal<Db>) -> Self {
        Self {
            db,
            table: "outbox_event".to_string(),
        }
    }

    fn port_err(prefix: &str, err: impl std::fmt::Display) -> StasisError {
        StasisError::PortFailure(format!("{prefix}: {err}"))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
struct OutboxRecord {
    event_id: String,
    status: String,
    publish_attempts: u32,
    published_at: Option<DateTime<Utc>>,
    next_attempt_at: Option<DateTime<Utc>>,
    last_publish_error: Option<String>,
    event_type: String,
    job_id: String,
    correlation_id: String,
    causation_id: String,
    trace_id: String,
    sttp_input_node_id: String,
    sttp_output_node_id: Option<String>,
    execution_id: Option<String>,
    occurred_at: DateTime<Utc>,
    message: Option<String>,
}

impl From<OutboxEvent> for OutboxRecord {
    fn from(value: OutboxEvent) -> Self {
        Self {
            event_id: value.event_id,
            status: match value.status {
                OutboxStatus::Pending => "pending".to_string(),
                OutboxStatus::Published => "published".to_string(),
                OutboxStatus::Failed => "failed".to_string(),
            },
            publish_attempts: value.publish_attempts,
            published_at: value.published_at,
            next_attempt_at: value.next_attempt_at,
            last_publish_error: value.last_publish_error,
            event_type: match value.event.event_type {
                RuntimeEventType::JobSucceeded => "job_succeeded".to_string(),
                RuntimeEventType::JobRetryScheduled => "job_retry_scheduled".to_string(),
                RuntimeEventType::JobDeadLettered => "job_dead_lettered".to_string(),
            },
            job_id: value.event.job_id,
            correlation_id: value.event.correlation_id,
            causation_id: value.event.causation_id,
            trace_id: value.event.trace_id,
            sttp_input_node_id: value.event.sttp_input_node_id,
            sttp_output_node_id: value.event.sttp_output_node_id,
            execution_id: value.event.execution_id,
            occurred_at: value.event.occurred_at,
            message: value.event.message,
        }
    }
}

impl TryFrom<OutboxRecord> for OutboxEvent {
    type Error = StasisError;

    fn try_from(value: OutboxRecord) -> std::result::Result<Self, Self::Error> {
        let status = match value.status.as_str() {
            "pending" => OutboxStatus::Pending,
            "published" => OutboxStatus::Published,
            "failed" => OutboxStatus::Failed,
            other => {
                return Err(StasisError::PortFailure(format!(
                    "invalid outbox status: {other}"
                )));
            }
        };

        let event_type = match value.event_type.as_str() {
            "job_succeeded" => RuntimeEventType::JobSucceeded,
            "job_retry_scheduled" => RuntimeEventType::JobRetryScheduled,
            "job_dead_lettered" => RuntimeEventType::JobDeadLettered,
            other => {
                return Err(StasisError::PortFailure(format!(
                    "invalid runtime event type: {other}"
                )));
            }
        };

        Ok(Self {
            event_id: value.event_id,
            status,
            publish_attempts: value.publish_attempts,
            published_at: value.published_at,
            next_attempt_at: value.next_attempt_at,
            last_publish_error: value.last_publish_error,
            event: RuntimeEvent {
                event_type,
                job_id: value.job_id,
                correlation_id: value.correlation_id,
                causation_id: value.causation_id,
                trace_id: value.trace_id,
                sttp_input_node_id: value.sttp_input_node_id,
                sttp_output_node_id: value.sttp_output_node_id,
                execution_id: value.execution_id,
                occurred_at: value.occurred_at,
                message: value.message,
            },
        })
    }
}

#[async_trait]
impl OutboxStore for SurrealOutboxStore {
    async fn insert(&self, event: OutboxEvent) -> Result<()> {
        self.save(event).await
    }

    async fn save(&self, event: OutboxEvent) -> Result<()> {
        let record: OutboxRecord = event.into();
        self.db
            .query("UPSERT type::record($table, $id) CONTENT $data")
            .bind(("table", self.table.clone()))
            .bind(("id", record.event_id.clone()))
            .bind(("data", record))
            .await
            .map_err(|e| Self::port_err("save outbox event", e))?;

        Ok(())
    }

    async fn get(&self, event_id: &str) -> Result<Option<OutboxEvent>> {
        let mut response = self
            .db
            .query("SELECT * FROM type::record($table, $id)")
            .bind(("table", self.table.clone()))
            .bind(("id", event_id.to_string()))
            .await
            .map_err(|e| Self::port_err("load outbox event", e))?;

        let row: Option<OutboxRecord> = response
            .take(0)
            .map_err(|e| Self::port_err("decode outbox event", e))?;

        row.map(OutboxEvent::try_from).transpose()
    }

    async fn list_pending(&self, limit: usize) -> Result<Vec<OutboxEvent>> {
        let mut response = self
            .db
            .query("SELECT * FROM type::table($table)")
            .bind(("table", self.table.clone()))
            .await
            .map_err(|e| Self::port_err("list outbox events", e))?;

        let rows: Vec<OutboxRecord> = response
            .take(0)
            .map_err(|e| Self::port_err("decode outbox events", e))?;

        let mut events: Vec<OutboxEvent> = rows
            .into_iter()
            .filter_map(|row| OutboxEvent::try_from(row).ok())
            .filter(|evt| evt.status == OutboxStatus::Pending)
            .collect();

        events.sort_by_key(|evt| evt.event.occurred_at);
        events.truncate(limit);
        Ok(events)
    }

    async fn list_by_job_id(&self, job_id: &str) -> Result<Vec<OutboxEvent>> {
        let mut response = self
            .db
            .query("SELECT * FROM type::table($table) WHERE job_id = $job_id")
            .bind(("table", self.table.clone()))
            .bind(("job_id", job_id.to_string()))
            .await
            .map_err(|e| Self::port_err("list outbox events by job", e))?;

        let rows: Vec<OutboxRecord> = response
            .take(0)
            .map_err(|e| Self::port_err("decode outbox events by job", e))?;

        let mut events: Vec<OutboxEvent> = rows
            .into_iter()
            .filter_map(|row| OutboxEvent::try_from(row).ok())
            .collect();

        events.sort_by_key(|evt| evt.event.occurred_at);
        Ok(events)
    }

    async fn list_by_execution_id(&self, execution_id: &str) -> Result<Vec<OutboxEvent>> {
        let mut response = self
            .db
            .query("SELECT * FROM type::table($table) WHERE execution_id = $execution_id")
            .bind(("table", self.table.clone()))
            .bind(("execution_id", execution_id.to_string()))
            .await
            .map_err(|e| Self::port_err("list outbox events by execution id", e))?;

        let rows: Vec<OutboxRecord> = response
            .take(0)
            .map_err(|e| Self::port_err("decode outbox events by execution id", e))?;

        let mut events: Vec<OutboxEvent> = rows
            .into_iter()
            .filter_map(|row| OutboxEvent::try_from(row).ok())
            .collect();

        events.sort_by_key(|evt| evt.event.occurred_at);
        Ok(events)
    }

    async fn prune_non_pending_before(&self, cutoff: DateTime<Utc>) -> Result<usize> {
        let mut response = self
            .db
            .query("SELECT * FROM type::table($table)")
            .bind(("table", self.table.clone()))
            .await
            .map_err(|e| Self::port_err("list outbox events for prune", e))?;

        let rows: Vec<OutboxRecord> = response
            .take(0)
            .map_err(|e| Self::port_err("decode outbox events for prune", e))?;

        let mut removed = 0usize;
        for row in rows {
            let is_pending = row.status == "pending";
            if !is_pending && row.occurred_at <= cutoff {
                self.db
                    .query("DELETE type::record($table, $id)")
                    .bind(("table", self.table.clone()))
                    .bind(("id", row.event_id))
                    .await
                    .map_err(|e| Self::port_err("delete pruned outbox event", e))?;
                removed += 1;
            }
        }

        Ok(removed)
    }

}
