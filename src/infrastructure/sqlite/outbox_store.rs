use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::errors::Result;
use crate::domain::runtime::outbox::{OutboxEvent, OutboxStatus, RuntimeEvent, RuntimeEventType};
use crate::domain::runtime::provenance::{ProvenanceRef, SttpProvenanceAdapter};
use crate::ports::outbound::runtime::outbox_store::OutboxStore;

use super::codec::{decode_json, decode_time, encode_json, encode_time, sql_err};
use super::session::SqliteSession;

#[derive(Clone)]
pub struct SqliteOutboxStore {
    session: SqliteSession,
}

impl SqliteOutboxStore {
    pub fn new(session: SqliteSession) -> Self {
        Self { session }
    }
}

const COLS: &str = "event_id, status, publish_attempts, published_at, next_attempt_at, last_publish_error, \
     event_type, job_id, thread_id, correlation_id, causation_id, trace_id, sttp_input_node_id, \
     sttp_output_node_id, input_provenance_json, output_provenance_json, execution_id, \
     input_memory_query_id, input_memory_query_fingerprint, output_memory_node_id, retrieval_path, \
     occurred_at, message";

fn status_str(status: &OutboxStatus) -> &'static str {
    match status {
        OutboxStatus::Pending => "pending",
        OutboxStatus::Published => "published",
        OutboxStatus::Failed => "failed",
    }
}

fn parse_status(raw: &str) -> Result<OutboxStatus> {
    match raw {
        "pending" => Ok(OutboxStatus::Pending),
        "published" => Ok(OutboxStatus::Published),
        "failed" => Ok(OutboxStatus::Failed),
        other => Err(sql_err("decode outbox status", other)),
    }
}

fn event_type_str(value: &RuntimeEventType) -> &'static str {
    match value {
        RuntimeEventType::JobSucceeded => "job_succeeded",
        RuntimeEventType::JobRetryScheduled => "job_retry_scheduled",
        RuntimeEventType::JobDeadLettered => "job_dead_lettered",
        RuntimeEventType::JobPublished => "job_published",
        RuntimeEventType::JobCanceled => "job_canceled",
    }
}

fn parse_event_type(raw: &str) -> Result<RuntimeEventType> {
    match raw {
        "job_succeeded" => Ok(RuntimeEventType::JobSucceeded),
        "job_retry_scheduled" => Ok(RuntimeEventType::JobRetryScheduled),
        "job_dead_lettered" => Ok(RuntimeEventType::JobDeadLettered),
        "job_published" => Ok(RuntimeEventType::JobPublished),
        "job_canceled" => Ok(RuntimeEventType::JobCanceled),
        other => Err(sql_err("decode runtime event type", other)),
    }
}

fn map_event(row: &rusqlite::Row<'_>) -> rusqlite::Result<OutboxEvent> {
    let convert = |err: crate::domain::errors::StasisError| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::other(err.to_string())),
        )
    };
    let status = parse_status(&row.get::<_, String>(1)?).map_err(convert)?;
    let event_type = parse_event_type(&row.get::<_, String>(6)?).map_err(convert)?;
    let published_at: Option<i64> = row.get(3)?;
    let next_attempt_at: Option<i64> = row.get(4)?;
    let occurred_at: i64 = row.get(21)?;
    let input_json: Option<String> = row.get(14)?;
    let output_json: Option<String> = row.get(15)?;
    let sttp_input: String = row.get(12)?;
    let sttp_output: Option<String> = row.get(13)?;
    let decode = |value: i64| decode_time(value).map_err(convert);
    Ok(OutboxEvent {
        event_id: row.get(0)?,
        status,
        publish_attempts: u32::try_from(row.get::<_, i64>(2)?).unwrap_or(0),
        published_at: published_at.map(decode).transpose()?,
        next_attempt_at: next_attempt_at.map(decode).transpose()?,
        last_publish_error: row.get(5)?,
        event: RuntimeEvent {
            event_type,
            job_id: row.get(7)?,
            thread_id: row.get(8)?,
            correlation_id: row.get(9)?,
            causation_id: row.get(10)?,
            trace_id: row.get(11)?,
            input_provenance: input_json
                .as_deref()
                .and_then(|json| decode_json::<ProvenanceRef>(json).ok())
                .or_else(|| SttpProvenanceAdapter::from_compat_string(&sttp_input)),
            output_provenance: output_json
                .as_deref()
                .and_then(|json| decode_json::<ProvenanceRef>(json).ok())
                .or_else(|| {
                    sttp_output
                        .as_deref()
                        .and_then(SttpProvenanceAdapter::from_compat_string)
                }),
            execution_id: row.get(16)?,
            input_memory_query_id: row.get(17)?,
            input_memory_query_fingerprint: row.get(18)?,
            output_memory_node_id: row.get(19)?,
            retrieval_path: row.get(20)?,
            occurred_at: decode(occurred_at)?,
            message: row.get(22)?,
        },
    })
}

fn save_params(event: &OutboxEvent) -> Result<Vec<Box<dyn rusqlite::types::ToSql>>> {
    let input_json = event
        .event
        .input_provenance
        .as_ref()
        .map(encode_json)
        .transpose()?;
    let output_json = event
        .event
        .output_provenance
        .as_ref()
        .map(encode_json)
        .transpose()?;
    Ok(vec![
        Box::new(event.event_id.clone()),
        Box::new(status_str(&event.status).to_string()),
        Box::new(i64::from(event.publish_attempts)),
        Box::new(event.published_at.map(encode_time)),
        Box::new(event.next_attempt_at.map(encode_time)),
        Box::new(event.last_publish_error.clone()),
        Box::new(event_type_str(&event.event.event_type).to_string()),
        Box::new(event.event.job_id.clone()),
        Box::new(event.event.thread_id.clone()),
        Box::new(event.event.correlation_id.clone()),
        Box::new(event.event.causation_id.clone()),
        Box::new(event.event.trace_id.clone()),
        Box::new(SttpProvenanceAdapter::to_compat_string(
            event.event.input_provenance.as_ref(),
        )),
        Box::new(SttpProvenanceAdapter::from_optional(
            event.event.output_provenance.as_ref(),
        )),
        Box::new(input_json),
        Box::new(output_json),
        Box::new(event.event.execution_id.clone()),
        Box::new(event.event.input_memory_query_id.clone()),
        Box::new(event.event.input_memory_query_fingerprint.clone()),
        Box::new(event.event.output_memory_node_id.clone()),
        Box::new(event.event.retrieval_path.clone()),
        Box::new(encode_time(event.event.occurred_at)),
        Box::new(event.event.message.clone()),
    ])
}

impl SqliteOutboxStore {
    fn save_event(&self, event: &OutboxEvent) -> Result<()> {
        let params = save_params(event)?;
        let refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();
        let placeholders = (1..=23)
            .map(|n| format!("?{n}"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "INSERT INTO outbox_event ({COLS}) VALUES ({placeholders})
             ON CONFLICT(event_id) DO UPDATE SET
                status = excluded.status,
                publish_attempts = excluded.publish_attempts,
                published_at = excluded.published_at,
                next_attempt_at = excluded.next_attempt_at,
                last_publish_error = excluded.last_publish_error,
                event_type = excluded.event_type,
                job_id = excluded.job_id,
                thread_id = excluded.thread_id,
                correlation_id = excluded.correlation_id,
                causation_id = excluded.causation_id,
                trace_id = excluded.trace_id,
                sttp_input_node_id = excluded.sttp_input_node_id,
                sttp_output_node_id = excluded.sttp_output_node_id,
                input_provenance_json = excluded.input_provenance_json,
                output_provenance_json = excluded.output_provenance_json,
                execution_id = excluded.execution_id,
                input_memory_query_id = excluded.input_memory_query_id,
                input_memory_query_fingerprint = excluded.input_memory_query_fingerprint,
                output_memory_node_id = excluded.output_memory_node_id,
                retrieval_path = excluded.retrieval_path,
                occurred_at = excluded.occurred_at,
                message = excluded.message"
        );
        self.session
            .execute(&sql, rusqlite::params_from_iter(refs))?;
        Ok(())
    }

    fn list_where(&self, sql: &str, params: impl rusqlite::Params) -> Result<Vec<OutboxEvent>> {
        let mut events = self.session.query_map(sql, params, map_event)?;
        events.sort_by_key(|event| event.event.occurred_at);
        Ok(events)
    }
}

#[async_trait]
impl OutboxStore for SqliteOutboxStore {
    async fn insert(&self, event: OutboxEvent) -> Result<()> {
        self.save_event(&event)
    }

    async fn save(&self, event: OutboxEvent) -> Result<()> {
        self.save_event(&event)
    }

    async fn get(&self, event_id: &str) -> Result<Option<OutboxEvent>> {
        self.session.query_opt(
            &format!("SELECT {COLS} FROM outbox_event WHERE event_id = ?1"),
            [event_id],
            map_event,
        )
    }

    async fn list_pending(&self, limit: usize) -> Result<Vec<OutboxEvent>> {
        let mut events = self.session.query_map(
            &format!(
                "SELECT {COLS} FROM outbox_event
                 WHERE status = 'pending'
                 ORDER BY COALESCE(next_attempt_at, occurred_at) ASC
                 LIMIT ?1"
            ),
            [limit as i64],
            map_event,
        )?;
        events.sort_by_key(|event| event.next_attempt_at.unwrap_or(event.event.occurred_at));
        Ok(events)
    }

    async fn list_by_job_id(&self, job_id: &str) -> Result<Vec<OutboxEvent>> {
        self.list_where(
            &format!("SELECT {COLS} FROM outbox_event WHERE job_id = ?1"),
            [job_id],
        )
    }

    async fn list_by_thread_id(&self, thread_id: &str) -> Result<Vec<OutboxEvent>> {
        self.list_where(
            &format!("SELECT {COLS} FROM outbox_event WHERE thread_id = ?1"),
            [thread_id],
        )
    }

    async fn list_by_thread_prefix(&self, thread_prefix: &str) -> Result<Vec<OutboxEvent>> {
        let prefix = thread_prefix.to_string();
        let mut events = self.session.query_map(
            &format!("SELECT {COLS} FROM outbox_event WHERE thread_id IS NOT NULL"),
            [],
            map_event,
        )?;
        events.retain(|event| {
            event
                .event
                .thread_id
                .as_deref()
                .is_some_and(|thread_id| thread_id.starts_with(&prefix))
        });
        events.sort_by_key(|event| event.event.occurred_at);
        Ok(events)
    }

    async fn list_by_execution_id(&self, execution_id: &str) -> Result<Vec<OutboxEvent>> {
        self.list_where(
            &format!("SELECT {COLS} FROM outbox_event WHERE execution_id = ?1"),
            [execution_id],
        )
    }

    async fn prune_non_pending_before(&self, cutoff: DateTime<Utc>) -> Result<usize> {
        self.session.execute(
            "DELETE FROM outbox_event
             WHERE status != 'pending' AND occurred_at <= ?1",
            [encode_time(cutoff)],
        )
    }
}
