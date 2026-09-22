use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::{Surreal, engine::any::Any};
use surrealdb_types::SurrealValue;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::job_continuation::{
    ChildJobSpec, ContinuationStatus, ContinuationTrigger, JobContinuation,
};
use crate::ports::outbound::runtime::job_continuation_store::JobContinuationStore;

#[derive(Clone)]
pub struct SurrealJobContinuationStore {
    db: Surreal<Any>,
    table: String,
}

impl SurrealJobContinuationStore {
    pub fn new(db: Surreal<Any>) -> Self {
        Self {
            db,
            table: "job_continuation".to_string(),
        }
    }

    fn port_err(prefix: &str, err: impl std::fmt::Display) -> StasisError {
        StasisError::PortFailure(format!("{prefix}: {err}"))
    }

    fn missing_table(err: &impl std::fmt::Display) -> bool {
        let message = err.to_string();
        message.contains("does not exist") && message.contains("job_continuation")
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, SurrealValue)]
struct ContinuationRow {
    continuation_id: String,
    parent_job_id: String,
    trigger: String,
    status: String,
    child_spec_json: String,
    child_job_id: Option<String>,
    parent_output_json: Option<String>,
    claim_token: Option<String>,
    created_at: DateTime<Utc>,
}

impl TryFrom<JobContinuation> for ContinuationRow {
    type Error = StasisError;

    fn try_from(value: JobContinuation) -> std::result::Result<Self, Self::Error> {
        let child_spec_json = serde_json::to_string(&value.child).map_err(|err| {
            StasisError::PortFailure(format!("serialize continuation child spec: {err}"))
        })?;
        Ok(Self {
            continuation_id: value.id,
            parent_job_id: value.parent_job_id,
            trigger: value.trigger.as_str().to_string(),
            status: value.status.as_str().to_string(),
            child_spec_json,
            child_job_id: value.child_job_id,
            parent_output_json: value.parent_output_json,
            claim_token: value.claim_token,
            created_at: value.created_at,
        })
    }
}

impl TryFrom<ContinuationRow> for JobContinuation {
    type Error = StasisError;

    fn try_from(value: ContinuationRow) -> std::result::Result<Self, Self::Error> {
        let trigger = ContinuationTrigger::parse(&value.trigger).ok_or_else(|| {
            StasisError::PortFailure(format!("invalid continuation trigger: {}", value.trigger))
        })?;
        let status = ContinuationStatus::parse(&value.status).ok_or_else(|| {
            StasisError::PortFailure(format!("invalid continuation status: {}", value.status))
        })?;
        let child: ChildJobSpec = serde_json::from_str(&value.child_spec_json).map_err(|err| {
            StasisError::PortFailure(format!("decode continuation child spec: {err}"))
        })?;
        Ok(Self {
            id: value.continuation_id,
            parent_job_id: value.parent_job_id,
            trigger,
            status,
            child,
            child_job_id: value.child_job_id,
            parent_output_json: value.parent_output_json,
            claim_token: value.claim_token,
            created_at: value.created_at,
        })
    }
}

#[async_trait]
impl JobContinuationStore for SurrealJobContinuationStore {
    async fn insert(&self, record: JobContinuation) -> Result<()> {
        let row = ContinuationRow::try_from(record)?;
        self.db
            .query("CREATE type::record($table, $id) CONTENT $data")
            .bind(("table", self.table.clone()))
            .bind(("id", row.continuation_id.clone()))
            .bind(("data", row))
            .await
            .map_err(|err| Self::port_err("insert job continuation", err))?;
        Ok(())
    }

    async fn get(&self, id: &str) -> Result<Option<JobContinuation>> {
        let mut response = match self
            .db
            .query("SELECT * FROM type::record($table, $id)")
            .bind(("table", self.table.clone()))
            .bind(("id", id.to_string()))
            .await
        {
            Ok(response) => response,
            Err(err) if Self::missing_table(&err) => return Ok(None),
            Err(err) => return Err(Self::port_err("get job continuation", err)),
        };
        let row: Option<ContinuationRow> = match response.take(0) {
            Ok(row) => row,
            Err(err) if Self::missing_table(&err) => return Ok(None),
            Err(err) => return Err(Self::port_err("decode job continuation", err)),
        };
        row.map(JobContinuation::try_from).transpose()
    }

    async fn save(&self, record: JobContinuation) -> Result<()> {
        let row = ContinuationRow::try_from(record)?;
        self.db
            .query("UPSERT type::record($table, $id) CONTENT $data")
            .bind(("table", self.table.clone()))
            .bind(("id", row.continuation_id.clone()))
            .bind(("data", row))
            .await
            .map_err(|err| Self::port_err("save job continuation", err))?;
        Ok(())
    }

    async fn list_pending_by_parent(&self, parent_job_id: &str) -> Result<Vec<JobContinuation>> {
        let mut response = match self
            .db
            .query(
                "SELECT * FROM type::table($table) WHERE parent_job_id = $parent AND status = 'pending'",
            )
            .bind(("table", self.table.clone()))
            .bind(("parent", parent_job_id.to_string()))
            .await
        {
            Ok(response) => response,
            Err(err) if Self::missing_table(&err) => return Ok(Vec::new()),
            Err(err) => return Err(Self::port_err("list job continuations", err)),
        };
        let rows: Vec<ContinuationRow> = match response.take(0) {
            Ok(rows) => rows,
            Err(err) if Self::missing_table(&err) => return Ok(Vec::new()),
            Err(err) => return Err(Self::port_err("decode job continuations", err)),
        };
        rows.into_iter().map(JobContinuation::try_from).collect()
    }

    async fn try_claim(&self, id: &str, claim_token: &str) -> Result<bool> {
        let mut response = match self
            .db
            .query(
                "UPDATE type::record($table, $id) SET status = 'settling', claim_token = $claim WHERE status = 'pending' RETURN AFTER",
            )
            .bind(("table", self.table.clone()))
            .bind(("id", id.to_string()))
            .bind(("claim", claim_token.to_string()))
            .await
        {
            Ok(response) => response,
            Err(err) if Self::missing_table(&err) => return Ok(false),
            Err(err) => return Err(Self::port_err("claim job continuation", err)),
        };
        let row: Option<ContinuationRow> = match response.take(0) {
            Ok(row) => row,
            Err(err) if Self::missing_table(&err) => return Ok(false),
            Err(err) => return Err(Self::port_err("decode claimed continuation", err)),
        };
        Ok(row.is_some_and(|row| {
            row.status == "settling" && row.claim_token.as_deref() == Some(claim_token)
        }))
    }

    async fn get_by_child(&self, child_job_id: &str) -> Result<Option<JobContinuation>> {
        let mut response = match self
            .db
            .query("SELECT * FROM type::table($table) WHERE child_job_id = $child LIMIT 1")
            .bind(("table", self.table.clone()))
            .bind(("child", child_job_id.to_string()))
            .await
        {
            Ok(response) => response,
            Err(err) if Self::missing_table(&err) => return Ok(None),
            Err(err) => return Err(Self::port_err("get continuation by child", err)),
        };
        let rows: Vec<ContinuationRow> = match response.take(0) {
            Ok(rows) => rows,
            Err(err) if Self::missing_table(&err) => return Ok(None),
            Err(err) => return Err(Self::port_err("decode continuation by child", err)),
        };
        rows.into_iter()
            .next()
            .map(JobContinuation::try_from)
            .transpose()
    }
}
