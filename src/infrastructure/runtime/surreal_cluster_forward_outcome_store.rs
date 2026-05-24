use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::{engine::any::Any, Surreal};
use surrealdb_types::SurrealValue;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::cluster_node::ClusterForwardOutcome;
use crate::ports::outbound::runtime::cluster_forward_outcome_store::ClusterForwardOutcomeStore;

#[derive(Clone)]
pub struct SurrealClusterForwardOutcomeStore {
    db: Surreal<Any>,
    table: String,
}

impl SurrealClusterForwardOutcomeStore {
    pub fn new(db: Surreal<Any>) -> Self {
        Self {
            db,
            table: "cluster_forward_outcome".to_string(),
        }
    }

    fn port_err(prefix: &str, err: impl std::fmt::Display) -> StasisError {
        StasisError::PortFailure(format!("{prefix}: {err}"))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, SurrealValue)]
struct ClusterForwardOutcomeRecord {
    target_region: String,
    command_name: String,
    correlation_id: Option<String>,
    accepted: bool,
    attempts: u32,
    error: Option<String>,
    completed_at: DateTime<Utc>,
}

impl From<ClusterForwardOutcomeRecord> for ClusterForwardOutcome {
    fn from(value: ClusterForwardOutcomeRecord) -> Self {
        Self {
            target_region: value.target_region,
            command_name: value.command_name,
            correlation_id: value.correlation_id,
            accepted: value.accepted,
            attempts: value.attempts,
            error: value.error,
            completed_at: value.completed_at,
        }
    }
}

impl From<ClusterForwardOutcome> for ClusterForwardOutcomeRecord {
    fn from(value: ClusterForwardOutcome) -> Self {
        Self {
            target_region: value.target_region,
            command_name: value.command_name,
            correlation_id: value.correlation_id,
            accepted: value.accepted,
            attempts: value.attempts,
            error: value.error,
            completed_at: value.completed_at,
        }
    }
}

#[async_trait]
impl ClusterForwardOutcomeStore for SurrealClusterForwardOutcomeStore {
    async fn record(&self, outcome: ClusterForwardOutcome) -> Result<()> {
        self.db
            .query("CREATE type::table($table) CONTENT $data")
            .bind(("table", self.table.clone()))
            .bind(("data", ClusterForwardOutcomeRecord::from(outcome)))
            .await
            .map_err(|e| Self::port_err("record cluster forward outcome", e))?;

        Ok(())
    }

    async fn list_recent(&self, limit: usize) -> Result<Vec<ClusterForwardOutcome>> {
        let mut response = match self
            .db
            .query("SELECT * FROM type::table($table) ORDER BY completed_at DESC LIMIT $limit")
            .bind(("table", self.table.clone()))
            .bind(("limit", limit.max(1)))
            .await
        {
            Ok(response) => response,
            Err(err) => {
                let message = err.to_string();
                if message.contains("does not exist") && message.contains(&self.table) {
                    return Ok(Vec::new());
                }
                return Err(Self::port_err("list cluster forward outcomes", err));
            }
        };

        let rows: Vec<ClusterForwardOutcomeRecord> = match response.take(0) {
            Ok(rows) => rows,
            Err(err) => {
                let message = err.to_string();
                if message.contains("does not exist") && message.contains(&self.table) {
                    return Ok(Vec::new());
                }
                return Err(Self::port_err("decode cluster forward outcomes", err));
            }
        };

        Ok(rows
            .into_iter()
            .map(ClusterForwardOutcome::from)
            .collect::<Vec<_>>())
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};
    use surrealdb::{engine::any::Any, Surreal};

    use crate::domain::runtime::cluster_node::ClusterForwardOutcome;
    use crate::ports::outbound::runtime::cluster_forward_outcome_store::ClusterForwardOutcomeStore;

    use super::SurrealClusterForwardOutcomeStore;

    async fn store() -> SurrealClusterForwardOutcomeStore {
        let db = Surreal::<Any>::init();
        db.connect("mem://")
            .await
            .expect("surreal mem db should initialize");
        db.use_ns("stasis")
            .use_db("test")
            .await
            .expect("namespace/database should initialize");
        SurrealClusterForwardOutcomeStore::new(db)
    }

    #[tokio::test]
    async fn records_and_lists_recent_outcomes_in_descending_order() {
        let store = store().await;
        let now = Utc::now();

        store
            .record(ClusterForwardOutcome {
                target_region: "eu-west".to_string(),
                command_name: "coordinator.handoff".to_string(),
                correlation_id: Some("corr-1".to_string()),
                accepted: true,
                attempts: 1,
                error: None,
                completed_at: now,
            })
            .await
            .expect("first outcome should record");

        store
            .record(ClusterForwardOutcome {
                target_region: "us-east".to_string(),
                command_name: "queue_ownership.rebalance".to_string(),
                correlation_id: Some("corr-2".to_string()),
                accepted: false,
                attempts: 3,
                error: Some("timeout".to_string()),
                completed_at: now + Duration::seconds(1),
            })
            .await
            .expect("second outcome should record");

        let outcomes = store.list_recent(2).await.expect("list should succeed");
        assert_eq!(outcomes.len(), 2);
        assert_eq!(outcomes[0].correlation_id.as_deref(), Some("corr-2"));
        assert_eq!(outcomes[1].correlation_id.as_deref(), Some("corr-1"));
    }

    #[tokio::test]
    async fn list_recent_respects_limit() {
        let store = store().await;
        let now = Utc::now();

        for idx in 0..3 {
            store
                .record(ClusterForwardOutcome {
                    target_region: "eu-west".to_string(),
                    command_name: "coordinator.failover".to_string(),
                    correlation_id: Some(format!("corr-{idx}")),
                    accepted: idx % 2 == 0,
                    attempts: 1,
                    error: None,
                    completed_at: now + Duration::seconds(i64::from(idx)),
                })
                .await
                .expect("outcome should record");
        }

        let outcomes = store.list_recent(1).await.expect("list should succeed");
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].correlation_id.as_deref(), Some("corr-2"));
    }
}
