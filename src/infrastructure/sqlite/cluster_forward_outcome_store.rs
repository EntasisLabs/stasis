use async_trait::async_trait;

use crate::domain::errors::Result;
use crate::domain::runtime::cluster_node::ClusterForwardOutcome;
use crate::ports::outbound::runtime::cluster_forward_outcome_store::ClusterForwardOutcomeStore;

use super::codec::{decode_time, encode_time};
use super::session::SqliteSession;

#[derive(Clone)]
pub struct SqliteClusterForwardOutcomeStore {
    session: SqliteSession,
}

impl SqliteClusterForwardOutcomeStore {
    pub fn new(session: SqliteSession) -> Self {
        Self { session }
    }
}

#[async_trait]
impl ClusterForwardOutcomeStore for SqliteClusterForwardOutcomeStore {
    async fn record(&self, outcome: ClusterForwardOutcome) -> Result<()> {
        self.session.execute(
            "INSERT INTO cluster_forward_outcome (
                target_region, command_name, correlation_id, accepted, attempts, error, completed_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![
                outcome.target_region,
                outcome.command_name,
                outcome.correlation_id,
                i64::from(outcome.accepted),
                i64::from(outcome.attempts),
                outcome.error,
                encode_time(outcome.completed_at),
            ],
        )?;
        Ok(())
    }

    async fn list_recent(&self, limit: usize) -> Result<Vec<ClusterForwardOutcome>> {
        let limit = i64::try_from(limit.max(1)).unwrap_or(i64::MAX);
        self.session.query_map(
            "SELECT target_region, command_name, correlation_id, accepted, attempts, error, completed_at
             FROM cluster_forward_outcome ORDER BY completed_at DESC LIMIT ?1",
            rusqlite::params![limit],
            |row| {
                let conv = |err: crate::domain::errors::StasisError| {
                    rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(
                        err.to_string(),
                    )))
                };
                Ok(ClusterForwardOutcome {
                    target_region: row.get(0)?,
                    command_name: row.get(1)?,
                    correlation_id: row.get(2)?,
                    accepted: row.get::<_, i64>(3)? != 0,
                    attempts: u32::try_from(row.get::<_, i64>(4)?).unwrap_or(0),
                    error: row.get(5)?,
                    completed_at: decode_time(row.get(6)?).map_err(conv)?,
                })
            },
        )
    }
}
