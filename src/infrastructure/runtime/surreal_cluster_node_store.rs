use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::{Surreal, engine::local::Db};
use surrealdb_types::SurrealValue;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::cluster_node::{
    ClusterNode, ClusterNodeHeartbeat, ClusterNodeRole, NewClusterNode,
};
use crate::ports::outbound::runtime::cluster_node_store::ClusterNodeStore;

#[derive(Clone)]
pub struct SurrealClusterNodeStore {
    db: Surreal<Db>,
    table: String,
}

impl SurrealClusterNodeStore {
    pub fn new(db: Surreal<Db>) -> Self {
        Self {
            db,
            table: "cluster_node".to_string(),
        }
    }

    fn port_err(prefix: &str, err: impl std::fmt::Display) -> StasisError {
        StasisError::PortFailure(format!("{prefix}: {err}"))
    }

    async fn load_record(&self, node_id: &str) -> Result<Option<ClusterNodeRecord>> {
        let mut response = match self
            .db
            .query("SELECT * FROM type::record($table, $id)")
            .bind(("table", self.table.clone()))
            .bind(("id", node_id.to_string()))
            .await
        {
            Ok(response) => response,
            Err(err) => {
                let message = err.to_string();
                if message.contains("does not exist") && message.contains(&self.table) {
                    return Ok(None);
                }
                return Err(Self::port_err("load cluster node", err));
            }
        };

        let row: Option<ClusterNodeRecord> = match response.take(0) {
            Ok(row) => row,
            Err(err) => {
                let message = err.to_string();
                if message.contains("does not exist") && message.contains(&self.table) {
                    return Ok(None);
                }
                return Err(Self::port_err("decode cluster node", err));
            }
        };

        Ok(row)
    }

    async fn save_record(&self, record: ClusterNodeRecord) -> Result<()> {
        self.db
            .query("UPSERT type::record($table, $id) CONTENT $data")
            .bind(("table", self.table.clone()))
            .bind(("id", record.node_id.clone()))
            .bind(("data", record))
            .await
            .map_err(|e| Self::port_err("save cluster node", e))?;
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, SurrealValue)]
struct ClusterNodeRecord {
    node_id: String,
    role: String,
    region: String,
    queue_ownership: Vec<String>,
    capability_tags: Vec<String>,
    heartbeat_at: DateTime<Utc>,
    lease_expires_at: DateTime<Utc>,
    metadata: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl TryFrom<ClusterNodeRecord> for ClusterNode {
    type Error = StasisError;

    fn try_from(value: ClusterNodeRecord) -> std::result::Result<Self, Self::Error> {
        let role = match value.role.as_str() {
            "coordinator" => ClusterNodeRole::Coordinator,
            "scheduler" => ClusterNodeRole::Scheduler,
            "worker" => ClusterNodeRole::Worker,
            other => {
                return Err(StasisError::PortFailure(format!(
                    "invalid cluster node role: {other}"
                )));
            }
        };

        Ok(Self {
            node_id: value.node_id,
            role,
            region: value.region,
            queue_ownership: value.queue_ownership,
            capability_tags: value.capability_tags,
            heartbeat_at: value.heartbeat_at,
            lease_expires_at: value.lease_expires_at,
            metadata: value.metadata,
            created_at: value.created_at,
            updated_at: value.updated_at,
        })
    }
}

impl From<ClusterNode> for ClusterNodeRecord {
    fn from(value: ClusterNode) -> Self {
        let role = match value.role {
            ClusterNodeRole::Coordinator => "coordinator".to_string(),
            ClusterNodeRole::Scheduler => "scheduler".to_string(),
            ClusterNodeRole::Worker => "worker".to_string(),
        };

        Self {
            node_id: value.node_id,
            role,
            region: value.region,
            queue_ownership: value.queue_ownership,
            capability_tags: value.capability_tags,
            heartbeat_at: value.heartbeat_at,
            lease_expires_at: value.lease_expires_at,
            metadata: value.metadata,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}

impl From<NewClusterNode> for ClusterNodeRecord {
    fn from(value: NewClusterNode) -> Self {
        ClusterNodeRecord::from(value.into_record())
    }
}

#[async_trait]
impl ClusterNodeStore for SurrealClusterNodeStore {
    async fn register(&self, node: NewClusterNode) -> Result<ClusterNode> {
        let record: ClusterNodeRecord = node.into();
        if self.load_record(&record.node_id).await?.is_some() {
            return Err(StasisError::PortFailure(format!(
                "cluster node already exists: {}",
                record.node_id
            )));
        }

        self.save_record(record.clone()).await?;
        ClusterNode::try_from(record)
    }

    async fn heartbeat(&self, heartbeat: ClusterNodeHeartbeat) -> Result<Option<ClusterNode>> {
        let Some(existing) = self.load_record(&heartbeat.node_id).await? else {
            return Ok(None);
        };

        let mut node = ClusterNode::try_from(existing)?;
        node.heartbeat_at = heartbeat.heartbeat_at;
        node.lease_expires_at = heartbeat.heartbeat_at + Duration::seconds(heartbeat.lease_ttl_seconds.max(1));
        if let Some(queue_ownership) = heartbeat.queue_ownership {
            node.queue_ownership = queue_ownership;
        }
        if let Some(capability_tags) = heartbeat.capability_tags {
            node.capability_tags = capability_tags;
        }
        if heartbeat.metadata.is_some() {
            node.metadata = heartbeat.metadata;
        }
        node.updated_at = heartbeat.heartbeat_at;

        self.save_record(node.clone().into()).await?;
        Ok(Some(node))
    }

    async fn get(&self, node_id: &str) -> Result<Option<ClusterNode>> {
        self.load_record(node_id)
            .await?
            .map(ClusterNode::try_from)
            .transpose()
    }

    async fn list(&self) -> Result<Vec<ClusterNode>> {
        let mut response = match self
            .db
            .query("SELECT * FROM type::table($table)")
            .bind(("table", self.table.clone()))
            .await
        {
            Ok(response) => response,
            Err(err) => {
                let message = err.to_string();
                if message.contains("does not exist") && message.contains(&self.table) {
                    return Ok(Vec::new());
                }
                return Err(Self::port_err("list cluster nodes", err));
            }
        };

        let rows: Vec<ClusterNodeRecord> = match response.take(0) {
            Ok(rows) => rows,
            Err(err) => {
                let message = err.to_string();
                if message.contains("does not exist") && message.contains(&self.table) {
                    return Ok(Vec::new());
                }
                return Err(Self::port_err("decode cluster nodes", err));
            }
        };

        let mut nodes = Vec::with_capacity(rows.len());
        for row in rows {
            nodes.push(ClusterNode::try_from(row)?);
        }
        nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        Ok(nodes)
    }

    async fn remove(&self, node_id: &str) -> Result<bool> {
        let mut response = match self
            .db
            .query("DELETE type::record($table, $id) RETURN BEFORE")
            .bind(("table", self.table.clone()))
            .bind(("id", node_id.to_string()))
            .await
        {
            Ok(response) => response,
            Err(err) => {
                let message = err.to_string();
                if message.contains("does not exist") && message.contains(&self.table) {
                    return Ok(false);
                }
                return Err(Self::port_err("remove cluster node", err));
            }
        };

        let deleted: Option<ClusterNodeRecord> = match response.take(0) {
            Ok(value) => value,
            Err(err) => {
                let message = err.to_string();
                if message.contains("does not exist") && message.contains(&self.table) {
                    return Ok(false);
                }
                return Err(Self::port_err("decode removed cluster node", err));
            }
        };

        Ok(deleted.is_some())
    }

    async fn prune_expired(&self, now: DateTime<Utc>) -> Result<u64> {
        let mut response = match self
            .db
            .query("DELETE type::table($table) WHERE lease_expires_at < $now RETURN BEFORE")
            .bind(("table", self.table.clone()))
            .bind(("now", now))
            .await
        {
            Ok(response) => response,
            Err(err) => {
                let message = err.to_string();
                if message.contains("does not exist") && message.contains(&self.table) {
                    return Ok(0);
                }
                return Err(Self::port_err("prune expired cluster nodes", err));
            }
        };

        let deleted: Vec<ClusterNodeRecord> = match response.take(0) {
            Ok(rows) => rows,
            Err(err) => {
                let message = err.to_string();
                if message.contains("does not exist") && message.contains(&self.table) {
                    return Ok(0);
                }
                return Err(Self::port_err("decode pruned cluster nodes", err));
            }
        };

        Ok(deleted.len() as u64)
    }
}
