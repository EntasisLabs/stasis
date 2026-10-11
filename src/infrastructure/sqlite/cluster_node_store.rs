use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::cluster_node::{
    ClusterNode, ClusterNodeHeartbeat, ClusterNodeRole, NewClusterNode,
};
use crate::ports::outbound::runtime::cluster_node_store::ClusterNodeStore;

use super::codec::{decode_string_list, decode_time, encode_string_list, encode_time, sql_err};
use super::session::SqliteSession;

#[derive(Clone)]
pub struct SqliteClusterNodeStore {
    session: SqliteSession,
}

impl SqliteClusterNodeStore {
    pub fn new(session: SqliteSession) -> Self {
        Self { session }
    }

    fn role_str(role: ClusterNodeRole) -> &'static str {
        match role {
            ClusterNodeRole::Coordinator => "coordinator",
            ClusterNodeRole::Scheduler => "scheduler",
            ClusterNodeRole::Worker => "worker",
        }
    }

    fn parse_role(raw: &str) -> Result<ClusterNodeRole> {
        match raw {
            "coordinator" => Ok(ClusterNodeRole::Coordinator),
            "scheduler" => Ok(ClusterNodeRole::Scheduler),
            "worker" => Ok(ClusterNodeRole::Worker),
            other => Err(sql_err("decode cluster node role", other)),
        }
    }

    fn map_node(row: &rusqlite::Row<'_>) -> rusqlite::Result<ClusterNode> {
        let role_raw: String = row.get(1)?;
        let queues: String = row.get(3)?;
        let tags: String = row.get(4)?;
        let heartbeat_at: i64 = row.get(5)?;
        let lease_expires_at: i64 = row.get(6)?;
        let created_at: i64 = row.get(8)?;
        let updated_at: i64 = row.get(9)?;
        let conv = |err: StasisError| {
            rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(err.to_string())))
        };
        Ok(ClusterNode {
            node_id: row.get(0)?,
            role: Self::parse_role(&role_raw).map_err(conv)?,
            region: row.get(2)?,
            queue_ownership: decode_string_list(&queues).map_err(conv)?,
            capability_tags: decode_string_list(&tags).map_err(conv)?,
            heartbeat_at: decode_time(heartbeat_at).map_err(conv)?,
            lease_expires_at: decode_time(lease_expires_at).map_err(conv)?,
            metadata: row.get(7)?,
            created_at: decode_time(created_at).map_err(conv)?,
            updated_at: decode_time(updated_at).map_err(conv)?,
        })
    }

    fn save(&self, node: &ClusterNode) -> Result<()> {
        self.session.execute(
            "INSERT INTO cluster_node (
                node_id, role, region, queue_ownership_json, capability_tags_json,
                heartbeat_at, lease_expires_at, metadata, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
             ON CONFLICT(node_id) DO UPDATE SET
                role = excluded.role,
                region = excluded.region,
                queue_ownership_json = excluded.queue_ownership_json,
                capability_tags_json = excluded.capability_tags_json,
                heartbeat_at = excluded.heartbeat_at,
                lease_expires_at = excluded.lease_expires_at,
                metadata = excluded.metadata,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at",
            rusqlite::params![
                node.node_id,
                Self::role_str(node.role),
                node.region,
                encode_string_list(&node.queue_ownership)?,
                encode_string_list(&node.capability_tags)?,
                encode_time(node.heartbeat_at),
                encode_time(node.lease_expires_at),
                node.metadata,
                encode_time(node.created_at),
                encode_time(node.updated_at),
            ],
        )?;
        Ok(())
    }
}

#[async_trait]
impl ClusterNodeStore for SqliteClusterNodeStore {
    async fn register(&self, node: NewClusterNode) -> Result<ClusterNode> {
        let record = node.into_record();
        if self.get(&record.node_id).await?.is_some() {
            return Err(StasisError::PortFailure(format!(
                "cluster node already exists: {}",
                record.node_id
            )));
        }
        self.save(&record)?;
        Ok(record)
    }

    async fn heartbeat(&self, heartbeat: ClusterNodeHeartbeat) -> Result<Option<ClusterNode>> {
        let Some(mut node) = self.get(&heartbeat.node_id).await? else {
            return Ok(None);
        };
        node.heartbeat_at = heartbeat.heartbeat_at;
        node.lease_expires_at =
            heartbeat.heartbeat_at + Duration::seconds(heartbeat.lease_ttl_seconds.max(1));
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
        self.save(&node)?;
        Ok(Some(node))
    }

    async fn get(&self, node_id: &str) -> Result<Option<ClusterNode>> {
        self.session.query_opt(
            "SELECT node_id, role, region, queue_ownership_json, capability_tags_json,
                    heartbeat_at, lease_expires_at, metadata, created_at, updated_at
             FROM cluster_node WHERE node_id = ?1",
            rusqlite::params![node_id],
            Self::map_node,
        )
    }

    async fn list(&self) -> Result<Vec<ClusterNode>> {
        let mut nodes = self.session.query_map(
            "SELECT node_id, role, region, queue_ownership_json, capability_tags_json,
                    heartbeat_at, lease_expires_at, metadata, created_at, updated_at
             FROM cluster_node ORDER BY node_id ASC",
            [],
            Self::map_node,
        )?;
        nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        Ok(nodes)
    }

    async fn remove(&self, node_id: &str) -> Result<bool> {
        Ok(self.session.execute(
            "DELETE FROM cluster_node WHERE node_id = ?1",
            rusqlite::params![node_id],
        )? > 0)
    }

    async fn prune_expired(&self, now: DateTime<Utc>) -> Result<u64> {
        let deleted = self.session.execute(
            "DELETE FROM cluster_node WHERE lease_expires_at < ?1",
            rusqlite::params![encode_time(now)],
        )?;
        Ok(deleted as u64)
    }
}
