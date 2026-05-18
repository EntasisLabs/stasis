use chrono::{DateTime, Duration, Utc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClusterNodeRole {
    Coordinator,
    Scheduler,
    Worker,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueueOwnershipMode {
    MultiOwner,
    SingleOwner,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClusterNodeHealth {
    Healthy,
    Degraded,
    Offline,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterNode {
    pub node_id: String,
    pub role: ClusterNodeRole,
    pub region: String,
    pub queue_ownership: Vec<String>,
    pub capability_tags: Vec<String>,
    pub heartbeat_at: DateTime<Utc>,
    pub lease_expires_at: DateTime<Utc>,
    pub metadata: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewClusterNode {
    pub node_id: String,
    pub role: ClusterNodeRole,
    pub region: String,
    pub queue_ownership: Vec<String>,
    pub capability_tags: Vec<String>,
    pub heartbeat_at: DateTime<Utc>,
    pub lease_ttl_seconds: i64,
    pub metadata: Option<String>,
}

impl NewClusterNode {
    pub fn into_record(self) -> ClusterNode {
        let now = self.heartbeat_at;
        let lease_ttl_seconds = self.lease_ttl_seconds.max(1);
        ClusterNode {
            node_id: self.node_id,
            role: self.role,
            region: self.region,
            queue_ownership: self.queue_ownership,
            capability_tags: self.capability_tags,
            heartbeat_at: now,
            lease_expires_at: now + Duration::seconds(lease_ttl_seconds),
            metadata: self.metadata,
            created_at: now,
            updated_at: now,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterNodeHeartbeat {
    pub node_id: String,
    pub heartbeat_at: DateTime<Utc>,
    pub lease_ttl_seconds: i64,
    pub queue_ownership: Option<Vec<String>>,
    pub capability_tags: Option<Vec<String>>,
    pub metadata: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterNodeHealthSnapshot {
    pub node: ClusterNode,
    pub health: ClusterNodeHealth,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterForwardCommand {
    pub target_region: String,
    pub command_name: String,
    pub payload: String,
    pub correlation_id: Option<String>,
    pub issued_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterForwardOutcome {
    pub target_region: String,
    pub command_name: String,
    pub correlation_id: Option<String>,
    pub accepted: bool,
    pub attempts: u32,
    pub error: Option<String>,
    pub completed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterControlEvent {
    ExpiredNodesPruned {
        pruned_count: u64,
        occurred_at: DateTime<Utc>,
    },
}
