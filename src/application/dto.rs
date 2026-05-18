use chrono::{DateTime, Utc};

use crate::domain::runtime::cluster_node::{
    ClusterNode, ClusterNodeHealth, ClusterNodeHealthSnapshot, ClusterNodeRole,
    QueueOwnershipMode,
};
use crate::domain::runtime::delivery_endpoint::DeliveryProtocol;

#[derive(Clone, Debug)]
pub struct RegisterAgentRequest {
    pub id: String,
    pub name: String,
    pub system_prompt: String,
}

#[derive(Clone, Debug)]
pub struct InvokeAgentRequest {
    pub agent_id: String,
    pub user_prompt: String,
}

#[derive(Clone, Debug)]
pub struct InvokeAgentResponse {
    pub agent_id: String,
    pub completion: String,
}

#[derive(Clone, Debug)]
pub struct RegisterDeliveryEndpointRequest {
    pub endpoint_id: String,
    pub name: String,
    pub protocol: DeliveryProtocol,
    pub target: String,
    pub metadata: Option<String>,
}

#[derive(Clone, Debug)]
pub struct RegisterDeliveryEndpointResponse {
    pub endpoint_id: String,
    pub enabled: bool,
}

#[derive(Clone, Debug)]
pub struct SetDeliveryEndpointEnabledRequest {
    pub endpoint_id: String,
    pub enabled: bool,
}

#[derive(Clone, Debug, Default)]
pub struct ListEndpointDiagnosticsReadModelRequest {
    pub endpoint_ids: Option<Vec<String>>,
    pub protocol: Option<DeliveryProtocol>,
    pub min_failure_count: Option<u64>,
    pub stale_after_seconds: Option<i64>,
    pub unhealthy_only: bool,
    pub include_disabled: bool,
    pub offset: usize,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct EndpointDiagnosticsReadModelRow {
    pub endpoint_id: String,
    pub endpoint_name: String,
    pub protocol: DeliveryProtocol,
    pub target: String,
    pub enabled: bool,
    pub success_count: u64,
    pub failure_count: u64,
    pub last_event_id: Option<String>,
    pub last_error: Option<String>,
    pub last_success_at: Option<DateTime<Utc>>,
    pub last_failure_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub unhealthy: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EndpointFailureTrendDirection {
    Improving,
    Stable,
    Worsening,
}

#[derive(Clone, Debug)]
pub struct EndpointFailureRateTrendRow {
    pub endpoint_id: String,
    pub endpoint_name: String,
    pub protocol: DeliveryProtocol,
    pub enabled: bool,
    pub success_count: u64,
    pub failure_count: u64,
    pub total_attempts: u64,
    pub failure_rate: f64,
    pub trend: EndpointFailureTrendDirection,
    pub last_success_at: Option<DateTime<Utc>>,
    pub last_failure_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Default)]
pub struct ListTopUnhealthyEndpointsRequest {
    pub protocol: Option<DeliveryProtocol>,
    pub include_disabled: bool,
    pub limit: usize,
}

#[derive(Clone, Debug, Default)]
pub struct ListEndpointFailureRateTrendsRequest {
    pub protocol: Option<DeliveryProtocol>,
    pub include_disabled: bool,
    pub min_total_attempts: Option<u64>,
    pub limit: usize,
}

#[derive(Clone, Debug)]
pub struct PruneEndpointDeliveryStatusesRequest {
    pub updated_before: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct RegisterClusterNodeRequest {
    pub node_id: String,
    pub role: ClusterNodeRole,
    pub region: String,
    pub queue_ownership: Vec<String>,
    pub capability_tags: Vec<String>,
    pub heartbeat_at: DateTime<Utc>,
    pub lease_ttl_seconds: i64,
    pub queue_ownership_mode: Option<QueueOwnershipMode>,
    pub metadata: Option<String>,
}

#[derive(Clone, Debug)]
pub struct HeartbeatClusterNodeRequest {
    pub node_id: String,
    pub heartbeat_at: DateTime<Utc>,
    pub lease_ttl_seconds: i64,
    pub queue_ownership_mode: Option<QueueOwnershipMode>,
    pub queue_ownership: Option<Vec<String>>,
    pub capability_tags: Option<Vec<String>>,
    pub metadata: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct ListClusterNodeHealthRequest {
    pub role: Option<ClusterNodeRole>,
    pub region: Option<String>,
    pub capability_tag: Option<String>,
    pub queue: Option<String>,
    pub health: Option<ClusterNodeHealth>,
    pub offset: usize,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct ListQueueOwnershipHealthRequest {
    pub queue_prefix: Option<String>,
}

#[derive(Clone, Debug)]
pub struct QueueOwnershipHealthRow {
    pub queue: String,
    pub owners: Vec<String>,
    pub healthy_owners: usize,
    pub degraded_owners: usize,
    pub offline_owners: usize,
}

#[derive(Clone, Debug)]
pub struct PruneExpiredClusterNodesRequest {
    pub now: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct RunClusterHeartbeatSweepRequest {
    pub now: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct RunClusterHeartbeatSweepResponse {
    pub pruned_nodes: u64,
    pub emitted_events: u64,
}

#[derive(Clone, Debug)]
pub struct ForwardClusterCommandRequest {
    pub target_region: String,
    pub command_name: String,
    pub payload: String,
    pub correlation_id: Option<String>,
    pub issued_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct ForwardClusterCommandResponse {
    pub accepted: bool,
}

#[derive(Clone, Debug)]
pub struct InitiateCoordinatorHandoffRequest {
    pub target_region: String,
    pub coordinator_node_id: String,
    pub queue_scope: Option<Vec<String>>,
    pub reason: Option<String>,
    pub correlation_id: Option<String>,
    pub issued_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct InitiateCoordinatorHandoffResponse {
    pub accepted: bool,
    pub command_name: String,
}

#[derive(Clone, Debug)]
pub struct InitiateCoordinatorFailoverRequest {
    pub target_region: String,
    pub coordinator_node_id: String,
    pub failover_to_node_id: Option<String>,
    pub queue_scope: Option<Vec<String>>,
    pub reason: Option<String>,
    pub correlation_id: Option<String>,
    pub issued_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct InitiateCoordinatorFailoverResponse {
    pub accepted: bool,
    pub command_name: String,
}

#[derive(Clone, Debug)]
pub struct RebalanceQueueOwnershipRequest {
    pub target_region: String,
    pub queue: String,
    pub desired_owners: Vec<String>,
    pub strategy: Option<String>,
    pub reason: Option<String>,
    pub correlation_id: Option<String>,
    pub issued_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct RebalanceQueueOwnershipResponse {
    pub accepted: bool,
    pub command_name: String,
}

#[derive(Clone, Debug, Default)]
pub struct ListClusterForwardOutcomesRequest {
    pub target_region: Option<String>,
    pub command_name: Option<String>,
    pub accepted: Option<bool>,
    pub limit: usize,
}

#[derive(Clone, Debug)]
pub struct ClusterForwardOutcomeRow {
    pub target_region: String,
    pub command_name: String,
    pub correlation_id: Option<String>,
    pub accepted: bool,
    pub attempts: u32,
    pub error: Option<String>,
    pub completed_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct ClusterNodeHealthRow {
    pub snapshot: ClusterNodeHealthSnapshot,
}

impl ClusterNodeHealthRow {
    pub fn node(&self) -> &ClusterNode {
        &self.snapshot.node
    }
}
