use async_trait::async_trait;

use crate::application::dto::{
    ClusterForwardOutcomeRow, ClusterNodeHealthRow,
    ForwardClusterCommandRequest,
    ForwardClusterCommandResponse, HeartbeatClusterNodeRequest,
    InitiateCoordinatorFailoverRequest,
    InitiateCoordinatorFailoverResponse,
    InitiateCoordinatorHandoffRequest,
    InitiateCoordinatorHandoffResponse,
    EndpointDiagnosticsReadModelRow, ListEndpointDiagnosticsReadModelRequest,
    EndpointFailureRateTrendRow, ListEndpointFailureRateTrendsRequest,
    ListClusterForwardOutcomesRequest,
    ListClusterNodeHealthRequest, ListQueueOwnershipHealthRequest,
    ListTopUnhealthyEndpointsRequest, PruneEndpointDeliveryStatusesRequest,
    PruneExpiredClusterNodesRequest, QueueOwnershipHealthRow,
    RebalanceQueueOwnershipRequest, RebalanceQueueOwnershipResponse,
    RunClusterHeartbeatSweepRequest, RunClusterHeartbeatSweepResponse,
    RegisterClusterNodeRequest,
    RegisterDeliveryEndpointRequest, RegisterDeliveryEndpointResponse,
    SetDeliveryEndpointEnabledRequest,
};
use crate::domain::errors::Result;
use crate::domain::runtime::cluster_node::ClusterNode;
use crate::domain::runtime::delivery_endpoint::DeliveryEndpoint;
use crate::domain::runtime::endpoint_delivery_status::EndpointDeliveryStatus;

#[async_trait]
pub trait ControlPlaneCommands {
    async fn register_delivery_endpoint(
        &self,
        request: RegisterDeliveryEndpointRequest,
    ) -> Result<RegisterDeliveryEndpointResponse>;
    async fn set_delivery_endpoint_enabled(
        &self,
        request: SetDeliveryEndpointEnabledRequest,
    ) -> Result<()>;
    async fn list_delivery_endpoints(&self) -> Result<Vec<DeliveryEndpoint>>;
    async fn get_endpoint_delivery_status(
        &self,
        endpoint_id: &str,
    ) -> Result<Option<EndpointDeliveryStatus>>;
    async fn list_endpoint_delivery_statuses(&self) -> Result<Vec<EndpointDeliveryStatus>>;
    async fn list_endpoint_diagnostics_read_model(
        &self,
        request: ListEndpointDiagnosticsReadModelRequest,
    ) -> Result<Vec<EndpointDiagnosticsReadModelRow>>;
    async fn list_top_unhealthy_endpoints(
        &self,
        request: ListTopUnhealthyEndpointsRequest,
    ) -> Result<Vec<EndpointDiagnosticsReadModelRow>>;
    async fn list_endpoint_failure_rate_trends(
        &self,
        request: ListEndpointFailureRateTrendsRequest,
    ) -> Result<Vec<EndpointFailureRateTrendRow>>;
    async fn prune_endpoint_delivery_statuses(
        &self,
        request: PruneEndpointDeliveryStatusesRequest,
    ) -> Result<u64>;
    async fn register_cluster_node(&self, request: RegisterClusterNodeRequest) -> Result<ClusterNode>;
    async fn heartbeat_cluster_node(&self, request: HeartbeatClusterNodeRequest) -> Result<ClusterNode>;
    async fn list_cluster_node_health(
        &self,
        request: ListClusterNodeHealthRequest,
    ) -> Result<Vec<ClusterNodeHealthRow>>;
    async fn list_queue_ownership_health(
        &self,
        request: ListQueueOwnershipHealthRequest,
    ) -> Result<Vec<QueueOwnershipHealthRow>>;
    async fn prune_expired_cluster_nodes(
        &self,
        request: PruneExpiredClusterNodesRequest,
    ) -> Result<u64>;
    async fn run_cluster_heartbeat_sweep(
        &self,
        request: RunClusterHeartbeatSweepRequest,
    ) -> Result<RunClusterHeartbeatSweepResponse>;
    async fn forward_cluster_command(
        &self,
        request: ForwardClusterCommandRequest,
    ) -> Result<ForwardClusterCommandResponse>;
    async fn initiate_coordinator_handoff(
        &self,
        request: InitiateCoordinatorHandoffRequest,
    ) -> Result<InitiateCoordinatorHandoffResponse>;
    async fn list_cluster_forward_outcomes(
        &self,
        request: ListClusterForwardOutcomesRequest,
    ) -> Result<Vec<ClusterForwardOutcomeRow>>;
    async fn initiate_coordinator_failover(
        &self,
        request: InitiateCoordinatorFailoverRequest,
    ) -> Result<InitiateCoordinatorFailoverResponse>;
    async fn rebalance_queue_ownership(
        &self,
        request: RebalanceQueueOwnershipRequest,
    ) -> Result<RebalanceQueueOwnershipResponse>;
}