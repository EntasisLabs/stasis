use std::sync::Arc;

use async_trait::async_trait;

use crate::application::dto::{
    ClusterForwardOutcomeRow, ClusterNodeHealthRow,
    ForwardClusterCommandRequest, ForwardClusterCommandResponse,
    HeartbeatClusterNodeRequest, InitiateCoordinatorHandoffRequest,
    InitiateCoordinatorHandoffResponse,
    InitiateCoordinatorFailoverRequest,
    InitiateCoordinatorFailoverResponse,
    EndpointDiagnosticsReadModelRow, EndpointFailureRateTrendRow,
    ListClusterForwardOutcomesRequest,
    ListEndpointDiagnosticsReadModelRequest, ListEndpointFailureRateTrendsRequest,
    ListClusterNodeHealthRequest, ListQueueOwnershipHealthRequest,
    ListTopUnhealthyEndpointsRequest, PruneEndpointDeliveryStatusesRequest,
    PruneExpiredClusterNodesRequest, QueueOwnershipHealthRow,
    RunClusterHeartbeatSweepRequest, RunClusterHeartbeatSweepResponse,
    RebalanceQueueOwnershipRequest, RebalanceQueueOwnershipResponse,
    RegisterClusterNodeRequest,
    RegisterDeliveryEndpointRequest, RegisterDeliveryEndpointResponse,
    SetDeliveryEndpointEnabledRequest,
};
use crate::application::use_cases::manage_cluster_nodes::{
    ForwardClusterControlCommand, HeartbeatClusterNode,
    InitiateCoordinatorFailover, InitiateCoordinatorHandoff,
    ListClusterForwardOutcomes, RebalanceQueueOwnership,
    ListClusterNodeHealth, ListQueueOwnershipHealth,
    PruneExpiredClusterNodes, RegisterClusterNode,
    RunClusterHeartbeatSweep,
};
use crate::application::use_cases::manage_delivery_endpoints::{
    ListDeliveryEndpoints, RegisterDeliveryEndpoint, SetDeliveryEndpointEnabled,
};
use crate::application::use_cases::query_endpoint_delivery_statuses::{
    GetEndpointDeliveryStatus, ListEndpointDeliveryStatuses,
    ListEndpointDiagnosticsReadModel, ListEndpointFailureRateTrends,
    ListTopUnhealthyEndpoints, PruneEndpointDeliveryStatuses,
};
use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::cluster_node::ClusterNode;
use crate::domain::runtime::delivery_endpoint::DeliveryEndpoint;
use crate::domain::runtime::endpoint_delivery_status::EndpointDeliveryStatus;
use crate::ports::inbound::control_plane_commands::ControlPlaneCommands;
use crate::ports::outbound::runtime::cluster_control_event_sink::ClusterControlEventSink;
use crate::ports::outbound::runtime::cluster_command_forwarder::ClusterCommandForwarder;
use crate::ports::outbound::runtime::cluster_forward_outcome_store::ClusterForwardOutcomeStore;
use crate::ports::outbound::runtime::cluster_node_store::ClusterNodeStore;
use crate::ports::outbound::runtime::delivery_endpoint_store::DeliveryEndpointStore;
use crate::ports::outbound::runtime::endpoint_delivery_status_store::EndpointDeliveryStatusStore;

#[derive(Clone)]
pub struct ControlPlaneSdk<S>
where
    S: DeliveryEndpointStore + ClusterNodeStore,
{
    register_delivery_endpoint: RegisterDeliveryEndpoint<S>,
    set_delivery_endpoint_enabled: SetDeliveryEndpointEnabled<S>,
    list_delivery_endpoints: ListDeliveryEndpoints<S>,
    get_endpoint_delivery_status: Option<GetEndpointDeliveryStatus<Arc<dyn EndpointDeliveryStatusStore>>>,
    list_endpoint_delivery_statuses:
        Option<ListEndpointDeliveryStatuses<Arc<dyn EndpointDeliveryStatusStore>>>,
    list_endpoint_diagnostics_read_model:
        Option<ListEndpointDiagnosticsReadModel<S, Arc<dyn EndpointDeliveryStatusStore>>>,
    list_top_unhealthy_endpoints:
        Option<ListTopUnhealthyEndpoints<S, Arc<dyn EndpointDeliveryStatusStore>>>,
    list_endpoint_failure_rate_trends:
        Option<ListEndpointFailureRateTrends<S, Arc<dyn EndpointDeliveryStatusStore>>>,
    prune_endpoint_delivery_statuses:
        Option<PruneEndpointDeliveryStatuses<Arc<dyn EndpointDeliveryStatusStore>>>,
    register_cluster_node: RegisterClusterNode<S>,
    heartbeat_cluster_node: HeartbeatClusterNode<S>,
    list_cluster_node_health: ListClusterNodeHealth<S>,
    list_queue_ownership_health: ListQueueOwnershipHealth<S>,
    prune_expired_cluster_nodes: PruneExpiredClusterNodes<S>,
    run_cluster_heartbeat_sweep:
        Option<RunClusterHeartbeatSweep<S, Arc<dyn ClusterControlEventSink>>>,
    forward_cluster_control_command:
        Option<ForwardClusterControlCommand<Arc<dyn ClusterCommandForwarder>>>,
    initiate_coordinator_handoff:
        Option<InitiateCoordinatorHandoff<Arc<dyn ClusterCommandForwarder>>>,
    initiate_coordinator_failover:
        Option<InitiateCoordinatorFailover<Arc<dyn ClusterCommandForwarder>>>,
    rebalance_queue_ownership:
        Option<RebalanceQueueOwnership<Arc<dyn ClusterCommandForwarder>>>,
    list_cluster_forward_outcomes:
        Option<ListClusterForwardOutcomes<Arc<dyn ClusterForwardOutcomeStore>>>,
}

impl<S> ControlPlaneSdk<S>
where
    S: DeliveryEndpointStore + ClusterNodeStore + Clone,
{
    pub fn new(store: S) -> Self {
        let list_delivery_store = store.clone();
        let register_cluster_store = store.clone();
        let heartbeat_cluster_store = store.clone();
        let list_cluster_health_store = store.clone();
        let list_queue_health_store = store.clone();
        Self {
            register_delivery_endpoint: RegisterDeliveryEndpoint::new(store.clone()),
            set_delivery_endpoint_enabled: SetDeliveryEndpointEnabled::new(store.clone()),
            list_delivery_endpoints: ListDeliveryEndpoints::new(list_delivery_store),
            get_endpoint_delivery_status: None,
            list_endpoint_delivery_statuses: None,
            list_endpoint_diagnostics_read_model: None,
            list_top_unhealthy_endpoints: None,
            list_endpoint_failure_rate_trends: None,
            prune_endpoint_delivery_statuses: None,
            register_cluster_node: RegisterClusterNode::new(register_cluster_store),
            heartbeat_cluster_node: HeartbeatClusterNode::new(heartbeat_cluster_store),
            list_cluster_node_health: ListClusterNodeHealth::new(list_cluster_health_store),
            list_queue_ownership_health: ListQueueOwnershipHealth::new(list_queue_health_store),
            prune_expired_cluster_nodes: PruneExpiredClusterNodes::new(store),
            run_cluster_heartbeat_sweep: None,
            forward_cluster_control_command: None,
            initiate_coordinator_handoff: None,
            initiate_coordinator_failover: None,
            rebalance_queue_ownership: None,
            list_cluster_forward_outcomes: None,
        }
    }

    pub fn new_with_cluster_event_sink(
        store: S,
        cluster_event_sink: Arc<dyn ClusterControlEventSink>,
    ) -> Self {
        let list_delivery_store = store.clone();
        let register_cluster_store = store.clone();
        let heartbeat_cluster_store = store.clone();
        let list_cluster_health_store = store.clone();
        let list_queue_health_store = store.clone();
        let sweep_store = store.clone();
        Self {
            register_delivery_endpoint: RegisterDeliveryEndpoint::new(store.clone()),
            set_delivery_endpoint_enabled: SetDeliveryEndpointEnabled::new(store.clone()),
            list_delivery_endpoints: ListDeliveryEndpoints::new(list_delivery_store),
            get_endpoint_delivery_status: None,
            list_endpoint_delivery_statuses: None,
            list_endpoint_diagnostics_read_model: None,
            list_top_unhealthy_endpoints: None,
            list_endpoint_failure_rate_trends: None,
            prune_endpoint_delivery_statuses: None,
            register_cluster_node: RegisterClusterNode::new(register_cluster_store),
            heartbeat_cluster_node: HeartbeatClusterNode::new(heartbeat_cluster_store),
            list_cluster_node_health: ListClusterNodeHealth::new(list_cluster_health_store),
            list_queue_ownership_health: ListQueueOwnershipHealth::new(list_queue_health_store),
            prune_expired_cluster_nodes: PruneExpiredClusterNodes::new(store),
            run_cluster_heartbeat_sweep: Some(RunClusterHeartbeatSweep::new(
                sweep_store,
                cluster_event_sink,
            )),
            forward_cluster_control_command: None,
            initiate_coordinator_handoff: None,
            initiate_coordinator_failover: None,
            rebalance_queue_ownership: None,
            list_cluster_forward_outcomes: None,
        }
    }

    pub fn new_with_status_store(
        store: S,
        status_store: Arc<dyn EndpointDeliveryStatusStore>,
    ) -> Self {
        let list_store = store.clone();
        let list_delivery_store = store.clone();
        let register_cluster_store = store.clone();
        let heartbeat_cluster_store = store.clone();
        let list_cluster_health_store = store.clone();
        let list_queue_health_store = store.clone();
        Self {
            register_delivery_endpoint: RegisterDeliveryEndpoint::new(store.clone()),
            set_delivery_endpoint_enabled: SetDeliveryEndpointEnabled::new(store.clone()),
            list_delivery_endpoints: ListDeliveryEndpoints::new(list_delivery_store),
            get_endpoint_delivery_status: Some(GetEndpointDeliveryStatus::new(status_store.clone())),
            list_endpoint_delivery_statuses: Some(ListEndpointDeliveryStatuses::new(status_store.clone())),
            list_endpoint_diagnostics_read_model: Some(ListEndpointDiagnosticsReadModel::new(
                list_store.clone(),
                status_store.clone(),
            )),
            list_top_unhealthy_endpoints: Some(ListTopUnhealthyEndpoints::new(
                list_store.clone(),
                status_store.clone(),
            )),
            list_endpoint_failure_rate_trends: Some(ListEndpointFailureRateTrends::new(
                list_store,
                status_store.clone(),
            )),
            prune_endpoint_delivery_statuses: Some(PruneEndpointDeliveryStatuses::new(status_store)),
            register_cluster_node: RegisterClusterNode::new(register_cluster_store),
            heartbeat_cluster_node: HeartbeatClusterNode::new(heartbeat_cluster_store),
            list_cluster_node_health: ListClusterNodeHealth::new(list_cluster_health_store),
            list_queue_ownership_health: ListQueueOwnershipHealth::new(list_queue_health_store),
            prune_expired_cluster_nodes: PruneExpiredClusterNodes::new(store),
            run_cluster_heartbeat_sweep: None,
            forward_cluster_control_command: None,
            initiate_coordinator_handoff: None,
            initiate_coordinator_failover: None,
            rebalance_queue_ownership: None,
            list_cluster_forward_outcomes: None,
        }
    }

    pub fn new_with_status_store_and_cluster_event_sink(
        store: S,
        status_store: Arc<dyn EndpointDeliveryStatusStore>,
        cluster_event_sink: Arc<dyn ClusterControlEventSink>,
    ) -> Self {
        let list_store = store.clone();
        let list_delivery_store = store.clone();
        let register_cluster_store = store.clone();
        let heartbeat_cluster_store = store.clone();
        let list_cluster_health_store = store.clone();
        let list_queue_health_store = store.clone();
        let sweep_store = store.clone();
        Self {
            register_delivery_endpoint: RegisterDeliveryEndpoint::new(store.clone()),
            set_delivery_endpoint_enabled: SetDeliveryEndpointEnabled::new(store.clone()),
            list_delivery_endpoints: ListDeliveryEndpoints::new(list_delivery_store),
            get_endpoint_delivery_status: Some(GetEndpointDeliveryStatus::new(status_store.clone())),
            list_endpoint_delivery_statuses: Some(ListEndpointDeliveryStatuses::new(status_store.clone())),
            list_endpoint_diagnostics_read_model: Some(ListEndpointDiagnosticsReadModel::new(
                list_store.clone(),
                status_store.clone(),
            )),
            list_top_unhealthy_endpoints: Some(ListTopUnhealthyEndpoints::new(
                list_store.clone(),
                status_store.clone(),
            )),
            list_endpoint_failure_rate_trends: Some(ListEndpointFailureRateTrends::new(
                list_store,
                status_store.clone(),
            )),
            prune_endpoint_delivery_statuses: Some(PruneEndpointDeliveryStatuses::new(status_store)),
            register_cluster_node: RegisterClusterNode::new(register_cluster_store),
            heartbeat_cluster_node: HeartbeatClusterNode::new(heartbeat_cluster_store),
            list_cluster_node_health: ListClusterNodeHealth::new(list_cluster_health_store),
            list_queue_ownership_health: ListQueueOwnershipHealth::new(list_queue_health_store),
            prune_expired_cluster_nodes: PruneExpiredClusterNodes::new(store),
            run_cluster_heartbeat_sweep: Some(RunClusterHeartbeatSweep::new(
                sweep_store,
                cluster_event_sink,
            )),
            forward_cluster_control_command: None,
            initiate_coordinator_handoff: None,
            initiate_coordinator_failover: None,
            rebalance_queue_ownership: None,
            list_cluster_forward_outcomes: None,
        }
    }

    pub fn with_cluster_command_forwarder(
        mut self,
        command_forwarder: Arc<dyn ClusterCommandForwarder>,
    ) -> Self {
        self.forward_cluster_control_command =
            Some(ForwardClusterControlCommand::new(command_forwarder.clone()));
        self.initiate_coordinator_handoff =
            Some(InitiateCoordinatorHandoff::new(command_forwarder.clone()));
        self.initiate_coordinator_failover =
            Some(InitiateCoordinatorFailover::new(command_forwarder.clone()));
        self.rebalance_queue_ownership =
            Some(RebalanceQueueOwnership::new(command_forwarder));
        self
    }

    pub fn with_cluster_forward_outcome_store(
        mut self,
        outcome_store: Arc<dyn ClusterForwardOutcomeStore>,
    ) -> Self {
        self.list_cluster_forward_outcomes = Some(ListClusterForwardOutcomes::new(outcome_store));
        self
    }

    pub async fn register_delivery_endpoint(
        &self,
        request: RegisterDeliveryEndpointRequest,
    ) -> Result<RegisterDeliveryEndpointResponse> {
        self.register_delivery_endpoint.execute(request).await
    }

    pub async fn set_delivery_endpoint_enabled(
        &self,
        request: SetDeliveryEndpointEnabledRequest,
    ) -> Result<()> {
        self.set_delivery_endpoint_enabled.execute(request).await
    }

    pub async fn list_delivery_endpoints(&self) -> Result<Vec<DeliveryEndpoint>> {
        self.list_delivery_endpoints.execute().await
    }

    pub async fn get_endpoint_delivery_status(
        &self,
        endpoint_id: &str,
    ) -> Result<Option<EndpointDeliveryStatus>> {
        let Some(use_case) = &self.get_endpoint_delivery_status else {
            return Err(StasisError::PortFailure(
                "endpoint delivery status store is not configured".to_string(),
            ));
        };

        use_case.execute(endpoint_id).await
    }

    pub async fn list_endpoint_delivery_statuses(&self) -> Result<Vec<EndpointDeliveryStatus>> {
        let Some(use_case) = &self.list_endpoint_delivery_statuses else {
            return Err(StasisError::PortFailure(
                "endpoint delivery status store is not configured".to_string(),
            ));
        };

        use_case.execute().await
    }

    pub async fn list_endpoint_diagnostics_read_model(
        &self,
        request: ListEndpointDiagnosticsReadModelRequest,
    ) -> Result<Vec<EndpointDiagnosticsReadModelRow>> {
        let Some(use_case) = &self.list_endpoint_diagnostics_read_model else {
            return Err(StasisError::PortFailure(
                "endpoint delivery status store is not configured".to_string(),
            ));
        };

        use_case.execute(&request).await
    }

    pub async fn list_top_unhealthy_endpoints(
        &self,
        request: ListTopUnhealthyEndpointsRequest,
    ) -> Result<Vec<EndpointDiagnosticsReadModelRow>> {
        let Some(use_case) = &self.list_top_unhealthy_endpoints else {
            return Err(StasisError::PortFailure(
                "endpoint delivery status store is not configured".to_string(),
            ));
        };

        use_case.execute(&request).await
    }

    pub async fn list_endpoint_failure_rate_trends(
        &self,
        request: ListEndpointFailureRateTrendsRequest,
    ) -> Result<Vec<EndpointFailureRateTrendRow>> {
        let Some(use_case) = &self.list_endpoint_failure_rate_trends else {
            return Err(StasisError::PortFailure(
                "endpoint delivery status store is not configured".to_string(),
            ));
        };

        use_case.execute(&request).await
    }

    pub async fn prune_endpoint_delivery_statuses(
        &self,
        request: PruneEndpointDeliveryStatusesRequest,
    ) -> Result<u64> {
        let Some(use_case) = &self.prune_endpoint_delivery_statuses else {
            return Err(StasisError::PortFailure(
                "endpoint delivery status store is not configured".to_string(),
            ));
        };

        use_case.execute(request.updated_before).await
    }

    pub async fn register_cluster_node(&self, request: RegisterClusterNodeRequest) -> Result<ClusterNode> {
        self.register_cluster_node.execute(request).await
    }

    pub async fn heartbeat_cluster_node(&self, request: HeartbeatClusterNodeRequest) -> Result<ClusterNode> {
        self.heartbeat_cluster_node.execute(request).await
    }

    pub async fn list_cluster_node_health(
        &self,
        request: ListClusterNodeHealthRequest,
    ) -> Result<Vec<ClusterNodeHealthRow>> {
        self.list_cluster_node_health.execute(&request).await
    }

    pub async fn list_queue_ownership_health(
        &self,
        request: ListQueueOwnershipHealthRequest,
    ) -> Result<Vec<QueueOwnershipHealthRow>> {
        self.list_queue_ownership_health.execute(&request).await
    }

    pub async fn prune_expired_cluster_nodes(
        &self,
        request: PruneExpiredClusterNodesRequest,
    ) -> Result<u64> {
        self.prune_expired_cluster_nodes.execute(request).await
    }

    pub async fn run_cluster_heartbeat_sweep(
        &self,
        request: RunClusterHeartbeatSweepRequest,
    ) -> Result<RunClusterHeartbeatSweepResponse> {
        let Some(use_case) = &self.run_cluster_heartbeat_sweep else {
            return Err(StasisError::PortFailure(
                "cluster control event sink is not configured".to_string(),
            ));
        };

        use_case.execute(request).await
    }

    pub async fn forward_cluster_command(
        &self,
        request: ForwardClusterCommandRequest,
    ) -> Result<ForwardClusterCommandResponse> {
        let Some(use_case) = &self.forward_cluster_control_command else {
            return Err(StasisError::PortFailure(
                "cluster command forwarder is not configured".to_string(),
            ));
        };

        use_case.execute(request).await
    }

    pub async fn initiate_coordinator_handoff(
        &self,
        request: InitiateCoordinatorHandoffRequest,
    ) -> Result<InitiateCoordinatorHandoffResponse> {
        let Some(use_case) = &self.initiate_coordinator_handoff else {
            return Err(StasisError::PortFailure(
                "cluster command forwarder is not configured".to_string(),
            ));
        };

        use_case.execute(request).await
    }

    pub async fn list_cluster_forward_outcomes(
        &self,
        request: ListClusterForwardOutcomesRequest,
    ) -> Result<Vec<ClusterForwardOutcomeRow>> {
        let Some(use_case) = &self.list_cluster_forward_outcomes else {
            return Err(StasisError::PortFailure(
                "cluster forward outcome store is not configured".to_string(),
            ));
        };

        use_case.execute(&request).await
    }

    pub async fn initiate_coordinator_failover(
        &self,
        request: InitiateCoordinatorFailoverRequest,
    ) -> Result<InitiateCoordinatorFailoverResponse> {
        let Some(use_case) = &self.initiate_coordinator_failover else {
            return Err(StasisError::PortFailure(
                "cluster command forwarder is not configured".to_string(),
            ));
        };

        use_case.execute(request).await
    }

    pub async fn rebalance_queue_ownership(
        &self,
        request: RebalanceQueueOwnershipRequest,
    ) -> Result<RebalanceQueueOwnershipResponse> {
        let Some(use_case) = &self.rebalance_queue_ownership else {
            return Err(StasisError::PortFailure(
                "cluster command forwarder is not configured".to_string(),
            ));
        };

        use_case.execute(request).await
    }
}

#[async_trait]
impl<S> ControlPlaneCommands for ControlPlaneSdk<S>
where
    S: DeliveryEndpointStore + ClusterNodeStore + Clone + Send + Sync,
{
    async fn register_delivery_endpoint(
        &self,
        request: RegisterDeliveryEndpointRequest,
    ) -> Result<RegisterDeliveryEndpointResponse> {
        self.register_delivery_endpoint.execute(request).await
    }

    async fn set_delivery_endpoint_enabled(
        &self,
        request: SetDeliveryEndpointEnabledRequest,
    ) -> Result<()> {
        self.set_delivery_endpoint_enabled.execute(request).await
    }

    async fn list_delivery_endpoints(&self) -> Result<Vec<DeliveryEndpoint>> {
        self.list_delivery_endpoints.execute().await
    }

    async fn get_endpoint_delivery_status(
        &self,
        endpoint_id: &str,
    ) -> Result<Option<EndpointDeliveryStatus>> {
        self.get_endpoint_delivery_status(endpoint_id).await
    }

    async fn list_endpoint_delivery_statuses(&self) -> Result<Vec<EndpointDeliveryStatus>> {
        self.list_endpoint_delivery_statuses().await
    }

    async fn list_endpoint_diagnostics_read_model(
        &self,
        request: ListEndpointDiagnosticsReadModelRequest,
    ) -> Result<Vec<EndpointDiagnosticsReadModelRow>> {
        self.list_endpoint_diagnostics_read_model(request).await
    }

    async fn list_top_unhealthy_endpoints(
        &self,
        request: ListTopUnhealthyEndpointsRequest,
    ) -> Result<Vec<EndpointDiagnosticsReadModelRow>> {
        self.list_top_unhealthy_endpoints(request).await
    }

    async fn list_endpoint_failure_rate_trends(
        &self,
        request: ListEndpointFailureRateTrendsRequest,
    ) -> Result<Vec<EndpointFailureRateTrendRow>> {
        self.list_endpoint_failure_rate_trends(request).await
    }

    async fn prune_endpoint_delivery_statuses(
        &self,
        request: PruneEndpointDeliveryStatusesRequest,
    ) -> Result<u64> {
        self.prune_endpoint_delivery_statuses(request).await
    }

    async fn register_cluster_node(&self, request: RegisterClusterNodeRequest) -> Result<ClusterNode> {
        self.register_cluster_node(request).await
    }

    async fn heartbeat_cluster_node(&self, request: HeartbeatClusterNodeRequest) -> Result<ClusterNode> {
        self.heartbeat_cluster_node(request).await
    }

    async fn list_cluster_node_health(
        &self,
        request: ListClusterNodeHealthRequest,
    ) -> Result<Vec<ClusterNodeHealthRow>> {
        self.list_cluster_node_health(request).await
    }

    async fn list_queue_ownership_health(
        &self,
        request: ListQueueOwnershipHealthRequest,
    ) -> Result<Vec<QueueOwnershipHealthRow>> {
        self.list_queue_ownership_health(request).await
    }

    async fn prune_expired_cluster_nodes(
        &self,
        request: PruneExpiredClusterNodesRequest,
    ) -> Result<u64> {
        self.prune_expired_cluster_nodes(request).await
    }

    async fn run_cluster_heartbeat_sweep(
        &self,
        request: RunClusterHeartbeatSweepRequest,
    ) -> Result<RunClusterHeartbeatSweepResponse> {
        self.run_cluster_heartbeat_sweep(request).await
    }

    async fn forward_cluster_command(
        &self,
        request: ForwardClusterCommandRequest,
    ) -> Result<ForwardClusterCommandResponse> {
        self.forward_cluster_command(request).await
    }

    async fn initiate_coordinator_handoff(
        &self,
        request: InitiateCoordinatorHandoffRequest,
    ) -> Result<InitiateCoordinatorHandoffResponse> {
        self.initiate_coordinator_handoff(request).await
    }

    async fn list_cluster_forward_outcomes(
        &self,
        request: ListClusterForwardOutcomesRequest,
    ) -> Result<Vec<ClusterForwardOutcomeRow>> {
        self.list_cluster_forward_outcomes(request).await
    }

    async fn initiate_coordinator_failover(
        &self,
        request: InitiateCoordinatorFailoverRequest,
    ) -> Result<InitiateCoordinatorFailoverResponse> {
        self.initiate_coordinator_failover(request).await
    }

    async fn rebalance_queue_ownership(
        &self,
        request: RebalanceQueueOwnershipRequest,
    ) -> Result<RebalanceQueueOwnershipResponse> {
        self.rebalance_queue_ownership(request).await
    }
}

#[cfg(test)]
mod tests {
    use crate::application::dto::{
        ForwardClusterCommandRequest, HeartbeatClusterNodeRequest,
        InitiateCoordinatorFailoverRequest, InitiateCoordinatorHandoffRequest,
        ListClusterForwardOutcomesRequest, RebalanceQueueOwnershipRequest,
        ListClusterNodeHealthRequest,
        ListQueueOwnershipHealthRequest,
        ListEndpointDiagnosticsReadModelRequest, ListEndpointFailureRateTrendsRequest,
        ListTopUnhealthyEndpointsRequest, PruneEndpointDeliveryStatusesRequest,
        PruneExpiredClusterNodesRequest, RegisterClusterNodeRequest,
        RunClusterHeartbeatSweepRequest,
        RegisterDeliveryEndpointRequest, SetDeliveryEndpointEnabledRequest,
    };
    use crate::domain::runtime::cluster_node::{
        ClusterControlEvent, ClusterNodeHealth, ClusterNodeRole, QueueOwnershipMode,
    };
    use crate::domain::runtime::delivery_endpoint::DeliveryProtocol;
    use crate::infrastructure::runtime::composite_control_plane_store::CompositeControlPlaneStore;
    use crate::infrastructure::runtime::in_memory_cluster_control_event_sink::InMemoryClusterControlEventSink;
    use crate::infrastructure::runtime::in_memory_cluster_command_forwarder::InMemoryClusterCommandForwarder;
    use crate::infrastructure::runtime::in_memory_cluster_forward_outcome_store::InMemoryClusterForwardOutcomeStore;
    use crate::infrastructure::runtime::in_memory_cluster_node_store::InMemoryClusterNodeStore;
    use crate::infrastructure::runtime::in_memory_delivery_endpoint_store::InMemoryDeliveryEndpointStore;
    use crate::infrastructure::runtime::in_memory_endpoint_delivery_status_store::InMemoryEndpointDeliveryStatusStore;
    use crate::ports::outbound::runtime::cluster_forward_outcome_store::ClusterForwardOutcomeStore;
    use crate::ports::outbound::runtime::endpoint_delivery_status_store::EndpointDeliveryStatusStore;
    use chrono::Utc;
    use std::sync::Arc;

    use super::ControlPlaneSdk;

    fn store() -> CompositeControlPlaneStore<InMemoryDeliveryEndpointStore, InMemoryClusterNodeStore> {
        CompositeControlPlaneStore::new(
            InMemoryDeliveryEndpointStore::default(),
            InMemoryClusterNodeStore::default(),
        )
    }

    #[tokio::test]
    async fn control_plane_sdk_manages_endpoint_registry() {
        let sdk = ControlPlaneSdk::new(store());

        sdk.register_delivery_endpoint(RegisterDeliveryEndpointRequest {
            endpoint_id: "endpoint.billing.kafka".to_string(),
            name: "Billing Kafka".to_string(),
            protocol: DeliveryProtocol::Kafka,
            target: "kafka://broker:9092/billing.events".to_string(),
            metadata: None,
        })
        .await
        .expect("registration should succeed");

        sdk.set_delivery_endpoint_enabled(SetDeliveryEndpointEnabledRequest {
            endpoint_id: "endpoint.billing.kafka".to_string(),
            enabled: false,
        })
        .await
        .expect("toggle should succeed");

        let endpoints = sdk
            .list_delivery_endpoints()
            .await
            .expect("list should succeed");
        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].endpoint_id, "endpoint.billing.kafka");
        assert!(!endpoints[0].enabled);

        let node = sdk
            .register_cluster_node(RegisterClusterNodeRequest {
                node_id: "node.worker.1".to_string(),
                role: ClusterNodeRole::Worker,
                region: "us-east".to_string(),
                queue_ownership: vec!["default".to_string()],
                capability_tags: vec!["gpu".to_string()],
                heartbeat_at: Utc::now(),
                lease_ttl_seconds: 30,
                queue_ownership_mode: Some(QueueOwnershipMode::SingleOwner),
                metadata: None,
            })
            .await
            .expect("cluster node registration should succeed");
        assert_eq!(node.node_id, "node.worker.1");

        let _ = sdk
            .heartbeat_cluster_node(HeartbeatClusterNodeRequest {
                node_id: "node.worker.1".to_string(),
                heartbeat_at: Utc::now(),
                lease_ttl_seconds: 30,
                queue_ownership_mode: Some(QueueOwnershipMode::SingleOwner),
                queue_ownership: Some(vec!["priority".to_string()]),
                capability_tags: None,
                metadata: Some("v2".to_string()),
            })
            .await
            .expect("cluster heartbeat should succeed");

        let health = sdk
            .list_cluster_node_health(ListClusterNodeHealthRequest {
                role: Some(ClusterNodeRole::Worker),
                region: Some("us-east".to_string()),
                capability_tag: Some("gpu".to_string()),
                queue: Some("priority".to_string()),
                health: Some(ClusterNodeHealth::Healthy),
                offset: 0,
                limit: Some(10),
            })
            .await
            .expect("cluster health list should succeed");
        assert_eq!(health.len(), 1);

        let queue_health = sdk
            .list_queue_ownership_health(ListQueueOwnershipHealthRequest {
                queue_prefix: Some("prio".to_string()),
            })
            .await
            .expect("queue ownership health should succeed");
        assert_eq!(queue_health.len(), 1);

        let pruned = sdk
            .prune_expired_cluster_nodes(PruneExpiredClusterNodesRequest {
                now: Utc::now() + chrono::Duration::hours(1),
            })
            .await
            .expect("cluster prune should succeed");
        assert_eq!(pruned, 1);
    }

    #[tokio::test]
    async fn control_plane_sdk_lists_endpoint_delivery_statuses_when_configured() {
        let status_store = Arc::new(InMemoryEndpointDeliveryStatusStore::default());
        status_store
            .record_success("endpoint.billing.kafka", "evt-1", Utc::now())
            .await
            .expect("status record should succeed");

        let sdk = ControlPlaneSdk::new_with_status_store(
            store(),
            status_store,
        );

        let statuses = sdk
            .list_endpoint_delivery_statuses()
            .await
            .expect("list statuses should succeed");

        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].endpoint_id, "endpoint.billing.kafka");

        sdk.register_delivery_endpoint(RegisterDeliveryEndpointRequest {
            endpoint_id: "endpoint.billing.kafka".to_string(),
            name: "Billing Kafka".to_string(),
            protocol: DeliveryProtocol::Kafka,
            target: "kafka://broker:9092/billing.events".to_string(),
            metadata: None,
        })
        .await
        .expect("registration should succeed");

        let rows = sdk
            .list_endpoint_diagnostics_read_model(ListEndpointDiagnosticsReadModelRequest {
                unhealthy_only: false,
                include_disabled: true,
                offset: 0,
                limit: Some(10),
                ..Default::default()
            })
            .await
            .expect("read model list should succeed");
        assert_eq!(rows.len(), 1);

        let unhealthy = sdk
            .list_top_unhealthy_endpoints(ListTopUnhealthyEndpointsRequest {
                protocol: None,
                include_disabled: true,
                limit: 5,
            })
            .await
            .expect("top unhealthy should succeed");
        assert_eq!(unhealthy.len(), 0);

        let trends = sdk
            .list_endpoint_failure_rate_trends(ListEndpointFailureRateTrendsRequest {
                protocol: None,
                include_disabled: true,
                min_total_attempts: Some(1),
                limit: 5,
            })
            .await
            .expect("failure trend should succeed");
        assert_eq!(trends.len(), 1);

        let deleted = sdk
            .prune_endpoint_delivery_statuses(PruneEndpointDeliveryStatusesRequest {
                updated_before: Utc::now() + chrono::Duration::seconds(1),
            })
            .await
            .expect("prune should succeed");
        assert_eq!(deleted, 1);
    }

    #[tokio::test]
    async fn control_plane_sdk_errors_when_status_store_is_not_configured() {
        let sdk = ControlPlaneSdk::new(store());

        let err = sdk
            .get_endpoint_delivery_status("endpoint.missing")
            .await
            .expect_err("expected status store configuration error");

        assert!(
            err.to_string()
                .contains("endpoint delivery status store is not configured")
        );
    }

    #[tokio::test]
    async fn control_plane_sdk_runs_cluster_heartbeat_sweep_with_events() {
        let event_sink = Arc::new(InMemoryClusterControlEventSink::default());
        let sdk = ControlPlaneSdk::new_with_cluster_event_sink(store(), event_sink.clone());

        sdk.register_cluster_node(RegisterClusterNodeRequest {
            node_id: "node.worker.sweep".to_string(),
            role: ClusterNodeRole::Worker,
            region: "us-east".to_string(),
            queue_ownership: vec!["default".to_string()],
            capability_tags: vec![],
            heartbeat_at: Utc::now() - chrono::Duration::hours(2),
            lease_ttl_seconds: 5,
            queue_ownership_mode: Some(QueueOwnershipMode::SingleOwner),
            metadata: None,
        })
        .await
        .expect("registration should succeed");

        let response = sdk
            .run_cluster_heartbeat_sweep(RunClusterHeartbeatSweepRequest { now: Utc::now() })
            .await
            .expect("sweep should succeed");
        assert_eq!(response.pruned_nodes, 1);
        assert_eq!(response.emitted_events, 1);

        let events = event_sink.events().expect("event list should succeed");
        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            ClusterControlEvent::ExpiredNodesPruned { pruned_count: 1, .. }
        ));
    }

    #[tokio::test]
    async fn control_plane_sdk_forwards_cluster_commands_when_configured() {
        let forwarder = Arc::new(InMemoryClusterCommandForwarder::default());
        let sdk = ControlPlaneSdk::new(store()).with_cluster_command_forwarder(forwarder.clone());

        let response = sdk
            .forward_cluster_command(ForwardClusterCommandRequest {
                target_region: "eu-west".to_string(),
                command_name: "scheduler.pause_queue".to_string(),
                payload: "{\"queue\":\"default\"}".to_string(),
                correlation_id: Some("cmd-123".to_string()),
                issued_at: Utc::now(),
            })
            .await
            .expect("forward should succeed");

        assert!(response.accepted);
        let commands = forwarder
            .forwarded_commands()
            .expect("commands list should succeed");
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].target_region, "eu-west");
        assert_eq!(commands[0].command_name, "scheduler.pause_queue");
    }

    #[tokio::test]
    async fn control_plane_sdk_errors_when_cluster_forwarder_is_not_configured() {
        let sdk = ControlPlaneSdk::new(store());

        let err = sdk
            .forward_cluster_command(ForwardClusterCommandRequest {
                target_region: "us-east".to_string(),
                command_name: "scheduler.resume_queue".to_string(),
                payload: "{\"queue\":\"default\"}".to_string(),
                correlation_id: None,
                issued_at: Utc::now(),
            })
            .await
            .expect_err("expected cluster forwarder configuration error");

        assert!(
            err.to_string()
                .contains("cluster command forwarder is not configured")
        );
    }

    #[tokio::test]
    async fn control_plane_sdk_initiates_coordinator_handoff_when_configured() {
        let forwarder = Arc::new(InMemoryClusterCommandForwarder::default());
        let sdk = ControlPlaneSdk::new(store()).with_cluster_command_forwarder(forwarder.clone());

        let response = sdk
            .initiate_coordinator_handoff(InitiateCoordinatorHandoffRequest {
                target_region: "us-west".to_string(),
                coordinator_node_id: "node.coordinator.1".to_string(),
                queue_scope: Some(vec!["default".to_string(), "priority".to_string()]),
                reason: Some("planned-maintenance".to_string()),
                correlation_id: Some("handoff-1".to_string()),
                issued_at: Utc::now(),
            })
            .await
            .expect("handoff should succeed");

        assert!(response.accepted);
        assert_eq!(response.command_name, "coordinator.handoff");

        let commands = forwarder
            .forwarded_commands()
            .expect("commands list should succeed");
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].command_name, "coordinator.handoff");
        assert!(commands[0].payload.contains("node.coordinator.1"));
    }

    #[tokio::test]
    async fn control_plane_sdk_lists_cluster_forward_outcomes_when_configured() {
        let outcomes = Arc::new(InMemoryClusterForwardOutcomeStore::default());
        outcomes
            .record(crate::domain::runtime::cluster_node::ClusterForwardOutcome {
                target_region: "eu-west".to_string(),
                command_name: "coordinator.handoff".to_string(),
                correlation_id: Some("handoff-2".to_string()),
                accepted: true,
                attempts: 2,
                error: None,
                completed_at: Utc::now(),
            })
            .await
            .expect("record should succeed");

        let sdk = ControlPlaneSdk::new(store()).with_cluster_forward_outcome_store(outcomes);

        let rows = sdk
            .list_cluster_forward_outcomes(ListClusterForwardOutcomesRequest {
                target_region: Some("eu-west".to_string()),
                command_name: Some("coordinator.handoff".to_string()),
                accepted: Some(true),
                limit: 10,
            })
            .await
            .expect("list outcomes should succeed");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].command_name, "coordinator.handoff");
        assert_eq!(rows[0].attempts, 2);
    }

    #[tokio::test]
    async fn control_plane_sdk_initiates_coordinator_failover_when_configured() {
        let forwarder = Arc::new(InMemoryClusterCommandForwarder::default());
        let sdk = ControlPlaneSdk::new(store()).with_cluster_command_forwarder(forwarder.clone());

        let response = sdk
            .initiate_coordinator_failover(InitiateCoordinatorFailoverRequest {
                target_region: "us-central".to_string(),
                coordinator_node_id: "node.coordinator.2".to_string(),
                failover_to_node_id: Some("node.coordinator.3".to_string()),
                queue_scope: Some(vec!["default".to_string()]),
                reason: Some("health-degraded".to_string()),
                correlation_id: Some("failover-1".to_string()),
                issued_at: Utc::now(),
            })
            .await
            .expect("failover should succeed");

        assert!(response.accepted);
        assert_eq!(response.command_name, "coordinator.failover");

        let commands = forwarder
            .forwarded_commands()
            .expect("commands list should succeed");
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].command_name, "coordinator.failover");
        assert!(commands[0].payload.contains("node.coordinator.3"));
    }

    #[tokio::test]
    async fn control_plane_sdk_rebalances_queue_ownership_when_configured() {
        let forwarder = Arc::new(InMemoryClusterCommandForwarder::default());
        let sdk = ControlPlaneSdk::new(store()).with_cluster_command_forwarder(forwarder.clone());

        let response = sdk
            .rebalance_queue_ownership(RebalanceQueueOwnershipRequest {
                target_region: "eu-north".to_string(),
                queue: "priority".to_string(),
                desired_owners: vec!["node.worker.7".to_string(), "node.worker.8".to_string()],
                strategy: Some("least-loaded".to_string()),
                reason: Some("capacity-adjustment".to_string()),
                correlation_id: Some("rebalance-1".to_string()),
                issued_at: Utc::now(),
            })
            .await
            .expect("rebalance should succeed");

        assert!(response.accepted);
        assert_eq!(response.command_name, "queue_ownership.rebalance");

        let commands = forwarder
            .forwarded_commands()
            .expect("commands list should succeed");
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].command_name, "queue_ownership.rebalance");
        assert!(commands[0].payload.contains("priority"));
    }
}