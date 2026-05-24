use std::collections::BTreeMap;

use chrono::{DateTime, Duration, Utc};

use crate::application::dto::{
    ClusterForwardOutcomeRow, ClusterNodeHealthRow, ForwardClusterCommandRequest,
    ForwardClusterCommandResponse, HeartbeatClusterNodeRequest, InitiateCoordinatorFailoverRequest,
    InitiateCoordinatorFailoverResponse, InitiateCoordinatorHandoffRequest,
    InitiateCoordinatorHandoffResponse, ListClusterForwardOutcomesRequest,
    ListClusterNodeHealthRequest, ListQueueOwnershipHealthRequest, PruneExpiredClusterNodesRequest,
    QueueOwnershipHealthRow, RebalanceQueueOwnershipRequest, RebalanceQueueOwnershipResponse,
    RegisterClusterNodeRequest, RunClusterHeartbeatSweepRequest, RunClusterHeartbeatSweepResponse,
};
use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::cluster_node::{
    ClusterControlEvent, ClusterForwardCommand, ClusterNode, ClusterNodeHealth,
    ClusterNodeHealthSnapshot, ClusterNodeHeartbeat, NewClusterNode, QueueOwnershipMode,
};
use crate::ports::outbound::runtime::cluster_command_forwarder::ClusterCommandForwarder;
use crate::ports::outbound::runtime::cluster_control_event_sink::ClusterControlEventSink;
use crate::ports::outbound::runtime::cluster_forward_outcome_store::ClusterForwardOutcomeStore;
use crate::ports::outbound::runtime::cluster_node_store::ClusterNodeStore;
use serde_json::json;

#[derive(Clone)]
pub struct RegisterClusterNode<S>
where
    S: ClusterNodeStore,
{
    store: S,
}

impl<S> RegisterClusterNode<S>
where
    S: ClusterNodeStore,
{
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub async fn execute(&self, request: RegisterClusterNodeRequest) -> Result<ClusterNode> {
        if request.node_id.trim().is_empty() {
            return Err(StasisError::PortFailure(
                "node_id must not be empty".to_string(),
            ));
        }
        if request.region.trim().is_empty() {
            return Err(StasisError::PortFailure(
                "region must not be empty".to_string(),
            ));
        }

        let mode = request
            .queue_ownership_mode
            .unwrap_or(QueueOwnershipMode::MultiOwner);
        validate_queue_ownership(
            &self.store,
            &request.node_id,
            &request.queue_ownership,
            mode,
            request.heartbeat_at,
        )
        .await?;

        self.store
            .register(NewClusterNode {
                node_id: request.node_id,
                role: request.role,
                region: request.region,
                queue_ownership: request.queue_ownership,
                capability_tags: request.capability_tags,
                heartbeat_at: request.heartbeat_at,
                lease_ttl_seconds: request.lease_ttl_seconds,
                metadata: request.metadata,
            })
            .await
    }
}

#[derive(Clone)]
pub struct HeartbeatClusterNode<S>
where
    S: ClusterNodeStore,
{
    store: S,
}

impl<S> HeartbeatClusterNode<S>
where
    S: ClusterNodeStore,
{
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub async fn execute(&self, request: HeartbeatClusterNodeRequest) -> Result<ClusterNode> {
        if let Some(queue_ownership) = &request.queue_ownership {
            let mode = request
                .queue_ownership_mode
                .unwrap_or(QueueOwnershipMode::MultiOwner);
            validate_queue_ownership(
                &self.store,
                &request.node_id,
                queue_ownership,
                mode,
                request.heartbeat_at,
            )
            .await?;
        }

        let updated = self
            .store
            .heartbeat(ClusterNodeHeartbeat {
                node_id: request.node_id.clone(),
                heartbeat_at: request.heartbeat_at,
                lease_ttl_seconds: request.lease_ttl_seconds,
                queue_ownership: request.queue_ownership,
                capability_tags: request.capability_tags,
                metadata: request.metadata,
            })
            .await?;

        updated.ok_or_else(|| {
            StasisError::PortFailure(format!("cluster node not found: {}", request.node_id))
        })
    }
}

#[derive(Clone)]
pub struct RunClusterHeartbeatSweep<S, E>
where
    S: ClusterNodeStore,
    E: ClusterControlEventSink,
{
    store: S,
    event_sink: E,
}

#[derive(Clone)]
pub struct ForwardClusterControlCommand<F>
where
    F: ClusterCommandForwarder,
{
    forwarder: F,
}

#[derive(Clone)]
pub struct InitiateCoordinatorHandoff<F>
where
    F: ClusterCommandForwarder,
{
    forwarder: F,
}

#[derive(Clone)]
pub struct InitiateCoordinatorFailover<F>
where
    F: ClusterCommandForwarder,
{
    forwarder: F,
}

#[derive(Clone)]
pub struct RebalanceQueueOwnership<F>
where
    F: ClusterCommandForwarder,
{
    forwarder: F,
}

impl<F> InitiateCoordinatorHandoff<F>
where
    F: ClusterCommandForwarder,
{
    pub fn new(forwarder: F) -> Self {
        Self { forwarder }
    }

    pub async fn execute(
        &self,
        request: InitiateCoordinatorHandoffRequest,
    ) -> Result<InitiateCoordinatorHandoffResponse> {
        if request.target_region.trim().is_empty() {
            return Err(StasisError::PortFailure(
                "target_region must not be empty".to_string(),
            ));
        }
        if request.coordinator_node_id.trim().is_empty() {
            return Err(StasisError::PortFailure(
                "coordinator_node_id must not be empty".to_string(),
            ));
        }

        let payload = json!({
            "coordinator_node_id": request.coordinator_node_id,
            "queue_scope": request.queue_scope,
            "reason": request.reason,
        })
        .to_string();

        let command_name = "coordinator.handoff".to_string();
        let accepted = self
            .forwarder
            .forward(ClusterForwardCommand {
                target_region: request.target_region,
                command_name: command_name.clone(),
                payload,
                correlation_id: request.correlation_id,
                issued_at: request.issued_at,
            })
            .await?;

        Ok(InitiateCoordinatorHandoffResponse {
            accepted,
            command_name,
        })
    }
}

impl<F> InitiateCoordinatorFailover<F>
where
    F: ClusterCommandForwarder,
{
    pub fn new(forwarder: F) -> Self {
        Self { forwarder }
    }

    pub async fn execute(
        &self,
        request: InitiateCoordinatorFailoverRequest,
    ) -> Result<InitiateCoordinatorFailoverResponse> {
        if request.target_region.trim().is_empty() {
            return Err(StasisError::PortFailure(
                "target_region must not be empty".to_string(),
            ));
        }
        if request.coordinator_node_id.trim().is_empty() {
            return Err(StasisError::PortFailure(
                "coordinator_node_id must not be empty".to_string(),
            ));
        }

        let payload = json!({
            "coordinator_node_id": request.coordinator_node_id,
            "failover_to_node_id": request.failover_to_node_id,
            "queue_scope": request.queue_scope,
            "reason": request.reason,
        })
        .to_string();

        let command_name = "coordinator.failover".to_string();
        let accepted = self
            .forwarder
            .forward(ClusterForwardCommand {
                target_region: request.target_region,
                command_name: command_name.clone(),
                payload,
                correlation_id: request.correlation_id,
                issued_at: request.issued_at,
            })
            .await?;

        Ok(InitiateCoordinatorFailoverResponse {
            accepted,
            command_name,
        })
    }
}

impl<F> RebalanceQueueOwnership<F>
where
    F: ClusterCommandForwarder,
{
    pub fn new(forwarder: F) -> Self {
        Self { forwarder }
    }

    pub async fn execute(
        &self,
        request: RebalanceQueueOwnershipRequest,
    ) -> Result<RebalanceQueueOwnershipResponse> {
        if request.target_region.trim().is_empty() {
            return Err(StasisError::PortFailure(
                "target_region must not be empty".to_string(),
            ));
        }
        if request.queue.trim().is_empty() {
            return Err(StasisError::PortFailure(
                "queue must not be empty".to_string(),
            ));
        }
        if request.desired_owners.is_empty() {
            return Err(StasisError::PortFailure(
                "desired_owners must not be empty".to_string(),
            ));
        }

        let payload = json!({
            "queue": request.queue,
            "desired_owners": request.desired_owners,
            "strategy": request.strategy,
            "reason": request.reason,
        })
        .to_string();

        let command_name = "queue_ownership.rebalance".to_string();
        let accepted = self
            .forwarder
            .forward(ClusterForwardCommand {
                target_region: request.target_region,
                command_name: command_name.clone(),
                payload,
                correlation_id: request.correlation_id,
                issued_at: request.issued_at,
            })
            .await?;

        Ok(RebalanceQueueOwnershipResponse {
            accepted,
            command_name,
        })
    }
}

#[derive(Clone)]
pub struct ListClusterForwardOutcomes<S>
where
    S: ClusterForwardOutcomeStore,
{
    store: S,
}

impl<S> ListClusterForwardOutcomes<S>
where
    S: ClusterForwardOutcomeStore,
{
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub async fn execute(
        &self,
        request: &ListClusterForwardOutcomesRequest,
    ) -> Result<Vec<ClusterForwardOutcomeRow>> {
        let limit = request.limit.max(1);
        let outcomes = self.store.list_recent(limit).await?;

        Ok(outcomes
            .into_iter()
            .filter(|outcome| {
                request
                    .target_region
                    .as_ref()
                    .map(|region| &outcome.target_region == region)
                    .unwrap_or(true)
            })
            .filter(|outcome| {
                request
                    .command_name
                    .as_ref()
                    .map(|command_name| &outcome.command_name == command_name)
                    .unwrap_or(true)
            })
            .filter(|outcome| {
                request
                    .accepted
                    .map(|accepted| outcome.accepted == accepted)
                    .unwrap_or(true)
            })
            .map(|outcome| ClusterForwardOutcomeRow {
                target_region: outcome.target_region,
                command_name: outcome.command_name,
                correlation_id: outcome.correlation_id,
                accepted: outcome.accepted,
                attempts: outcome.attempts,
                error: outcome.error,
                completed_at: outcome.completed_at,
            })
            .collect())
    }
}

impl<F> ForwardClusterControlCommand<F>
where
    F: ClusterCommandForwarder,
{
    pub fn new(forwarder: F) -> Self {
        Self { forwarder }
    }

    pub async fn execute(
        &self,
        request: ForwardClusterCommandRequest,
    ) -> Result<ForwardClusterCommandResponse> {
        if request.target_region.trim().is_empty() {
            return Err(StasisError::PortFailure(
                "target_region must not be empty".to_string(),
            ));
        }
        if request.command_name.trim().is_empty() {
            return Err(StasisError::PortFailure(
                "command_name must not be empty".to_string(),
            ));
        }
        if request.payload.trim().is_empty() {
            return Err(StasisError::PortFailure(
                "payload must not be empty".to_string(),
            ));
        }

        let accepted = self
            .forwarder
            .forward(ClusterForwardCommand {
                target_region: request.target_region,
                command_name: request.command_name,
                payload: request.payload,
                correlation_id: request.correlation_id,
                issued_at: request.issued_at,
            })
            .await?;

        Ok(ForwardClusterCommandResponse { accepted })
    }
}

impl<S, E> RunClusterHeartbeatSweep<S, E>
where
    S: ClusterNodeStore,
    E: ClusterControlEventSink,
{
    pub fn new(store: S, event_sink: E) -> Self {
        Self { store, event_sink }
    }

    pub async fn execute(
        &self,
        request: RunClusterHeartbeatSweepRequest,
    ) -> Result<RunClusterHeartbeatSweepResponse> {
        let pruned = self.store.prune_expired(request.now).await?;
        if pruned == 0 {
            return Ok(RunClusterHeartbeatSweepResponse {
                pruned_nodes: 0,
                emitted_events: 0,
            });
        }

        self.event_sink
            .emit(ClusterControlEvent::ExpiredNodesPruned {
                pruned_count: pruned,
                occurred_at: request.now,
            })
            .await?;

        Ok(RunClusterHeartbeatSweepResponse {
            pruned_nodes: pruned,
            emitted_events: 1,
        })
    }
}

#[derive(Clone)]
pub struct ListClusterNodeHealth<S>
where
    S: ClusterNodeStore,
{
    store: S,
}

impl<S> ListClusterNodeHealth<S>
where
    S: ClusterNodeStore,
{
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub async fn execute(
        &self,
        request: &ListClusterNodeHealthRequest,
    ) -> Result<Vec<ClusterNodeHealthRow>> {
        let now = Utc::now();
        let mut rows = self
            .store
            .list()
            .await?
            .into_iter()
            .filter(|node| {
                request
                    .role
                    .as_ref()
                    .map(|role| &node.role == role)
                    .unwrap_or(true)
            })
            .filter(|node| {
                request
                    .region
                    .as_ref()
                    .map(|region| &node.region == region)
                    .unwrap_or(true)
            })
            .filter(|node| {
                request
                    .capability_tag
                    .as_ref()
                    .map(|tag| node.capability_tags.iter().any(|value| value == tag))
                    .unwrap_or(true)
            })
            .filter(|node| {
                request
                    .queue
                    .as_ref()
                    .map(|queue| node.queue_ownership.iter().any(|value| value == queue))
                    .unwrap_or(true)
            })
            .map(|node| {
                let health = classify_health(&node, now);
                ClusterNodeHealthRow {
                    snapshot: ClusterNodeHealthSnapshot { node, health },
                }
            })
            .filter(|row| {
                request
                    .health
                    .as_ref()
                    .map(|health| &row.snapshot.health == health)
                    .unwrap_or(true)
            })
            .collect::<Vec<_>>();

        rows.sort_by(|left, right| {
            right
                .snapshot
                .node
                .updated_at
                .cmp(&left.snapshot.node.updated_at)
                .then_with(|| left.snapshot.node.node_id.cmp(&right.snapshot.node.node_id))
        });

        let offset = request.offset;
        if offset >= rows.len() {
            return Ok(Vec::new());
        }

        let limit = request.limit.unwrap_or(rows.len().saturating_sub(offset));
        Ok(rows.into_iter().skip(offset).take(limit).collect())
    }
}

#[derive(Clone)]
pub struct ListQueueOwnershipHealth<S>
where
    S: ClusterNodeStore,
{
    store: S,
}

impl<S> ListQueueOwnershipHealth<S>
where
    S: ClusterNodeStore,
{
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub async fn execute(
        &self,
        request: &ListQueueOwnershipHealthRequest,
    ) -> Result<Vec<QueueOwnershipHealthRow>> {
        let now = Utc::now();
        let nodes = self.store.list().await?;

        let mut by_queue: BTreeMap<String, QueueOwnershipHealthRow> = BTreeMap::new();
        for node in nodes {
            let health = classify_health(&node, now);

            for queue in &node.queue_ownership {
                if let Some(prefix) = &request.queue_prefix
                    && !queue.starts_with(prefix)
                {
                    continue;
                }

                let row =
                    by_queue
                        .entry(queue.clone())
                        .or_insert_with(|| QueueOwnershipHealthRow {
                            queue: queue.clone(),
                            owners: Vec::new(),
                            healthy_owners: 0,
                            degraded_owners: 0,
                            offline_owners: 0,
                        });

                row.owners.push(node.node_id.clone());
                match health {
                    ClusterNodeHealth::Healthy => row.healthy_owners += 1,
                    ClusterNodeHealth::Degraded => row.degraded_owners += 1,
                    ClusterNodeHealth::Offline => row.offline_owners += 1,
                }
            }
        }

        let mut rows = by_queue.into_values().collect::<Vec<_>>();
        for row in &mut rows {
            row.owners.sort();
        }
        rows.sort_by(|left, right| {
            right
                .offline_owners
                .cmp(&left.offline_owners)
                .then_with(|| right.degraded_owners.cmp(&left.degraded_owners))
                .then_with(|| left.queue.cmp(&right.queue))
        });
        Ok(rows)
    }
}

#[derive(Clone)]
pub struct PruneExpiredClusterNodes<S>
where
    S: ClusterNodeStore,
{
    store: S,
}

impl<S> PruneExpiredClusterNodes<S>
where
    S: ClusterNodeStore,
{
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub async fn execute(&self, request: PruneExpiredClusterNodesRequest) -> Result<u64> {
        self.store.prune_expired(request.now).await
    }
}

fn classify_health(node: &ClusterNode, now: DateTime<Utc>) -> ClusterNodeHealth {
    if node.lease_expires_at < now {
        return ClusterNodeHealth::Offline;
    }

    let stale_threshold = node.heartbeat_at + Duration::seconds(30);
    if stale_threshold < now {
        ClusterNodeHealth::Degraded
    } else {
        ClusterNodeHealth::Healthy
    }
}

async fn validate_queue_ownership<S>(
    store: &S,
    node_id: &str,
    requested_queues: &[String],
    mode: QueueOwnershipMode,
    at: DateTime<Utc>,
) -> Result<()>
where
    S: ClusterNodeStore,
{
    if requested_queues.is_empty() || mode == QueueOwnershipMode::MultiOwner {
        return Ok(());
    }

    let existing = store.list().await?;
    for node in existing {
        if node.node_id == node_id || node.lease_expires_at < at {
            continue;
        }

        if let Some(conflict) = requested_queues
            .iter()
            .find(|queue| node.queue_ownership.iter().any(|owned| owned == *queue))
        {
            return Err(StasisError::PortFailure(format!(
                "queue ownership conflict for queue={} with active node={}",
                conflict, node.node_id
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};

    use crate::application::dto::{
        HeartbeatClusterNodeRequest, ListClusterNodeHealthRequest, ListQueueOwnershipHealthRequest,
        PruneExpiredClusterNodesRequest, RegisterClusterNodeRequest,
        RunClusterHeartbeatSweepRequest,
    };
    use crate::domain::runtime::cluster_node::{
        ClusterControlEvent, ClusterNodeHealth, ClusterNodeRole, QueueOwnershipMode,
    };
    use crate::infrastructure::runtime::in_memory_cluster_control_event_sink::InMemoryClusterControlEventSink;
    use crate::infrastructure::runtime::in_memory_cluster_node_store::InMemoryClusterNodeStore;

    use super::{
        HeartbeatClusterNode, ListClusterNodeHealth, ListQueueOwnershipHealth,
        PruneExpiredClusterNodes, RegisterClusterNode, RunClusterHeartbeatSweep,
    };

    #[tokio::test]
    async fn register_heartbeat_and_health_views_work() {
        let store = InMemoryClusterNodeStore::default();
        let register = RegisterClusterNode::new(store.clone());
        let heartbeat = HeartbeatClusterNode::new(store.clone());
        let health = ListClusterNodeHealth::new(store.clone());
        let queue_health = ListQueueOwnershipHealth::new(store.clone());
        let prune = PruneExpiredClusterNodes::new(store);

        let now = Utc::now();
        register
            .execute(RegisterClusterNodeRequest {
                node_id: "node.worker.a".to_string(),
                role: ClusterNodeRole::Worker,
                region: "us-east".to_string(),
                queue_ownership: vec!["default".to_string(), "priority".to_string()],
                capability_tags: vec!["gpu".to_string()],
                heartbeat_at: now,
                lease_ttl_seconds: 15,
                queue_ownership_mode: None,
                metadata: None,
            })
            .await
            .expect("registration should succeed");

        let rows = health
            .execute(&ListClusterNodeHealthRequest {
                role: Some(ClusterNodeRole::Worker),
                region: Some("us-east".to_string()),
                capability_tag: Some("gpu".to_string()),
                queue: Some("default".to_string()),
                health: Some(ClusterNodeHealth::Healthy),
                offset: 0,
                limit: Some(10),
            })
            .await
            .expect("health list should succeed");
        assert_eq!(rows.len(), 1);

        heartbeat
            .execute(HeartbeatClusterNodeRequest {
                node_id: "node.worker.a".to_string(),
                heartbeat_at: now + Duration::seconds(5),
                lease_ttl_seconds: 30,
                queue_ownership_mode: None,
                queue_ownership: Some(vec!["default".to_string()]),
                capability_tags: None,
                metadata: Some("v2".to_string()),
            })
            .await
            .expect("heartbeat should succeed");

        let queue_rows = queue_health
            .execute(&ListQueueOwnershipHealthRequest { queue_prefix: None })
            .await
            .expect("queue health should succeed");
        assert_eq!(queue_rows.len(), 1);
        assert_eq!(queue_rows[0].queue, "default");

        let deleted = prune
            .execute(PruneExpiredClusterNodesRequest {
                now: now + Duration::minutes(10),
            })
            .await
            .expect("prune should succeed");
        assert_eq!(deleted, 1);

        let event_sink = InMemoryClusterControlEventSink::default();
        let sweep =
            RunClusterHeartbeatSweep::new(InMemoryClusterNodeStore::default(), event_sink.clone());
        let response = sweep
            .execute(RunClusterHeartbeatSweepRequest { now: Utc::now() })
            .await
            .expect("sweep should succeed");
        assert_eq!(response.pruned_nodes, 0);
        assert_eq!(response.emitted_events, 0);

        let events = event_sink.events().expect("event list should succeed");
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn single_owner_mode_rejects_conflicting_queue_registration() {
        let store = InMemoryClusterNodeStore::default();
        let register = RegisterClusterNode::new(store.clone());
        let now = Utc::now();

        register
            .execute(RegisterClusterNodeRequest {
                node_id: "node.worker.a".to_string(),
                role: ClusterNodeRole::Worker,
                region: "us-east".to_string(),
                queue_ownership: vec!["default".to_string()],
                capability_tags: vec![],
                heartbeat_at: now,
                lease_ttl_seconds: 60,
                queue_ownership_mode: Some(QueueOwnershipMode::SingleOwner),
                metadata: None,
            })
            .await
            .expect("initial registration should succeed");

        let err = register
            .execute(RegisterClusterNodeRequest {
                node_id: "node.worker.b".to_string(),
                role: ClusterNodeRole::Worker,
                region: "us-east".to_string(),
                queue_ownership: vec!["default".to_string()],
                capability_tags: vec![],
                heartbeat_at: now,
                lease_ttl_seconds: 60,
                queue_ownership_mode: Some(QueueOwnershipMode::SingleOwner),
                metadata: None,
            })
            .await
            .expect_err("conflicting single owner queue should fail");

        assert!(
            err.to_string()
                .contains("queue ownership conflict for queue=default")
        );

        let sweep_sink = InMemoryClusterControlEventSink::default();
        let sweep = RunClusterHeartbeatSweep::new(store, sweep_sink.clone());
        let response = sweep
            .execute(RunClusterHeartbeatSweepRequest {
                now: now + Duration::hours(2),
            })
            .await
            .expect("sweep should succeed");
        assert_eq!(response.pruned_nodes, 1);
        assert_eq!(response.emitted_events, 1);

        let events = sweep_sink.events().expect("event list should succeed");
        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            ClusterControlEvent::ExpiredNodesPruned {
                pruned_count: 1,
                ..
            }
        ));
    }
}
