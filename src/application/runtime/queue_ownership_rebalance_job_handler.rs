use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;
use serde_json::json;

use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::domain::errors::Result;
use crate::domain::runtime::cluster_node::ClusterNodeHeartbeat;
use crate::domain::runtime::job::Job;
use crate::ports::outbound::runtime::cluster_node_store::ClusterNodeStore;

#[derive(Clone)]
pub struct QueueOwnershipRebalanceJobHandler {
    cluster_store: Arc<dyn ClusterNodeStore>,
}

#[derive(Deserialize)]
struct QueueRebalancePayload {
    queue: String,
    desired_owners: Vec<String>,
    strategy: Option<String>,
    reason: Option<String>,
}

impl QueueOwnershipRebalanceJobHandler {
    pub fn new(cluster_store: Arc<dyn ClusterNodeStore>) -> Self {
        Self { cluster_store }
    }

    fn parse_payload(raw: &str) -> std::result::Result<QueueRebalancePayload, String> {
        let payload: QueueRebalancePayload = serde_json::from_str(raw)
            .map_err(|err| format!("invalid queue rebalance payload json: {err}"))?;

        if payload.queue.trim().is_empty() {
            return Err("queue must not be empty".to_string());
        }
        if payload.desired_owners.is_empty() {
            return Err("desired_owners must not be empty".to_string());
        }
        if payload.desired_owners.iter().any(|owner| owner.trim().is_empty()) {
            return Err("desired_owners must not contain empty values".to_string());
        }

        Ok(payload)
    }

    fn failure(message: impl Into<String>) -> JobExecutionOutcome {
        JobExecutionOutcome::FatalFailure {
            message: message.into(),
            execution_id: None,
            diagnostics: None,
        }
    }

    fn retryable(message: impl Into<String>) -> JobExecutionOutcome {
        JobExecutionOutcome::RetryableFailure {
            message: message.into(),
            execution_id: None,
            diagnostics: None,
        }
    }

    fn remaining_ttl_seconds(lease_expires_at: chrono::DateTime<Utc>, now: chrono::DateTime<Utc>) -> i64 {
        lease_expires_at
            .signed_duration_since(now)
            .num_seconds()
            .max(1)
    }
}

#[async_trait]
impl JobHandler for QueueOwnershipRebalanceJobHandler {
    fn job_type(&self) -> &'static str {
        "workflow.stasis.cluster.queue_ownership_rebalance"
    }

    async fn execute(&self, job: &Job) -> Result<JobExecutionOutcome> {
        let payload = match Self::parse_payload(&job.payload_ref) {
            Ok(payload) => payload,
            Err(message) => return Ok(Self::failure(message)),
        };

        let now = Utc::now();
        let nodes = match self.cluster_store.list().await {
            Ok(nodes) => nodes,
            Err(err) => {
                return Ok(Self::retryable(format!(
                    "failed to load cluster nodes for rebalance: {err}"
                )));
            }
        };

        let active_nodes = nodes
            .into_iter()
            .filter(|node| node.lease_expires_at >= now)
            .collect::<Vec<_>>();

        let desired = payload
            .desired_owners
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();

        if let Some(missing) = desired
            .iter()
            .find(|owner| !active_nodes.iter().any(|node| &node.node_id == *owner))
        {
            return Ok(Self::failure(format!(
                "desired owner is not active: {}",
                missing
            )));
        }

        let mut updated_nodes = 0usize;

        for node in active_nodes {
            let owns_queue = node.queue_ownership.iter().any(|q| q == &payload.queue);
            let should_own = desired.contains(&node.node_id);
            if owns_queue == should_own {
                continue;
            }

            let mut queues = node.queue_ownership.into_iter().collect::<BTreeSet<_>>();
            if should_own {
                queues.insert(payload.queue.clone());
            } else {
                queues.remove(&payload.queue);
            }

            let ttl = Self::remaining_ttl_seconds(node.lease_expires_at, now);
            let result = self
                .cluster_store
                .heartbeat(ClusterNodeHeartbeat {
                    node_id: node.node_id,
                    heartbeat_at: now,
                    lease_ttl_seconds: ttl,
                    queue_ownership: Some(queues.into_iter().collect::<Vec<_>>()),
                    capability_tags: None,
                    metadata: Some(
                        json!({
                            "action": "queue_ownership_rebalance",
                            "queue": payload.queue,
                            "strategy": payload.strategy,
                            "reason": payload.reason,
                        })
                        .to_string(),
                    ),
                })
                .await;

            if result.is_err() {
                return Ok(Self::retryable("failed to update node during queue rebalance"));
            }

            updated_nodes += 1;
        }

        let diagnostics = json!({
            "status": "success",
            "queue": payload.queue,
            "updated_nodes": updated_nodes,
            "desired_owners": desired.into_iter().collect::<Vec<_>>(),
            "strategy": payload.strategy,
        })
        .to_string();

        Ok(JobExecutionOutcome::Success {
            sttp_output_node_id: "sttp:out:stasis:cluster:queue_ownership_rebalance".to_string(),
            execution_id: None,
            diagnostics: Some(diagnostics),
        })
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use std::sync::Arc;

    use crate::application::runtime::in_memory_runtime::JobHandler;
    use crate::application::runtime::queue_ownership_rebalance_job_handler::QueueOwnershipRebalanceJobHandler;
    use crate::domain::runtime::cluster_node::{ClusterNodeRole, NewClusterNode};
    use crate::domain::runtime::job::{BackoffPolicy, Job, JobState};
    use crate::infrastructure::runtime::in_memory_cluster_node_store::InMemoryClusterNodeStore;
    use crate::ports::outbound::runtime::cluster_node_store::ClusterNodeStore;

    fn sample_job(payload_ref: String) -> Job {
        Job {
            id: "job.cluster.rebalance.1".to_string(),
            queue: "cluster-control".to_string(),
            job_type: "workflow.stasis.cluster.queue_ownership_rebalance".to_string(),
            payload_ref,
            state: JobState::Enqueued,
            priority: 100,
            attempts: 0,
            max_attempts: 3,
            backoff_policy: BackoffPolicy::default(),
            idempotency_key: "idem-rebalance".to_string(),
            correlation_id: "corr-rebalance".to_string(),
            causation_id: "cause-rebalance".to_string(),
            trace_id: "trace-rebalance".to_string(),
            sttp_input_node_id: "sttp:in:cluster:rebalance".to_string(),
            sttp_output_node_id: None,
            lease_owner: None,
            lease_expires_at: None,
            heartbeat_at: None,
            scheduled_at: Utc::now(),
            started_at: None,
            finished_at: None,
            last_error: None,
        }
    }

    #[tokio::test]
    async fn handler_rebalances_queue_ownership_across_nodes() {
        let store = InMemoryClusterNodeStore::default();
        let now = Utc::now();

        store
            .register(NewClusterNode {
                node_id: "node.a".to_string(),
                role: ClusterNodeRole::Worker,
                region: "us-east".to_string(),
                queue_ownership: vec!["priority".to_string()],
                capability_tags: vec![],
                heartbeat_at: now,
                lease_ttl_seconds: 60,
                metadata: None,
            })
            .await
            .expect("register node a should succeed");

        store
            .register(NewClusterNode {
                node_id: "node.b".to_string(),
                role: ClusterNodeRole::Worker,
                region: "us-east".to_string(),
                queue_ownership: vec![],
                capability_tags: vec![],
                heartbeat_at: now,
                lease_ttl_seconds: 60,
                metadata: None,
            })
            .await
            .expect("register node b should succeed");

        let handler = QueueOwnershipRebalanceJobHandler::new(Arc::new(store.clone()));
        let payload = serde_json::json!({
            "queue": "priority",
            "desired_owners": ["node.b"],
            "strategy": "least-loaded",
            "reason": "capacity",
        })
        .to_string();

        let outcome = handler
            .execute(&sample_job(payload))
            .await
            .expect("handler execution should succeed");

        assert!(matches!(
            outcome,
            crate::application::runtime::in_memory_runtime::JobExecutionOutcome::Success { .. }
        ));

        let node_a = store
            .get("node.a")
            .await
            .expect("node a get should succeed")
            .expect("node a should exist");
        let node_b = store
            .get("node.b")
            .await
            .expect("node b get should succeed")
            .expect("node b should exist");

        assert!(!node_a.queue_ownership.iter().any(|q| q == "priority"));
        assert!(node_b.queue_ownership.iter().any(|q| q == "priority"));
    }
}
