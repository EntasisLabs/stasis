use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use reqwest::StatusCode;
use serde::Serialize;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::cluster_node::{ClusterForwardCommand, ClusterForwardOutcome};
use crate::ports::outbound::runtime::cluster_command_forwarder::ClusterCommandForwarder;
use crate::ports::outbound::runtime::cluster_forward_outcome_store::ClusterForwardOutcomeStore;
use crate::ports::outbound::runtime::runtime_metrics::RuntimeMetrics;

pub const CLUSTER_FORWARD_ATTEMPTS_TOTAL: &str = "cluster_forward_attempts_total";
pub const CLUSTER_FORWARD_RETRIES_TOTAL: &str = "cluster_forward_retries_total";
pub const CLUSTER_FORWARD_SUCCESSES_TOTAL: &str = "cluster_forward_successes_total";
pub const CLUSTER_FORWARD_FAILURES_TOTAL: &str = "cluster_forward_failures_total";
pub const CLUSTER_FORWARD_NO_ROUTE_TOTAL: &str = "cluster_forward_no_route_total";
pub const CLUSTER_FORWARD_REJECTED_TOTAL: &str = "cluster_forward_rejected_total";
pub const CLUSTER_FORWARD_DURATION_MS: &str = "cluster_forward_duration_ms";
pub const CLUSTER_FORWARD_IDEMPOTENT_HITS_TOTAL: &str = "cluster_forward_idempotent_hits_total";

#[derive(Clone)]
struct DedupeEntry {
    accepted: bool,
    observed_at: Instant,
}

#[derive(Clone)]
pub struct HttpClusterCommandForwarder {
    client: reqwest::Client,
    region_targets: BTreeMap<String, String>,
    authorization_bearer: Option<String>,
    metrics: Option<Arc<dyn RuntimeMetrics>>,
    outcome_store: Option<Arc<dyn ClusterForwardOutcomeStore>>,
    max_attempts: u32,
    base_backoff_ms: u64,
    max_backoff_ms: u64,
    idempotency_ttl: Duration,
    dedupe_cache: Arc<RwLock<BTreeMap<String, DedupeEntry>>>,
}

impl HttpClusterCommandForwarder {
    pub fn new(region_targets: BTreeMap<String, String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            region_targets,
            authorization_bearer: None,
            metrics: None,
            outcome_store: None,
            max_attempts: 3,
            base_backoff_ms: 100,
            max_backoff_ms: 2_000,
            idempotency_ttl: Duration::from_secs(300),
            dedupe_cache: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub fn with_region_target(
        mut self,
        region: impl Into<String>,
        endpoint_url: impl Into<String>,
    ) -> Self {
        self.region_targets
            .insert(region.into(), endpoint_url.into());
        self
    }

    pub fn with_bearer_token(mut self, token: impl Into<String>) -> Self {
        self.authorization_bearer = Some(token.into());
        self
    }

    pub fn with_metrics(mut self, metrics: Arc<dyn RuntimeMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn with_outcome_store(mut self, store: Arc<dyn ClusterForwardOutcomeStore>) -> Self {
        self.outcome_store = Some(store);
        self
    }

    pub fn with_retry_policy(
        mut self,
        max_attempts: u32,
        base_backoff_ms: u64,
        max_backoff_ms: u64,
    ) -> Self {
        self.max_attempts = max_attempts.max(1);
        self.base_backoff_ms = base_backoff_ms.max(1);
        self.max_backoff_ms = max_backoff_ms.max(self.base_backoff_ms);
        self
    }

    pub fn with_idempotency_ttl(mut self, ttl: Duration) -> Self {
        self.idempotency_ttl = ttl;
        self
    }

    fn endpoint_for_region(&self, region: &str) -> Option<&str> {
        self.region_targets.get(region).map(String::as_str)
    }

    fn compute_backoff_millis(&self, attempt: u32) -> u64 {
        let factor = 2u64.saturating_pow(attempt.saturating_sub(1));
        let candidate = self.base_backoff_ms.saturating_mul(factor);
        candidate.min(self.max_backoff_ms)
    }

    fn should_retry_status(status: StatusCode) -> bool {
        status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS
    }

    fn dedupe_key(command: &ClusterForwardCommand) -> Option<String> {
        command.correlation_id.as_ref().and_then(|correlation_id| {
            let correlation_id = correlation_id.trim();
            if correlation_id.is_empty() {
                return None;
            }

            Some(format!(
                "{}|{}|{}",
                command.target_region, command.command_name, correlation_id
            ))
        })
    }

    fn load_dedupe_hit(&self, command: &ClusterForwardCommand) -> Option<bool> {
        let key = Self::dedupe_key(command)?;
        let now = Instant::now();

        let mut cache = self.dedupe_cache.write().ok()?;
        let Some(entry) = cache.get(&key).cloned() else {
            return None;
        };

        if now.duration_since(entry.observed_at) > self.idempotency_ttl {
            cache.remove(&key);
            return None;
        }

        Some(entry.accepted)
    }

    fn remember_dedupe_result(&self, command: &ClusterForwardCommand, accepted: bool) {
        let Some(key) = Self::dedupe_key(command) else {
            return;
        };

        let Ok(mut cache) = self.dedupe_cache.write() else {
            return;
        };

        cache.insert(
            key,
            DedupeEntry {
                accepted,
                observed_at: Instant::now(),
            },
        );
    }

    async fn record_outcome(
        &self,
        command: &ClusterForwardCommand,
        accepted: bool,
        attempts: u32,
        error: Option<String>,
    ) {
        let Some(store) = &self.outcome_store else {
            return;
        };

        let _ = store
            .record(ClusterForwardOutcome {
                target_region: command.target_region.clone(),
                command_name: command.command_name.clone(),
                correlation_id: command.correlation_id.clone(),
                accepted,
                attempts,
                error,
                completed_at: chrono::Utc::now(),
            })
            .await;
    }
}

#[derive(Debug, Serialize)]
struct ForwardCommandPayload<'a> {
    target_region: &'a str,
    command_name: &'a str,
    payload: &'a str,
    correlation_id: Option<&'a str>,
    issued_at: String,
}

#[async_trait]
impl ClusterCommandForwarder for HttpClusterCommandForwarder {
    async fn forward(&self, command: ClusterForwardCommand) -> Result<bool> {
        let start = Instant::now();
        if let Some(accepted) = self.load_dedupe_hit(&command) {
            if let Some(metrics) = &self.metrics {
                metrics.incr_counter(CLUSTER_FORWARD_IDEMPOTENT_HITS_TOTAL, 1);
                metrics.observe_duration_ms(CLUSTER_FORWARD_DURATION_MS, 0);
            }
            return Ok(accepted);
        }

        let Some(endpoint_url) = self.endpoint_for_region(&command.target_region) else {
            if let Some(metrics) = &self.metrics {
                metrics.incr_counter(CLUSTER_FORWARD_NO_ROUTE_TOTAL, 1);
                metrics.incr_counter(CLUSTER_FORWARD_FAILURES_TOTAL, 1);
            }
            let err_msg = format!(
                "no cluster forward endpoint configured for region={}",
                command.target_region
            );
            self.record_outcome(&command, false, 0, Some(err_msg.clone()))
                .await;
            return Err(StasisError::PortFailure(format!("{}", err_msg)));
        };

        let request_body = ForwardCommandPayload {
            target_region: &command.target_region,
            command_name: &command.command_name,
            payload: &command.payload,
            correlation_id: command.correlation_id.as_deref(),
            issued_at: command.issued_at.to_rfc3339(),
        };

        for attempt in 1..=self.max_attempts {
            if let Some(metrics) = &self.metrics {
                metrics.incr_counter(CLUSTER_FORWARD_ATTEMPTS_TOTAL, 1);
            }

            let mut request = self.client.post(endpoint_url).json(&request_body);
            if let Some(token) = &self.authorization_bearer {
                request = request.bearer_auth(token);
            }

            let send_result = request.send().await;
            match send_result {
                Ok(response) if response.status().is_success() => {
                    if let Some(metrics) = &self.metrics {
                        metrics.incr_counter(CLUSTER_FORWARD_SUCCESSES_TOTAL, 1);
                        metrics.observe_duration_ms(
                            CLUSTER_FORWARD_DURATION_MS,
                            start.elapsed().as_millis() as u64,
                        );
                    }
                    self.remember_dedupe_result(&command, true);
                    self.record_outcome(&command, true, attempt, None).await;
                    return Ok(true);
                }
                Ok(response) if Self::should_retry_status(response.status()) => {
                    if attempt == self.max_attempts {
                        if let Some(metrics) = &self.metrics {
                            metrics.incr_counter(CLUSTER_FORWARD_FAILURES_TOTAL, 1);
                            metrics.observe_duration_ms(
                                CLUSTER_FORWARD_DURATION_MS,
                                start.elapsed().as_millis() as u64,
                            );
                        }
                        let err_msg = format!(
                            "cluster forward failed after retries with status={} region={} command={}",
                            response.status(),
                            command.target_region,
                            command.command_name
                        );
                        self.record_outcome(&command, false, attempt, Some(err_msg.clone()))
                            .await;
                        return Err(StasisError::PortFailure(format!("{}", err_msg)));
                    }

                    if let Some(metrics) = &self.metrics {
                        metrics.incr_counter(CLUSTER_FORWARD_RETRIES_TOTAL, 1);
                    }
                }
                Ok(response) => {
                    if let Some(metrics) = &self.metrics {
                        metrics.incr_counter(CLUSTER_FORWARD_REJECTED_TOTAL, 1);
                        metrics.incr_counter(CLUSTER_FORWARD_FAILURES_TOTAL, 1);
                        metrics.observe_duration_ms(
                            CLUSTER_FORWARD_DURATION_MS,
                            start.elapsed().as_millis() as u64,
                        );
                    }
                    let err_msg = format!(
                        "cluster forward rejected with status={} region={} command={}",
                        response.status(),
                        command.target_region,
                        command.command_name
                    );
                    self.record_outcome(&command, false, attempt, Some(err_msg.clone()))
                        .await;
                    return Err(StasisError::PortFailure(format!("{}", err_msg)));
                }
                Err(err) => {
                    if attempt == self.max_attempts {
                        if let Some(metrics) = &self.metrics {
                            metrics.incr_counter(CLUSTER_FORWARD_FAILURES_TOTAL, 1);
                            metrics.observe_duration_ms(
                                CLUSTER_FORWARD_DURATION_MS,
                                start.elapsed().as_millis() as u64,
                            );
                        }
                        let err_msg = format!(
                            "cluster forward request failed region={} command={} error={err}",
                            command.target_region, command.command_name
                        );
                        self.record_outcome(&command, false, attempt, Some(err_msg.clone()))
                            .await;
                        return Err(StasisError::PortFailure(format!("{}", err_msg)));
                    }

                    if let Some(metrics) = &self.metrics {
                        metrics.incr_counter(CLUSTER_FORWARD_RETRIES_TOTAL, 1);
                    }
                }
            }

            let delay_ms = self.compute_backoff_millis(attempt);
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }

        Err(StasisError::PortFailure(
            "cluster forward exhausted retries unexpectedly".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use chrono::Utc;

    use crate::domain::runtime::cluster_node::ClusterForwardCommand;
    use crate::infrastructure::runtime::in_memory_runtime_metrics::InMemoryRuntimeMetrics;
    use crate::ports::outbound::runtime::cluster_command_forwarder::ClusterCommandForwarder;

    use super::HttpClusterCommandForwarder;
    use super::{
        CLUSTER_FORWARD_ATTEMPTS_TOTAL, CLUSTER_FORWARD_FAILURES_TOTAL,
        CLUSTER_FORWARD_IDEMPOTENT_HITS_TOTAL, CLUSTER_FORWARD_NO_ROUTE_TOTAL,
        CLUSTER_FORWARD_RETRIES_TOTAL, CLUSTER_FORWARD_SUCCESSES_TOTAL,
    };

    async fn start_sequence_server(status_codes: Vec<u16>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener address should be available");

        tokio::spawn(async move {
            for status_code in status_codes {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };

                let mut buf = [0u8; 2048];
                let _ = socket.read(&mut buf).await;

                let reason = match status_code {
                    200 => "OK",
                    429 => "Too Many Requests",
                    500 => "Internal Server Error",
                    _ => "Status",
                };

                let response = format!(
                    "HTTP/1.1 {} {}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    status_code, reason
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });

        format!("http://{}", addr)
    }

    fn sample_command() -> ClusterForwardCommand {
        ClusterForwardCommand {
            target_region: "eu-west".to_string(),
            command_name: "scheduler.pause_queue".to_string(),
            payload: "{\"queue\":\"default\"}".to_string(),
            correlation_id: Some("cmd-1".to_string()),
            issued_at: Utc::now(),
        }
    }

    #[test]
    fn backoff_scales_exponentially_with_cap() {
        let forwarder =
            HttpClusterCommandForwarder::new(BTreeMap::new()).with_retry_policy(5, 100, 250);

        assert_eq!(forwarder.compute_backoff_millis(1), 100);
        assert_eq!(forwarder.compute_backoff_millis(2), 200);
        assert_eq!(forwarder.compute_backoff_millis(3), 250);
        assert_eq!(forwarder.compute_backoff_millis(4), 250);
    }

    #[test]
    fn retry_policy_normalizes_invalid_inputs() {
        let forwarder =
            HttpClusterCommandForwarder::new(BTreeMap::new()).with_retry_policy(0, 0, 0);

        assert_eq!(forwarder.max_attempts, 1);
        assert_eq!(forwarder.base_backoff_ms, 1);
        assert_eq!(forwarder.max_backoff_ms, 1);
    }

    #[tokio::test]
    async fn returns_error_when_region_target_is_not_configured() {
        let forwarder = HttpClusterCommandForwarder::new(BTreeMap::new());

        let err = forwarder
            .forward(sample_command())
            .await
            .expect_err("expected configuration error");

        assert!(
            err.to_string()
                .contains("no cluster forward endpoint configured for region=eu-west")
        );
    }

    #[tokio::test]
    async fn retries_on_retryable_status_then_succeeds() {
        let endpoint_url = start_sequence_server(vec![500, 200]).await;
        let metrics = Arc::new(InMemoryRuntimeMetrics::default());
        let forwarder = HttpClusterCommandForwarder::new(BTreeMap::new())
            .with_region_target("eu-west", endpoint_url)
            .with_retry_policy(3, 1, 2)
            .with_metrics(metrics.clone());

        let accepted = forwarder
            .forward(sample_command())
            .await
            .expect("forward should succeed after retry");
        assert!(accepted);

        let snapshot = metrics.snapshot();
        assert_eq!(
            snapshot.counters.get(CLUSTER_FORWARD_ATTEMPTS_TOTAL),
            Some(&2)
        );
        assert_eq!(
            snapshot.counters.get(CLUSTER_FORWARD_RETRIES_TOTAL),
            Some(&1)
        );
        assert_eq!(
            snapshot.counters.get(CLUSTER_FORWARD_SUCCESSES_TOTAL),
            Some(&1)
        );
    }

    #[tokio::test]
    async fn records_no_route_and_failure_metrics_when_region_is_missing() {
        let metrics = Arc::new(InMemoryRuntimeMetrics::default());
        let forwarder =
            HttpClusterCommandForwarder::new(BTreeMap::new()).with_metrics(metrics.clone());

        let _ = forwarder
            .forward(sample_command())
            .await
            .expect_err("expected no-route error");

        let snapshot = metrics.snapshot();
        assert_eq!(
            snapshot.counters.get(CLUSTER_FORWARD_NO_ROUTE_TOTAL),
            Some(&1)
        );
        assert_eq!(
            snapshot.counters.get(CLUSTER_FORWARD_FAILURES_TOTAL),
            Some(&1)
        );
    }

    #[tokio::test]
    async fn deduplicates_repeated_correlation_id_within_ttl() {
        let endpoint_url = start_sequence_server(vec![200]).await;
        let metrics = Arc::new(InMemoryRuntimeMetrics::default());
        let forwarder = HttpClusterCommandForwarder::new(BTreeMap::new())
            .with_region_target("eu-west", endpoint_url)
            .with_metrics(metrics.clone());

        let first = forwarder
            .forward(sample_command())
            .await
            .expect("first forward should succeed");
        assert!(first);

        let second = forwarder
            .forward(sample_command())
            .await
            .expect("second forward should use dedupe cache");
        assert!(second);

        let snapshot = metrics.snapshot();
        assert_eq!(
            snapshot.counters.get(CLUSTER_FORWARD_ATTEMPTS_TOTAL),
            Some(&1)
        );
        assert_eq!(
            snapshot.counters.get(CLUSTER_FORWARD_IDEMPOTENT_HITS_TOTAL),
            Some(&1)
        );
    }
}
