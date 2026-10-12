use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};

use tokio::sync::watch;

use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::application::runtime::job_context::{JobContext, JobContextServices};
use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::durable_wait::{DurableSignalRecord, DurableWaitStatus};
use crate::domain::runtime::job::{Job, JobState};
use crate::domain::runtime::typed_contract::StasisEvent;
use crate::ports::outbound::runtime::clock::Clock;
use crate::ports::outbound::runtime::durable_wait_store::DurableWaitStore;
use crate::ports::outbound::runtime::job_store::JobStore;

/// Save Running, then accept only the exact grant produced by that CAS.
/// A successor (even the same owner) is never execution authority for this call.
pub async fn running_snapshot(store: &dyn JobStore, mut job: Job) -> Result<Option<Job>> {
    let expected_version = job
        .version
        .checked_add(1)
        .ok_or_else(|| StasisError::PortFailure("version exhausted".into()))?;
    job.state = JobState::Running;
    store.save(job.clone()).await?;
    let Some(current) = store.get(&job.id).await? else {
        return Ok(None);
    };
    if current.execution_epoch != job.execution_epoch
        || current.incarnation != job.incarnation
        || current.lease_owner != job.lease_owner
        || current.version != expected_version
        || current.state != JobState::Running
    {
        return Ok(None);
    }
    Ok(Some(current))
}

pub type InFlightMap = Arc<RwLock<HashMap<String, watch::Sender<bool>>>>;

pub struct InFlightGuard {
    map: InFlightMap,
    job_id: String,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        if let Ok(mut map) = self.map.write() {
            map.remove(&self.job_id);
        }
    }
}

pub fn new_in_flight_map() -> InFlightMap {
    Arc::new(RwLock::new(HashMap::new()))
}

pub fn begin_in_flight(
    map: &InFlightMap,
    job_id: &str,
) -> Result<(watch::Receiver<bool>, InFlightGuard)> {
    let (tx, rx) = watch::channel(false);
    {
        let mut guard = map
            .write()
            .map_err(|_| StasisError::PortFailure("in-flight cancel lock poisoned".into()))?;
        guard.insert(job_id.to_string(), tx);
    }
    Ok((
        rx,
        InFlightGuard {
            map: map.clone(),
            job_id: job_id.to_string(),
        },
    ))
}

pub fn request_cancel(map: &InFlightMap, job_id: &str, epoch: u64) {
    if let Ok(map) = map.read() {
        if let Some(tx) = map.get(&format!("{}:execution:{}", job_id, epoch)) {
            let _ = tx.send(true);
        }
    }
}

pub fn is_cancel_flagged(map: &InFlightMap, job_id: &str) -> bool {
    map.read()
        .ok()
        .and_then(|map| map.get(job_id).map(|tx| *tx.borrow()))
        .unwrap_or(false)
}

pub fn is_terminal(state: &JobState) -> bool {
    matches!(
        state,
        JobState::Succeeded | JobState::Failed | JobState::DeadLetter | JobState::Canceled
    )
}

pub async fn execute_handler(
    handler: Option<Arc<dyn JobHandler>>,
    job: &Job,
    worker_id: &str,
    services: JobContextServices,
    in_flight: &InFlightMap,
) -> Result<(JobExecutionOutcome, InFlightGuard)> {
    let Some(handler) = handler else {
        let (_rx, guard) = begin_in_flight(
            in_flight,
            &format!("{}:execution:{}", job.id, job.execution_epoch),
        )?;
        return Ok((
            JobExecutionOutcome::FatalFailure {
                message: format!("no handler registered for job_type={}", job.job_type),
                execution_id: None,
                diagnostics: None,
            },
            guard,
        ));
    };
    let (rx, guard) = begin_in_flight(
        in_flight,
        &format!("{}:execution:{}", job.id, job.execution_epoch),
    )?;
    let ctx = JobContext::new(
        job,
        worker_id,
        rx,
        services,
        crate::application::runtime::job_lifecycle::DEFAULT_JOB_LEASE_SECONDS,
    );
    let outcome = match handler.execute_with_context(job, ctx).await {
        Ok(outcome) => outcome,
        Err(error) => JobExecutionOutcome::RetryableFailure {
            message: error.to_string(),
            execution_id: None,
            diagnostics: None,
        },
    };
    Ok((outcome, guard))
}

pub async fn job_was_canceled(
    job_store: &dyn JobStore,
    in_flight: &InFlightMap,
    job_id: &str,
) -> Result<bool> {
    if is_cancel_flagged(in_flight, job_id) {
        return Ok(true);
    }
    Ok(job_store
        .get(job_id)
        .await?
        .map(|job| job.state == JobState::Canceled)
        .unwrap_or(false))
}

pub async fn cancel_job(
    job_store: &dyn JobStore,
    wait_store: &dyn DurableWaitStore,
    in_flight: &InFlightMap,
    clock: &dyn Clock,
    job_id: &str,
) -> Result<Option<Job>> {
    let Some(job) = job_store.cancel_execution(job_id).await? else {
        return Ok(None);
    };
    request_cancel(in_flight, job_id, job.execution_epoch);
    for wait in wait_store.list_pending_by_job(job_id).await? {
        if wait.incarnation != job.incarnation {
            continue;
        }
        wait_store
            .complete_wait(
                &wait.wait_id,
                DurableWaitStatus::Cancelled,
                None,
                None,
                clock.now(),
            )
            .await?;
    }
    Ok(Some(job))
}

pub fn chrono_ttl(ttl: std::time::Duration) -> chrono::Duration {
    chrono::Duration::from_std(ttl).unwrap_or_else(|_| chrono::Duration::seconds(0))
}

pub fn durable_signal_id(signal_type: &str, correlation_key: &str, payload: &str) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    signal_type.hash(&mut hasher);
    correlation_key.hash(&mut hasher);
    payload.hash(&mut hasher);
    format!(
        "sig:{signal_type}:{correlation_key}:{:016x}",
        hasher.finish()
    )
}

pub async fn signal_event<E: StasisEvent>(
    job_store: &dyn JobStore,
    wait_store: &dyn DurableWaitStore,
    clock: &dyn Clock,
    correlation_key: String,
    event: E,
) -> Result<bool> {
    let payload = serde_json::to_string(&event)
        .map_err(|err| StasisError::PortFailure(format!("serialize signal: {err}")))?;
    // Capture recipients BEFORE publication. A delayed dispatcher cannot target a recreated job.
    let all_waits = wait_store.list_by_signal(E::NAME, &correlation_key).await?;
    let mut waits = Vec::new();
    let mut targets = Vec::new();
    // A logical recipient is sampled at most once, even when several waits share it.
    // Never broaden an already-observed logical ID to a replacement generation.
    let mut captured = std::collections::HashMap::new();
    for wait in &all_waits {
        if !captured.contains_key(&wait.job_id) {
            captured.insert(
                wait.job_id.clone(),
                job_store.get(&wait.job_id).await?.map(|j| j.incarnation),
            );
        }
        if captured.get(&wait.job_id) == Some(&Some(wait.incarnation)) && wait.incarnation != 0 {
            targets.push(wait.target_identity());
            if wait.status == DurableWaitStatus::Pending {
                waits.push(wait.clone());
            }
        }
    }
    // The direct-key read may race deletion/recreation. It can supply a buffered
    // recipient only when that logical ID was not already observed through waits.
    if let Some(job) = job_store.get(&correlation_key).await?
        && !captured.contains_key(&job.id)
        && job.incarnation != 0
    {
        targets.push(
            crate::domain::runtime::durable_wait::DurableWaitRecord::identity(
                &job.id,
                job.incarnation,
            ),
        );
    }
    targets.sort();
    targets.dedup();
    let target_incarnations = if targets.is_empty() && all_waits.is_empty() {
        None
    } else {
        Some(targets)
    };
    let identity = serde_json::to_string(&(&correlation_key, &target_incarnations))
        .map_err(|e| StasisError::PortFailure(e.to_string()))?;
    let signal_id = durable_signal_id(E::NAME, &identity, &payload);
    let now = clock.now();
    let inserted = wait_store
        .insert_signal(DurableSignalRecord {
            signal_id: signal_id.clone(),
            target_incarnations,
            signal_type: E::NAME.to_string(),
            correlation_key: correlation_key.clone(),
            payload_json: payload.clone(),
            created_at: now,
        })
        .await?;
    if !inserted {
        return Ok(false);
    }

    for wait in waits {
        let _ = wait_store
            .complete_wait(
                &wait.wait_id,
                DurableWaitStatus::Signaled,
                Some(payload.clone()),
                Some(signal_id.clone()),
                now,
            )
            .await?;
        job_store
            .wake_incarnation(&wait.job_id, wait.incarnation)
            .await?;
    }
    Ok(true)
}
