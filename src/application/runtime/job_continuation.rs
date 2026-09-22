use std::sync::Arc;

use chrono::{DateTime, Utc};

use crate::application::runtime::job_lifecycle::JobLifecycleEvent;
use crate::application::runtime::typed_job::encode_typed_payload;
use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::job::{Job, JobState, NewJob};
use crate::domain::runtime::job_continuation::{
    ChildJobSpec, ContinuationStatus, ContinuationTrigger, JobContinuation,
};
use crate::domain::runtime::placement::PlacementConstraints;
use crate::domain::runtime::typed_contract::{RetryPolicy, StasisJob};
use crate::ports::outbound::runtime::clock::Clock;
use crate::ports::outbound::runtime::id_generator::IdGenerator;
use crate::ports::outbound::runtime::job_continuation_store::JobContinuationStore;
use crate::ports::outbound::runtime::job_store::JobStore;

/// Result of registering a continuation. `child_job_id` is set when the parent was already terminal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContinuationReceipt {
    pub continuation_id: String,
    pub child_job_id: Option<String>,
}

pub struct ContinuationBuilder<C> {
    parent_job_id: String,
    payload: C,
    trigger: ContinuationTrigger,
    queue: Option<String>,
    priority: Option<i32>,
    retry: Option<RetryPolicy>,
    idempotency_key: Option<String>,
    correlation_id: Option<String>,
    placement: Option<PlacementConstraints>,
    clock: Arc<dyn Clock>,
    id_generator: Arc<dyn IdGenerator>,
    job_store: Arc<dyn JobStore>,
    continuations: Arc<dyn JobContinuationStore>,
}

impl<C: StasisJob> ContinuationBuilder<C> {
    pub fn new(
        parent_job_id: String,
        payload: C,
        clock: Arc<dyn Clock>,
        id_generator: Arc<dyn IdGenerator>,
        job_store: Arc<dyn JobStore>,
        continuations: Arc<dyn JobContinuationStore>,
    ) -> Self {
        Self {
            parent_job_id,
            payload,
            trigger: ContinuationTrigger::Succeeded,
            queue: None,
            priority: None,
            retry: None,
            idempotency_key: None,
            correlation_id: None,
            placement: None,
            clock,
            id_generator,
            job_store,
            continuations,
        }
    }

    pub fn trigger(mut self, trigger: ContinuationTrigger) -> Self {
        self.trigger = trigger;
        self
    }

    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = Some(queue.into());
        self
    }

    pub fn priority(mut self, priority: i32) -> Self {
        self.priority = Some(priority);
        self
    }

    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = Some(retry);
        self
    }

    pub fn idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }

    pub fn correlation_id(mut self, correlation_id: impl Into<String>) -> Self {
        self.correlation_id = Some(correlation_id.into());
        self
    }

    pub fn placement(mut self, placement: PlacementConstraints) -> Self {
        self.placement = Some(placement);
        self
    }

    pub async fn send(self) -> Result<ContinuationReceipt> {
        if self.job_store.get(&self.parent_job_id).await?.is_none() {
            return Err(StasisError::PortFailure(format!(
                "parent job not found for continuation: {}",
                self.parent_job_id
            )));
        }
        let declared = C::declaration();
        let retry = self.retry.unwrap_or(declared.retry);
        let payload_ref = encode_typed_payload(&self.payload)?;
        let id = self.id_generator.next_id("continuation");
        let created_at = self.clock.now();
        let record = JobContinuation {
            id: id.clone(),
            parent_job_id: self.parent_job_id.clone(),
            trigger: self.trigger,
            status: ContinuationStatus::Pending,
            child: ChildJobSpec {
                job_type: C::NAME.to_string(),
                payload_ref,
                queue: self.queue.unwrap_or(declared.queue),
                priority: self.priority.unwrap_or(declared.priority),
                max_attempts: retry.max_attempts,
                backoff: retry.backoff,
                idempotency_key: self.idempotency_key,
                correlation_id: self.correlation_id,
                placement: self.placement.unwrap_or(declared.placement),
            },
            child_job_id: None,
            parent_output_json: None,
            claim_token: None,
            created_at,
        };
        self.continuations.insert(record).await?;
        let parent = self
            .job_store
            .get(&self.parent_job_id)
            .await?
            .ok_or_else(|| {
                StasisError::PortFailure(format!(
                    "parent job disappeared before continuation settle: {}",
                    self.parent_job_id
                ))
            })?;
        if let Some(event) = event_for_terminal_state(&parent) {
            let detail = match parent.state {
                JobState::Succeeded => None,
                _ => parent.last_error.clone(),
            };
            settle_continuations(
                self.continuations.as_ref(),
                self.job_store.as_ref(),
                self.clock.as_ref(),
                self.id_generator.as_ref(),
                &parent,
                &event,
                detail,
            )
            .await?;
        }
        let stored = self.continuations.get(&id).await?.ok_or_else(|| {
            StasisError::PortFailure(format!("continuation disappeared after insert: {id}"))
        })?;
        Ok(ContinuationReceipt {
            continuation_id: id,
            child_job_id: stored.child_job_id,
        })
    }
}

pub async fn settle_continuations(
    continuations: &dyn JobContinuationStore,
    jobs: &dyn JobStore,
    clock: &dyn Clock,
    ids: &dyn IdGenerator,
    parent: &Job,
    event: &JobLifecycleEvent,
    parent_output_json: Option<String>,
) -> Result<()> {
    let Some(actual) = trigger_for_event(event) else {
        return Ok(());
    };
    let detail = parent_output_json.or_else(|| detail_from_event(event));
    let pending = continuations.list_pending_by_parent(&parent.id).await?;
    for mut record in pending {
        let token = ids.next_id("continuation-claim");
        if !continuations.try_claim(&record.id, &token).await? {
            continue;
        }
        if !record.trigger.matches_actual(actual) {
            record.status = ContinuationStatus::Discarded;
            record.claim_token = Some(token);
            continuations.save(record).await?;
            continue;
        }
        let child_id = ids.next_id("job");
        let now = clock.now();
        let child = child_job(&record, parent, child_id.clone(), now);
        if let Err(err) = jobs.insert(child).await {
            record.status = ContinuationStatus::Pending;
            record.claim_token = None;
            let _ = continuations.save(record).await;
            return Err(err);
        }
        record.status = ContinuationStatus::Materialized;
        record.child_job_id = Some(child_id);
        record.parent_output_json = detail.clone();
        record.claim_token = Some(token);
        continuations.save(record).await?;
    }
    Ok(())
}

fn trigger_for_event(event: &JobLifecycleEvent) -> Option<ContinuationTrigger> {
    match event {
        JobLifecycleEvent::Succeeded => Some(ContinuationTrigger::Succeeded),
        JobLifecycleEvent::DeadLettered { .. } => Some(ContinuationTrigger::Failed),
        JobLifecycleEvent::Canceled { .. } => Some(ContinuationTrigger::Canceled),
        JobLifecycleEvent::Deferred { .. } | JobLifecycleEvent::RetryScheduled { .. } => None,
    }
}

fn detail_from_event(event: &JobLifecycleEvent) -> Option<String> {
    match event {
        JobLifecycleEvent::DeadLettered { message } => Some(message.clone()),
        JobLifecycleEvent::Canceled { reason } => Some(reason.clone()),
        _ => None,
    }
}

fn event_for_terminal_state(job: &Job) -> Option<JobLifecycleEvent> {
    match job.state {
        JobState::Succeeded => Some(JobLifecycleEvent::Succeeded),
        JobState::Failed | JobState::DeadLetter => Some(JobLifecycleEvent::DeadLettered {
            message: job
                .last_error
                .clone()
                .unwrap_or_else(|| "parent failed".into()),
        }),
        JobState::Canceled => Some(JobLifecycleEvent::Canceled {
            reason: job
                .last_error
                .clone()
                .unwrap_or_else(|| "job cancelled".into()),
        }),
        JobState::Enqueued | JobState::Leased | JobState::Running => None,
    }
}

fn child_job(record: &JobContinuation, parent: &Job, child_id: String, now: DateTime<Utc>) -> Job {
    let idempotency_key = record
        .child
        .idempotency_key
        .clone()
        .unwrap_or_else(|| format!("idem-{child_id}"));
    let correlation_id = record
        .child
        .correlation_id
        .clone()
        .unwrap_or_else(|| parent.correlation_id.clone());
    NewJob {
        id: child_id,
        queue: record.child.queue.clone(),
        job_type: record.child.job_type.clone(),
        payload_ref: record.child.payload_ref.clone(),
        priority: record.child.priority,
        max_attempts: record.child.max_attempts,
        idempotency_key,
        correlation_id,
        causation_id: parent.id.clone(),
        trace_id: parent.trace_id.clone(),
        input_provenance: parent.output_provenance.clone(),
        placement: record.child.placement.clone(),
        scheduled_at: now,
        backoff_policy: record.child.backoff.clone(),
    }
    .into_job()
}
