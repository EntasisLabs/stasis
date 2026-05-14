use std::collections::HashSet;

use crate::domain::errors::{Result, StasisError};
use crate::domain::runtime::job_attempt::JobAttempt;
use crate::domain::runtime::outbox::OutboxEvent;
use crate::ports::outbound::runtime::job_attempt_store::JobAttemptStore;
use crate::ports::outbound::runtime::outbox_store::OutboxStore;

#[derive(Clone, Debug, Default)]
pub struct RuntimeLineageQuery {
    pub job_id: Option<String>,
    pub execution_id: Option<String>,
    pub guardrail_code: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct RuntimeLineageReport {
    pub attempts: Vec<JobAttempt>,
    pub lineage_events: Vec<OutboxEvent>,
}

#[derive(Clone)]
pub struct InvestigateRuntimeLineage<A, O>
where
    A: JobAttemptStore,
    O: OutboxStore,
{
    attempt_store: A,
    outbox_store: O,
}

impl<A, O> InvestigateRuntimeLineage<A, O>
where
    A: JobAttemptStore,
    O: OutboxStore,
{
    pub fn new(attempt_store: A, outbox_store: O) -> Self {
        Self {
            attempt_store,
            outbox_store,
        }
    }

    pub async fn execute(&self, query: RuntimeLineageQuery) -> Result<RuntimeLineageReport> {
        if query.job_id.is_none() && query.execution_id.is_none() && query.guardrail_code.is_none() {
            return Err(StasisError::PortFailure(
                "lineage query requires at least one selector: job_id, execution_id, or guardrail_code"
                    .to_string(),
            ));
        }

        let mut attempts = if let Some(job_id) = query.job_id.as_deref() {
            self.attempt_store.list_by_job_id(job_id).await?
        } else if let Some(execution_id) = query.execution_id.as_deref() {
            self.attempt_store.list_by_execution_id(execution_id).await?
        } else {
            self.attempt_store
                .list_by_guardrail_code(query.guardrail_code.as_deref().unwrap_or_default())
                .await?
        };

        if let Some(execution_id) = query.execution_id.as_deref() {
            attempts.retain(|attempt| attempt.execution_id.as_deref() == Some(execution_id));
        }
        if let Some(guardrail_code) = query.guardrail_code.as_deref() {
            attempts.retain(|attempt| attempt.guardrail_code.as_deref() == Some(guardrail_code));
        }
        if let Some(job_id) = query.job_id.as_deref() {
            attempts.retain(|attempt| attempt.job_id == job_id);
        }

        let mut lineage_events = if let Some(job_id) = query.job_id.as_deref() {
            self.outbox_store.list_by_job_id(job_id).await?
        } else if let Some(execution_id) = query.execution_id.as_deref() {
            self.outbox_store.list_by_execution_id(execution_id).await?
        } else {
            let mut out = Vec::new();
            let mut seen = HashSet::new();
            let job_ids: HashSet<String> = attempts.iter().map(|attempt| attempt.job_id.clone()).collect();
            for job_id in job_ids {
                for event in self.outbox_store.list_by_job_id(&job_id).await? {
                    if seen.insert(event.event_id.clone()) {
                        out.push(event);
                    }
                }
            }
            out
        };

        if let Some(execution_id) = query.execution_id.as_deref() {
            lineage_events
                .retain(|event| event.event.execution_id.as_deref() == Some(execution_id));
        }
        if let Some(job_id) = query.job_id.as_deref() {
            lineage_events.retain(|event| event.event.job_id == job_id);
        }

        lineage_events.sort_by_key(|event| event.event.occurred_at);
        attempts.sort_by_key(|attempt| attempt.attempt_number);

        Ok(RuntimeLineageReport {
            attempts,
            lineage_events,
        })
    }
}
