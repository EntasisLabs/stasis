use std::collections::HashSet;

use serde_json::Value as JsonValue;

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
    pub thread_id: Option<String>,
    pub include_thread_ancestry: bool,
}

#[derive(Clone, Debug, Default)]
pub struct RuntimeLineageReport {
    pub attempts: Vec<JobAttempt>,
    pub lineage_events: Vec<OutboxEvent>,
    pub thread_ancestry: Vec<String>,
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
        if query.job_id.is_none()
            && query.execution_id.is_none()
            && query.guardrail_code.is_none()
            && query.thread_id.is_none()
        {
            return Err(StasisError::PortFailure(
                "lineage query requires at least one selector: job_id, execution_id, guardrail_code, or thread_id"
                    .to_string(),
            ));
        }

        let mut thread_selected_events = Vec::new();

        let mut attempts = if let Some(job_id) = query.job_id.as_deref() {
            self.attempt_store.list_by_job_id(job_id).await?
        } else if let Some(execution_id) = query.execution_id.as_deref() {
            self.attempt_store
                .list_by_execution_id(execution_id)
                .await?
        } else if let Some(guardrail_code) = query.guardrail_code.as_deref() {
            self.attempt_store
                .list_by_guardrail_code(guardrail_code)
                .await?
        } else if let Some(thread_id) = query.thread_id.as_deref() {
            let mut out = Vec::new();
            let mut seen_jobs = HashSet::new();
            thread_selected_events = self
                .list_events_for_thread_selector(thread_id, query.include_thread_ancestry)
                .await?;

            for event in &thread_selected_events {
                if seen_jobs.insert(event.event.job_id.clone()) {
                    out.extend(
                        self.attempt_store
                            .list_by_job_id(&event.event.job_id)
                            .await?,
                    );
                }
            }
            out
        } else {
            Vec::new()
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
        if let Some(thread_id) = query.thread_id.as_deref() {
            let thread_selected_job_ids: HashSet<String> = thread_selected_events
                .iter()
                .map(|event| event.event.job_id.clone())
                .collect();
            attempts.retain(|attempt| {
                if thread_selected_job_ids.contains(&attempt.job_id) {
                    return true;
                }
                let (root_thread_id, branch_thread_ids) = extract_thread_diagnostics(attempt);
                thread_selector_matches(
                    root_thread_id.as_deref(),
                    &branch_thread_ids,
                    thread_id,
                    query.include_thread_ancestry,
                )
            });
        }

        let mut lineage_events = if let Some(job_id) = query.job_id.as_deref() {
            self.outbox_store.list_by_job_id(job_id).await?
        } else if let Some(execution_id) = query.execution_id.as_deref() {
            self.outbox_store.list_by_execution_id(execution_id).await?
        } else if let Some(thread_id) = query.thread_id.as_deref() {
            if thread_selected_events.is_empty() {
                self.list_events_for_thread_selector(thread_id, query.include_thread_ancestry)
                    .await?
            } else {
                thread_selected_events
            }
        } else {
            let mut out = Vec::new();
            let mut seen = HashSet::new();
            let job_ids: HashSet<String> = attempts
                .iter()
                .map(|attempt| attempt.job_id.clone())
                .collect();
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
        let selected_job_ids: HashSet<String> = attempts
            .iter()
            .map(|attempt| attempt.job_id.clone())
            .collect();
        lineage_events.retain(|event| selected_job_ids.contains(&event.event.job_id));

        let thread_ancestry = derive_thread_ancestry(
            &attempts,
            &lineage_events,
            query.thread_id.as_deref(),
            query.include_thread_ancestry,
        );

        lineage_events.sort_by_key(|event| event.event.occurred_at);
        attempts.sort_by_key(|attempt| attempt.attempt_number);

        Ok(RuntimeLineageReport {
            attempts,
            lineage_events,
            thread_ancestry,
        })
    }
}

impl<A, O> InvestigateRuntimeLineage<A, O>
where
    A: JobAttemptStore,
    O: OutboxStore,
{
    async fn list_events_for_thread_selector(
        &self,
        selector_thread_id: &str,
        include_thread_ancestry: bool,
    ) -> Result<Vec<OutboxEvent>> {
        let mut out = self
            .outbox_store
            .list_by_thread_id(selector_thread_id)
            .await?;

        if include_thread_ancestry {
            if let Some(parent_thread_id) = parent_thread_id_for_branch(selector_thread_id) {
                out.extend(
                    self.outbox_store
                        .list_by_thread_id(parent_thread_id)
                        .await?,
                );
            } else {
                let branch_prefix = format!("{selector_thread_id}::branch::");
                out.extend(
                    self.outbox_store
                        .list_by_thread_prefix(&branch_prefix)
                        .await?,
                );
            }

            dedupe_events(&mut out);
        }

        out.sort_by_key(|event| event.event.occurred_at);
        Ok(out)
    }
}

fn dedupe_events(events: &mut Vec<OutboxEvent>) {
    let mut seen = HashSet::new();
    events.retain(|event| seen.insert(event.event_id.clone()));
}

fn extract_thread_diagnostics(attempt: &JobAttempt) -> (Option<String>, Vec<String>) {
    let Some(raw) = attempt.diagnostics.as_deref() else {
        return (None, Vec::new());
    };
    let Ok(parsed) = serde_json::from_str::<JsonValue>(raw) else {
        return (None, Vec::new());
    };

    let root_thread_id = parsed
        .get("thread_id")
        .and_then(|value| value.as_str())
        .map(|value| value.to_string());
    let branch_thread_ids = parsed
        .get("branch_thread_ids")
        .and_then(|value| value.as_array())
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value.as_str().map(|item| item.to_string()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    (root_thread_id, branch_thread_ids)
}

fn parent_thread_id_for_branch(thread_id: &str) -> Option<&str> {
    thread_id.split_once("::branch::").map(|(parent, _)| parent)
}

fn thread_selector_matches(
    root_thread_id: Option<&str>,
    branch_thread_ids: &[String],
    selector_thread_id: &str,
    include_thread_ancestry: bool,
) -> bool {
    if root_thread_id == Some(selector_thread_id)
        || branch_thread_ids.iter().any(|id| id == selector_thread_id)
    {
        return true;
    }

    if !include_thread_ancestry {
        return false;
    }

    if let Some(parent_selector) = parent_thread_id_for_branch(selector_thread_id) {
        return root_thread_id == Some(parent_selector)
            && branch_thread_ids.iter().any(|id| id == selector_thread_id);
    }

    root_thread_id == Some(selector_thread_id)
        && branch_thread_ids
            .iter()
            .any(|id| id.starts_with(&format!("{selector_thread_id}::branch::")))
}

fn derive_thread_ancestry(
    attempts: &[JobAttempt],
    lineage_events: &[OutboxEvent],
    selector_thread_id: Option<&str>,
    include_thread_ancestry: bool,
) -> Vec<String> {
    let Some(selector_thread_id) = selector_thread_id else {
        return Vec::new();
    };

    let mut ancestry = HashSet::new();
    ancestry.insert(selector_thread_id.to_string());

    if !include_thread_ancestry {
        let mut out = ancestry.into_iter().collect::<Vec<_>>();
        out.sort();
        return out;
    }

    if let Some(parent) = parent_thread_id_for_branch(selector_thread_id) {
        ancestry.insert(parent.to_string());
    }

    let descendant_prefix = format!("{selector_thread_id}::branch::");
    for event in lineage_events {
        let Some(thread_id) = event.event.thread_id.as_deref() else {
            continue;
        };
        if thread_id == selector_thread_id || thread_id.starts_with(&descendant_prefix) {
            ancestry.insert(thread_id.to_string());
        }
    }

    for attempt in attempts {
        let (root_thread_id, branch_thread_ids) = extract_thread_diagnostics(attempt);
        if let Some(root_thread_id) = root_thread_id {
            if root_thread_id == selector_thread_id {
                ancestry.insert(root_thread_id);
                ancestry.extend(branch_thread_ids);
                continue;
            }
            if branch_thread_ids.iter().any(|id| id == selector_thread_id) {
                ancestry.insert(root_thread_id);
            }
        }
    }

    let mut out = ancestry.into_iter().collect::<Vec<_>>();
    out.sort();
    out
}
