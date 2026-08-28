use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use stasis::domain::runtime::delivery_endpoint::{
    DeliveryProtocol, NewDeliveryEndpoint,
};
use stasis::domain::runtime::recurring::RecurringDefinition;
use stasis::ports::outbound::runtime::delivery_endpoint_store::DeliveryEndpointStore;
use stasis::sdk::runtime_sdk::RuntimeSdk;

use crate::error::StasisdError;
use crate::model::{
    DesiredState, OnRemovePolicy, StasisdEndpoint, StasisdEndpointProtocol, StasisdSchedule,
};
use crate::provenance::{
    is_managed_endpoint_id, is_managed_recurring_id, managed_endpoint_id, managed_recurring_id,
    strip_managed_endpoint_prefix, strip_managed_prefix,
};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    pub created: Vec<String>,
    pub updated: Vec<String>,
    pub drained: Vec<String>,
    pub orphaned: Vec<String>,
    pub unchanged: Vec<String>,
    pub skipped_cancel: Vec<String>,
    pub endpoint_created: Vec<String>,
    pub endpoint_updated: Vec<String>,
    pub endpoint_disabled: Vec<String>,
    pub endpoint_unchanged: Vec<String>,
}

/// Apply desired schedules onto a runtime, touching only `stasisd:`-managed definitions.
///
/// Removal policy defaults to `drain` for managed ids that disappeared from desired state.
#[cfg(test)]
pub async fn reconcile(
    runtime: &RuntimeSdk,
    desired: &DesiredState,
) -> Result<ReconcileReport, StasisdError> {
    reconcile_with_endpoint_store(runtime, desired, None).await
}

/// Reconcile schedules and optional declarative delivery endpoints.
pub async fn reconcile_with_endpoint_store(
    runtime: &RuntimeSdk,
    desired: &DesiredState,
    endpoint_store: Option<Arc<dyn DeliveryEndpointStore>>,
) -> Result<ReconcileReport, StasisdError> {
    let removal_policies = desired
        .schedules
        .iter()
        .map(|schedule| (schedule.id.clone(), schedule.on_remove.clone()))
        .collect();
    let mut report =
        reconcile_with_removal_policies(runtime, desired, &removal_policies).await?;
    if let Some(store) = endpoint_store {
        let endpoint_report = reconcile_endpoints(store.as_ref(), desired).await?;
        report.endpoint_created = endpoint_report.endpoint_created;
        report.endpoint_updated = endpoint_report.endpoint_updated;
        report.endpoint_disabled = endpoint_report.endpoint_disabled;
        report.endpoint_unchanged = endpoint_report.endpoint_unchanged;
    }
    Ok(report)
}

/// Reconcile managed delivery endpoints (`stasisd:endpoint:<id>`).
pub async fn reconcile_endpoints(
    store: &dyn DeliveryEndpointStore,
    desired: &DesiredState,
) -> Result<ReconcileReport, StasisdError> {
    let existing = store
        .list()
        .await
        .map_err(|err| StasisdError::Runtime(err.to_string()))?;
    let managed: Vec<_> = existing
        .into_iter()
        .filter(|ep| is_managed_endpoint_id(&ep.endpoint_id))
        .collect();

    let desired_by_id: HashMap<String, &StasisdEndpoint> = desired
        .endpoints
        .iter()
        .map(|endpoint| (managed_endpoint_id(&endpoint.id), endpoint))
        .collect();

    let mut report = ReconcileReport::default();
    let now = Utc::now();

    for endpoint in &desired.endpoints {
        let runtime_id = managed_endpoint_id(&endpoint.id);
        let next = endpoint_to_new(endpoint, now)?;
        if let Some(current) = managed.iter().find(|ep| ep.endpoint_id == runtime_id) {
            if endpoint_effectively_equal(current, &next) {
                report.endpoint_unchanged.push(runtime_id);
                continue;
            }
            store
                .upsert(next)
                .await
                .map_err(|err| StasisdError::Runtime(err.to_string()))?;
            report.endpoint_updated.push(runtime_id);
        } else {
            store
                .upsert(next)
                .await
                .map_err(|err| StasisdError::Runtime(err.to_string()))?;
            report.endpoint_created.push(runtime_id);
        }
    }

    // Build removal policies from config ids that may still appear in documents
    // even when filtered out of desired.endpoints; default drain disables the endpoint.
    let mut removal_policies: HashMap<String, OnRemovePolicy> = HashMap::new();
    for document in &desired.documents {
        for endpoint in &document.endpoints {
            removal_policies.insert(endpoint.id.clone(), endpoint.on_remove.clone());
        }
    }
    for endpoint in &desired.endpoints {
        removal_policies.insert(endpoint.id.clone(), endpoint.on_remove.clone());
    }

    for current in &managed {
        if desired_by_id.contains_key(&current.endpoint_id) {
            continue;
        }
        let config_id = strip_managed_endpoint_prefix(&current.endpoint_id)
            .unwrap_or(current.endpoint_id.as_str())
            .to_string();
        let removal = removal_policies
            .get(&config_id)
            .cloned()
            .unwrap_or(OnRemovePolicy::Drain);
        match removal {
            OnRemovePolicy::Orphan => {
                report.orphaned.push(current.endpoint_id.clone());
            }
            OnRemovePolicy::Drain | OnRemovePolicy::Cancel => {
                store
                    .set_enabled(&current.endpoint_id, false)
                    .await
                    .map_err(|err| StasisdError::Runtime(err.to_string()))?;
                report.endpoint_disabled.push(current.endpoint_id.clone());
            }
        }
    }

    Ok(report)
}

fn endpoint_to_new(
    endpoint: &StasisdEndpoint,
    now: chrono::DateTime<Utc>,
) -> Result<NewDeliveryEndpoint, StasisdError> {
    let protocol = match endpoint.protocol {
        StasisdEndpointProtocol::HttpWebhook => DeliveryProtocol::HttpWebhook,
        StasisdEndpointProtocol::Tcp => DeliveryProtocol::Tcp,
        StasisdEndpointProtocol::Kafka => DeliveryProtocol::Kafka,
        StasisdEndpointProtocol::RabbitMq => DeliveryProtocol::RabbitMq,
    };
    let metadata = if endpoint.metadata.is_null()
        || endpoint
            .metadata
            .as_object()
            .map(|o| o.is_empty())
            .unwrap_or(false)
    {
        None
    } else {
        Some(
            serde_json::to_string(&endpoint.metadata)
                .map_err(|err| StasisdError::Validation(format!("endpoint metadata: {err}")))?,
        )
    };
    Ok(NewDeliveryEndpoint {
        endpoint_id: managed_endpoint_id(&endpoint.id),
        name: endpoint.name.clone(),
        protocol,
        target: endpoint.target.clone(),
        metadata,
        created_at: now,
    })
}

fn endpoint_effectively_equal(
    current: &stasis::domain::runtime::delivery_endpoint::DeliveryEndpoint,
    next: &NewDeliveryEndpoint,
) -> bool {
    current.endpoint_id == next.endpoint_id
        && current.name == next.name
        && current.protocol == next.protocol
        && current.target == next.target
        && current.metadata == next.metadata
        && current.enabled
}

/// Reconcile with an explicit removal-policy lookup for schedules that disappeared.
pub async fn reconcile_with_removal_policies(
    runtime: &RuntimeSdk,
    desired: &DesiredState,
    removal_policies: &HashMap<String, OnRemovePolicy>,
) -> Result<ReconcileReport, StasisdError> {
    let existing = runtime
        .list_recurring()
        .await
        .map_err(|err| StasisdError::Runtime(err.to_string()))?;
    let managed: Vec<RecurringDefinition> = existing
        .into_iter()
        .filter(|def| is_managed_recurring_id(&def.id))
        .collect();

    let desired_by_runtime_id: HashMap<String, &StasisdSchedule> = desired
        .schedules
        .iter()
        .map(|schedule| (managed_recurring_id(&schedule.id), schedule))
        .collect();

    let mut report = ReconcileReport::default();
    let now = Utc::now();

    for schedule in &desired.schedules {
        let runtime_id = managed_recurring_id(&schedule.id);
        if let Some(current) = managed.iter().find(|def| def.id == runtime_id) {
            let next = schedule_to_definition(schedule, now, Some(current))?;
            if recurring_effectively_equal(current, &next) {
                report.unchanged.push(runtime_id);
                continue;
            }
            runtime
                .save_recurring(next)
                .await
                .map_err(|err| StasisdError::Runtime(err.to_string()))?;
            report.updated.push(runtime_id);
        } else {
            let next = schedule_to_definition(schedule, now, None)?;
            runtime
                .register_recurring(next)
                .await
                .map_err(|err| StasisdError::Runtime(err.to_string()))?;
            report.created.push(runtime_id);
        }
    }

    for current in &managed {
        if desired_by_runtime_id.contains_key(&current.id) {
            continue;
        }
        let schedule_id = strip_managed_prefix(&current.id)
            .unwrap_or(current.id.as_str())
            .to_string();
        let policy = removal_policies
            .get(&schedule_id)
            .cloned()
            .unwrap_or(OnRemovePolicy::Drain);

        match policy {
            OnRemovePolicy::Drain => {
                let mut disabled = current.clone();
                disabled.enabled = false;
                disabled.lease_owner = None;
                disabled.lease_expires_at = None;
                runtime
                    .save_recurring(disabled)
                    .await
                    .map_err(|err| StasisdError::Runtime(err.to_string()))?;
                report.drained.push(current.id.clone());
            }
            OnRemovePolicy::Orphan => {
                report.orphaned.push(current.id.clone());
            }
            OnRemovePolicy::Cancel => {
                // Cancel requires in-flight job attribution; Phase 1 disables future
                // materializations and records that cancel was not fully applied.
                let mut disabled = current.clone();
                disabled.enabled = false;
                runtime
                    .save_recurring(disabled)
                    .await
                    .map_err(|err| StasisdError::Runtime(err.to_string()))?;
                report.skipped_cancel.push(current.id.clone());
            }
        }
    }

    Ok(report)
}

fn schedule_to_definition(
    schedule: &StasisdSchedule,
    now: chrono::DateTime<Utc>,
    previous: Option<&RecurringDefinition>,
) -> Result<RecurringDefinition, StasisdError> {
    let payload_template_ref = serde_json::to_string(&schedule.payload)
        .map_err(|err| StasisdError::Validation(format!("payload encode failed: {err}")))?;

    let cron_or_tz_changed = previous
        .map(|def| def.cron_expr != schedule.cron || def.timezone != schedule.timezone)
        .unwrap_or(true);

    let mut definition = RecurringDefinition {
        id: managed_recurring_id(&schedule.id),
        queue: schedule.queue.clone(),
        job_type: schedule.job_type.clone(),
        payload_template_ref,
        cron_expr: schedule.cron.clone(),
        timezone: schedule.timezone.clone(),
        jitter_seconds: schedule.jitter_seconds,
        enabled: schedule.enabled,
        max_attempts: schedule.max_attempts,
        next_run_at: previous.map(|def| def.next_run_at).unwrap_or(now),
        last_run_at: previous.and_then(|def| def.last_run_at),
        lease_owner: previous.and_then(|def| def.lease_owner.clone()),
        lease_expires_at: previous.and_then(|def| def.lease_expires_at),
    };

    if cron_or_tz_changed {
        definition.next_run_at = definition
            .compute_next_run_at(now)
            .map_err(|err| StasisdError::Validation(err.to_string()))?;
    }

    Ok(definition)
}

fn recurring_effectively_equal(left: &RecurringDefinition, right: &RecurringDefinition) -> bool {
    left.id == right.id
        && left.queue == right.queue
        && left.job_type == right.job_type
        && left.payload_template_ref == right.payload_template_ref
        && left.cron_expr == right.cron_expr
        && left.timezone == right.timezone
        && left.jitter_seconds == right.jitter_seconds
        && left.enabled == right.enabled
        && left.max_attempts == right.max_attempts
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{DesiredState, OnRemovePolicy, StasisdSchedule};
    use serde_json::json;
    use stasis::domain::runtime::recurring::RecurringDefinition;
    use stasis::sdk::runtime_sdk::RuntimeSdk;

    fn schedule(id: &str, cron: &str) -> StasisdSchedule {
        StasisdSchedule {
            id: id.into(),
            enabled: true,
            queue: "agents".into(),
            job_type: "workflow.stasis.prompt".into(),
            cron: cron.into(),
            timezone: "UTC".into(),
            jitter_seconds: 0,
            max_attempts: 3,
            on_remove: OnRemovePolicy::Drain,
            payload: json!({"user_prompt": "hi"}),
        }
    }

    // 7-field cron dialect used by RecurringDefinition / cron 0.12.

    fn desired(schedules: Vec<StasisdSchedule>) -> DesiredState {
        DesiredState {
            sources: Vec::new(),
            documents: Vec::new(),
            schedules,
            endpoints: Vec::new(),
            mcp_gateways: Vec::new(),
            diagnostics: Vec::new(),
        }
    }

    #[tokio::test]
    async fn creates_updates_and_drains_managed_defs() {
        let runtime = RuntimeSdk::in_memory().await.unwrap();
        let report = reconcile(
            &runtime,
            &desired(vec![schedule("nightly", "0 0 2 * * * *")]),
        )
        .await
        .unwrap();
        assert_eq!(report.created, vec!["stasisd:nightly".to_string()]);

        let report = reconcile(
            &runtime,
            &desired(vec![schedule("nightly", "0 0 3 * * * *")]),
        )
        .await
        .unwrap();
        assert_eq!(report.updated, vec!["stasisd:nightly".to_string()]);

        let report = reconcile(&runtime, &desired(vec![])).await.unwrap();
        assert_eq!(report.drained, vec!["stasisd:nightly".to_string()]);

        let defs = runtime.list_recurring().await.unwrap();
        let nightly = defs.iter().find(|d| d.id == "stasisd:nightly").unwrap();
        assert!(!nightly.enabled);
    }

    #[tokio::test]
    async fn does_not_touch_unmanaged_definitions() {
        let runtime = RuntimeSdk::in_memory().await.unwrap();
        runtime
            .register_recurring(RecurringDefinition {
                id: "manual".into(),
                queue: "agents".into(),
                job_type: "workflow.stasis.prompt".into(),
                payload_template_ref: "{}".into(),
                cron_expr: "0 0 * * * * *".into(),
                timezone: "UTC".into(),
                jitter_seconds: 0,
                enabled: true,
                max_attempts: 3,
                next_run_at: Utc::now(),
                last_run_at: None,
                lease_owner: None,
                lease_expires_at: None,
            })
            .await
            .unwrap();

        let report = reconcile(
            &runtime,
            &desired(vec![schedule("managed", "0 0 * * * * *")]),
        )
        .await
        .unwrap();
        assert_eq!(report.created, vec!["stasisd:managed".to_string()]);

        let report = reconcile(&runtime, &desired(vec![])).await.unwrap();
        assert_eq!(report.drained, vec!["stasisd:managed".to_string()]);

        let defs = runtime.list_recurring().await.unwrap();
        let manual = defs.iter().find(|d| d.id == "manual").unwrap();
        assert!(manual.enabled);
    }

    #[tokio::test]
    async fn unchanged_when_definition_matches() {
        let runtime = RuntimeSdk::in_memory().await.unwrap();
        reconcile(
            &runtime,
            &desired(vec![schedule("s", "0 0 * * * * *")]),
        )
        .await
        .unwrap();
        let report = reconcile(
            &runtime,
            &desired(vec![schedule("s", "0 0 * * * * *")]),
        )
        .await
        .unwrap();
        assert_eq!(report.unchanged, vec!["stasisd:s".to_string()]);
        assert!(report.updated.is_empty());
    }

    #[tokio::test]
    async fn orphan_policy_leaves_definition_enabled() {
        let runtime = RuntimeSdk::in_memory().await.unwrap();
        reconcile(
            &runtime,
            &desired(vec![schedule("keep", "0 0 * * * * *")]),
        )
        .await
        .unwrap();

        let mut policies = HashMap::new();
        policies.insert("keep".into(), OnRemovePolicy::Orphan);
        let report = reconcile_with_removal_policies(&runtime, &desired(vec![]), &policies)
            .await
            .unwrap();
        assert_eq!(report.orphaned, vec!["stasisd:keep".to_string()]);
        let defs = runtime.list_recurring().await.unwrap();
        assert!(defs.iter().any(|d| d.id == "stasisd:keep" && d.enabled));
    }

    #[tokio::test]
    async fn cancel_policy_disables_and_records_skip() {
        let runtime = RuntimeSdk::in_memory().await.unwrap();
        reconcile(&runtime, &desired(vec![schedule("c", "0 0 * * * * *")]))
            .await
            .unwrap();
        let mut policies = HashMap::new();
        policies.insert("c".into(), OnRemovePolicy::Cancel);
        let report = reconcile_with_removal_policies(&runtime, &desired(vec![]), &policies)
            .await
            .unwrap();
        assert_eq!(report.skipped_cancel, vec!["stasisd:c".to_string()]);
        let defs = runtime.list_recurring().await.unwrap();
        assert!(defs.iter().any(|d| d.id == "stasisd:c" && !d.enabled));
    }

    #[tokio::test]
    async fn drained_definition_does_not_materialize() {
        let runtime = RuntimeSdk::in_memory().await.unwrap();
        reconcile(
            &runtime,
            &desired(vec![schedule("due", "0/1 * * * * * *")]),
        )
        .await
        .unwrap();

        let mut defs = runtime.list_recurring().await.unwrap();
        let mut def = defs.remove(0);
        def.next_run_at = Utc::now() - chrono::Duration::hours(1);
        runtime.save_recurring(def).await.unwrap();

        let produced = runtime.materialize_recurring_now("sched-1").await.unwrap();
        assert!(produced >= 1);

        reconcile(&runtime, &desired(vec![])).await.unwrap();
        let produced_after_drain = runtime.materialize_recurring_now("sched-2").await.unwrap();
        assert_eq!(produced_after_drain, 0);
    }
}
