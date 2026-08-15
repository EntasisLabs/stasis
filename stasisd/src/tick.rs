use stasis::sdk::runtime_sdk::RuntimeSdk;

use crate::error::StasisdError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TickOptions {
    pub scheduler_id: String,
    pub worker_id: String,
    pub queues: Vec<String>,
    pub process_limit: usize,
    pub publish_limit: usize,
}

impl Default for TickOptions {
    fn default() -> Self {
        Self {
            scheduler_id: "stasisd".into(),
            worker_id: "stasisd".into(),
            queues: vec!["agents".into(), "default".into()],
            process_limit: 10,
            publish_limit: 100,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TickReport {
    pub materialized: usize,
    pub processed: usize,
    pub published: usize,
    pub stale_recovered: usize,
    pub stale_dead_lettered: usize,
}

pub async fn tick_once(
    runtime: &RuntimeSdk,
    options: &TickOptions,
) -> Result<TickReport, StasisdError> {
    let stale = runtime
        .recover_stale()
        .await
        .map_err(|err| StasisdError::Runtime(err.to_string()))?;

    let materialized = runtime
        .materialize_recurring_now(&options.scheduler_id)
        .await
        .map_err(|err| StasisdError::Runtime(err.to_string()))?;

    let mut processed = 0usize;
    for queue in &options.queues {
        for _ in 0..options.process_limit {
            match runtime
                .process_once(queue, &options.worker_id)
                .await
                .map_err(|err| StasisdError::Runtime(err.to_string()))?
            {
                Some(_) => processed += 1,
                None => break,
            }
        }
    }

    let published = runtime
        .publish_pending_events(options.publish_limit)
        .await
        .map_err(|err| StasisdError::Runtime(err.to_string()))?;

    Ok(TickReport {
        materialized,
        processed,
        published,
        stale_recovered: stale.recovered,
        stale_dead_lettered: stale.dead_lettered,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{DesiredState, OnRemovePolicy, StasisdSchedule};
    use crate::reconcile::reconcile;
    use async_trait::async_trait;
    use chrono::{Duration, Utc};
    use serde_json::json;
    use stasis::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
    use stasis::application::runtime::stasis_runtime_builder::StasisRuntimeBuilder;
    use stasis::domain::runtime::job::Job;
    use stasis::prelude::RuntimeBackend;
    use stasis::sdk::runtime_sdk::RuntimeSdk;

    struct FakePromptHandler;

    #[async_trait]
    impl JobHandler for FakePromptHandler {
        fn job_type(&self) -> &'static str {
            "workflow.stasis.prompt"
        }

        async fn execute(&self, job: &Job) -> stasis::domain::errors::Result<JobExecutionOutcome> {
            Ok(JobExecutionOutcome::Success {
                sttp_output_node_id: format!("sttp:fake:{}", job.id),
                execution_id: None,
                diagnostics: Some(r#"{"provider":"fake-prompt","status":"success"}"#.into()),
            })
        }
    }

    #[tokio::test]
    async fn tick_materializes_and_processes_fake_prompt() {
        let runtime = RuntimeSdk::from_builder(
            StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
                .without_prompt_handler()
                .with_extra_handler(FakePromptHandler),
        )
        .await
        .unwrap();

        let schedule = StasisdSchedule {
            id: "prompt-once".into(),
            enabled: true,
            queue: "agents".into(),
            job_type: "workflow.stasis.prompt".into(),
            cron: "0/1 * * * * * *".into(),
            timezone: "UTC".into(),
            jitter_seconds: 0,
            max_attempts: 1,
            on_remove: OnRemovePolicy::Drain,
            payload: json!({"user_prompt": "hello"}),
        };
        reconcile(
            &runtime,
            &DesiredState {
                sources: vec![],
                documents: vec![],
                schedules: vec![schedule],
                endpoints: vec![],
                mcp_gateways: vec![],
                diagnostics: vec![],
            },
        )
        .await
        .unwrap();

        let mut defs = runtime.list_recurring().await.unwrap();
        let mut def = defs.remove(0);
        def.next_run_at = Utc::now() - Duration::hours(1);
        runtime.save_recurring(def).await.unwrap();

        let report = tick_once(
            &runtime,
            &TickOptions {
                queues: vec!["agents".into()],
                process_limit: 5,
                ..TickOptions::default()
            },
        )
        .await
        .unwrap();

        assert!(report.materialized >= 1);
        assert!(report.processed >= 1);
        let stats = runtime.stats_snapshot(10).await.unwrap();
        assert!(stats.succeeded_jobs >= 1);
    }
}
