use chrono::{DateTime, Utc};

use crate::application::runtime::job_context::JobContext;
use crate::application::runtime::job_lifecycle::JobLifecycleEvent;
use crate::domain::errors::Result;
use crate::domain::runtime::job::Job;

#[derive(Clone, Debug)]
pub enum JobExecutionOutcome {
    Success {
        output_provenance: Option<crate::domain::runtime::provenance::ProvenanceRef>,
        execution_id: Option<String>,
        diagnostics: Option<String>,
    },
    RetryableFailure {
        message: String,
        execution_id: Option<String>,
        diagnostics: Option<String>,
    },
    FatalFailure {
        message: String,
        execution_id: Option<String>,
        diagnostics: Option<String>,
    },
    /// Re-enqueue without consuming an attempt (durable external wait).
    Deferred {
        scheduled_at: DateTime<Utc>,
        message: String,
        execution_id: Option<String>,
        diagnostics: Option<String>,
    },
}

crate::async_store! {
pub trait JobHandler: Send + Sync {
    fn job_type(&self) -> &'static str;
    async fn execute(&self, job: &Job) -> Result<JobExecutionOutcome>;
    async fn execute_with_context(
        &self,
        job: &Job,
        _ctx: JobContext,
    ) -> Result<JobExecutionOutcome> {
        self.execute(job).await
    }

    async fn on_lifecycle(&self, job: &Job, event: &JobLifecycleEvent) -> Result<()> {
        let _ = (job, event);
        Ok(())
    }
}
}
