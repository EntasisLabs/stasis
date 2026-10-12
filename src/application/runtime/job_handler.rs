use crate::application::runtime::job_context::JobContext;
use crate::application::runtime::job_lifecycle::JobLifecycleEvent;
use crate::domain::errors::Result;
use crate::domain::runtime::job::Job;

pub use crate::domain::runtime::execution::JobExecutionOutcome;

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

    async fn on_lifecycle_committed(
        &self,
        job: &Job,
        event: &JobLifecycleEvent,
        _idempotency_key: &str,
    ) -> Result<()> {
        self.on_lifecycle(job, event).await
    }

    async fn on_lifecycle(&self, job: &Job, event: &JobLifecycleEvent) -> Result<()> {
        let _ = (job, event);
        Ok(())
    }
}
}
