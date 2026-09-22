use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use stasis::application::runtime::job_context::{JobConsumeError, JobContext, JobResult};
use stasis::application::runtime::typed_job::JobConsumer;
use stasis::domain::runtime::job::JobState;
use stasis::domain::runtime::job_continuation::ContinuationTrigger;
use stasis::domain::runtime::typed_contract::{JobDeclaration, RetryPolicy, StasisJob};
use stasis::sdk::runtime_sdk::RuntimeSdk;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct DeclaredJob {
    marker: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct DeclaredOutput {
    marker: String,
}

impl StasisJob for DeclaredJob {
    const NAME: &'static str = "declared_job";
    const VERSION: u32 = 1;
    type Output = DeclaredOutput;

    fn declaration() -> JobDeclaration {
        JobDeclaration::default()
            .queue("declared-q")
            .priority(7)
            .retry(RetryPolicy::exponential(2))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NotifyJob {
    note: String,
}

impl StasisJob for NotifyJob {
    const NAME: &'static str = "notify_job";
    const VERSION: u32 = 1;
    type Output = String;

    fn declaration() -> JobDeclaration {
        JobDeclaration::default().queue("notify")
    }
}

struct DeclaredConsumer;

#[async_trait]
impl JobConsumer<DeclaredJob> for DeclaredConsumer {
    async fn consume(&self, job: DeclaredJob, _ctx: JobContext) -> JobResult<DeclaredOutput> {
        Ok(DeclaredOutput { marker: job.marker })
    }
}

struct FatalConsumer;

#[async_trait]
impl JobConsumer<DeclaredJob> for FatalConsumer {
    async fn consume(&self, _job: DeclaredJob, _ctx: JobContext) -> JobResult<DeclaredOutput> {
        Err(JobConsumeError::Fatal("declared failed".into()))
    }
}

struct NotifyConsumer;

#[async_trait]
impl JobConsumer<NotifyJob> for NotifyConsumer {
    async fn consume(&self, _job: NotifyJob, ctx: JobContext) -> JobResult<String> {
        let parent_output = ctx.parent_output_json().await?;
        let parent_id = ctx.parent_job_id().await?;
        ctx.progress(serde_json::json!({
            "parent_id": parent_id,
            "parent_output": parent_output,
        }))
        .await?;
        Ok(parent_output.unwrap_or_default())
    }
}

struct ContinueFromConsumer;

#[async_trait]
impl JobConsumer<DeclaredJob> for ContinueFromConsumer {
    async fn consume(&self, job: DeclaredJob, ctx: JobContext) -> JobResult<DeclaredOutput> {
        ctx.continue_with(NotifyJob {
            note: format!("after-{}", job.marker),
        })
        .send()
        .await?;
        Ok(DeclaredOutput { marker: job.marker })
    }
}

async fn sdk(name: &str) -> RuntimeSdk {
    if name == "memory" {
        RuntimeSdk::in_memory().await.expect("memory sdk")
    } else {
        RuntimeSdk::surreal_mem("stasis", name)
            .await
            .expect("surreal sdk")
    }
}

async fn declaration_defaults_apply_until_overridden(name: &str) {
    let sdk = sdk(name).await;
    sdk.register_consumer(DeclaredConsumer).unwrap();
    let id = sdk
        .enqueue_job(DeclaredJob {
            marker: "alpha".into(),
        })
        .send()
        .await
        .unwrap();
    let job = sdk.get_job(&id).await.unwrap().unwrap();
    assert_eq!(job.queue, "declared-q");
    assert_eq!(job.priority, 7);
    assert_eq!(job.max_attempts, 2);

    let overridden = sdk
        .enqueue_job(DeclaredJob {
            marker: "beta".into(),
        })
        .queue("override")
        .send()
        .await
        .unwrap();
    let job = sdk.get_job(&overridden).await.unwrap().unwrap();
    assert_eq!(job.queue, "override");
    assert_eq!(job.priority, 7);
}

async fn success_continuation_runs_with_parent_output(name: &str) {
    let sdk = sdk(name).await;
    sdk.register_consumer(DeclaredConsumer).unwrap();
    sdk.register_consumer(NotifyConsumer).unwrap();
    let parent_id = sdk
        .enqueue_job(DeclaredJob {
            marker: "parent-marker".into(),
        })
        .send()
        .await
        .unwrap();
    let receipt = sdk
        .continue_with(
            &parent_id,
            NotifyJob {
                note: "next".into(),
            },
        )
        .send()
        .await
        .unwrap();
    assert!(receipt.child_job_id.is_none());

    sdk.process_once("declared-q", "worker")
        .await
        .unwrap()
        .expect("parent processed");
    let child_id = sdk
        .process_once("notify", "worker")
        .await
        .unwrap()
        .expect("continuation processed");
    let child = sdk.get_job(&child_id).await.unwrap().unwrap();
    assert_eq!(child.state, JobState::Succeeded);
    assert_eq!(child.causation_id, parent_id);
    let progress = child.progress_json.expect("parent output recorded");
    assert!(progress.contains("parent-marker"));
    assert!(progress.contains(&parent_id));
}

async fn failed_trigger_skips_success_continuation(name: &str) {
    let sdk = sdk(name).await;
    sdk.register_consumer(FatalConsumer).unwrap();
    sdk.register_consumer(NotifyConsumer).unwrap();
    let parent_id = sdk
        .enqueue_job(DeclaredJob {
            marker: "boom".into(),
        })
        .send()
        .await
        .unwrap();
    sdk.continue_with(
        &parent_id,
        NotifyJob {
            note: "on-success".into(),
        },
    )
    .send()
    .await
    .unwrap();
    let failed = sdk
        .continue_with(
            &parent_id,
            NotifyJob {
                note: "on-failure".into(),
            },
        )
        .trigger(ContinuationTrigger::Failed)
        .send()
        .await
        .unwrap();
    assert!(failed.child_job_id.is_none());

    sdk.process_once("declared-q", "worker").await.unwrap();
    let parent = sdk.get_job(&parent_id).await.unwrap().unwrap();
    assert_eq!(parent.state, JobState::DeadLetter);

    let child_id = sdk
        .process_once("notify", "worker")
        .await
        .unwrap()
        .expect("failure continuation");
    let child = sdk.get_job(&child_id).await.unwrap().unwrap();
    assert_eq!(child.state, JobState::Succeeded);
    let progress = child.progress_json.unwrap();
    assert!(progress.contains("declared failed"));
    assert!(
        sdk.process_once("notify", "worker")
            .await
            .unwrap()
            .is_none(),
        "success continuation must not materialize"
    );
}

async fn cancel_and_already_terminal_materialize(name: &str) {
    let sdk = sdk(name).await;
    sdk.register_consumer(DeclaredConsumer).unwrap();
    sdk.register_consumer(NotifyConsumer).unwrap();

    let canceled_parent = sdk
        .enqueue_job(DeclaredJob {
            marker: "cancel-me".into(),
        })
        .send()
        .await
        .unwrap();
    sdk.continue_with(
        &canceled_parent,
        NotifyJob {
            note: "after-cancel".into(),
        },
    )
    .trigger(ContinuationTrigger::Canceled)
    .send()
    .await
    .unwrap();
    assert!(sdk.cancel(&canceled_parent).await.unwrap());
    let canceled_child = sdk
        .process_once("notify", "worker")
        .await
        .unwrap()
        .expect("cancel continuation");
    let child = sdk.get_job(&canceled_child).await.unwrap().unwrap();
    assert_eq!(child.causation_id, canceled_parent);

    let done_parent = sdk
        .enqueue_job(DeclaredJob {
            marker: "already-done".into(),
        })
        .send()
        .await
        .unwrap();
    sdk.process_once("declared-q", "worker").await.unwrap();
    let receipt = sdk
        .continue_with(
            &done_parent,
            NotifyJob {
                note: "late".into(),
            },
        )
        .send()
        .await
        .unwrap();
    let late_child = receipt.child_job_id.expect("already-terminal child");
    sdk.process_once("notify", "worker").await.unwrap();
    let late = sdk.get_job(&late_child).await.unwrap().unwrap();
    assert_eq!(late.state, JobState::Succeeded);
    assert_eq!(late.causation_id, done_parent);
}

async fn in_job_continue_with_uses_child_declaration(name: &str) {
    let sdk = sdk(name).await;
    sdk.register_consumer(ContinueFromConsumer).unwrap();
    sdk.register_consumer(NotifyConsumer).unwrap();
    sdk.enqueue_job(DeclaredJob {
        marker: "from-handler".into(),
    })
    .send()
    .await
    .unwrap();
    sdk.process_once("declared-q", "worker").await.unwrap();
    let child_id = sdk
        .process_once("notify", "worker")
        .await
        .unwrap()
        .expect("in-job continuation");
    let child = sdk.get_job(&child_id).await.unwrap().unwrap();
    assert_eq!(child.queue, "notify");
    assert_eq!(child.state, JobState::Succeeded);
}

#[tokio::test]
async fn memory_declaration_defaults_apply_until_overridden() {
    declaration_defaults_apply_until_overridden("memory").await;
}

#[tokio::test]
async fn surreal_declaration_defaults_apply_until_overridden() {
    declaration_defaults_apply_until_overridden("decl_defaults").await;
}

#[tokio::test]
async fn memory_success_continuation_runs_with_parent_output() {
    success_continuation_runs_with_parent_output("memory").await;
}

#[tokio::test]
async fn surreal_success_continuation_runs_with_parent_output() {
    success_continuation_runs_with_parent_output("cont_success").await;
}

#[tokio::test]
async fn memory_failed_trigger_skips_success_continuation() {
    failed_trigger_skips_success_continuation("memory").await;
}

#[tokio::test]
async fn surreal_failed_trigger_skips_success_continuation() {
    failed_trigger_skips_success_continuation("cont_fail").await;
}

#[tokio::test]
async fn memory_cancel_and_already_terminal_materialize() {
    cancel_and_already_terminal_materialize("memory").await;
}

#[tokio::test]
async fn surreal_cancel_and_already_terminal_materialize() {
    cancel_and_already_terminal_materialize("cont_cancel").await;
}

#[tokio::test]
async fn memory_in_job_continue_with_uses_child_declaration() {
    in_job_continue_with_uses_child_declaration("memory").await;
}

#[tokio::test]
async fn surreal_in_job_continue_with_uses_child_declaration() {
    in_job_continue_with_uses_child_declaration("cont_in_job").await;
}

#[tokio::test]
async fn missing_parent_is_rejected() {
    let sdk = RuntimeSdk::in_memory().await.unwrap();
    let err = sdk
        .continue_with(
            "missing",
            NotifyJob {
                note: "nope".into(),
            },
        )
        .send()
        .await
        .expect_err("missing parent");
    assert!(err.to_string().contains("parent job not found"));
}
