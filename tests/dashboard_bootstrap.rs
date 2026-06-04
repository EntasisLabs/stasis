use stasis::dashboard::{
    build_dashboard_query_service, DashboardBootstrapOptions, DashboardQueryService,
    WorkflowSaveRequest,
};

fn valid_workflow_source() -> &'static str {
    r#"
import core from "grapheme/core"

query Echo {
  core.echo(message: "ping") {
    state {
      current
    }
  }
}
"#
}

#[tokio::test]
async fn dashboard_bootstrap_workflow_execute_runs_saved_grapheme_job() {
    let service = build_dashboard_query_service(DashboardBootstrapOptions { seed_demo: false })
        .await
        .expect("bootstrap in-memory dashboard");

    let source = valid_workflow_source();
    let saved = service
        .workflow_save(WorkflowSaveRequest {
            workflow_id: "wf.bootstrap.exec".to_string(),
            queue: "queue.bootstrap.exec".to_string(),
            source: source.to_string(),
            compile_mode_hint: None,
            graph_state_json: None,
            graph_modules_csv: None,
            graph_function_steps_csv: Some("core.echo".to_string()),
            graph_function_inputs_json: Some(
                r#"{"node-fn-core-echo-1":"{\"message\":\"ping\"}"}"#.to_string(),
            ),
        })
        .await
        .expect("workflow save should succeed");

    let executed = service
        .workflow_execute("wf.bootstrap.exec", "", "workflow-test")
        .await
        .expect("workflow execute should succeed");

    assert_eq!(executed.workflow_id, "wf.bootstrap.exec");
    assert_eq!(executed.queue, "queue.bootstrap.exec");
    assert_eq!(executed.revision_id, saved.revision_id);

    let job_id = executed
        .leased_job_id
        .expect("workflow execute should enqueue and lease a grapheme job");

    let jobs = service.jobs_stream().await.expect("jobs stream should load");
    let job = jobs
        .items
        .iter()
        .find(|row| row.id == job_id)
        .expect("executed job should appear in dashboard stream");
    assert_eq!(job.status, "succeeded");
    assert_eq!(job.queue, "queue.bootstrap.exec");
}

#[tokio::test]
async fn dashboard_bootstrap_in_memory_builds_without_demo_seed() {
    let service = build_dashboard_query_service(DashboardBootstrapOptions { seed_demo: false })
        .await
        .expect("bootstrap in-memory dashboard");

    let jobs = service.jobs_stream().await.expect("list jobs");
    assert!(jobs.items.is_empty());
}

#[tokio::test]
async fn dashboard_bootstrap_demo_seed_populates_jobs_and_endpoints() {
    let service = build_dashboard_query_service(DashboardBootstrapOptions { seed_demo: true })
        .await
        .expect("bootstrap demo dashboard");

    let jobs = service.jobs_stream().await.expect("list jobs");
    assert!(
        jobs.items.len() >= 2,
        "expected demo jobs after seed, got {}",
        jobs.items.len()
    );

    let endpoints = service.endpoint_stream().await.expect("list endpoints");
    assert!(
        endpoints.items.len() >= 2,
        "expected demo endpoints after seed, got {}",
        endpoints.items.len()
    );
}

#[tokio::test]
async fn dashboard_bootstrap_default_options_reads_demo_seed_env() {
    unsafe {
        std::env::set_var("STASIS_DASHBOARD_DEMO_SEED", "false");
        std::env::set_var("STASIS_DASHBOARD_RUNTIME_BACKEND", "in-memory");
    }

    let service = build_dashboard_query_service(DashboardBootstrapOptions::default())
        .await
        .expect("bootstrap with default options");

    let jobs = service.jobs_stream().await.expect("list jobs");
    assert!(jobs.items.is_empty());

    unsafe {
        std::env::remove_var("STASIS_DASHBOARD_DEMO_SEED");
        std::env::remove_var("STASIS_DASHBOARD_RUNTIME_BACKEND");
    }

    let _ = service;
}
