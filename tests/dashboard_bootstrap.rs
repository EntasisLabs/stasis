use stasis::dashboard::{
    build_dashboard_query_service, DashboardBootstrapOptions, DashboardQueryService,
};

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
