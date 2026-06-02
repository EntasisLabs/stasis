use std::sync::Arc;

use axum::Router;

use crate::dashboard::handlers;
use crate::dashboard::service::DashboardQueryService;
use crate::dashboard::DashboardState;

/// Router extension that mounts the built-in dashboard routes into an existing Axum app.
///
/// This mirrors a Hangfire-style integration point while preserving standalone dashboard support.
pub trait DashboardRouterExt {
    /// Adds dashboard routes using default dashboard state configuration.
    fn add_dashboard<S>(self, service: Arc<S>) -> Self
    where
        S: DashboardQueryService + 'static;

    /// Adds dashboard routes and allows configuring dashboard state (authz, role claims, etc.).
    fn add_dashboard_with<S, F>(self, service: Arc<S>, configure: F) -> Self
    where
        S: DashboardQueryService + 'static,
        F: FnOnce(DashboardState) -> DashboardState;
}

impl DashboardRouterExt for Router {
    fn add_dashboard<S>(self, service: Arc<S>) -> Self
    where
        S: DashboardQueryService + 'static,
    {
        self.add_dashboard_with(service, |state| state)
    }

    fn add_dashboard_with<S, F>(self, service: Arc<S>, configure: F) -> Self
    where
        S: DashboardQueryService + 'static,
        F: FnOnce(DashboardState) -> DashboardState,
    {
        let service: Arc<dyn DashboardQueryService> = service;
        let state = configure(DashboardState::new(service));
        self.merge(handlers::router(state))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use axum::Router;
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use crate::application::runtime::runtime_factory::{RuntimeBackend, RuntimeFactory};
    use crate::dashboard::dto::{
        ClusterMapDto, DashboardDto, EndpointRowDto, InspectorView, JobRowDto, OutboxEventRowDto,
        RecurringDefinitionRowDto, SystemKpiDto, UiListPanel,
    };
    use crate::dashboard::integration::DashboardRouterExt;
    use crate::dashboard::service::{
        DashboardQueryService, InspectEntity, RuntimeDashboardQueryService,
        WorkflowDiagnostic, WorkflowDiagnosticSeverity, WorkflowDiagnosticsResult,
        WorkflowExecuteResult, WorkflowSaveRequest, WorkflowSaveResult,
        WorkflowSavedRevisionSummary,
    };
    use crate::domain::errors::{Result, StasisError};
    use crate::ports::outbound::runtime::workflow_reflection::{
        WorkflowModuleInfoReflection, WorkflowModuleSearchReflection,
        WorkflowModuleTypesReflection, WorkflowSourceReflection,
    };

    #[derive(Clone)]
    struct StubDashboardService;

    impl StubDashboardService {
        fn unsupported<T>() -> Result<T> {
            Err(StasisError::PortFailure("unsupported in test".to_string()))
        }
    }

    #[async_trait]
    impl DashboardQueryService for StubDashboardService {
        async fn dashboard(&self, _inspect: Option<InspectEntity>) -> Result<DashboardDto> {
            Ok(DashboardDto {
                kpis: SystemKpiDto {
                    succeeded_jobs: 0,
                    failed_jobs: 0,
                    enqueued_jobs: 0,
                    running_jobs: 0,
                    pending_outbox: 0,
                    failed_outbox: 0,
                    healthy_nodes: 0,
                    degraded_nodes: 0,
                    offline_nodes: 0,
                    endpoint_failure_rate: "0.0%".to_string(),
                },
                job_stream: UiListPanel::<JobRowDto> {
                    items: vec![],
                    total: Some(0),
                    cursor: None,
                },
                outbox_stream: UiListPanel::<OutboxEventRowDto> {
                    items: vec![],
                    total: Some(0),
                    cursor: None,
                },
                cluster_map: ClusterMapDto { nodes: vec![] },
                inspector: InspectorView::None,
            })
        }

        async fn jobs_stream(&self) -> Result<UiListPanel<JobRowDto>> {
            Self::unsupported()
        }

        async fn outbox_stream(&self) -> Result<UiListPanel<OutboxEventRowDto>> {
            Self::unsupported()
        }

        async fn endpoint_stream(&self) -> Result<UiListPanel<EndpointRowDto>> {
            Self::unsupported()
        }

        async fn recurring_stream(&self) -> Result<UiListPanel<RecurringDefinitionRowDto>> {
            Self::unsupported()
        }

        async fn cluster_stream(&self) -> Result<ClusterMapDto> {
            Self::unsupported()
        }

        async fn scheduler_materialize_now(&self, _scheduler_id: &str) -> Result<usize> {
            Self::unsupported()
        }

        async fn scheduler_process_queue_once(
            &self,
            _queue: &str,
            _worker_id: &str,
        ) -> Result<Option<String>> {
            Self::unsupported()
        }

        async fn scheduler_publish_pending_now(&self, _limit: usize) -> Result<usize> {
            Self::unsupported()
        }

        async fn scheduler_replay_dead_letter_now(&self, _job_id: &str) -> Result<bool> {
            Self::unsupported()
        }

        async fn workflow_save(&self, _request: WorkflowSaveRequest) -> Result<WorkflowSaveResult> {
            Self::unsupported()
        }

        async fn workflow_execute(
            &self,
            _workflow_id: &str,
            _queue: &str,
            _worker_id: &str,
        ) -> Result<WorkflowExecuteResult> {
            Self::unsupported()
        }

        async fn workflow_reflect_source(&self, _source: &str) -> Result<WorkflowSourceReflection> {
            Self::unsupported()
        }

        async fn workflow_modules_search(&self, _query: &str) -> Result<WorkflowModuleSearchReflection> {
            Self::unsupported()
        }

        async fn workflow_module_info(&self, _module_id: &str) -> Result<Option<WorkflowModuleInfoReflection>> {
            Self::unsupported()
        }

        async fn workflow_module_types(&self, _module_id: &str) -> Result<Option<WorkflowModuleTypesReflection>> {
            Self::unsupported()
        }

        async fn workflow_saved_revision_summary(
            &self,
            _workflow_id: &str,
        ) -> Result<Option<WorkflowSavedRevisionSummary>> {
            Self::unsupported()
        }

        async fn workflow_lsp_diagnostics(
            &self,
            _source: &str,
        ) -> Result<WorkflowDiagnosticsResult> {
            Ok(WorkflowDiagnosticsResult {
                enabled: false,
                provider: "disabled".to_string(),
                summary: "LSP diagnostics are disabled. Enable the dashboard-lsp feature to activate diagnostics preview.".to_string(),
                diagnostics: vec![WorkflowDiagnostic {
                    severity: WorkflowDiagnosticSeverity::Info,
                    message: "dashboard-lsp feature is not enabled".to_string(),
                    code: Some("LSP_DISABLED".to_string()),
                    line: None,
                    column: None,
                }],
            })
        }

        async fn inspect(&self, _entity: InspectEntity) -> Result<InspectorView> {
            Self::unsupported()
        }
    }

    #[tokio::test]
    async fn add_dashboard_mounts_dashboard_routes() {
        let app: Router = Router::new().add_dashboard(Arc::new(StubDashboardService));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/dashboard")
                    .method("GET")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should build");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn add_dashboard_with_applies_action_auth_configuration() {
        let app: Router = Router::new().add_dashboard_with(
            Arc::new(StubDashboardService),
            |state: crate::dashboard::DashboardState| state.with_action_auth_bearer_token("token-1"),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/action/scheduler/materialize")
                    .method("POST")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should build");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn add_dashboard_mounts_with_surreal_mem_runtime_service() {
        let runtime = RuntimeFactory::build(RuntimeBackend::surreal_mem(
            "stasis",
            "dashboard_integration_test",
        ))
        .await
        .expect("surreal mem runtime should build");

        let service = Arc::new(RuntimeDashboardQueryService::from_runtime_composition(runtime));
        let app: Router = Router::new().add_dashboard(service);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/dashboard")
                    .method("GET")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should build");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn add_dashboard_exposes_workflow_reflection_stream_route() {
        let runtime = RuntimeFactory::build(RuntimeBackend::surreal_mem(
            "stasis",
            "dashboard_reflection_route_test",
        ))
        .await
        .expect("surreal mem runtime should build");

        let service = Arc::new(RuntimeDashboardQueryService::from_runtime_composition(runtime));
        let app: Router = Router::new().add_dashboard(service);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/stream/workflow-reflection?workflow_id=wf-int&queue=queue.int")
                    .method("GET")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should build");

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let html = String::from_utf8(body.to_vec()).expect("body should be utf8");
        assert!(html.contains("Flow Insights"));
        assert!(html.contains("Saved vs Live Drift"));
        assert!(html.contains("Readiness Guidance"));
        assert!(html.contains("Module Catalog"));
        assert!(html.contains("Grapheme Source Preview"));
    }

    #[tokio::test]
    async fn add_dashboard_exposes_workflow_reflection_module_drill_down_route() {
        let runtime = RuntimeFactory::build(RuntimeBackend::surreal_mem(
            "stasis",
            "dashboard_reflection_drilldown_test",
        ))
        .await
        .expect("surreal mem runtime should build");

        let service = Arc::new(RuntimeDashboardQueryService::from_runtime_composition(runtime));
        let app: Router = Router::new().add_dashboard(service);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/stream/workflow-reflection?workflow_id=wf-int&queue=queue.int&module_id=core")
                    .method("GET")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should build");

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let html = String::from_utf8(body.to_vec()).expect("body should be utf8");
        assert!(html.contains("entrypoint="));
        assert!(html.contains("ops="));
    }

    #[tokio::test]
    async fn add_dashboard_exposes_workflow_reflection_filter_empty_state() {
        let runtime = RuntimeFactory::build(RuntimeBackend::surreal_mem(
            "stasis",
            "dashboard_reflection_filter_test",
        ))
        .await
        .expect("surreal mem runtime should build");

        let service = Arc::new(RuntimeDashboardQueryService::from_runtime_composition(runtime));
        let app: Router = Router::new().add_dashboard(service);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/stream/workflow-reflection?workflow_id=wf-int&queue=queue.int&module_id=core&effect=__none__")
                    .method("GET")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should build");

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let html = String::from_utf8(body.to_vec()).expect("body should be utf8");
        assert!(html.contains("No exported operations matched selected filters."));
    }

    #[tokio::test]
    async fn add_dashboard_exposes_workflow_reflection_source_override_content() {
        let runtime = RuntimeFactory::build(RuntimeBackend::surreal_mem(
            "stasis",
            "dashboard_reflection_source_override_test",
        ))
        .await
        .expect("surreal mem runtime should build");

        let service = Arc::new(RuntimeDashboardQueryService::from_runtime_composition(runtime));
        let app: Router = Router::new().add_dashboard(service);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/stream/workflow-reflection?workflow_id=wf-int&queue=queue.int&source=custom_source_preview")
                    .method("GET")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should build");

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let html = String::from_utf8(body.to_vec()).expect("body should be utf8");
        assert!(html.contains("custom_source_preview"));
    }

    #[tokio::test]
    async fn add_dashboard_reflection_preserves_filters_and_source_across_module_cycle() {
        let runtime = RuntimeFactory::build(RuntimeBackend::surreal_mem(
            "stasis",
            "dashboard_reflection_source_filter_cycle_test",
        ))
        .await
        .expect("surreal mem runtime should build");

        let service = Arc::new(RuntimeDashboardQueryService::from_runtime_composition(runtime));
        let app: Router = Router::new().add_dashboard(service);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/stream/workflow-reflection?workflow_id=wf-int&queue=queue.int&source=custom_source_preview&module_id=core&capability=qa_capability&effect=qa_effect&op=qa_op")
                    .method("GET")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should build");

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let html = String::from_utf8(body.to_vec()).expect("body should be utf8");

        assert!(html.contains("Saved vs Live Drift"));
        assert!(html.contains("Readiness Guidance"));
        assert!(html.contains("custom_source_preview"));
        assert!(html.contains("value=\"qa_capability\""));
        assert!(html.contains("value=\"qa_effect\""));
        assert!(html.contains("value=\"qa_op\""));
        assert!(html.contains("No exported operations matched selected filters."));
    }

}
