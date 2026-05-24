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
    use axum::body::Body;
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
    };
    use crate::domain::errors::{Result, StasisError};

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
        let runtime = RuntimeFactory::build(RuntimeBackend::SurrealMem {
            namespace: "stasis".to_string(),
            database: "dashboard_integration_test".to_string(),
        })
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
}
