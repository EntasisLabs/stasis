pub mod assets;
pub mod bootstrap;
pub mod dto;
pub mod handlers;
pub mod htmx;
#[cfg(feature = "dashboard-embedded")]
pub mod integration;
pub mod mappers;
pub mod service;
pub mod trace_context;

pub use bootstrap::{
	build_dashboard_query_service, resolve_dashboard_runtime_backend, DashboardBootstrapOptions,
};
pub use handlers::{DashboardState, router};
#[cfg(feature = "dashboard-embedded")]
pub use integration::DashboardRouterExt;
pub use service::{
	DashboardQueryService, InMemoryDashboardQueryService, InspectEntity,
	RuntimeDashboardQueryService, WorkflowExecuteResult, WorkflowSaveRequest,
	WorkflowSaveResult,
};
