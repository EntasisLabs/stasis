pub mod assets;
pub mod dto;
pub mod handlers;
pub mod htmx;
pub mod mappers;
pub mod service;

pub use handlers::{DashboardState, router};
pub use service::{DashboardQueryService, InMemoryDashboardQueryService, InspectEntity};
