pub mod noop;

#[cfg(feature = "otel")]
pub mod otel;

pub use noop::{NoopRuntimeTelemetry, NoopRuntimeTracing};

#[cfg(feature = "otel")]
pub use otel::OpenTelemetryTelemetry;
