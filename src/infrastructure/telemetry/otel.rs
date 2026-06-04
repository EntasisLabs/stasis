use std::sync::OnceLock;

use opentelemetry::global;
use opentelemetry::global::BoxedTracer;
use opentelemetry::metrics::{Counter, Histogram, Meter};
use opentelemetry::trace::{TraceContextExt, Tracer};
use opentelemetry::{Context, KeyValue};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;

use crate::application::config::env::{non_empty, with_default};
use crate::domain::errors::{Result, StasisError};
use crate::ports::outbound::runtime::runtime_metrics::RuntimeMetrics;
use crate::ports::outbound::runtime::runtime_tracing::{
    OtelAttribute, OtelAttributeValue, RuntimeTracing, SpanGuard, TraceContext,
};

static OTEL_INIT: OnceLock<std::result::Result<(), String>> = OnceLock::new();
static SERVICE_NAME: OnceLock<String> = OnceLock::new();

fn ensure_otel_initialized() -> Result<()> {
    match OTEL_INIT.get_or_init(|| init_otel_providers().map_err(|err| err.to_string())) {
        Ok(()) => Ok(()),
        Err(message) => Err(StasisError::PortFailure(message.clone())),
    }
}

pub struct OpenTelemetryTelemetry {
    tracer: BoxedTracer,
    meter: Meter,
}

impl OpenTelemetryTelemetry {
    pub fn from_env() -> Result<std::sync::Arc<Self>> {
        if !otel_enabled() {
            return Err(StasisError::PortFailure(
                "OpenTelemetry disabled via STASIS_OTEL_ENABLED".to_string(),
            ));
        }

        ensure_otel_initialized()?;

        let service_name = resolve_service_name();
        let tracer = global::tracer(service_name.clone());
        let meter = global::meter("stasis-runtime");

        Ok(std::sync::Arc::new(Self { tracer, meter }))
    }

    fn key_value(attribute: &OtelAttribute) -> KeyValue {
        match &attribute.value {
            OtelAttributeValue::String(value) => KeyValue::new(attribute.key, value.clone()),
            OtelAttributeValue::Int(value) => KeyValue::new(attribute.key, *value),
            OtelAttributeValue::Bool(value) => KeyValue::new(attribute.key, *value),
        }
    }

    fn counter(&self, name: &str) -> Counter<u64> {
        self.meter.u64_counter(name.to_string()).build()
    }

    fn histogram(&self, name: &str) -> Histogram<f64> {
        self.meter.f64_histogram(name.to_string()).build()
    }
}

impl RuntimeMetrics for OpenTelemetryTelemetry {
    fn incr_counter(&self, name: &str, value: u64) {
        self.counter(name).add(value, &[]);
    }

    fn observe_duration_ms(&self, name: &str, duration_ms: u64) {
        self.histogram(name).record(duration_ms as f64, &[]);
    }
}

impl RuntimeTracing for OpenTelemetryTelemetry {
    fn start_span(&self, name: &'static str, attributes: &[OtelAttribute]) -> SpanGuard {
        self.start_span_with_trace_context(name, attributes, None)
    }

    fn start_span_with_trace_context(
        &self,
        name: &'static str,
        attributes: &[OtelAttribute],
        parent: Option<&TraceContext>,
    ) -> SpanGuard {
        let otel_attributes: Vec<KeyValue> = attributes.iter().map(Self::key_value).collect();
        let mut builder = self
            .tracer
            .span_builder(name)
            .with_attributes(otel_attributes);

        if let Some(parent) = parent {
            if let Ok(trace_id) = opentelemetry::trace::TraceId::from_hex(&parent.trace_id) {
                builder = builder.with_trace_id(trace_id);
                if let Ok(span_id) = opentelemetry::trace::SpanId::from_hex(&parent.span_id) {
                    builder = builder.with_span_id(span_id);
                }
            }
        }

        let span = builder.start(&self.tracer);
        SpanGuard::new(Box::new(span))
    }

    fn active_trace_context(&self) -> Option<TraceContext> {
        let cx = Context::current();
        let span = cx.span();
        let context = span.span_context();
        if !context.is_valid() {
            return None;
        }

        Some(TraceContext {
            trace_id: format!("{:032x}", context.trace_id()),
            span_id: format!("{:016x}", context.span_id()),
            trace_flags: context.trace_flags().to_u8(),
        })
    }
}

pub fn otel_enabled() -> bool {
    !matches!(
        non_empty("STASIS_OTEL_ENABLED").as_deref(),
        Some("0") | Some("false") | Some("no") | Some("off")
    )
}

fn resolve_service_name() -> String {
    SERVICE_NAME
        .get_or_init(|| {
            non_empty("STASIS_OTEL_SERVICE_NAME")
                .or_else(|| non_empty("OTEL_SERVICE_NAME"))
                .unwrap_or_else(|| with_default("STASIS_OTEL_SERVICE_NAME", "stasis-runtime"))
        })
        .clone()
}

fn init_otel_providers() -> Result<()> {
    global::set_text_map_propagator(TraceContextPropagator::new());

    let service_name = resolve_service_name();
    let resource = Resource::builder()
        .with_service_name(service_name.clone())
        .build();

    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .build()
        .map_err(|err| StasisError::PortFailure(err.to_string()))?;
    let tracer_provider = SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_batch_exporter(span_exporter)
        .build();
    global::set_tracer_provider(tracer_provider);

    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .build()
        .map_err(|err| StasisError::PortFailure(err.to_string()))?;
    let reader = PeriodicReader::builder(metric_exporter).build();
    let meter_provider = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_reader(reader)
        .build();
    global::set_meter_provider(meter_provider);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::telemetry::keys::JOB_SUCCEEDED_TOTAL;
    use std::sync::{Mutex, OnceLock};

    fn test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .expect("otel test lock should be available")
    }

    #[test]
    fn otel_enabled_respects_master_switch() {
        let _guard = test_lock();
        unsafe {
            std::env::set_var("STASIS_OTEL_ENABLED", "false");
        }
        assert!(!otel_enabled());
        unsafe {
            std::env::remove_var("STASIS_OTEL_ENABLED");
        }
    }

    #[test]
    fn open_telemetry_metrics_increment_counter_without_panic() {
        let _guard = test_lock();
        let telemetry = OpenTelemetryTelemetry {
            tracer: global::tracer("stasis-test"),
            meter: global::meter("stasis-test"),
        };
        telemetry.incr_counter(JOB_SUCCEEDED_TOTAL, 1);
        telemetry.observe_duration_ms(
            crate::application::telemetry::keys::JOB_PROCESS_DURATION_MS,
            5,
        );
    }
}
