/// W3C-compatible trace identifiers carried through jobs and spans.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceContext {
    pub trace_id: String,
    pub span_id: String,
    pub trace_flags: u8,
}

/// Typed span attribute values (never include secrets or raw payloads).
#[derive(Clone, Debug)]
pub enum OtelAttributeValue {
    String(String),
    Int(i64),
    Bool(bool),
}

/// OpenTelemetry span attribute key/value pair.
#[derive(Clone, Debug)]
pub struct OtelAttribute {
    pub key: &'static str,
    pub value: OtelAttributeValue,
}

impl OtelAttribute {
    pub fn string(key: &'static str, value: impl Into<String>) -> Self {
        Self {
            key,
            value: OtelAttributeValue::String(value.into()),
        }
    }

    pub fn int(key: &'static str, value: i64) -> Self {
        Self {
            key,
            value: OtelAttributeValue::Int(value),
        }
    }

    pub fn bool(key: &'static str, value: bool) -> Self {
        Self {
            key,
            value: OtelAttributeValue::Bool(value),
        }
    }
}

/// Ends an active span when dropped.
pub struct SpanGuard {
    inner: Option<Box<dyn Send + Sync>>,
}

impl SpanGuard {
    pub fn noop() -> Self {
        Self { inner: None }
    }

    pub(crate) fn new(inner: Box<dyn Send + Sync>) -> Self {
        Self {
            inner: Some(inner),
        }
    }
}

impl Drop for SpanGuard {
    fn drop(&mut self) {
        let _ = self.inner.take();
    }
}

/// Span lifecycle port for runtime observability.
pub trait RuntimeTracing: Send + Sync {
    fn start_span(&self, name: &'static str, attributes: &[OtelAttribute]) -> SpanGuard;

    fn start_span_with_trace_context(
        &self,
        name: &'static str,
        attributes: &[OtelAttribute],
        parent: Option<&TraceContext>,
    ) -> SpanGuard;

    fn active_trace_context(&self) -> Option<TraceContext>;
}

/// Runs a closure inside a child span of the current context.
pub fn in_span<F, R>(
    tracing: &dyn RuntimeTracing,
    name: &'static str,
    attributes: &[OtelAttribute],
    f: F,
) -> R
where
    F: FnOnce() -> R,
{
    let _guard = tracing.start_span(name, attributes);
    f()
}
