use axum::extract::Request;
use axum::http::HeaderMap;
use axum::middleware::Next;
use axum::response::Response;

use crate::application::telemetry::propagation::parse_traceparent;
use crate::application::telemetry::request_context;
use crate::ports::outbound::runtime::runtime_tracing::TraceContext;

pub fn extract_traceparent_from_headers(headers: &HeaderMap) -> Option<TraceContext> {
    headers
        .get("traceparent")
        .or_else(|| headers.get("Traceparent"))
        .and_then(|value| value.to_str().ok())
        .and_then(|header| parse_traceparent(header).ok())
}

pub async fn propagate_inbound_trace_context(request: Request, next: Next) -> Response {
    let Some(trace) = extract_traceparent_from_headers(request.headers()) else {
        return next.run(request).await;
    };

    request_context::scope_inbound_trace(trace, async move { next.run(request).await }).await
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderMap;

    use super::*;

    #[test]
    fn extract_traceparent_from_headers_is_case_insensitive_on_key() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "traceparent",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
                .parse()
                .expect("valid header value"),
        );

        let context = extract_traceparent_from_headers(&headers).expect("trace context");
        assert_eq!(context.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
    }
}
