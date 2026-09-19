use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get, post};
use axum::{Json, Router};
use serde_json::{Value, json};

use crate::error::ApiError;
use crate::quota::QuotaStore;
use crate::subject::{subject_from_headers, utc_date_yyyy_mm_dd};

const CHAT_COMPLETIONS_PATH: &str = "/v1/chat/completions";
const MODELS_PATH: &str = "/v1/models";

const STRIP_HEADERS: &[&str] = &[
    "authorization",
    "proxy-authorization",
    "cookie",
    "set-cookie",
    "x-api-key",
    "api-key",
    "x-openai-api-key",
    "host",
    "content-length",
    "connection",
    "keep-alive",
    "proxy-connection",
    "transfer-encoding",
    "te",
    "trailer",
    "upgrade",
    "expect",
];

#[derive(Clone)]
pub struct ProxyState {
    pub client: reqwest::Client,
    pub openai_api_key: String,
    pub openai_base_url: String,
    pub allowed_models: Arc<Vec<String>>,
    pub quota: QuotaStore,
}

impl ProxyState {
    pub fn new(
        openai_api_key: String,
        openai_base_url: String,
        allowed_models: Vec<String>,
        quota: QuotaStore,
    ) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(60))
            .build()?;
        Ok(Self {
            client,
            openai_api_key,
            openai_base_url,
            allowed_models: Arc::new(allowed_models),
            quota,
        })
    }
}

pub fn router(state: ProxyState) -> Router {
    Router::new()
        .route(CHAT_COMPLETIONS_PATH, post(chat_completions))
        .route(MODELS_PATH, get(models))
        .fallback(any(reject_path))
        .with_state(state)
}

async fn reject_path() -> ApiError {
    ApiError::not_found("path is not allowed on the demo host").with_code("path_not_allowed")
}

async fn models(State(state): State<ProxyState>) -> impl IntoResponse {
    let data: Vec<Value> = state
        .allowed_models
        .iter()
        .map(|id| {
            json!({
                "id": id,
                "object": "model",
                "owned_by": "stasis-demo-host",
            })
        })
        .collect();
    Json(json!({ "object": "list", "data": data }))
}

async fn chat_completions(
    State(state): State<ProxyState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let subject = subject_from_headers(&headers)?;
    let subject_hex = subject.to_hex();
    let date = utc_date_yyyy_mm_dd();
    state.quota.check(&subject_hex, &date)?;

    let mut payload: Value = serde_json::from_slice(&body)
        .map_err(|_| ApiError::bad_request("request body must be JSON").with_param("body"))?;
    let Some(object) = payload.as_object_mut() else {
        return Err(ApiError::bad_request("request body must be a JSON object"));
    };

    if object.get("stream").and_then(Value::as_bool) == Some(true) {
        return Err(
            ApiError::bad_request("streaming is not supported on the demo host")
                .with_param("stream")
                .with_code("stream_not_supported"),
        );
    }

    let model = object
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string();
    if model.is_empty() {
        return Err(ApiError::bad_request("model is required").with_param("model"));
    }
    if !state.allowed_models.iter().any(|allowed| allowed == &model) {
        return Err(ApiError::bad_request(format!(
            "model '{model}' is not allowed on the demo host"
        ))
        .with_param("model")
        .with_code("model_not_allowed"));
    }

    let url = upstream_url(&state.openai_base_url, CHAT_COMPLETIONS_PATH);
    let mut request = state
        .client
        .request(Method::POST, &url)
        .header(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {}", state.openai_api_key),
        )
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(payload.to_string());

    for (name, value) in &headers {
        if should_forward(name) {
            request = request.header(name.clone(), value.clone());
        }
    }

    let upstream = request
        .send()
        .await
        .map_err(|_| ApiError::bad_gateway("upstream request failed"))?;

    let status =
        StatusCode::from_u16(upstream.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let upstream_headers = upstream.headers().clone();
    let bytes = upstream
        .bytes()
        .await
        .map_err(|_| ApiError::bad_gateway("upstream body failed"))?;

    if status.is_success() {
        let tokens = usage_tokens(&bytes);
        state.quota.record_success(&subject_hex, &date, tokens);
    }

    let mut response = Response::new(bytes.into());
    *response.status_mut() = status;
    let dest = response.headers_mut();
    if let Some(content_type) = upstream_headers.get(reqwest::header::CONTENT_TYPE) {
        if let Ok(value) = HeaderValue::from_bytes(content_type.as_bytes()) {
            dest.insert(axum::http::header::CONTENT_TYPE, value);
        }
    }
    Ok(response)
}

pub fn upstream_url(base: &str, request_path: &str) -> String {
    let base = base.trim_end_matches('/');
    let suffix = if base.ends_with("/v1") && request_path.starts_with("/v1/") {
        &request_path[3..]
    } else {
        request_path
    };
    format!("{base}{suffix}")
}

fn should_forward(name: &HeaderName) -> bool {
    let lower = name.as_str();
    if STRIP_HEADERS.contains(&lower) {
        return false;
    }
    if lower == crate::subject::SUBJECT_HEADER || lower == crate::subject::SUBJECT_HEADER_ALT {
        return false;
    }
    true
}

fn usage_tokens(body: &Bytes) -> u64 {
    let Ok(parsed) = serde_json::from_slice::<Value>(body) else {
        return 0;
    };
    let Some(usage) = parsed.get("usage") else {
        return 0;
    };
    let prompt = usage
        .get("prompt_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let completion = usage
        .get("completion_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    prompt.saturating_add(completion)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};

    use axum::body::Body;
    use axum::extract::Request;
    use axum::routing::post;
    use http_body_util::BodyExt;
    use tokio::net::TcpListener;
    use tower::ServiceExt;

    use crate::quota::QuotaLimits;
    use crate::subject::SUBJECT_HEADER;

    fn subject_header() -> (String, [u8; 32]) {
        let key = [11u8; 32];
        (hex::encode(key), key)
    }

    async fn spawn_openai_stub(captured: Arc<Mutex<Option<(HeaderMap, Bytes)>>>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/v1/chat/completions",
            post(move |headers: HeaderMap, body: Bytes| {
                let captured = captured.clone();
                async move {
                    *captured.lock().expect("capture") = Some((headers, body));
                    Json(json!({
                        "id": "chatcmpl-test",
                        "choices": [{
                            "index": 0,
                            "message": { "role": "assistant", "content": "ok" },
                            "finish_reason": "stop"
                        }],
                        "usage": {
                            "prompt_tokens": 3,
                            "completion_tokens": 1,
                            "total_tokens": 4
                        }
                    }))
                }
            }),
        );
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        format!("http://{addr}/v1")
    }

    fn proxy_app(base: String, quota: QuotaStore) -> Router {
        let state = ProxyState::new(
            "server-secret-key".into(),
            base,
            vec!["gpt-4o-mini".into()],
            quota,
        )
        .unwrap();
        router(state)
    }

    async fn send(app: Router, request: Request<Body>) -> (StatusCode, HeaderMap, String) {
        let response = app.oneshot(request).await.unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, headers, String::from_utf8_lossy(&body).into_owned())
    }

    fn chat_request(subject: &str, body: &str) -> Request<Body> {
        Request::post(CHAT_COMPLETIONS_PATH)
            .header("authorization", "Bearer client-should-be-stripped")
            .header("x-api-key", "client-api-key")
            .header(SUBJECT_HEADER, subject)
            .header("content-type", "application/json")
            .header("x-demo-forward", "keep-me")
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    #[test]
    fn maps_v1_base_without_doubling() {
        assert_eq!(
            upstream_url("https://api.openai.com/v1", "/v1/chat/completions"),
            "https://api.openai.com/v1/chat/completions"
        );
        assert_eq!(
            upstream_url("https://api.openai.com", "/v1/chat/completions"),
            "https://api.openai.com/v1/chat/completions"
        );
    }

    #[tokio::test]
    async fn strips_authorization_and_injects_server_key() {
        let captured = Arc::new(Mutex::new(None));
        let base = spawn_openai_stub(captured.clone()).await;
        let (subject, _) = subject_header();
        let app = proxy_app(base, QuotaStore::new(QuotaLimits::default()));
        let (status, _, body) = send(
            app,
            chat_request(
                &subject,
                r#"{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}]}"#,
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("chatcmpl-test"));

        let (headers, _) = captured
            .lock()
            .unwrap()
            .clone()
            .expect("upstream saw a request");
        let auth = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert_eq!(auth, "Bearer server-secret-key");
        assert!(headers.get("x-api-key").is_none());
        assert!(headers.get("cookie").is_none());
        assert_eq!(
            headers.get("x-demo-forward").and_then(|v| v.to_str().ok()),
            Some("keep-me")
        );
        assert!(headers.get(SUBJECT_HEADER).is_none());
        assert!(!auth.contains("client-should-be-stripped"));
    }

    #[tokio::test]
    async fn refuses_disallowed_path() {
        let (subject, _) = subject_header();
        let app = proxy_app(
            "http://127.0.0.1:9/v1".into(),
            QuotaStore::new(QuotaLimits::default()),
        );
        let (status, _, body) = send(
            app,
            Request::post("/v1/embeddings")
                .header(SUBJECT_HEADER, subject)
                .header("content-type", "application/json")
                .body(Body::from(r#"{"model":"gpt-4o-mini"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(body.contains("path_not_allowed"));
    }

    #[tokio::test]
    async fn refuses_disallowed_model() {
        let (subject, _) = subject_header();
        let app = proxy_app(
            "http://127.0.0.1:9/v1".into(),
            QuotaStore::new(QuotaLimits::default()),
        );
        let (status, _, body) = send(
            app,
            chat_request(&subject, r#"{"model":"gpt-4o","messages":[]}"#),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("model_not_allowed"));
    }

    #[tokio::test]
    async fn quota_trips_after_n_via_proxy() {
        let captured = Arc::new(Mutex::new(None));
        let base = spawn_openai_stub(captured).await;
        let quota = QuotaStore::new(QuotaLimits {
            daily_completions: Some(1),
            daily_tokens: None,
        });
        let (subject, _) = subject_header();
        let body = r#"{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}]}"#;

        let (first, _, _) = send(
            proxy_app(base.clone(), quota.clone()),
            chat_request(&subject, body),
        )
        .await;
        let (second, _, second_body) =
            send(proxy_app(base, quota), chat_request(&subject, body)).await;
        assert_eq!(first, StatusCode::OK);
        assert_eq!(second, StatusCode::TOO_MANY_REQUESTS);
        assert!(second_body.contains("daily_limit_exceeded"));
    }

    #[tokio::test]
    async fn models_stub_lists_allowlist() {
        let app = proxy_app(
            "http://127.0.0.1:9/v1".into(),
            QuotaStore::new(QuotaLimits::default()),
        );
        let (status, _, body) =
            send(app, Request::get(MODELS_PATH).body(Body::empty()).unwrap()).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("gpt-4o-mini"));
    }
}
