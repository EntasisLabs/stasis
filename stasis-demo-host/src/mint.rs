use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};

use crate::error::ApiError;
use crate::rate_limit::SlidingWindowLimiter;
use crate::subject::{parse_public_key_bytes, subject_hex};

/// Issues a subject-bound `usi1.` token. Production wraps `urspace::Site`.
pub trait TokenMinter: Send + Sync {
    fn mint(&self, subject: [u8; 32], ttl: Duration, max_sessions: u32) -> anyhow::Result<String>;
}

#[derive(Clone)]
pub struct MintState {
    pub minter: Arc<dyn TokenMinter>,
    pub per_ip: Arc<SlidingWindowLimiter>,
    pub per_subject: Arc<SlidingWindowLimiter>,
    pub mint_ttl: Duration,
    pub max_sessions: u32,
    pub trust_proxy: bool,
}

#[derive(Debug, Deserialize)]
pub struct BootstrapRequest {
    pub public_key: Vec<u8>,
}

pub fn router(state: MintState, cors_origins: &[String]) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/bootstrap", post(bootstrap))
        .layer(cors_layer(cors_origins))
        .with_state(state)
}

fn cors_layer(origins: &[String]) -> CorsLayer {
    let layer = CorsLayer::new()
        .allow_methods([
            axum::http::Method::GET,
            axum::http::Method::POST,
            axum::http::Method::OPTIONS,
        ])
        .allow_headers([axum::http::header::CONTENT_TYPE]);
    if origins.len() == 1 && origins[0] == "*" {
        layer.allow_origin(Any)
    } else {
        let parsed: Vec<HeaderValue> = origins
            .iter()
            .filter_map(|origin| origin.parse().ok())
            .collect();
        layer.allow_origin(AllowOrigin::list(parsed))
    }
}

async fn health() -> impl IntoResponse {
    Json(json!({ "ok": true, "service": "stasis-demo-host" }))
}

async fn bootstrap(
    State(state): State<MintState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<BootstrapRequest>,
) -> Result<Response, ApiError> {
    let subject = parse_public_key_bytes(&body.public_key)?;
    let ip_key = client_ip(&headers, peer, state.trust_proxy);
    state.per_ip.try_acquire(&ip_key)?;
    state.per_subject.try_acquire(&subject_hex(&subject))?;

    let token = state
        .minter
        .mint(subject, state.mint_ttl, state.max_sessions)
        .map_err(|_| {
            tracing::info!("mint rejected");
            ApiError::bad_request("public_key is not a valid session public key")
                .with_param("public_key")
        })?;

    if !token.starts_with("usi1.") {
        tracing::error!("minter returned a non-usi1 token");
        return Err(ApiError::bad_gateway("mint failed"));
    }

    tracing::info!("mint accepted");
    Ok((StatusCode::OK, token).into_response())
}

fn client_ip(headers: &HeaderMap, peer: SocketAddr, trust_proxy: bool) -> String {
    if trust_proxy
        && let Some(forwarded) = headers
            .get("x-forwarded-for")
            .and_then(|value| value.to_str().ok())
        && let Some(first) = forwarded.split(',').next()
    {
        let first = first.trim();
        if !first.is_empty() {
            return first.to_string();
        }
    }
    peer.ip().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    struct OkMinter;

    impl TokenMinter for OkMinter {
        fn mint(
            &self,
            _subject: [u8; 32],
            _ttl: Duration,
            _max_sessions: u32,
        ) -> anyhow::Result<String> {
            Ok("usi1.test-token".into())
        }
    }

    struct RejectMinter;

    impl TokenMinter for RejectMinter {
        fn mint(
            &self,
            _subject: [u8; 32],
            _ttl: Duration,
            _max_sessions: u32,
        ) -> anyhow::Result<String> {
            anyhow::bail!("mint subject is not a valid public key")
        }
    }

    fn test_state(minter: Arc<dyn TokenMinter>) -> MintState {
        MintState {
            minter,
            per_ip: Arc::new(SlidingWindowLimiter::new(20, Duration::from_secs(60))),
            per_subject: Arc::new(SlidingWindowLimiter::new(20, Duration::from_secs(60))),
            mint_ttl: Duration::from_secs(30),
            max_sessions: 1,
            trust_proxy: false,
        }
    }

    fn app(minter: Arc<dyn TokenMinter>) -> Router {
        router(test_state(minter), &["*".into()])
    }

    fn with_connect_info(mut request: Request<Body>) -> Request<Body> {
        request.extensions_mut().insert(ConnectInfo(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            40000,
        )));
        request
    }

    async fn send(app: Router, request: Request<Body>) -> (StatusCode, String) {
        let response = app
            .oneshot(with_connect_info(request))
            .await
            .expect("router");
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8_lossy(&bytes).into_owned())
    }

    #[tokio::test]
    async fn mint_rejects_short_public_key() {
        let (status, body) = send(
            app(Arc::new(OkMinter)),
            Request::post("/bootstrap")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"public_key":[1,2,3]}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("public_key"));
        assert!(!body.contains("usi1."));
    }

    #[tokio::test]
    async fn mint_rejects_missing_public_key() {
        let (status, _) = send(
            app(Arc::new(OkMinter)),
            Request::post("/bootstrap")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"foo":1}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn mint_rejects_invalid_curve_point_from_site() {
        let key = [1u8; 32];
        let (status, body) = send(
            app(Arc::new(RejectMinter)),
            Request::post("/bootstrap")
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"public_key":{}}}"#,
                    serde_json::to_string(&key.to_vec()).unwrap()
                )))
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("public_key"));
        assert!(!body.contains("usi1."));
    }

    #[tokio::test]
    async fn mint_returns_plain_usi1_token() {
        let key = [7u8; 32];
        let (status, body) = send(
            app(Arc::new(OkMinter)),
            Request::post("/bootstrap")
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"public_key":{}}}"#,
                    serde_json::to_string(&key.to_vec()).unwrap()
                )))
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "usi1.test-token");
    }

    #[tokio::test]
    async fn health_ok() {
        let (status, body) = send(
            app(Arc::new(OkMinter)),
            Request::get("/health").body(Body::empty()).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("stasis-demo-host"));
    }

    #[tokio::test]
    async fn mint_rate_limit_by_subject() {
        let limiter = Arc::new(SlidingWindowLimiter::new(1, Duration::from_secs(60)));
        let state = MintState {
            minter: Arc::new(OkMinter),
            per_ip: Arc::new(SlidingWindowLimiter::new(20, Duration::from_secs(60))),
            per_subject: limiter,
            mint_ttl: Duration::from_secs(30),
            max_sessions: 1,
            trust_proxy: false,
        };
        let app = router(state, &["*".into()]);
        let key = [3u8; 32];
        let request = || {
            Request::post("/bootstrap")
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"public_key":{}}}"#,
                    serde_json::to_string(&key.to_vec()).unwrap()
                )))
                .unwrap()
        };
        let (first, _) = send(app.clone(), request()).await;
        let (second, body) = send(app, request()).await;
        assert_eq!(first, StatusCode::OK);
        assert_eq!(second, StatusCode::TOO_MANY_REQUESTS);
        assert!(body.contains("Too many mint requests"));
    }
}
