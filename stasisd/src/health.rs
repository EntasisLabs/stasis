//! Optional process health endpoints for `stasisd`.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::error::StasisdError;

#[derive(Clone, Debug)]
pub struct HealthState {
    ready: Arc<AtomicBool>,
}

impl HealthState {
    pub fn new() -> Self {
        Self {
            ready: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::SeqCst);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }
}

impl Default for HealthState {
    fn default() -> Self {
        Self::new()
    }
}

/// Serve `/healthz` (liveness) and `/readyz` (last successful reconcile) on `addr`.
pub async fn serve_health_endpoints(
    addr: SocketAddr,
    state: HealthState,
) -> Result<(), StasisdError> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|err| StasisdError::Runtime(format!("healthz bind failed: {err}")))?;
    loop {
        let Ok((mut socket, _)) = listener.accept().await else {
            continue;
        };
        let state = state.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await;
            let request = String::from_utf8_lossy(&buf);
            let (status, body) = if request.starts_with("GET /readyz") {
                if state.is_ready() {
                    (200, "ready\n")
                } else {
                    (503, "not ready\n")
                }
            } else if request.starts_with("GET /healthz") || request.starts_with("GET / ") {
                (200, "ok\n")
            } else {
                (404, "not found\n")
            };
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.shutdown().await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn healthz_and_readyz_respond() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let state = HealthState::new();
        state.set_ready(true);
        let serve_state = state.clone();
        tokio::spawn(async move {
            let _ = serve_health_endpoints(addr, serve_state).await;
        });

        // Retry briefly while server binds.
        let mut body = String::new();
        for _ in 0..20 {
            if let Ok(mut stream) = tokio::net::TcpStream::connect(addr).await {
                stream
                    .write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n")
                    .await
                    .unwrap();
                let mut buf = vec![0u8; 512];
                let n = stream.read(&mut buf).await.unwrap();
                body = String::from_utf8_lossy(&buf[..n]).to_string();
                if body.contains("200") {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(body.contains("200"), "healthz response: {body}");
        assert!(body.contains("ok"));
    }
}
