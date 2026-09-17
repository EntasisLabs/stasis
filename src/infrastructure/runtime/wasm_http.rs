//! Make `reqwest` WASM `fetch` usable from `Send` async traits.
//!
//! Browser `fetch` futures are `!Send`. Native `reqwest` is `Send`. Callers
//! (webhook / cluster HTTP) stay on `Send` ports; wasm hops through
//! `spawn_local` and a oneshot so the outer future remains `Send`.

use crate::domain::errors::{Result, StasisError};

pub async fn send_collecting(
    request: reqwest::RequestBuilder,
) -> Result<(reqwest::StatusCode, Vec<u8>)> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        let response = request
            .send()
            .await
            .map_err(|err| StasisError::PortFailure(format!("http request failed: {err}")))?;
        let status = response.status();
        let bytes = response.bytes().await.map_err(|err| {
            StasisError::PortFailure(format!("http response body failed: {err}"))
        })?;
        Ok((status, bytes.to_vec()))
    }

    #[cfg(target_arch = "wasm32")]
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        wasm_bindgen_futures::spawn_local(async move {
            let result = async {
                let response = request.send().await.map_err(|err| err.to_string())?;
                let status = response.status();
                let bytes = response
                    .bytes()
                    .await
                    .map_err(|err| err.to_string())?
                    .to_vec();
                Ok::<_, String>((status, bytes))
            }
            .await;
            let _ = tx.send(result);
        });
        rx.await
            .map_err(|_| StasisError::PortFailure("http task dropped".to_string()))?
            .map_err(|err| StasisError::PortFailure(format!("http request failed: {err}")))
    }
}
