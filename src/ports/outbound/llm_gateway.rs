use crate::domain::errors::Result;

/// LLM completion port used by `StasisSdk`.
///
/// On `wasm32-unknown-unknown`, futures are `?Send` because browser `fetch`
/// (`reqwest` wasm) is not `Send`. Native stays `Send` for the multi-thread runtime.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait LlmGateway: Send + Sync {
    async fn complete(&self, prompt: &str) -> Result<String>;
}
