use async_trait::async_trait;
use anyhow::Result;
use chrono::Utc;
use serde_json::{Value, json};
use stasis::prelude::{
    RuntimeBackend, RuntimeComposition, StasisRuntimeBuilder, StasisTool,
};

struct MockWebSearchTool;

#[async_trait]
impl StasisTool for MockWebSearchTool {
    fn name(&self) -> &'static str {
        "stasis.web.search.mock"
    }

    async fn invoke(&self, input: Value) -> stasis::prelude::Result<Value> {
        let query = input
            .get("query")
            .and_then(|value| value.as_str())
            .unwrap_or("general research")
            .to_string();

        Ok(json!({
            "query": query,
            "results": [
                {
                    "title": "Rust ecosystem trends",
                    "snippet": "Growing adoption in platform tooling and backend services.",
                    "source": "mock://rust-trends-1"
                },
                {
                    "title": "Async Rust in production",
                    "snippet": "Tokio-based workloads continue to increase in operational maturity.",
                    "source": "mock://rust-trends-2"
                },
                {
                    "title": "AI infrastructure in Rust",
                    "snippet": "Teams are exploring Rust for inference gateways and orchestration services.",
                    "source": "mock://rust-trends-3"
                }
            ]
        }))
    }
}

pub async fn build_runtime(backend: RuntimeBackend) -> Result<RuntimeComposition> {
    let runtime = StasisRuntimeBuilder::new(backend)
        .with_tool(MockWebSearchTool)?
        .build()
        .await?;

    Ok(runtime)
}

pub fn parse_backend(value: Option<&str>) -> RuntimeBackend {
    match value.unwrap_or("in-memory") {
        "surreal-mem" => RuntimeBackend::SurrealMem {
            namespace: "medousa".to_string(),
            database: "runtime".to_string(),
        },
        _ => RuntimeBackend::InMemory,
    }
}

pub async fn process_once(runtime: &RuntimeComposition, worker_id: &str) -> Result<Option<String>> {
    let now = Utc::now();
    let result = match runtime {
        RuntimeComposition::InMemory(rt) => rt.process_once("default", worker_id, now).await?,
        RuntimeComposition::Surreal(rt) => rt.process_once("default", worker_id, now).await?,
    };

    Ok(result)
}

pub async fn publish_pending(runtime: &RuntimeComposition, limit: usize) -> Result<usize> {
    let now = Utc::now();
    let published = match runtime {
        RuntimeComposition::InMemory(rt) => rt.publish_pending_events(limit, now).await?,
        RuntimeComposition::Surreal(rt) => rt.publish_pending_events(limit, now).await?,
    };

    Ok(published)
}
