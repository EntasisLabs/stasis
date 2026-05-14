use std::sync::Arc;

use anyhow::Result;
use chrono::Utc;
use stasis::prelude::{
    GraphemeEchoJobHandler, GraphemeHealthcheckJobHandler, GraphemeJobHandler,
    GraphemeSdkWorkflowEngine, GraphemeTextOpsJobHandler, RuntimeBackend, RuntimeComposition,
    RuntimeFactory,
};

pub async fn build_runtime(backend: RuntimeBackend) -> Result<RuntimeComposition> {
    let runtime = RuntimeFactory::build(backend).await?;
    let workflow_engine = Arc::new(GraphemeSdkWorkflowEngine::new());

    match &runtime {
        RuntimeComposition::InMemory(rt) => {
            rt.register_handler(GraphemeJobHandler::new(workflow_engine.clone()))?;
            rt.register_handler(GraphemeHealthcheckJobHandler::new(workflow_engine.clone()))?;
            rt.register_handler(GraphemeEchoJobHandler::new(workflow_engine.clone()))?;
            rt.register_handler(GraphemeTextOpsJobHandler::new(workflow_engine.clone()))?;
        }
        RuntimeComposition::Surreal(rt) => {
            rt.register_handler(GraphemeJobHandler::new(workflow_engine.clone()))?;
            rt.register_handler(GraphemeHealthcheckJobHandler::new(workflow_engine.clone()))?;
            rt.register_handler(GraphemeEchoJobHandler::new(workflow_engine.clone()))?;
            rt.register_handler(GraphemeTextOpsJobHandler::new(workflow_engine.clone()))?;
        }
    }

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
