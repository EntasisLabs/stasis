use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use uuid::Uuid;

use stasis::prelude::{
    AiChatClient, BackoffPolicy, InMemoryToolRegistry, JobAttemptOutcome, JobAttemptStore,
    LocusContextReader, LocusContextWriter, LocusNodeStoreFactory, MemoryContextReader,
    MemoryContextWriter, MemoryRecallRequest, MemoryStoreRequest, NewJob, PromptExecutionPipeline,
    RuntimeBackend, RuntimeComposition, StasisError, StasisRuntimeBuilder, StasisTool,
};
use stasis::application::orchestration::tool_loop_pipeline::ToolLoopPipeline;

use crate::events::TuiEvent;
use crate::process_once;

// ── CognitionJobEnqueueTool ──────────────────────────────────────────────────

pub struct CognitionJobEnqueueTool {
    runtime: Arc<RuntimeComposition>,
    event_tx: mpsc::Sender<TuiEvent>,
}

impl CognitionJobEnqueueTool {
    pub fn new(runtime: Arc<RuntimeComposition>, event_tx: mpsc::Sender<TuiEvent>) -> Self {
        Self { runtime, event_tx }
    }
}

#[async_trait]
impl StasisTool for CognitionJobEnqueueTool {
    fn name(&self) -> &'static str {
        "cognition.job.enqueue"
    }

    fn description(&self) -> Option<&'static str> {
        Some(
            "Persist a job into the Stasis runtime for durable background execution. \
             Use this to schedule work: grapheme scripts, orchestration patterns, \
             memory operations, or any registered workflow handler. \
             Valid job_type values: workflow.grapheme.run, workflow.grapheme.echo, \
             workflow.stasis.orchestration.sequential, workflow.stasis.orchestration.concurrent, \
             workflow.stasis.orchestration.handoff, workflow.stasis.agent_session, \
             workflow.stasis.prompt.",
        )
    }

    fn input_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "job_type": {
                    "type": "string",
                    "description": "The job handler identifier, e.g. 'workflow.grapheme.run'"
                },
                "payload_ref": {
                    "type": "string",
                    "description": "Serialized job payload. For grapheme: 'grapheme:inline:<source>'. For JSON payloads: serialized JSON string."
                },
                "note": {
                    "type": "string",
                    "description": "Optional human-readable note about the intent of this job"
                }
            },
            "required": ["job_type", "payload_ref"]
        }))
    }

    async fn invoke(&self, input: Value) -> stasis::prelude::Result<Value> {
        let job_type = input
            .get("job_type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                StasisError::PortFailure(
                    "cognition.job.enqueue: job_type is required".to_string(),
                )
            })?;
        let payload_ref = input
            .get("payload_ref")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                StasisError::PortFailure(
                    "cognition.job.enqueue: payload_ref is required".to_string(),
                )
            })?;

        let job_id = format!("cognition-{}", Uuid::new_v4().simple());
        let now = Utc::now();

        let job = NewJob {
            id: job_id.clone(),
            queue: "default".to_string(),
            job_type: job_type.to_string(),
            payload_ref: payload_ref.to_string(),
            priority: 100,
            max_attempts: 1,
            idempotency_key: format!("idem-{job_id}"),
            correlation_id: job_id.clone(),
            causation_id: "cognition.tui".to_string(),
            trace_id: job_id.clone(),
            sttp_input_node_id: "sttp:in:cognition:enqueue".to_string(),
            scheduled_at: now,
            backoff_policy: BackoffPolicy::default(),
        };

        match &*self.runtime {
            RuntimeComposition::InMemory(rt) => rt.enqueue(job).await?,
            RuntimeComposition::Surreal(rt) => rt.enqueue(job).await?,
        }

        let _ = self
            .event_tx
            .send(TuiEvent::JobEnqueued {
                job_id: job_id.clone(),
                job_type: job_type.to_string(),
            })
            .await;

        Ok(json!({
            "job_id": job_id,
            "status": "enqueued",
            "note": input.get("note").and_then(|v| v.as_str()).unwrap_or("")
        }))
    }
}

// ── CognitionGraphemeRunTool ─────────────────────────────────────────────────

pub struct CognitionGraphemeRunTool {
    runtime: Arc<RuntimeComposition>,
    event_tx: mpsc::Sender<TuiEvent>,
}

impl CognitionGraphemeRunTool {
    pub fn new(runtime: Arc<RuntimeComposition>, event_tx: mpsc::Sender<TuiEvent>) -> Self {
        Self { runtime, event_tx }
    }
}

#[async_trait]
impl StasisTool for CognitionGraphemeRunTool {
    fn name(&self) -> &'static str {
        "cognition.grapheme.run"
    }

    fn description(&self) -> Option<&'static str> {
        Some(
            "Execute a Grapheme script synchronously and return the result. \
             Grapheme is a typed workflow scripting language. Use 'grapheme/core' for \
             built-in operations like echo. Scripts run sandboxed with guardrails enforced. \
             Example source: import core from \"grapheme/core\"\nquery Run { \
             core.echo(message: \"hello\") { state { current } } }",
        )
    }

    fn input_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "source": {
                    "type": "string",
                    "description": "Complete Grapheme source code. Must import from 'grapheme/core'."
                }
            },
            "required": ["source"]
        }))
    }

    async fn invoke(&self, input: Value) -> stasis::prelude::Result<Value> {
        let source = input
            .get("source")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                StasisError::PortFailure(
                    "cognition.grapheme.run: source is required".to_string(),
                )
            })?;

        let job_id = format!("cognition-gph-{}", Uuid::new_v4().simple());
        let now = Utc::now();

        let job = NewJob {
            id: job_id.clone(),
            queue: "default".to_string(),
            job_type: "workflow.grapheme.run".to_string(),
            payload_ref: format!("grapheme:inline:{source}"),
            priority: 100,
            max_attempts: 1,
            idempotency_key: format!("idem-{job_id}"),
            correlation_id: job_id.clone(),
            causation_id: "cognition.tui".to_string(),
            trace_id: job_id.clone(),
            sttp_input_node_id: "sttp:in:cognition:grapheme".to_string(),
            scheduled_at: now,
            backoff_policy: BackoffPolicy::default(),
        };

        match &*self.runtime {
            RuntimeComposition::InMemory(rt) => rt.enqueue(job).await?,
            RuntimeComposition::Surreal(rt) => rt.enqueue(job).await?,
        }

        let _ = self
            .event_tx
            .send(TuiEvent::ToolInvoked {
                tool_name: "cognition.grapheme.run".to_string(),
                input_summary: source.chars().take(60).collect(),
            })
            .await;

        let runtime_ref = Arc::clone(&self.runtime);
        let _ = process_once(&runtime_ref, "cognition.tui").await;

        let attempts = match &*runtime_ref {
            RuntimeComposition::InMemory(rt) => {
                rt.job_attempt_store.list_by_job_id(&job_id).await?
            }
            RuntimeComposition::Surreal(rt) => {
                rt.job_attempt_store.list_by_job_id(&job_id).await?
            }
        };

        let last = attempts.last();
        let succeeded = last
            .map(|a| a.outcome == JobAttemptOutcome::Succeeded)
            .unwrap_or(false);
        let execution_id = last.and_then(|a| a.execution_id.clone());
        let diagnostics = last.and_then(|a| a.diagnostics.as_deref()).map(|d| {
            serde_json::from_str::<Value>(d).unwrap_or_else(|_| json!({ "raw": d }))
        });

        let _ = self
            .event_tx
            .send(TuiEvent::JobProcessed {
                job_id: job_id.clone(),
                succeeded,
                execution_id: execution_id.clone(),
            })
            .await;

        Ok(json!({
            "job_id": job_id,
            "status": if succeeded { "succeeded" } else { "failed" },
            "execution_id": execution_id,
            "diagnostics": diagnostics
        }))
    }
}

// ── CognitionMemoryStoreTool ─────────────────────────────────────────────────

pub struct CognitionMemoryStoreTool {
    writer: Arc<dyn MemoryContextWriter>,
    session_id: String,
    event_tx: mpsc::Sender<TuiEvent>,
}

impl CognitionMemoryStoreTool {
    pub fn new(
        writer: Arc<dyn MemoryContextWriter>,
        session_id: String,
        event_tx: mpsc::Sender<TuiEvent>,
    ) -> Self {
        Self { writer, session_id, event_tx }
    }
}

#[async_trait]
impl StasisTool for CognitionMemoryStoreTool {
    fn name(&self) -> &'static str {
        "cognition.memory.store"
    }

    fn description(&self) -> Option<&'static str> {
        Some(
            "Persist a memory node into the Locus memory store for future recall across turns. \
             Use this to remember important context, decisions, insights, or any information \
             that should survive beyond the current conversation window.",
        )
    }

    fn input_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "content": {
                    "type": "string",
                    "description": "The content to store. Should be a concise, self-contained statement."
                },
                "tier": {
                    "type": "string",
                    "description": "Memory tier: 'insight', 'context', or 'decision'. Defaults to 'context'."
                }
            },
            "required": ["content"]
        }))
    }

    async fn invoke(&self, input: Value) -> stasis::prelude::Result<Value> {
        let content = input
            .get("content")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                StasisError::PortFailure(
                    "cognition.memory.store: content is required".to_string(),
                )
            })?;
        let tier = input
            .get("tier")
            .and_then(|v| v.as_str())
            .unwrap_or("context");

        let raw_node = json!({
            "content": content,
            "tier": tier
        })
        .to_string();

        let request = MemoryStoreRequest {
            session_id: self.session_id.clone(),
            raw_node,
        };

        let response = self.writer.store_context(&request).await?;

        let _ = self
            .event_tx
            .send(TuiEvent::ToolInvoked {
                tool_name: "cognition.memory.store".to_string(),
                input_summary: content.chars().take(50).collect(),
            })
            .await;

        Ok(json!({
            "node_id": response.node_id,
            "stored": response.valid,
            "validation_error": response.validation_error
        }))
    }
}

// ── CognitionMemoryRecallTool ────────────────────────────────────────────────

pub struct CognitionMemoryRecallTool {
    reader: Arc<dyn MemoryContextReader>,
    event_tx: mpsc::Sender<TuiEvent>,
}

impl CognitionMemoryRecallTool {
    pub fn new(reader: Arc<dyn MemoryContextReader>, event_tx: mpsc::Sender<TuiEvent>) -> Self {
        Self { reader, event_tx }
    }
}

#[async_trait]
impl StasisTool for CognitionMemoryRecallTool {
    fn name(&self) -> &'static str {
        "cognition.memory.recall"
    }

    fn description(&self) -> Option<&'static str> {
        Some(
            "Retrieve relevant memory nodes from the Locus store by semantic query. \
             Use this to surface previously stored context, decisions, or insights \
             relevant to the current moment of work.",
        )
    }

    fn input_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Natural language query describing what context to retrieve"
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum nodes to retrieve (1–20, default 5)",
                    "minimum": 1,
                    "maximum": 20
                }
            },
            "required": ["query"]
        }))
    }

    async fn invoke(&self, input: Value) -> stasis::prelude::Result<Value> {
        let query = input
            .get("query")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                StasisError::PortFailure(
                    "cognition.memory.recall: query is required".to_string(),
                )
            })?;
        let limit = input
            .get("limit")
            .and_then(|v| v.as_u64())
            .unwrap_or(5)
            .min(20) as usize;

        let request = MemoryRecallRequest {
            query_text: Some(query.to_string()),
            limit,
            ..Default::default()
        };

        let response = self.reader.recall(&request).await?;

        let _ = self
            .event_tx
            .send(TuiEvent::ToolInvoked {
                tool_name: "cognition.memory.recall".to_string(),
                input_summary: query.chars().take(50).collect(),
            })
            .await;

        Ok(json!({
            "retrieved": response.retrieved,
            "node_sync_keys": response.node_sync_keys,
            "has_more": response.has_more,
            "retrieval_path": response.retrieval_path,
            "fallback_triggered": response.fallback_triggered
        }))
    }
}

// ── Registry builder ─────────────────────────────────────────────────────────

pub struct TuiRuntime {
    pub runtime: Arc<RuntimeComposition>,
    pub tool_loop_pipeline: ToolLoopPipeline,
    pub memory_reader: Arc<dyn MemoryContextReader>,
    pub memory_writer: Arc<dyn MemoryContextWriter>,
}

pub async fn build_tui_runtime(
    backend: RuntimeBackend,
    provider: Option<&str>,
    model: Option<&str>,
    base_url: Option<&str>,
    session_id: &str,
    event_tx: mpsc::Sender<TuiEvent>,
) -> anyhow::Result<TuiRuntime> {
    use std::sync::Arc;

    let resolved_provider = crate::resolve_llm_provider(provider);
    let resolved_model = crate::resolve_llm_model(model);
    let resolved_base_url = crate::resolve_llm_base_url(Some(&resolved_provider), base_url);

    let chat_client: Arc<dyn AiChatClient> = Arc::new(
        stasis::infrastructure::llm::genai_chat_client::GenaiChatClient::from_provider_model_with_base_url(
            Some(&resolved_provider),
            &resolved_model,
            resolved_base_url.as_deref(),
        ),
    );

    let locus_store = LocusNodeStoreFactory::in_memory().await?;
    let memory_reader: Arc<dyn MemoryContextReader> =
        Arc::new(LocusContextReader::new(locus_store.clone()));
    let memory_writer: Arc<dyn MemoryContextWriter> =
        Arc::new(LocusContextWriter::new(locus_store));

    let runtime_composition = StasisRuntimeBuilder::new(backend)
        .with_chat_client(chat_client.clone())
        .with_memory_context_reader(memory_reader.clone())
        .with_memory_context_writer(memory_writer.clone())
        .build()
        .await?;

    let runtime = Arc::new(runtime_composition);

    let tool_registry = InMemoryToolRegistry::default();
    tool_registry.register_tool(CognitionJobEnqueueTool::new(
        runtime.clone(),
        event_tx.clone(),
    ))?;
    tool_registry.register_tool(CognitionGraphemeRunTool::new(
        runtime.clone(),
        event_tx.clone(),
    ))?;
    tool_registry.register_tool(CognitionMemoryStoreTool::new(
        memory_writer.clone(),
        session_id.to_string(),
        event_tx.clone(),
    ))?;
    tool_registry.register_tool(CognitionMemoryRecallTool::new(
        memory_reader.clone(),
        event_tx,
    ))?;

    let prompt_pipeline = PromptExecutionPipeline::new(chat_client);
    let tool_loop_pipeline =
        ToolLoopPipeline::new(prompt_pipeline, Arc::new(tool_registry));

    Ok(TuiRuntime {
        runtime,
        tool_loop_pipeline,
        memory_reader,
        memory_writer,
    })
}
