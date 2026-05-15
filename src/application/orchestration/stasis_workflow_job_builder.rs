use chrono::{DateTime, Utc};

use crate::application::orchestration::agent_session_payload::{
    AgentSessionJobPayload, AgentTurnJobPayload, MemoryAggregateJobPayload,
    MemoryRecallJobPayload, MemoryRollupJobPayload, MemorySchemaJobPayload,
    MemoryTransformJobPayload, PromptJobPayload, ToolLoopJobPayload,
};
use crate::domain::errors::Result;
use crate::domain::runtime::job::{BackoffPolicy, NewJob};

const JOB_TYPE_AGENT_SESSION: &str = "workflow.stasis.agent_session";
const JOB_TYPE_AGENT_TURN: &str = "workflow.stasis.agent_turn";
const JOB_TYPE_TOOL_LOOP: &str = "workflow.stasis.tool_loop";
const JOB_TYPE_PROMPT: &str = "workflow.stasis.prompt";
const JOB_TYPE_MEMORY_RECALL: &str = "workflow.stasis.memory.recall";
const JOB_TYPE_MEMORY_AGGREGATE: &str = "workflow.stasis.memory.aggregate";
const JOB_TYPE_MEMORY_TRANSFORM: &str = "workflow.stasis.memory.transform";
const JOB_TYPE_MEMORY_ROLLUP: &str = "workflow.stasis.memory.rollup";
const JOB_TYPE_MEMORY_SCHEMA: &str = "workflow.stasis.memory.schema";

#[derive(Clone, Debug)]
pub struct StasisWorkflowJobBuilder {
    id: String,
    job_type: String,
    payload_ref: String,
    queue: String,
    priority: i32,
    max_attempts: u32,
    idempotency_key: Option<String>,
    correlation_id: Option<String>,
    causation_id: String,
    trace_id: Option<String>,
    sttp_input_node_id: String,
    scheduled_at: DateTime<Utc>,
    backoff_policy: BackoffPolicy,
}

impl StasisWorkflowJobBuilder {
    pub fn for_agent_session(id: impl Into<String>, payload: &AgentSessionJobPayload) -> Result<Self> {
        Self::new(id.into(), JOB_TYPE_AGENT_SESSION, payload.to_payload_ref()?)
    }

    pub fn for_agent_turn(id: impl Into<String>, payload: &AgentTurnJobPayload) -> Result<Self> {
        Self::new(id.into(), JOB_TYPE_AGENT_TURN, payload.to_payload_ref()?)
    }

    pub fn for_tool_loop(id: impl Into<String>, payload: &ToolLoopJobPayload) -> Result<Self> {
        Self::new(id.into(), JOB_TYPE_TOOL_LOOP, payload.to_payload_ref()?)
    }

    pub fn for_prompt(id: impl Into<String>, payload: &PromptJobPayload) -> Result<Self> {
        Self::new(id.into(), JOB_TYPE_PROMPT, payload.to_payload_ref()?)
    }

    pub fn for_memory_recall(id: impl Into<String>, payload: &MemoryRecallJobPayload) -> Result<Self> {
        Self::new(id.into(), JOB_TYPE_MEMORY_RECALL, payload.to_payload_ref()?)
    }

    pub fn for_memory_aggregate(
        id: impl Into<String>,
        payload: &MemoryAggregateJobPayload,
    ) -> Result<Self> {
        Self::new(id.into(), JOB_TYPE_MEMORY_AGGREGATE, payload.to_payload_ref()?)
    }

    pub fn for_memory_transform(
        id: impl Into<String>,
        payload: &MemoryTransformJobPayload,
    ) -> Result<Self> {
        Self::new(id.into(), JOB_TYPE_MEMORY_TRANSFORM, payload.to_payload_ref()?)
    }

    pub fn for_memory_rollup(id: impl Into<String>, payload: &MemoryRollupJobPayload) -> Result<Self> {
        Self::new(id.into(), JOB_TYPE_MEMORY_ROLLUP, payload.to_payload_ref()?)
    }

    pub fn for_memory_schema(id: impl Into<String>, payload: &MemorySchemaJobPayload) -> Result<Self> {
        Self::new(id.into(), JOB_TYPE_MEMORY_SCHEMA, payload.to_payload_ref()?)
    }

    fn new(id: String, job_type: &'static str, payload_ref: String) -> Result<Self> {
        Ok(Self {
            id,
            job_type: job_type.to_string(),
            payload_ref,
            queue: "default".to_string(),
            priority: 100,
            max_attempts: 1,
            idempotency_key: None,
            correlation_id: None,
            causation_id: "stasis-client".to_string(),
            trace_id: None,
            sttp_input_node_id: "sttp:in:stasis:workflow".to_string(),
            scheduled_at: Utc::now(),
            backoff_policy: BackoffPolicy::default(),
        })
    }

    pub fn with_queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = queue.into();
        self
    }

    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    pub fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    pub fn with_idempotency_key(mut self, idempotency_key: impl Into<String>) -> Self {
        self.idempotency_key = Some(idempotency_key.into());
        self
    }

    pub fn with_correlation_id(mut self, correlation_id: impl Into<String>) -> Self {
        self.correlation_id = Some(correlation_id.into());
        self
    }

    pub fn with_causation_id(mut self, causation_id: impl Into<String>) -> Self {
        self.causation_id = causation_id.into();
        self
    }

    pub fn with_trace_id(mut self, trace_id: impl Into<String>) -> Self {
        self.trace_id = Some(trace_id.into());
        self
    }

    pub fn with_sttp_input_node_id(mut self, sttp_input_node_id: impl Into<String>) -> Self {
        self.sttp_input_node_id = sttp_input_node_id.into();
        self
    }

    pub fn with_scheduled_at(mut self, scheduled_at: DateTime<Utc>) -> Self {
        self.scheduled_at = scheduled_at;
        self
    }

    pub fn with_backoff_policy(mut self, backoff_policy: BackoffPolicy) -> Self {
        self.backoff_policy = backoff_policy;
        self
    }

    pub fn build(self) -> NewJob {
        let idempotency_key = self
            .idempotency_key
            .unwrap_or_else(|| format!("idem-{}", self.id));
        let correlation_id = self.correlation_id.unwrap_or_else(|| self.id.clone());
        let trace_id = self.trace_id.unwrap_or_else(|| self.id.clone());

        NewJob {
            id: self.id,
            queue: self.queue,
            job_type: self.job_type,
            payload_ref: self.payload_ref,
            priority: self.priority,
            max_attempts: self.max_attempts,
            idempotency_key,
            correlation_id,
            causation_id: self.causation_id,
            trace_id,
            sttp_input_node_id: self.sttp_input_node_id,
            scheduled_at: self.scheduled_at,
            backoff_policy: self.backoff_policy,
        }
    }
}
