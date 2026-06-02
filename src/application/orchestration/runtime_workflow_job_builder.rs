use chrono::{DateTime, Utc};

use crate::application::orchestration::runtime_job_payloads::{
    AgentSessionJobPayload, AgentTurnJobPayload, ConcurrentPatternJobPayload,
    HandoffPatternJobPayload, MemoryAggregateJobPayload, MemoryFindJobPayload,
    MemoryRecallJobPayload, MemoryRollupJobPayload, MemorySchemaJobPayload, MemoryTransformJobPayload,
    OrchestratorPatternJobPayload, PromptJobPayload, SequentialPatternJobPayload,
    ToolLoopJobPayload,
};
use crate::domain::errors::Result;
use crate::domain::runtime::job::{BackoffPolicy, NewJob};

const JOB_TYPE_AGENT_SESSION: &str = "workflow.stasis.agent_session";
const JOB_TYPE_AGENT_TURN: &str = "workflow.stasis.agent_turn";
const JOB_TYPE_TOOL_LOOP: &str = "workflow.stasis.tool_loop";
const JOB_TYPE_PROMPT: &str = "workflow.stasis.prompt";
const JOB_TYPE_MEMORY_RECALL: &str = "workflow.stasis.memory.recall";
const JOB_TYPE_MEMORY_FIND: &str = "workflow.stasis.memory.find";
const JOB_TYPE_MEMORY_AGGREGATE: &str = "workflow.stasis.memory.aggregate";
const JOB_TYPE_MEMORY_TRANSFORM: &str = "workflow.stasis.memory.transform";
const JOB_TYPE_MEMORY_ROLLUP: &str = "workflow.stasis.memory.rollup";
const JOB_TYPE_MEMORY_SCHEMA: &str = "workflow.stasis.memory.schema";
const JOB_TYPE_ORCHESTRATION_SEQUENTIAL: &str = "workflow.stasis.orchestration.sequential";
const JOB_TYPE_ORCHESTRATION_CONCURRENT: &str = "workflow.stasis.orchestration.concurrent";
const JOB_TYPE_ORCHESTRATION_HANDOFF: &str = "workflow.stasis.orchestration.handoff";
const JOB_TYPE_ORCHESTRATION_ORCHESTRATOR: &str = "workflow.stasis.orchestration.orchestrator";

#[derive(Clone, Debug)]
pub struct RuntimeWorkflowJobBuilder {
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

macro_rules! define_payload_builder {
    ($fn_name:ident, $payload_ty:ty, $job_type:expr) => {
        pub fn $fn_name(id: impl Into<String>, payload: &$payload_ty) -> Result<Self> {
            Self::new(id.into(), $job_type, payload.to_payload_ref()?)
        }
    };
}

impl RuntimeWorkflowJobBuilder {
    define_payload_builder!(for_agent_session, AgentSessionJobPayload, JOB_TYPE_AGENT_SESSION);
    define_payload_builder!(for_agent_turn, AgentTurnJobPayload, JOB_TYPE_AGENT_TURN);
    define_payload_builder!(for_tool_loop, ToolLoopJobPayload, JOB_TYPE_TOOL_LOOP);
    define_payload_builder!(for_prompt, PromptJobPayload, JOB_TYPE_PROMPT);
    define_payload_builder!(for_memory_recall, MemoryRecallJobPayload, JOB_TYPE_MEMORY_RECALL);
    define_payload_builder!(for_memory_find, MemoryFindJobPayload, JOB_TYPE_MEMORY_FIND);
    define_payload_builder!(
        for_memory_aggregate,
        MemoryAggregateJobPayload,
        JOB_TYPE_MEMORY_AGGREGATE
    );
    define_payload_builder!(
        for_memory_transform,
        MemoryTransformJobPayload,
        JOB_TYPE_MEMORY_TRANSFORM
    );
    define_payload_builder!(for_memory_rollup, MemoryRollupJobPayload, JOB_TYPE_MEMORY_ROLLUP);
    define_payload_builder!(for_memory_schema, MemorySchemaJobPayload, JOB_TYPE_MEMORY_SCHEMA);
    define_payload_builder!(
        for_orchestration_sequential,
        SequentialPatternJobPayload,
        JOB_TYPE_ORCHESTRATION_SEQUENTIAL
    );
    define_payload_builder!(
        for_orchestration_concurrent,
        ConcurrentPatternJobPayload,
        JOB_TYPE_ORCHESTRATION_CONCURRENT
    );
    define_payload_builder!(
        for_orchestration_handoff,
        HandoffPatternJobPayload,
        JOB_TYPE_ORCHESTRATION_HANDOFF
    );
    define_payload_builder!(
        for_orchestration_orchestrator,
        OrchestratorPatternJobPayload,
        JOB_TYPE_ORCHESTRATION_ORCHESTRATOR
    );

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
