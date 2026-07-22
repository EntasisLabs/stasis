//! Built-in job types accepted by `stasisd/v1` validation.

pub const KNOWN_JOB_TYPES: &[&str] = &[
    "workflow.stasis.agent_session",
    "workflow.stasis.agent_turn",
    "workflow.stasis.agent_turn.waitable",
    "workflow.stasis.tool_loop",
    "workflow.stasis.prompt",
    "workflow.stasis.memory.recall",
    "workflow.stasis.memory.find",
    "workflow.stasis.memory.aggregate",
    "workflow.stasis.memory.transform",
    "workflow.stasis.memory.rollup",
    "workflow.stasis.memory.schema",
    "workflow.stasis.memory.evict",
    "workflow.stasis.memory.graph",
    "workflow.stasis.orchestration.sequential",
    "workflow.stasis.orchestration.concurrent",
    "workflow.stasis.orchestration.handoff",
    "workflow.stasis.orchestration.orchestrator",
    "workflow.grapheme.run",
    "workflow.grapheme.healthcheck",
    "workflow.grapheme.echo",
    "workflow.grapheme.textops",
];

pub fn is_known_job_type(job_type: &str) -> bool {
    KNOWN_JOB_TYPES.contains(&job_type)
}
