# Production Agentic Workflows

## Outcome

Run production-style agentic jobs with real LLM providers, durable runtime wiring, memory controls, and orchestration patterns teams can copy directly.

## Prerequisites

Set provider and auth environment variables before running these patterns.

OpenAI example:

```bash
export STASIS_LLM_PROVIDER=openai
export STASIS_LLM_MODEL=gpt-4o-mini
export STASIS_OPENAI_API_KEY=your-key
```

Anthropic example:

```bash
export STASIS_LLM_PROVIDER=anthropic
export STASIS_LLM_MODEL=claude-3-5-haiku-latest
export STASIS_ANTHROPIC_API_KEY=your-key
```

Global fallback key is also supported:

```bash
export STASIS_LLM_API_KEY=your-key
```

## Runtime Bootstrap (real provider, tool loop ready)

```rust
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use stasis::application::orchestration::runtime_job_payloads::{
    AgentToolCallMode, MemoryFallbackPolicyPayload, MemoryPolicyPayload,
    MemoryStoreModePayload, MemoryStrictnessModePayload,
};
use stasis::domain::errors::Result;
use stasis::prelude::{RuntimeBackend, RuntimeSdk, StasisRuntimeBuilder};
use stasis::stasis_tool;

#[derive(Debug, Clone, Deserialize, JsonSchema)]
struct FetchKnowledgeBaseInput {
    topic: String,
}

#[derive(Debug, Clone, Serialize)]
struct FetchKnowledgeBaseOutput {
    topic: String,
    playbook: Vec<String>,
}

#[stasis_tool(
    name = "fetch_knowledge_base",
    description = "Returns internal playbook snippets for an operation topic"
)]
async fn fetch_knowledge_base(input: FetchKnowledgeBaseInput) -> Result<FetchKnowledgeBaseOutput> {
    Ok(FetchKnowledgeBaseOutput {
        topic: input.topic,
        playbook: vec![
            "Validate preconditions".to_string(),
            "Apply staged rollout".to_string(),
            "Capture diagnostics and rollback plan".to_string(),
        ],
    })
}

fn production_memory_policy() -> MemoryPolicyPayload {
    MemoryPolicyPayload {
        session_ids: None,
        tiers: Some(vec!["summary".to_string(), "episodic".to_string()]),
        from_utc: None,
        to_utc: None,
        limit: Some(12),
        alpha: Some(0.7),
        beta: Some(0.3),
        fallback_policy: Some(MemoryFallbackPolicyPayload::OnEmpty),
        strictness: Some(MemoryStrictnessModePayload::Balanced),
        query_text: None,
        include_explain: Some(true),
        store_mode: Some(MemoryStoreModePayload::SummaryOnly),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let backend = RuntimeBackend::InMemory;
    let builder = StasisRuntimeBuilder::new(backend).with_locus_memory();
    let builder = builder.with_tool(FetchKnowledgeBaseTool)?;

    let runtime = RuntimeSdk::from_builder(builder).await?;
    let _policy = production_memory_policy();

    // Keep this runtime instance and enqueue one or more jobs shown below.
    let _ = runtime;
    Ok(())
}
```

## Prompt Workflow Job

```rust
use stasis::application::orchestration::runtime_job_payloads::PromptJobPayload;
use stasis::application::orchestration::runtime_workflow_job_builder::RuntimeWorkflowJobBuilder;

let prompt_payload = PromptJobPayload {
    user_prompt: "Summarize queue saturation risks for this release".to_string(),
    system_prompt: Some("You are a reliability engineer. Be concise and actionable.".to_string()),
    policy_profile: Some("prod.sre".to_string()),
    model_hint: Some("fast-reasoning".to_string()),
    memory_policy: Some(production_memory_policy()),
};

let job = RuntimeWorkflowJobBuilder::for_prompt("job-prompt-prod-001", &prompt_payload)?
    .with_queue("default")
    .with_correlation_id("thread-prod-001")
    .with_sttp_input_node_id("sttp:in:prod:prompt:001")
    .build();

runtime.enqueue(job).await?;
```

## Tool Loop Workflow Job

```rust
use stasis::application::orchestration::runtime_job_payloads::{
    AgentToolCallMode, ToolLoopJobPayload,
};
use stasis::application::orchestration::runtime_workflow_job_builder::RuntimeWorkflowJobBuilder;

let tool_payload = ToolLoopJobPayload {
    user_prompt: "Use the tool and then produce an execution-ready rollout checklist.".to_string(),
    system_prompt: Some("Call tools when needed and cite tool output in the final answer.".to_string()),
    policy_profile: Some("prod.ops".to_string()),
    model_hint: Some("tool-use".to_string()),
    tool_name: "fetch_knowledge_base".to_string(),
    tool_input: Some(serde_json::json!({ "topic": "release rollout" })),
    tool_call_mode: Some(AgentToolCallMode::Strict),
    memory_policy: Some(production_memory_policy()),
};

let job = RuntimeWorkflowJobBuilder::for_tool_loop("job-tool-loop-prod-001", &tool_payload)?
    .with_queue("default")
    .with_correlation_id("thread-prod-001")
    .with_sttp_input_node_id("sttp:in:prod:tool-loop:001")
    .build();

runtime.enqueue(job).await?;
```

## Agent Turn Workflow Job

```rust
use stasis::application::orchestration::runtime_job_payloads::AgentTurnJobPayload;
use stasis::application::orchestration::runtime_workflow_job_builder::RuntimeWorkflowJobBuilder;

let turn_payload = AgentTurnJobPayload {
    agent_id: "incident_commander".to_string(),
    thread_id: Some("thread-prod-incident-42".to_string()),
    user_prompt: "Draft the first incident update for stakeholders.".to_string(),
    system_prompt: Some("Prioritize clarity, impact, and next checkpoint.".to_string()),
    policy_profile: Some("prod.incident".to_string()),
    model_hint: Some("balanced".to_string()),
    tool_name: "fetch_knowledge_base".to_string(),
    tool_input: Some(serde_json::json!({ "topic": "incident communications" })),
    tool_call_mode: Some(AgentToolCallMode::Auto),
    memory_policy: Some(production_memory_policy()),
};

let job = RuntimeWorkflowJobBuilder::for_agent_turn("job-agent-turn-prod-001", &turn_payload)?
    .with_queue("default")
    .with_correlation_id("thread-prod-incident-42")
    .with_sttp_input_node_id("sttp:in:prod:agent-turn:001")
    .build();

runtime.enqueue(job).await?;
```

## Agent Session Workflow Job (multi-participant)

```rust
use stasis::application::orchestration::runtime_job_payloads::{
    AgentSessionJobPayload, AgentSessionParticipantPayload,
};
use stasis::application::orchestration::runtime_workflow_job_builder::RuntimeWorkflowJobBuilder;

let session_payload = AgentSessionJobPayload {
    thread_id: Some("thread-prod-design-review-1".to_string()),
    initial_user_prompt: "Review this deployment plan for risk and sequencing.".to_string(),
    participants: vec![
        AgentSessionParticipantPayload {
            agent_id: "planner".to_string(),
            system_prompt: Some("Break the plan into milestones and dependencies.".to_string()),
            tool_name: "fetch_knowledge_base".to_string(),
            tool_input: Some(serde_json::json!({ "topic": "deployment sequencing" })),
        },
        AgentSessionParticipantPayload {
            agent_id: "sre_reviewer".to_string(),
            system_prompt: Some("Focus on blast radius and rollback readiness.".to_string()),
            tool_name: "fetch_knowledge_base".to_string(),
            tool_input: Some(serde_json::json!({ "topic": "rollback policy" })),
        },
    ],
    policy_profile: Some("prod.review".to_string()),
    model_hint: Some("balanced".to_string()),
    max_turns: Some(4),
    tool_call_mode: Some(AgentToolCallMode::Auto),
    memory_policy: Some(production_memory_policy()),
};

let job = RuntimeWorkflowJobBuilder::for_agent_session("job-agent-session-prod-001", &session_payload)?
    .with_queue("default")
    .with_correlation_id("thread-prod-design-review-1")
    .with_sttp_input_node_id("sttp:in:prod:agent-session:001")
    .build();

runtime.enqueue(job).await?;
```

## Sequential Pattern Workflow Job

```rust
use stasis::application::orchestration::runtime_job_payloads::{
    SequentialPatternJobPayload, SequentialStageJobPayload,
};
use stasis::application::orchestration::runtime_workflow_job_builder::RuntimeWorkflowJobBuilder;

let sequential_payload = SequentialPatternJobPayload {
    thread_id: Some("thread-prod-seq-1".to_string()),
    initial_user_prompt: "Ship a safe canary rollout for service A".to_string(),
    policy_profile: Some("prod.release".to_string()),
    model_hint: Some("balanced".to_string()),
    stages: vec![
        SequentialStageJobPayload {
            stage_id: "plan".to_string(),
            user_prompt_template: "{{input}}\nGenerate a rollout plan with milestones.".to_string(),
            system_prompt: None,
            policy_profile: None,
            model_hint: None,
        },
        SequentialStageJobPayload {
            stage_id: "risk".to_string(),
            user_prompt_template: "{{input}}\nList top 5 risks and mitigations.".to_string(),
            system_prompt: None,
            policy_profile: None,
            model_hint: None,
        },
    ],
};

let job = RuntimeWorkflowJobBuilder::for_orchestration_sequential(
    "job-orch-sequential-prod-001",
    &sequential_payload,
)?
.with_queue("default")
.with_correlation_id("thread-prod-seq-1")
.with_sttp_input_node_id("sttp:in:prod:sequential:001")
.build();

runtime.enqueue(job).await?;
```

## Concurrent Pattern Workflow Job

```rust
use stasis::application::orchestration::runtime_job_payloads::{
    ConcurrentBranchJobPayload, ConcurrentPatternJobPayload,
};
use stasis::application::orchestration::runtime_workflow_job_builder::RuntimeWorkflowJobBuilder;

let concurrent_payload = ConcurrentPatternJobPayload {
    thread_id: Some("thread-prod-concurrent-1".to_string()),
    initial_user_prompt: "Assess release quality from three angles".to_string(),
    policy_profile: Some("prod.release".to_string()),
    model_hint: Some("balanced".to_string()),
    merge_strategy: Some("append".to_string()),
    branches: vec![
        ConcurrentBranchJobPayload {
            branch_id: "security".to_string(),
            user_prompt_template: "{{input}}\nPerform a security review.".to_string(),
            system_prompt: None,
            policy_profile: None,
            model_hint: None,
        },
        ConcurrentBranchJobPayload {
            branch_id: "performance".to_string(),
            user_prompt_template: "{{input}}\nPerform a performance review.".to_string(),
            system_prompt: None,
            policy_profile: None,
            model_hint: None,
        },
    ],
};

let job = RuntimeWorkflowJobBuilder::for_orchestration_concurrent(
    "job-orch-concurrent-prod-001",
    &concurrent_payload,
)?
.with_queue("default")
.with_correlation_id("thread-prod-concurrent-1")
.with_sttp_input_node_id("sttp:in:prod:concurrent:001")
.build();

runtime.enqueue(job).await?;
```

## Handoff Pattern Workflow Job

```rust
use stasis::application::orchestration::runtime_job_payloads::{
    HandoffPatternJobPayload, HandoffTurnJobPayload,
};
use stasis::application::orchestration::runtime_workflow_job_builder::RuntimeWorkflowJobBuilder;

let handoff_payload = HandoffPatternJobPayload {
    thread_id: Some("thread-prod-handoff-1".to_string()),
    initial_user_prompt: "Create launch communication pack".to_string(),
    policy_profile: Some("prod.launch".to_string()),
    model_hint: Some("balanced".to_string()),
    turns: vec![
        HandoffTurnJobPayload {
            actor_id: "planner".to_string(),
            user_prompt_template: "{{input}}\nDraft the launch plan.".to_string(),
            system_prompt: None,
            policy_profile: None,
            model_hint: None,
        },
        HandoffTurnJobPayload {
            actor_id: "editor".to_string(),
            user_prompt_template: "{{input}}\nPolish tone and clarity for executive audience.".to_string(),
            system_prompt: None,
            policy_profile: None,
            model_hint: None,
        },
    ],
};

let job = RuntimeWorkflowJobBuilder::for_orchestration_handoff(
    "job-orch-handoff-prod-001",
    &handoff_payload,
)?
.with_queue("default")
.with_correlation_id("thread-prod-handoff-1")
.with_sttp_input_node_id("sttp:in:prod:handoff:001")
.build();

runtime.enqueue(job).await?;
```

## Orchestrator Pattern Workflow Job

```rust
use stasis::application::orchestration::runtime_job_payloads::{
    OrchestratorPatternJobPayload, OrchestratorRouteJobPayload,
};
use stasis::application::orchestration::runtime_workflow_job_builder::RuntimeWorkflowJobBuilder;

let orchestrator_payload = OrchestratorPatternJobPayload {
    thread_id: Some("thread-prod-orchestrator-1".to_string()),
    initial_user_prompt: "Need guidance for a high-risk release with potential rollback".to_string(),
    policy_profile: Some("prod.release".to_string()),
    model_hint: Some("balanced".to_string()),
    routes: vec![
        OrchestratorRouteJobPayload {
            route_id: "incident_path".to_string(),
            selector_keywords: vec!["rollback".to_string(), "incident".to_string()],
            user_prompt_template: "{{input}}\nRun incident-first release strategy.".to_string(),
            system_prompt: None,
            policy_profile: None,
            model_hint: None,
        },
        OrchestratorRouteJobPayload {
            route_id: "standard_path".to_string(),
            selector_keywords: vec!["normal".to_string(), "routine".to_string()],
            user_prompt_template: "{{input}}\nRun standard release checklist.".to_string(),
            system_prompt: None,
            policy_profile: None,
            model_hint: None,
        },
    ],
};

let job = RuntimeWorkflowJobBuilder::for_orchestration_orchestrator(
    "job-orch-orchestrator-prod-001",
    &orchestrator_payload,
)?
.with_queue("default")
.with_correlation_id("thread-prod-orchestrator-1")
.with_sttp_input_node_id("sttp:in:prod:orchestrator:001")
.build();

runtime.enqueue(job).await?;
```

## Worker Loop and Snapshot

```rust
while runtime.process_once("default", "worker-prod-1").await?.is_some() {}

let stats = runtime.stats_snapshot(100).await?;
println!(
    "enqueued={} running={} succeeded={} failed={} dead_letter={}",
    stats.enqueued_jobs,
    stats.running_jobs,
    stats.succeeded_jobs,
    stats.failed_jobs,
    stats.dead_letter_jobs
);
```

## Backend Profiles (In-Memory, Surreal WS, Surreal KV)

Use the dedicated profile bootstrap example when validating runtime storage wiring.

```bash
# In-memory
STASIS_EXAMPLE_RUNTIME_BACKEND=in-memory \
    cargo run --example runtime_backends_profiles

# Surreal websocket
STASIS_EXAMPLE_RUNTIME_BACKEND=surreal-ws \
STASIS_EXAMPLE_SURREAL_ENDPOINT=ws://127.0.0.1:8000/rpc \
STASIS_EXAMPLE_SURREAL_NAMESPACE=stasis \
STASIS_EXAMPLE_SURREAL_DATABASE=runtime \
    cargo run --example runtime_backends_profiles

# Surreal embedded KV
STASIS_EXAMPLE_RUNTIME_BACKEND=surreal-kv \
STASIS_EXAMPLE_SURREAL_KV_PATH=./data/runtime-surreal \
STASIS_EXAMPLE_SURREAL_NAMESPACE=stasis \
STASIS_EXAMPLE_SURREAL_DATABASE=runtime \
    cargo run --example runtime_backends_profiles
```

## Team Role Scenario Packs

Run a role-specific scenario bundle:

```bash
STASIS_EXAMPLE_DRY_RUN=1 STASIS_EXAMPLE_TEAM_PROFILE=sre cargo run --example team_role_workflows
STASIS_EXAMPLE_DRY_RUN=1 STASIS_EXAMPLE_TEAM_PROFILE=product cargo run --example team_role_workflows
STASIS_EXAMPLE_DRY_RUN=1 STASIS_EXAMPLE_TEAM_PROFILE=support cargo run --example team_role_workflows
```

Run the broader production workflow set with profile selection:

```bash
STASIS_EXAMPLE_DRY_RUN=1 STASIS_EXAMPLE_TEAM_PROFILE=all cargo run --example agentic_workflows_production
```

Unset `STASIS_EXAMPLE_DRY_RUN` and provide provider credentials to process queued jobs end-to-end.

## CI Smoke Harness

Use the smoke script for provider-safe checks in CI or pre-merge validation:

```bash
./scripts/smoke-agentic-workflows.sh
```

The harness runs example checks, dry-run execution for production examples, targeted integration tests, and `mdbook build` when `mdbook` is available.

## Production Notes

1. Use durable backends for shared environments (surreal-ws or surreal-kv).
2. Keep policy_profile values stable and environment-specific.
3. Use explicit correlation_id values to keep cross-job memory continuity predictable.
4. Turn on memory include_explain while tuning retrieval, then reduce noise for steady-state.
5. Keep tool schemas strict with additionalProperties set to false.
