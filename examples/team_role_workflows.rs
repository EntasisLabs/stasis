use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;

use stasis::application::orchestration::runtime_job_payloads::{
    AgentSessionJobPayload, AgentSessionParticipantPayload, AgentToolCallMode,
    AgentTurnJobPayload, ConcurrentBranchJobPayload, ConcurrentPatternJobPayload,
    HandoffPatternJobPayload, HandoffTurnJobPayload, OrchestratorPatternJobPayload,
    OrchestratorRouteJobPayload,
};
use stasis::application::orchestration::runtime_workflow_job_builder::RuntimeWorkflowJobBuilder;
use stasis::domain::errors::Result;
use stasis::domain::runtime::job::NewJob;
use stasis::prelude::{RuntimeBackend, RuntimeSdk, StasisRuntimeBuilder};
use stasis::stasis_tool;

#[derive(Clone, Copy)]
enum TeamRoleScenario {
    SreIncident,
    ProductPlanning,
    SupportTriage,
}

impl TeamRoleScenario {
    fn as_str(self) -> &'static str {
        match self {
            Self::SreIncident => "sre-incident",
            Self::ProductPlanning => "product-planning",
            Self::SupportTriage => "support-triage",
        }
    }
}

fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
}

fn resolve_team_scenario_from_env() -> TeamRoleScenario {
    let value = std::env::var("STASIS_EXAMPLE_TEAM_PROFILE")
        .ok()
        .map(|raw| raw.trim().to_ascii_lowercase())
        .unwrap_or_else(|| "sre-incident".to_string());

    match value.as_str() {
        "product" | "product-planning" | "planning" => TeamRoleScenario::ProductPlanning,
        "support" | "support-triage" | "triage" => TeamRoleScenario::SupportTriage,
        _ => TeamRoleScenario::SreIncident,
    }
}

fn has_stasis_llm_key() -> bool {
    const CANDIDATES: [&str; 4] = [
        "STASIS_LLM_API_KEY",
        "STASIS_OPENAI_API_KEY",
        "STASIS_ANTHROPIC_API_KEY",
        "STASIS_OLLAMA_API_KEY",
    ];

    CANDIDATES.into_iter().any(|key| {
        std::env::var(key)
            .ok()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false)
    })
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
struct FetchKnowledgeBaseInput {
    topic: String,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
struct FetchKnowledgeBaseOutput {
    topic: String,
    notes: Vec<String>,
}

#[stasis_tool(
    name = "fetch_knowledge_base",
    description = "Returns curated snippets for release operations topics",
    output_schema = true
)]
async fn fetch_knowledge_base(input: FetchKnowledgeBaseInput) -> Result<FetchKnowledgeBaseOutput> {
    Ok(FetchKnowledgeBaseOutput {
        topic: input.topic,
        notes: vec![
            "Prefer explicit rollback criteria before production push.".to_string(),
            "Confirm owners and checkpoints for each milestone.".to_string(),
            "Capture assumptions and unresolved risks in memory.".to_string(),
        ],
    })
}

fn build_sre_incident_jobs() -> Result<Vec<NewJob>> {
    let turn_payload = AgentTurnJobPayload {
        agent_id: "incident_commander".to_string(),
        thread_id: Some("thread-team-sre-incident".to_string()),
        user_prompt: "Summarize incident status and immediate mitigation options.".to_string(),
        system_prompt: Some("Write a concise update for engineering leadership.".to_string()),
        policy_profile: Some("prod.incident".to_string()),
        model_hint: Some("balanced".to_string()),
        reasoning_effort: None,
        tool_name: "fetch_knowledge_base".to_string(),
        tool_input: Some(json!({ "topic": "incident response" })),
        tool_call_mode: Some(AgentToolCallMode::Auto),
        memory_policy: None,
    };

    let handoff_payload = HandoffPatternJobPayload {
        thread_id: Some("thread-team-sre-handoff".to_string()),
        initial_user_prompt: "Draft customer-safe outage communication".to_string(),
        policy_profile: Some("prod.incident".to_string()),
        model_hint: Some("balanced".to_string()),
        reasoning_effort: None,
        turns: vec![
            HandoffTurnJobPayload {
                actor_id: "incident_commander".to_string(),
                user_prompt_template: "{{input}}\nCreate a factual technical summary.".to_string(),
                system_prompt: None,
                policy_profile: None,
                model_hint: None,
                reasoning_effort: None,
            },
            HandoffTurnJobPayload {
                actor_id: "comms_lead".to_string(),
                user_prompt_template:
                    "{{input}}\nRewrite for customer and executive readability.".to_string(),
                system_prompt: None,
                policy_profile: None,
                model_hint: None,
                reasoning_effort: None,
            },
        ],
    };

    Ok(vec![
        RuntimeWorkflowJobBuilder::for_agent_turn("job-team-sre-turn-001", &turn_payload)?
            .with_queue("default")
            .with_correlation_id("thread-team-sre")
            .with_sttp_input_node_id("sttp:in:team:sre:turn:001")
            .build(),
        RuntimeWorkflowJobBuilder::for_orchestration_handoff(
            "job-team-sre-handoff-001",
            &handoff_payload,
        )?
        .with_queue("default")
        .with_correlation_id("thread-team-sre")
        .with_sttp_input_node_id("sttp:in:team:sre:handoff:001")
        .build(),
    ])
}

fn build_product_planning_jobs() -> Result<Vec<NewJob>> {
    let session_payload = AgentSessionJobPayload {
        thread_id: Some("thread-team-product-session".to_string()),
        initial_user_prompt: "Evaluate roadmap tradeoffs between reliability and features.".to_string(),
        participants: vec![
            AgentSessionParticipantPayload {
                agent_id: "pm".to_string(),
                system_prompt: Some("Advocate for customer value and timeline realism.".to_string()),
                tool_name: "fetch_knowledge_base".to_string(),
                tool_input: Some(json!({ "topic": "roadmap planning" })),
            },
            AgentSessionParticipantPayload {
                agent_id: "staff_engineer".to_string(),
                system_prompt: Some("Highlight technical risk and architecture constraints.".to_string()),
                tool_name: "fetch_knowledge_base".to_string(),
                tool_input: Some(json!({ "topic": "architecture constraints" })),
            },
        ],
        policy_profile: Some("prod.product".to_string()),
        model_hint: Some("balanced".to_string()),
        reasoning_effort: None,
        max_turns: Some(4),
        tool_call_mode: Some(AgentToolCallMode::Auto),
        memory_policy: None,
    };

    Ok(vec![
        RuntimeWorkflowJobBuilder::for_agent_session("job-team-product-session-001", &session_payload)?
            .with_queue("default")
            .with_correlation_id("thread-team-product")
            .with_sttp_input_node_id("sttp:in:team:product:session:001")
            .build(),
    ])
}

fn build_support_triage_jobs() -> Result<Vec<NewJob>> {
    let concurrent_payload = ConcurrentPatternJobPayload {
        thread_id: Some("thread-team-support-concurrent".to_string()),
        initial_user_prompt: "Triage this incoming escalation for severity, ownership, and SLA."
            .to_string(),
        policy_profile: Some("prod.support".to_string()),
        model_hint: Some("balanced".to_string()),
        reasoning_effort: None,
        tool_call_mode: None,
        memory_policy: None,
        merge_strategy: Some("append".to_string()),
        branches: vec![
            ConcurrentBranchJobPayload::prompt(
                "severity",
                "{{input}}\nAssess severity with rationale.",
            ),
            ConcurrentBranchJobPayload::prompt(
                "owner",
                "{{input}}\nRecommend owning team and escalation path.",
            ),
        ],
    };

    let orchestrator_payload = OrchestratorPatternJobPayload {
        thread_id: Some("thread-team-support-orchestrator".to_string()),
        initial_user_prompt: "Customer says checkout is down in one region".to_string(),
        policy_profile: Some("prod.support".to_string()),
        model_hint: Some("balanced".to_string()),
        reasoning_effort: None,
        routes: vec![
            OrchestratorRouteJobPayload {
                route_id: "incident_path".to_string(),
                selector_keywords: vec!["down".to_string(), "outage".to_string()],
                user_prompt_template: "{{input}}\nRun incident workflow with immediate safeguards."
                    .to_string(),
                system_prompt: None,
                policy_profile: None,
                model_hint: None,
                reasoning_effort: None,
            },
            OrchestratorRouteJobPayload {
                route_id: "standard_path".to_string(),
                selector_keywords: vec!["slow".to_string(), "question".to_string()],
                user_prompt_template: "{{input}}\nRun standard support troubleshooting flow."
                    .to_string(),
                system_prompt: None,
                policy_profile: None,
                model_hint: None,
                reasoning_effort: None,
            },
        ],
    };

    Ok(vec![
        RuntimeWorkflowJobBuilder::for_orchestration_concurrent(
            "job-team-support-concurrent-001",
            &concurrent_payload,
        )?
        .with_queue("default")
        .with_correlation_id("thread-team-support")
        .with_sttp_input_node_id("sttp:in:team:support:concurrent:001")
        .build(),
        RuntimeWorkflowJobBuilder::for_orchestration_orchestrator(
            "job-team-support-orchestrator-001",
            &orchestrator_payload,
        )?
        .with_queue("default")
        .with_correlation_id("thread-team-support")
        .with_sttp_input_node_id("sttp:in:team:support:orchestrator:001")
        .build(),
    ])
}

fn build_jobs_for_scenario(scenario: TeamRoleScenario) -> Result<Vec<NewJob>> {
    match scenario {
        TeamRoleScenario::SreIncident => build_sre_incident_jobs(),
        TeamRoleScenario::ProductPlanning => build_product_planning_jobs(),
        TeamRoleScenario::SupportTriage => build_support_triage_jobs(),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let dry_run = env_flag("STASIS_EXAMPLE_DRY_RUN") || !has_stasis_llm_key();
    let scenario = resolve_team_scenario_from_env();

    let builder = StasisRuntimeBuilder::new(RuntimeBackend::InMemory)
        .with_locus_memory()
        .with_tool(FetchKnowledgeBaseTool)?;
    let runtime = RuntimeSdk::from_builder(builder).await?;

    let jobs = build_jobs_for_scenario(scenario)?;
    for job in jobs {
        runtime.enqueue(job).await?;
    }

    if dry_run {
        println!(
            "team role workflow dry-run scenario={} (set STASIS_EXAMPLE_DRY_RUN=0 and provider keys to process)",
            scenario.as_str()
        );
    } else {
        while runtime
            .process_once("default", "worker-team-role-1")
            .await?
            .is_some()
        {}
    }

    let stats = runtime.stats_snapshot(50).await?;
    println!(
        "scenario={} enqueued={} running={} succeeded={} failed={} dead_letter={}",
        scenario.as_str(),
        stats.enqueued_jobs,
        stats.running_jobs,
        stats.succeeded_jobs,
        stats.failed_jobs,
        stats.dead_letter_jobs
    );

    Ok(())
}
