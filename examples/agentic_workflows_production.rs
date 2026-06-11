use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;

use stasis::application::orchestration::runtime_job_payloads::{
    AgentSessionJobPayload, AgentSessionParticipantPayload, AgentToolCallMode,
    AgentTurnJobPayload, ConcurrentBranchJobPayload, ConcurrentPatternJobPayload,
    HandoffPatternJobPayload, HandoffTurnJobPayload, MemoryFallbackPolicyPayload,
    MemoryPolicyPayload, MemoryStoreModePayload, MemoryStrictnessModePayload,
    OrchestratorPatternJobPayload, OrchestratorRouteJobPayload, PromptJobPayload,
    SequentialPatternJobPayload, SequentialStageJobPayload, ToolLoopJobPayload,
};
use stasis::application::orchestration::runtime_workflow_job_builder::RuntimeWorkflowJobBuilder;
use stasis::application::composition::surreal_backend_config::{
    resolve_surreal_auth_from_env, resolve_surreal_database_from_env, resolve_surreal_namespace_from_env,
};
use stasis::domain::errors::{Result, StasisError};
use stasis::domain::runtime::job::NewJob;
use stasis::prelude::{RuntimeBackend, RuntimeSdk, StasisRuntimeBuilder, SurrealAuth};
use stasis::stasis_tool;

#[derive(Clone, Copy)]
enum TeamWorkflowProfile {
    All,
    SreIncident,
    ProductPlanning,
    SupportTriage,
}

impl TeamWorkflowProfile {
    fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::SreIncident => "sre-incident",
            Self::ProductPlanning => "product-planning",
            Self::SupportTriage => "support-triage",
        }
    }
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
struct FetchKnowledgeBaseInput {
    topic: String,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
struct FetchKnowledgeBaseOutput {
    topic: String,
    playbook: Vec<String>,
}

#[stasis_tool(
    name = "fetch_knowledge_base",
    description = "Returns internal playbook snippets for an operation topic",
    output_schema = true
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

fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
}

fn resolve_team_profile_from_env() -> TeamWorkflowProfile {
    let value = std::env::var("STASIS_EXAMPLE_TEAM_PROFILE")
        .ok()
        .map(|raw| raw.trim().to_ascii_lowercase())
        .unwrap_or_else(|| "all".to_string());

    match value.as_str() {
        "sre" | "sre-incident" | "incident" => TeamWorkflowProfile::SreIncident,
        "product" | "product-planning" | "planning" => TeamWorkflowProfile::ProductPlanning,
        "support" | "support-triage" | "triage" => TeamWorkflowProfile::SupportTriage,
        _ => TeamWorkflowProfile::All,
    }
}

fn resolve_surreal_namespace() -> String {
    resolve_surreal_namespace_from_env(
        "STASIS_EXAMPLE_SURREAL_NAMESPACE",
        Some("STASIS_DASHBOARD_SURREAL_NAMESPACE"),
        "stasis",
    )
}

fn resolve_surreal_database() -> String {
    resolve_surreal_database_from_env(
        "STASIS_EXAMPLE_SURREAL_DATABASE",
        Some("STASIS_DASHBOARD_SURREAL_DATABASE"),
        "runtime",
    )
}

fn resolve_surreal_auth() -> Option<SurrealAuth> {
    resolve_surreal_auth_from_env(
        "STASIS_EXAMPLE_SURREAL_USERNAME",
        "STASIS_EXAMPLE_SURREAL_PASSWORD",
        Some("STASIS_DASHBOARD_SURREAL_USERNAME"),
        Some("STASIS_DASHBOARD_SURREAL_PASSWORD"),
    )
}

fn apply_surreal_auth(backend: RuntimeBackend) -> RuntimeBackend {
    match resolve_surreal_auth() {
        Some(auth) => backend.with_surreal_auth(auth),
        None => backend,
    }
}

fn resolve_runtime_backend_from_env() -> Result<RuntimeBackend> {
    let backend = std::env::var("STASIS_EXAMPLE_RUNTIME_BACKEND")
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_else(|| "in-memory".to_string());

    match backend.as_str() {
        "in-memory" | "inmemory" => Ok(RuntimeBackend::InMemory),
        "surreal-mem" | "mem" => Ok(apply_surreal_auth(RuntimeBackend::surreal_mem(
            resolve_surreal_namespace(),
            resolve_surreal_database(),
        ))),
        "surreal-ws" | "ws" => {
            let endpoint = std::env::var("STASIS_EXAMPLE_SURREAL_ENDPOINT")
                .ok()
                .or_else(|| std::env::var("STASIS_DASHBOARD_SURREAL_ENDPOINT").ok())
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    StasisError::PortFailure(
                        "STASIS_EXAMPLE_SURREAL_ENDPOINT is required when STASIS_EXAMPLE_RUNTIME_BACKEND=surreal-ws"
                            .to_string(),
                    )
                })?;

            Ok(apply_surreal_auth(RuntimeBackend::surreal_ws(
                endpoint,
                resolve_surreal_namespace(),
                resolve_surreal_database(),
            )))
        }
        "surreal-kv" | "kv" => {
            let path = std::env::var("STASIS_EXAMPLE_SURREAL_KV_PATH")
                .ok()
                .or_else(|| std::env::var("STASIS_DASHBOARD_SURREAL_KV_PATH").ok())
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    StasisError::PortFailure(
                        "STASIS_EXAMPLE_SURREAL_KV_PATH is required when STASIS_EXAMPLE_RUNTIME_BACKEND=surreal-kv"
                            .to_string(),
                    )
                })?;

            Ok(apply_surreal_auth(RuntimeBackend::surreal_kv(
                path,
                resolve_surreal_namespace(),
                resolve_surreal_database(),
            )))
        }
        other => Err(StasisError::PortFailure(format!(
            "unsupported STASIS_EXAMPLE_RUNTIME_BACKEND='{other}'"
        ))),
    }
}

fn build_sre_incident_jobs(memory_policy: Option<MemoryPolicyPayload>) -> Result<Vec<NewJob>> {
    let prompt_payload = PromptJobPayload {
        user_prompt: "Summarize queue saturation risks for this release".to_string(),
        system_prompt: Some("You are a reliability engineer. Be concise and actionable.".to_string()),
        policy_profile: Some("prod.sre".to_string()),
        model_hint: Some("fast-reasoning".to_string()),
        memory_policy: memory_policy.clone(),
    };

    let prompt_job = RuntimeWorkflowJobBuilder::for_prompt("job-prompt-prod-001", &prompt_payload)?
        .with_queue("default")
        .with_correlation_id("thread-prod-sre-001")
        .with_sttp_input_node_id("sttp:in:prod:sre:prompt:001")
        .with_max_attempts(2)
        .build();

    let tool_payload = ToolLoopJobPayload {
        user_prompt: "Use the tool and produce an execution-ready incident containment checklist."
            .to_string(),
        system_prompt: Some("Call tools when needed and cite tool output in the final answer.".to_string()),
        policy_profile: Some("prod.ops".to_string()),
        model_hint: Some("tool-use".to_string()),
        tool_name: "fetch_knowledge_base".to_string(),
        tool_input: Some(json!({ "topic": "incident response" })),
        tool_call_mode: Some(AgentToolCallMode::Strict),
        memory_policy: memory_policy.clone(),
    };

    let tool_job = RuntimeWorkflowJobBuilder::for_tool_loop("job-tool-loop-prod-001", &tool_payload)?
        .with_queue("default")
        .with_correlation_id("thread-prod-sre-001")
        .with_sttp_input_node_id("sttp:in:prod:sre:tool-loop:001")
        .with_max_attempts(2)
        .build();

    let turn_payload = AgentTurnJobPayload {
        agent_id: "incident_commander".to_string(),
        thread_id: Some("thread-prod-incident-42".to_string()),
        user_prompt: "Draft the first incident update for stakeholders.".to_string(),
        system_prompt: Some("Prioritize clarity, impact, and next checkpoint.".to_string()),
        policy_profile: Some("prod.incident".to_string()),
        model_hint: Some("balanced".to_string()),
        tool_name: "fetch_knowledge_base".to_string(),
        tool_input: Some(json!({ "topic": "incident communications" })),
        tool_call_mode: Some(AgentToolCallMode::Auto),
        memory_policy,
    };

    let turn_job = RuntimeWorkflowJobBuilder::for_agent_turn("job-agent-turn-prod-001", &turn_payload)?
        .with_queue("default")
        .with_correlation_id("thread-prod-incident-42")
        .with_sttp_input_node_id("sttp:in:prod:sre:agent-turn:001")
        .with_max_attempts(2)
        .build();

    Ok(vec![prompt_job, tool_job, turn_job])
}

fn build_product_planning_jobs(memory_policy: Option<MemoryPolicyPayload>) -> Result<Vec<NewJob>> {
    let session_payload = AgentSessionJobPayload {
        thread_id: Some("thread-prod-design-review-1".to_string()),
        initial_user_prompt: "Review this deployment plan for risk and sequencing.".to_string(),
        participants: vec![
            AgentSessionParticipantPayload {
                agent_id: "planner".to_string(),
                system_prompt: Some("Break the plan into milestones and dependencies.".to_string()),
                tool_name: "fetch_knowledge_base".to_string(),
                tool_input: Some(json!({ "topic": "deployment sequencing" })),
            },
            AgentSessionParticipantPayload {
                agent_id: "sre_reviewer".to_string(),
                system_prompt: Some("Focus on blast radius and rollback readiness.".to_string()),
                tool_name: "fetch_knowledge_base".to_string(),
                tool_input: Some(json!({ "topic": "rollback policy" })),
            },
        ],
        policy_profile: Some("prod.review".to_string()),
        model_hint: Some("balanced".to_string()),
        max_turns: Some(4),
        tool_call_mode: Some(AgentToolCallMode::Auto),
        memory_policy: memory_policy.clone(),
    };

    let session_job = RuntimeWorkflowJobBuilder::for_agent_session(
        "job-agent-session-prod-001",
        &session_payload,
    )?
    .with_queue("default")
    .with_correlation_id("thread-prod-product-001")
    .with_sttp_input_node_id("sttp:in:prod:product:agent-session:001")
    .with_max_attempts(2)
    .build();

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

    let sequential_job = RuntimeWorkflowJobBuilder::for_orchestration_sequential(
        "job-orch-sequential-prod-001",
        &sequential_payload,
    )?
    .with_queue("default")
    .with_correlation_id("thread-prod-product-001")
    .with_sttp_input_node_id("sttp:in:prod:product:sequential:001")
    .with_max_attempts(2)
    .build();

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
                user_prompt_template:
                    "{{input}}\nPolish tone and clarity for executive audience.".to_string(),
                system_prompt: None,
                policy_profile: None,
                model_hint: None,
            },
        ],
    };

    let handoff_job = RuntimeWorkflowJobBuilder::for_orchestration_handoff(
        "job-orch-handoff-prod-001",
        &handoff_payload,
    )?
    .with_queue("default")
    .with_correlation_id("thread-prod-product-001")
    .with_sttp_input_node_id("sttp:in:prod:product:handoff:001")
    .with_max_attempts(2)
    .build();

    Ok(vec![session_job, sequential_job, handoff_job])
}

fn build_support_triage_jobs() -> Result<Vec<NewJob>> {
    let concurrent_payload = ConcurrentPatternJobPayload {
        thread_id: Some("thread-prod-concurrent-1".to_string()),
        initial_user_prompt: "Assess ticket escalation quality from three angles".to_string(),
        policy_profile: Some("prod.support".to_string()),
        model_hint: Some("balanced".to_string()),
        tool_call_mode: None,
        memory_policy: None,
        merge_strategy: Some("append".to_string()),
        branches: vec![
            ConcurrentBranchJobPayload::prompt(
                "severity",
                "{{input}}\nClassify severity and urgency.",
            ),
            ConcurrentBranchJobPayload::prompt(
                "owner",
                "{{input}}\nSuggest owning team and escalation target.",
            ),
        ],
    };

    let concurrent_job = RuntimeWorkflowJobBuilder::for_orchestration_concurrent(
        "job-orch-concurrent-prod-001",
        &concurrent_payload,
    )?
    .with_queue("default")
    .with_correlation_id("thread-prod-support-001")
    .with_sttp_input_node_id("sttp:in:prod:support:concurrent:001")
    .with_max_attempts(2)
    .build();

    let orchestrator_payload = OrchestratorPatternJobPayload {
        thread_id: Some("thread-prod-orchestrator-1".to_string()),
        initial_user_prompt: "Need guidance for urgent customer outage triage"
            .to_string(),
        policy_profile: Some("prod.support".to_string()),
        model_hint: Some("balanced".to_string()),
        routes: vec![
            OrchestratorRouteJobPayload {
                route_id: "incident_path".to_string(),
                selector_keywords: vec!["outage".to_string(), "incident".to_string()],
                user_prompt_template: "{{input}}\nRun incident triage playbook.".to_string(),
                system_prompt: None,
                policy_profile: None,
                model_hint: None,
            },
            OrchestratorRouteJobPayload {
                route_id: "standard_path".to_string(),
                selector_keywords: vec!["question".to_string(), "routine".to_string()],
                user_prompt_template: "{{input}}\nRun standard support triage checklist.".to_string(),
                system_prompt: None,
                policy_profile: None,
                model_hint: None,
            },
        ],
    };

    let orchestrator_job = RuntimeWorkflowJobBuilder::for_orchestration_orchestrator(
        "job-orch-orchestrator-prod-001",
        &orchestrator_payload,
    )?
    .with_queue("default")
    .with_correlation_id("thread-prod-support-001")
    .with_sttp_input_node_id("sttp:in:prod:support:orchestrator:001")
    .with_max_attempts(2)
    .build();

    Ok(vec![concurrent_job, orchestrator_job])
}

fn build_jobs_for_profile(
    profile: TeamWorkflowProfile,
    memory_policy: Option<MemoryPolicyPayload>,
) -> Result<Vec<NewJob>> {
    let mut jobs = Vec::<NewJob>::new();

    match profile {
        TeamWorkflowProfile::All => {
            jobs.extend(build_sre_incident_jobs(memory_policy.clone())?);
            jobs.extend(build_product_planning_jobs(memory_policy.clone())?);
            jobs.extend(build_support_triage_jobs()?);
        }
        TeamWorkflowProfile::SreIncident => {
            jobs.extend(build_sre_incident_jobs(memory_policy)?);
        }
        TeamWorkflowProfile::ProductPlanning => {
            jobs.extend(build_product_planning_jobs(memory_policy)?);
        }
        TeamWorkflowProfile::SupportTriage => {
            jobs.extend(build_support_triage_jobs()?);
        }
    }

    Ok(jobs)
}

fn has_stasis_llm_key() -> bool {
    [
        "STASIS_LLM_API_KEY",
        "STASIS_OPENAI_API_KEY",
        "STASIS_ANTHROPIC_API_KEY",
        "STASIS_OLLAMA_API_KEY",
    ]
    .iter()
    .any(|name| std::env::var(name).ok().is_some_and(|value| !value.trim().is_empty()))
}

#[tokio::main]
async fn main() -> Result<()> {
    let dry_run = env_flag("STASIS_EXAMPLE_DRY_RUN");
    let profile = resolve_team_profile_from_env();
    let backend = resolve_runtime_backend_from_env()?;

    if !dry_run && !has_stasis_llm_key() {
        println!(
            "Set STASIS_LLM_API_KEY (or STASIS_OPENAI_API_KEY / STASIS_ANTHROPIC_API_KEY / STASIS_OLLAMA_API_KEY) before running this example, or set STASIS_EXAMPLE_DRY_RUN=1"
        );
        return Ok(());
    }

    let builder = StasisRuntimeBuilder::new(backend).with_locus_memory();
    let builder = builder.with_tool(FetchKnowledgeBaseTool)?;
    let runtime = RuntimeSdk::from_builder(builder).await?;

    println!(
        "agentic_workflows_production profile={} dry_run={}",
        profile.as_str(),
        dry_run
    );

    let memory_policy = Some(production_memory_policy());
    let jobs = build_jobs_for_profile(profile, memory_policy)?;
    for job in jobs {
        runtime.enqueue(job).await?;
    }

    if dry_run {
        println!("dry-run mode enabled: jobs enqueued but not processed");
    } else {
        while runtime.process_once("default", "worker-prod-1").await?.is_some() {}
    }

    let stats = runtime.stats_snapshot(100).await?;
    println!(
        "enqueued={} running={} succeeded={} failed={} dead_letter={}",
        stats.enqueued_jobs,
        stats.running_jobs,
        stats.succeeded_jobs,
        stats.failed_jobs,
        stats.dead_letter_jobs
    );

    Ok(())
}
