use serde_json::json;

use stasis::application::orchestration::runtime_job_payloads::{
    AgentSessionJobPayload, AgentSessionParticipantPayload, AgentToolCallMode, PromptJobPayload,
    ToolLoopJobPayload,
};
use stasis::application::orchestration::runtime_workflow_job_builder::RuntimeWorkflowJobBuilder;
use stasis::prelude::{RuntimeBackend, RuntimeSdk, StasisRuntimeBuilder};

#[tokio::test]
async fn production_example_dry_run_jobs_enqueue_without_provider() {
    let runtime = RuntimeSdk::from_builder(
        StasisRuntimeBuilder::new(RuntimeBackend::InMemory).with_locus_memory(),
    )
    .await
    .expect("in-memory runtime should initialize");

    let prompt_payload = PromptJobPayload {
        user_prompt: "Summarize deployment risk before rollout".to_string(),
        system_prompt: Some("You are concise and operations-focused.".to_string()),
        policy_profile: Some("prod.sre".to_string()),
        model_hint: Some("fast-reasoning".to_string()),
        memory_policy: None,
    };

    let tool_payload = ToolLoopJobPayload {
        user_prompt: "Use the tool and draft an incident checklist.".to_string(),
        system_prompt: Some("Prefer deterministic structure in output.".to_string()),
        policy_profile: Some("prod.ops".to_string()),
        model_hint: Some("tool-use".to_string()),
        tool_name: "fetch_knowledge_base".to_string(),
        tool_input: Some(json!({ "topic": "incident" })),
        tool_call_mode: Some(AgentToolCallMode::Strict),
        memory_policy: None,
    };

    let session_payload = AgentSessionJobPayload {
        thread_id: Some("thread-test-session".to_string()),
        initial_user_prompt: "Evaluate launch readiness for this sprint".to_string(),
        participants: vec![
            AgentSessionParticipantPayload {
                agent_id: "planner".to_string(),
                system_prompt: Some("Focus on sequencing and dependencies.".to_string()),
                tool_name: "fetch_knowledge_base".to_string(),
                tool_input: Some(json!({ "topic": "sequencing" })),
            },
            AgentSessionParticipantPayload {
                agent_id: "sre".to_string(),
                system_prompt: Some("Focus on failure modes and rollback.".to_string()),
                tool_name: "fetch_knowledge_base".to_string(),
                tool_input: Some(json!({ "topic": "rollback" })),
            },
        ],
        policy_profile: Some("prod.review".to_string()),
        model_hint: Some("balanced".to_string()),
        max_turns: Some(2),
        tool_call_mode: Some(AgentToolCallMode::Auto),
        memory_policy: None,
    };

    let jobs = vec![
        RuntimeWorkflowJobBuilder::for_prompt("job-ci-prompt-001", &prompt_payload)
            .expect("prompt payload should build")
            .with_queue("default")
            .build(),
        RuntimeWorkflowJobBuilder::for_tool_loop("job-ci-tool-loop-001", &tool_payload)
            .expect("tool-loop payload should build")
            .with_queue("default")
            .build(),
        RuntimeWorkflowJobBuilder::for_agent_session("job-ci-session-001", &session_payload)
            .expect("session payload should build")
            .with_queue("default")
            .build(),
    ];

    for job in jobs {
        runtime
            .enqueue(job)
            .await
            .expect("enqueue should succeed in dry-run path");
    }

    let stats = runtime
        .stats_snapshot(20)
        .await
        .expect("stats snapshot should succeed");

    assert_eq!(stats.enqueued_jobs, 3);
    assert_eq!(stats.running_jobs, 0);
    assert_eq!(stats.succeeded_jobs, 0);
    assert_eq!(stats.failed_jobs, 0);
    assert_eq!(stats.dead_letter_jobs, 0);
}

#[tokio::test]
async fn surreal_mem_runtime_profile_boots_for_smoke() {
    let runtime = RuntimeSdk::from_builder(
        StasisRuntimeBuilder::new(RuntimeBackend::SurrealMem {
            namespace: "ci-smoke".to_string(),
            database: "runtime".to_string(),
        })
        .with_locus_memory(),
    )
    .await
    .expect("surreal-mem runtime should initialize");

    let prompt_payload = PromptJobPayload {
        user_prompt: "Surreal mem smoke prompt".to_string(),
        system_prompt: Some("Return concise output".to_string()),
        policy_profile: Some("prod.smoke".to_string()),
        model_hint: Some("fast-reasoning".to_string()),
        memory_policy: None,
    };

    let job = RuntimeWorkflowJobBuilder::for_prompt("job-ci-surreal-mem-001", &prompt_payload)
        .expect("prompt payload should build")
        .with_queue("default")
        .build();

    runtime
        .enqueue(job)
        .await
        .expect("enqueue should succeed in surreal-mem runtime");
}
