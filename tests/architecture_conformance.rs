use std::fs;

#[test]
fn medousa_cli_uses_stasis_runtime_workflow_paths() {
    let cli_source = include_str!("../medousa/src/bin/medousa_cli.rs");

    assert!(
        cli_source.contains("StasisWorkflowJobBuilder::for_agent_session"),
        "ask flow must use StasisWorkflowJobBuilder::for_agent_session"
    );
    assert!(
        cli_source.contains("StasisWorkflowJobBuilder::for_prompt"),
        "llm flow must use StasisWorkflowJobBuilder::for_prompt"
    );
}

#[test]
fn medousa_cli_does_not_use_direct_llm_adapter_construction() {
    let cli_source = include_str!("../medousa/src/bin/medousa_cli.rs");

    assert!(
        !cli_source.contains("GenaiChatClient"),
        "medousa cli should not construct GenaiChatClient directly"
    );
    assert!(
        !cli_source.contains("PromptExecutionPipeline::new"),
        "medousa cli should not instantiate prompt pipeline directly"
    );
}

#[test]
fn pr_template_references_rfc_and_plan() {
    let repo_root = env!("CARGO_MANIFEST_DIR");
    let template_path = format!("{repo_root}/.github/pull_request_template.md");
    let content = fs::read_to_string(template_path).expect("pull request template should exist");

    assert!(
        content.contains("stasis-framework-rfc.md"),
        "PR template must reference the architecture RFC"
    );
    assert!(
        content.contains("stasis-framework-implementation-plan.md"),
        "PR template must reference the implementation plan"
    );
}

#[test]
fn runtime_builder_registers_orchestration_pattern_handlers_by_default() {
    let builder_source = include_str!("../src/application/runtime/stasis_runtime_builder.rs");

    assert!(
        builder_source.contains("include_orchestration_pattern_handlers: true"),
        "runtime builder must enable orchestration pattern handlers by default"
    );
    assert!(
        builder_source.contains("without_orchestration_pattern_handlers"),
        "runtime builder must expose explicit orchestration handler disable toggle"
    );
    assert!(
        builder_source.contains("ConcurrentPatternJobHandler::new_with_thread_store"),
        "runtime builder must register concurrent orchestration handler"
    );
    assert!(
        builder_source.contains("SequentialPatternJobHandler::new_with_thread_store"),
        "runtime builder must register sequential orchestration handler"
    );
    assert!(
        builder_source.contains("HandoffPatternJobHandler::new_with_thread_store"),
        "runtime builder must register handoff orchestration handler"
    );
    assert!(
        builder_source.contains("OrchestratorPatternJobHandler::new_with_thread_store"),
        "runtime builder must register orchestrator orchestration handler"
    );
}

#[test]
fn orchestration_handlers_use_thread_store_port_not_infrastructure_impls() {
    let sequential = include_str!("../src/application/runtime/sequential_pattern_job_handler.rs");
    let concurrent = include_str!("../src/application/runtime/concurrent_pattern_job_handler.rs");
    let handoff = include_str!("../src/application/runtime/handoff_pattern_job_handler.rs");
    let orchestrator = include_str!("../src/application/runtime/orchestrator_pattern_job_handler.rs");

    for (name, source) in [
        ("sequential", sequential),
        ("concurrent", concurrent),
        ("handoff", handoff),
        ("orchestrator", orchestrator),
    ] {
        assert!(
            source.contains("ports::outbound::runtime::thread_store::ThreadStore"),
            "{name} handler must depend on ThreadStore port"
        );
        assert!(
            !source.contains("infrastructure::runtime::in_memory_thread_store"),
            "{name} handler must not depend on in-memory thread store implementation directly"
        );
        assert!(
            !source.contains("infrastructure::runtime::surreal_thread_store"),
            "{name} handler must not depend on Surreal thread store implementation directly"
        );
    }
}

#[test]
fn orchestration_handlers_include_standard_diagnostics_contract_fields() {
    let sequential = include_str!("../src/application/runtime/sequential_pattern_job_handler.rs");
    let concurrent = include_str!("../src/application/runtime/concurrent_pattern_job_handler.rs");
    let handoff = include_str!("../src/application/runtime/handoff_pattern_job_handler.rs");
    let orchestrator = include_str!("../src/application/runtime/orchestrator_pattern_job_handler.rs");

    for (name, source) in [
        ("sequential", sequential),
        ("concurrent", concurrent),
        ("handoff", handoff),
        ("orchestrator", orchestrator),
    ] {
        assert!(
            source.contains("\"status\": \"success\""),
            "{name} handler must emit success diagnostics status"
        );
        assert!(
            source.contains("\"status\": \"failure\""),
            "{name} handler must emit failure diagnostics status"
        );
        assert!(
            source.contains("\"pattern\":"),
            "{name} handler must emit diagnostics pattern"
        );
        assert!(
            source.contains("\"thread_id\":"),
            "{name} handler must emit diagnostics thread_id on success"
        );
        assert!(
            source.contains("\"termination_reason\":"),
            "{name} handler must emit diagnostics termination_reason on success"
        );
        assert!(
            source.contains("\"guardrail_code\": \"POLICY_VIOLATION\""),
            "{name} handler must emit policy violation guardrail code"
        );
        assert!(
            source.contains("\"policy_reason\":"),
            "{name} handler must emit policy violation reason"
        );
    }
}

#[test]
fn default_chat_middlewares_depend_on_ports_not_runtime_infrastructure() {
    let middleware_source = include_str!("../src/application/runtime/default_chat_middlewares.rs");

    assert!(
        middleware_source.contains("ports::outbound::ai_chat_client::AiChatClient"),
        "chat middleware must depend on AiChatClient port"
    );
    assert!(
        middleware_source.contains("ports::outbound::runtime::runtime_metrics::RuntimeMetrics"),
        "chat middleware must depend on RuntimeMetrics port"
    );
    assert!(
        !middleware_source.contains("infrastructure::runtime::"),
        "chat middleware must not import runtime infrastructure implementations directly"
    );
}
