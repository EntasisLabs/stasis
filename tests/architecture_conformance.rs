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
