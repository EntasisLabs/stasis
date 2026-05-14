use anyhow::{Result, anyhow};
use chrono::Utc;
use medousa::{build_runtime, parse_backend, process_once, publish_pending};
use serde_json::json;
use stasis::prelude::{
    AgentSessionJobPayload, AgentSessionParticipantPayload, JobAttemptStore, PromptJobPayload,
    RuntimeComposition,
    StasisWorkflowJobBuilder,
};
use stasis::prelude::AgentToolCallMode;

#[tokio::main]
async fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() {
        print_usage();
        return Ok(());
    }

    match args[0].as_str() {
        "ask" => {
            let backend = parse_backend(find_arg_value(&args, "--backend"));
            let runtime = build_runtime(backend).await?;
            let prompt = args
                .get(1)
                .ok_or_else(|| anyhow!("missing prompt: medousa ask <prompt>"))?;
            run_ask(&runtime, prompt).await
        }
        "llm" => {
            let backend = parse_backend(find_arg_value(&args, "--backend"));
            let runtime = build_runtime(backend).await?;
            let prompt = args
                .get(1)
                .ok_or_else(|| anyhow!("missing prompt: medousa llm <prompt>"))?;
            let model = find_arg_value(&args, "--model");
            run_llm(&runtime, prompt, model).await
        }
        _ => {
            print_usage();
            Ok(())
        }
    }
}

async fn run_llm(runtime: &RuntimeComposition, prompt: &str, model: Option<&str>) -> Result<()> {
    let now = Utc::now();
    let job_id = format!("medousa-llm-{}", now.timestamp_millis());
    let payload = PromptJobPayload {
        user_prompt: prompt.to_string(),
        system_prompt: Some(
            "You are Medousa, a practical research assistant. Be concise and structured."
                .to_string(),
        ),
        policy_profile: Some("default".to_string()),
        model_hint: model.map(|v| v.to_string()),
    };

    let new_job = StasisWorkflowJobBuilder::for_prompt(job_id.clone(), &payload)?
        .with_causation_id("medousa-cli")
        .with_sttp_input_node_id("sttp:in:medousa:llm")
        .with_scheduled_at(now)
        .build();

    match runtime {
        RuntimeComposition::InMemory(rt) => rt.enqueue(new_job).await?,
        RuntimeComposition::Surreal(rt) => rt.enqueue(new_job).await?,
    }

    process_once(runtime, "medousa-cli").await?;

    let attempts = match runtime {
        RuntimeComposition::InMemory(rt) => rt.job_attempt_store.list_by_job_id(&job_id).await?,
        RuntimeComposition::Surreal(rt) => rt.job_attempt_store.list_by_job_id(&job_id).await?,
    };
    let diagnostics_raw = attempts
        .last()
        .and_then(|attempt| attempt.diagnostics.as_deref())
        .ok_or_else(|| anyhow!("missing prompt diagnostics for {job_id}"))?;
    let diagnostics: serde_json::Value = serde_json::from_str(diagnostics_raw)?;
    let completion = diagnostics
        .get("output_text")
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing output_text in prompt diagnostics"))?;

    println!(
        "model_hint={}",
        model.unwrap_or("(provider default from runtime environment)")
    );
    println!("completion:\n{}", completion);
    Ok(())
}

async fn run_ask(runtime: &RuntimeComposition, prompt: &str) -> Result<()> {
    let now = Utc::now();
    let job_id = format!("medousa-ask-{}", now.timestamp_millis());
    let payload = AgentSessionJobPayload {
        thread_id: Some(job_id.clone()),
        initial_user_prompt: prompt.to_string(),
        participants: vec![AgentSessionParticipantPayload {
            agent_id: "medousa.researcher".to_string(),
            system_prompt: Some(
                "You are Medousa, a practical research assistant. Use tool evidence and cite findings succinctly.".to_string(),
            ),
            tool_name: "stasis.web.search.mock".to_string(),
            tool_input: Some(json!({ "query": prompt })),
        }],
        policy_profile: Some("default".to_string()),
        model_hint: None,
        max_turns: Some(1),
        tool_call_mode: Some(AgentToolCallMode::Auto),
    };

    let new_job = StasisWorkflowJobBuilder::for_agent_session(job_id.clone(), &payload)?
    .with_causation_id("medousa-cli")
    .with_sttp_input_node_id("sttp:in:medousa:ask")
    .with_scheduled_at(now)
    .build();

    match runtime {
        RuntimeComposition::InMemory(rt) => rt.enqueue(new_job).await?,
        RuntimeComposition::Surreal(rt) => rt.enqueue(new_job).await?,
    }

    let processed = process_once(runtime, "medousa-cli").await?;
    let published = publish_pending(runtime, 50).await?;

    println!("Medousa run submitted");
    println!("job_id={}", job_id);
    println!("processed={:?}", processed);
    println!("published_events={}", published);
    println!("next: medousa-daemon can be used for continuous orchestration loops");

    Ok(())
}

fn find_arg_value<'a>(args: &'a [String], key: &str) -> Option<&'a str> {
    let idx = args.iter().position(|arg| arg == key)?;
    args.get(idx + 1).map(|s| s.as_str())
}

fn print_usage() {
    println!("medousa-cli usage:");
    println!("  medousa-cli ask <prompt> [--backend in-memory|surreal-mem]");
    println!("  medousa-cli llm <prompt> [--model <model_name>] [--backend in-memory|surreal-mem]");
    println!("  note: ask uses workflow.stasis.agent_session through Stasis runtime orchestration");
}
