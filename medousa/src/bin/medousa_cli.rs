use anyhow::{Result, anyhow};
use chrono::Utc;
use medousa::{build_runtime, parse_backend, process_once, publish_pending};
use stasis::prelude::{BackoffPolicy, NewJob, RuntimeComposition};

#[tokio::main]
async fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() {
        print_usage();
        return Ok(());
    }

    let backend = parse_backend(find_arg_value(&args, "--backend"));
    let runtime = build_runtime(backend).await?;

    match args[0].as_str() {
        "ask" => {
            let prompt = args
                .get(1)
                .ok_or_else(|| anyhow!("missing prompt: medousa ask <prompt>"))?;
            run_ask(&runtime, prompt).await
        }
        _ => {
            print_usage();
            Ok(())
        }
    }
}

async fn run_ask(runtime: &RuntimeComposition, prompt: &str) -> Result<()> {
    let now = Utc::now();
    let job_id = format!("medousa-ask-{}", now.timestamp_millis());

    let new_job = NewJob {
        id: job_id.clone(),
        queue: "default".to_string(),
        job_type: "workflow.grapheme.textops".to_string(),
        payload_ref: format!(
            "{{\"mode\":\"summarize\",\"text\":\"{}\",\"max_items\":2}}",
            sanitize_for_json(prompt)
        ),
        priority: 100,
        max_attempts: 1,
        idempotency_key: format!("idem-{}", job_id),
        correlation_id: job_id.clone(),
        causation_id: "medousa-cli".to_string(),
        trace_id: job_id.clone(),
        sttp_input_node_id: "sttp:in:medousa:ask".to_string(),
        scheduled_at: now,
        backoff_policy: BackoffPolicy {
            base_delay_seconds: 1,
            max_delay_seconds: 8,
        },
    };

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

fn sanitize_for_json(value: &str) -> String {
    value
        .replace('\\', " ")
        .replace('"', "'")
        .replace('\n', " ")
        .replace('\r', " ")
}

fn find_arg_value<'a>(args: &'a [String], key: &str) -> Option<&'a str> {
    let idx = args.iter().position(|arg| arg == key)?;
    args.get(idx + 1).map(|s| s.as_str())
}

fn print_usage() {
    println!("medousa-cli usage:");
    println!("  medousa-cli ask <prompt> [--backend in-memory|surreal-mem]");
}
