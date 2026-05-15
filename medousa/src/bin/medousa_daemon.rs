use std::time::Duration;

use anyhow::Result;
use medousa::{build_runtime, parse_backend, process_once, publish_pending};

#[tokio::main]
async fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let backend = parse_backend(find_arg_value(&args, "--backend"));
    let provider = find_arg_value(&args, "--provider");
    let model = find_arg_value(&args, "--model");
    let base_url = find_arg_value(&args, "--base-url");
    let interval_ms = find_arg_value(&args, "--interval-ms")
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(1000);
    let once = args.iter().any(|arg| arg == "--once");

    let runtime = build_runtime(backend, provider, model, base_url).await?;
    println!("medousa-daemon started");

    if once {
        let processed = process_once(&runtime, "medousa-daemon").await?;
        let published = publish_pending(&runtime, 50).await?;
        println!("processed={:?} published={}", processed, published);
        return Ok(());
    }

    loop {
        let _ = process_once(&runtime, "medousa-daemon").await?;
        let _ = publish_pending(&runtime, 50).await?;

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("medousa-daemon stopping");
                break;
            }
            _ = tokio::time::sleep(Duration::from_millis(interval_ms)) => {}
        }
    }

    Ok(())
}

fn find_arg_value<'a>(args: &'a [String], key: &str) -> Option<&'a str> {
    let idx = args.iter().position(|arg| arg == key)?;
    args.get(idx + 1).map(|s| s.as_str())
}
