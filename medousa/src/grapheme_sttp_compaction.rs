use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use stasis::prelude::{
    AiChatClient, GenaiChatClient, PromptExecutionPipeline, PromptExecutionRequest, StasisError,
};

const DEFAULT_TRIGGER_BYTES: usize = 24 * 1024;
const DEFAULT_TARGET_CHUNK_CHARS: usize = 2400;
const DEFAULT_OVERLAP_CHARS: usize = 240;
const DEFAULT_MAX_CHUNKS: usize = 24;
const DEFAULT_MAX_SUMMARY_CHARS: usize = 800;
const DEFAULT_MAX_STTP_TEXT_CHARS: usize = 10_000;
const DEFAULT_CHUNK_TIMEOUT_MS: u64 = 120_000;
const DEFAULT_COMPOSER_TIMEOUT_MS: u64 = 120_000;
const DEFAULT_TOTAL_TIMEOUT_MS: u64 = 600_000;

const SUMMARIZER_SYSTEM_PROMPT: &str = "You summarize one bounded chunk of Grapheme output. Keep only facts present in the chunk. Preserve identifiers, metrics, errors, and decisions. Mark uncertainty explicitly. Output plain text only.";

const STTP_SCHEMA_INSTRUCTIONS: &str = "STTP plain-text node schema requirements:\n- include timestamp (UTC ISO8601)\n- tier=raw\n- include concise context_summary\n- include key observed facts and their confidence markers\n- include unresolved uncertainties\n- include retrieval hints (ids/hash/tool refs)\n- output plain text only; no markdown or code fences";

const COMPOSER_SYSTEM_PROMPT: &str = "You compose one final STTP plain-text node from chunk summaries. Do not invent facts. Resolve conflicts by marking uncertainty. Output plain text only.";

#[derive(Debug, Clone)]
pub struct GraphemeCompactionModelTarget {
    pub provider: String,
    pub model: String,
    pub base_url: Option<String>,
}

#[derive(Debug, Clone)]
struct CompactionConfig {
    enabled: bool,
    trigger_bytes: usize,
    target_chunk_chars: usize,
    overlap_chars: usize,
    max_chunks: usize,
    max_summary_chars: usize,
    max_sttp_chars: usize,
    chunk_timeout_ms: u64,
    composer_timeout_ms: u64,
    total_timeout_ms: u64,
}

impl CompactionConfig {
    fn from_env() -> Self {
        Self {
            enabled: env_bool("MEDOUSA_ENABLE_GRAPHEME_STTP_COMPACTION", false),
            trigger_bytes: env_usize(
                "MEDOUSA_GRAPHEME_COMPACTION_TRIGGER_BYTES",
                DEFAULT_TRIGGER_BYTES,
            )
            .max(1024),
            target_chunk_chars: env_usize(
                "MEDOUSA_GRAPHEME_COMPACTION_TARGET_CHUNK_CHARS",
                DEFAULT_TARGET_CHUNK_CHARS,
            )
            .max(512),
            overlap_chars: env_usize(
                "MEDOUSA_GRAPHEME_COMPACTION_OVERLAP_CHARS",
                DEFAULT_OVERLAP_CHARS,
            )
            .min(1000),
            max_chunks: env_usize("MEDOUSA_GRAPHEME_COMPACTION_MAX_CHUNKS", DEFAULT_MAX_CHUNKS)
                .max(1)
                .min(128),
            max_summary_chars: env_usize(
                "MEDOUSA_GRAPHEME_COMPACTION_MAX_SUMMARY_CHARS",
                DEFAULT_MAX_SUMMARY_CHARS,
            )
            .max(200),
            max_sttp_chars: env_usize(
                "MEDOUSA_GRAPHEME_COMPACTION_MAX_STTP_CHARS",
                DEFAULT_MAX_STTP_TEXT_CHARS,
            )
            .max(1000),
            chunk_timeout_ms: env_u64(
                "MEDOUSA_GRAPHEME_COMPACTION_CHUNK_TIMEOUT_MS",
                DEFAULT_CHUNK_TIMEOUT_MS,
            )
            .max(5_000),
            composer_timeout_ms: env_u64(
                "MEDOUSA_GRAPHEME_COMPACTION_COMPOSER_TIMEOUT_MS",
                DEFAULT_COMPOSER_TIMEOUT_MS,
            )
            .max(5_000),
            total_timeout_ms: env_u64(
                "MEDOUSA_GRAPHEME_COMPACTION_TOTAL_TIMEOUT_MS",
                DEFAULT_TOTAL_TIMEOUT_MS,
            )
            .max(10_000),
        }
    }
}

pub async fn maybe_compact_output_to_sttp(
    tool_name: &str,
    session_id: &str,
    output: Value,
    model_target: &GraphemeCompactionModelTarget,
) -> stasis::prelude::Result<Value> {
    let config = CompactionConfig::from_env();
    if !config.enabled {
        return Ok(output);
    }

    let serialized = serde_json::to_string(&output).unwrap_or_else(|_| output.to_string());
    let byte_size = serialized.len();
    if byte_size <= config.trigger_bytes {
        return Ok(output);
    }

    let started = Instant::now();
    let hash_meta = crate::payload_receipt::receipt_meta(&output, 0).ok_or_else(|| {
        StasisError::PortFailure("failed to compute compaction receipt metadata".to_string())
    })?;

    let artifact_ref = match crate::artifact_store::persist_tool_artifact(
        session_id,
        tool_name,
        "output",
        &hash_meta.hash64,
        hash_meta.byte_size,
        &output,
    ) {
        Ok(record) => json!({
            "artifact_id": record.artifact_id,
            "hash64": record.hash64,
            "byte_size": record.byte_size,
            "stored_at_utc": record.stored_at_utc,
        }),
        Err(err) => json!({
            "artifact_id": Value::Null,
            "hash64": hash_meta.hash64,
            "byte_size": hash_meta.byte_size,
            "store_error": err,
        }),
    };

    let chunks = chunk_text(
        &serialized,
        config.target_chunk_chars,
        config.overlap_chars,
        config.max_chunks,
    );

    let pipeline = build_prompt_pipeline(model_target);
    let mut summaries = Vec::new();
    let mut failure_count = 0usize;

    for (idx, chunk) in chunks.iter().enumerate() {
        if started.elapsed() > Duration::from_millis(config.total_timeout_ms) {
            failure_count = failure_count.saturating_add(chunks.len().saturating_sub(idx));
            break;
        }

        let prompt = format!(
            "Chunk {}/{} from oversized Grapheme tool output. Summarize only this chunk.\n\n[CHUNK]\n{}",
            idx + 1,
            chunks.len(),
            chunk
        );

        let request = PromptExecutionRequest::from_user_prompt(prompt)
            .with_system_prompt(SUMMARIZER_SYSTEM_PROMPT.to_string());

        match tokio::time::timeout(
            Duration::from_millis(config.chunk_timeout_ms),
            pipeline.execute(request),
        )
        .await
        {
            Ok(Ok(response)) => {
                summaries.push(truncate_chars(&response.text, config.max_summary_chars));
            }
            _ => {
                failure_count = failure_count.saturating_add(1);
            }
        }
    }

    if summaries.is_empty() {
        return Ok(compaction_fallback_output(
            &artifact_ref,
            chunks.len(),
            failure_count,
            started.elapsed().as_millis() as u64,
            "chunk_summarization_failed",
        ));
    }

    let composed = compose_sttp_node(&pipeline, &summaries, &config).await;
    let (sttp_text, composer_failed) = match composed {
        Ok(text) => (truncate_chars(&text, config.max_sttp_chars), false),
        Err(_) => (
            build_fallback_sttp_from_summaries(&summaries, config.max_sttp_chars),
            true,
        ),
    };

    let elapsed_ms = started.elapsed().as_millis() as u64;

    Ok(json!({
        "status": "compacted",
        "mode": "sttp_compaction",
        "original_artifact_ref": artifact_ref,
        "chunking": {
            "chunk_count": chunks.len(),
            "target_chunk_chars": config.target_chunk_chars,
            "overlap_chars": config.overlap_chars,
        },
        "summarization": {
            "summaries_count": summaries.len(),
            "failure_count": failure_count,
            "chunk_timeout_ms": config.chunk_timeout_ms,
            "composer_timeout_ms": config.composer_timeout_ms,
            "total_timeout_ms": config.total_timeout_ms,
            "elapsed_ms": elapsed_ms,
        },
        "sttp": {
            "schema_version": "sttp-1.0",
            "text_node": sttp_text,
        },
        "notes": [
            "oversized output compacted before main-agent context handoff",
            if composer_failed { "composer_fallback_used" } else { "composer_ok" }
        ]
    }))
}

fn build_prompt_pipeline(model_target: &GraphemeCompactionModelTarget) -> PromptExecutionPipeline {
    let chat_client: Arc<dyn AiChatClient> =
        Arc::new(GenaiChatClient::from_provider_model_with_base_url(
            Some(&model_target.provider),
            &model_target.model,
            model_target.base_url.as_deref(),
        ));
    PromptExecutionPipeline::new(chat_client)
}

async fn compose_sttp_node(
    pipeline: &PromptExecutionPipeline,
    summaries: &[String],
    config: &CompactionConfig,
) -> stasis::prelude::Result<String> {
    let summaries_body = summaries
        .iter()
        .enumerate()
        .map(|(idx, value)| format!("[{}] {}", idx + 1, value))
        .collect::<Vec<_>>()
        .join("\n\n");

    let prompt = format!(
        "Compose one STTP plain-text node from chunk summaries.\n\n[SCHEMA]\n{}\n\n[SUMMARIES]\n{}\n\nOutput plain text STTP only.",
        STTP_SCHEMA_INSTRUCTIONS, summaries_body
    );

    let request = PromptExecutionRequest::from_user_prompt(prompt)
        .with_system_prompt(COMPOSER_SYSTEM_PROMPT.to_string());

    let response = tokio::time::timeout(
        Duration::from_millis(config.composer_timeout_ms),
        pipeline.execute(request),
    )
    .await
    .map_err(|_| StasisError::PortFailure("sttp composer timed out".to_string()))??;

    Ok(response.text)
}

fn compaction_fallback_output(
    artifact_ref: &Value,
    chunk_count: usize,
    failure_count: usize,
    elapsed_ms: u64,
    reason: &str,
) -> Value {
    json!({
        "status": "compacted_fallback",
        "mode": "sttp_compaction",
        "reason": reason,
        "original_artifact_ref": artifact_ref,
        "chunking": {
            "chunk_count": chunk_count,
        },
        "summarization": {
            "summaries_count": 0,
            "failure_count": failure_count,
            "elapsed_ms": elapsed_ms,
        },
        "sttp": {
            "schema_version": "sttp-1.0",
            "text_node": "STTP node unavailable: summarization failed; consult original artifact_ref.",
        }
    })
}

fn build_fallback_sttp_from_summaries(summaries: &[String], max_chars: usize) -> String {
    let joined = summaries
        .iter()
        .enumerate()
        .map(|(idx, s)| format!("fact_{}: {}", idx + 1, s))
        .collect::<Vec<_>>()
        .join("\n");

    truncate_chars(
        &format!(
            "timestamp: {}\ntier: raw\ncontext_summary: Composed from chunk summaries with fallback composer.\nobservations:\n{}\nuncertainty: Composer fallback path used; verify against artifact_ref.",
            chrono::Utc::now().to_rfc3339(),
            joined
        ),
        max_chars,
    )
}

fn chunk_text(text: &str, target: usize, overlap: usize, max_chunks: usize) -> Vec<String> {
    let target = target.max(256);
    let overlap = overlap.min(target / 2);
    let chars: Vec<char> = text.chars().collect();
    if chars.is_empty() {
        return Vec::new();
    }

    let mut chunks = Vec::new();
    let mut start = 0usize;
    while start < chars.len() && chunks.len() < max_chunks {
        let end = (start + target).min(chars.len());
        chunks.push(chars[start..end].iter().collect::<String>());
        if end == chars.len() {
            break;
        }
        start = end.saturating_sub(overlap);
    }

    chunks
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let mut out: String = text.chars().take(max_chars).collect();
    out.push_str("...");
    out
}

fn env_bool(key: &str, default: bool) -> bool {
    match std::env::var(key) {
        Ok(raw) => matches!(
            raw.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => default,
    }
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::{chunk_text, truncate_chars};

    #[test]
    fn chunk_text_respects_max_chunks() {
        let text = "x".repeat(20_000);
        let chunks = chunk_text(&text, 1000, 100, 5);
        assert_eq!(chunks.len(), 5);
    }

    #[test]
    fn truncate_adds_ellipsis() {
        let out = truncate_chars("abcdef", 4);
        assert_eq!(out, "abcd...");
    }
}
