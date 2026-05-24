use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;

use crate::application::runtime::grapheme_job_handler::GraphemeJobHandler;
use crate::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use crate::domain::errors::Result;
use crate::domain::runtime::job::Job;
use crate::ports::outbound::runtime::workflow_engine::WorkflowEngine;

const MAX_TEXT_LEN: usize = 4096;
const DEFAULT_MAX_ITEMS: usize = 3;
const MAX_ITEMS_LIMIT: usize = 10;

#[derive(Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TextOpsMode {
    Summarize,
    ExtractKeywords,
}

#[derive(Deserialize)]
struct TextOpsPayload {
    mode: TextOpsMode,
    text: String,
    max_items: Option<usize>,
}

pub struct GraphemeTextOpsJobHandler {
    delegate: GraphemeJobHandler,
}

impl GraphemeTextOpsJobHandler {
    pub fn new(engine: Arc<dyn WorkflowEngine>) -> Self {
        Self {
            delegate: GraphemeJobHandler::new(engine),
        }
    }

    fn build_failure(message: String) -> JobExecutionOutcome {
        let diagnostics = json!({
            "provider": "grapheme-sdk",
            "status": "failure",
            "guardrail_code": "POLICY_VIOLATION",
            "policy_reason": &message,
        })
        .to_string();

        JobExecutionOutcome::FatalFailure {
            message,
            execution_id: None,
            diagnostics: Some(diagnostics),
        }
    }

    fn parse_payload(raw: &str) -> std::result::Result<TextOpsPayload, String> {
        let payload: TextOpsPayload = serde_json::from_str(raw)
            .map_err(|err| format!("policy violation: invalid textops payload json: {err}"))?;

        if payload.text.trim().is_empty() {
            return Err("policy violation: textops payload.text must be non-empty".to_string());
        }

        if payload.text.len() > MAX_TEXT_LEN {
            return Err(format!(
                "policy violation: textops payload.text exceeds max length {}",
                MAX_TEXT_LEN
            ));
        }

        let max_items = payload.max_items.unwrap_or(DEFAULT_MAX_ITEMS);
        if !(1..=MAX_ITEMS_LIMIT).contains(&max_items) {
            return Err(format!(
                "policy violation: textops payload.max_items must be between 1 and {}",
                MAX_ITEMS_LIMIT
            ));
        }

        Ok(TextOpsPayload {
            mode: payload.mode,
            text: payload.text,
            max_items: Some(max_items),
        })
    }

    fn summarize(text: &str, max_items: usize) -> String {
        let mut sentences = Vec::new();
        let mut current = String::new();

        for ch in text.chars() {
            current.push(ch);
            if matches!(ch, '.' | '!' | '?') {
                let sentence = current.trim();
                if !sentence.is_empty() {
                    sentences.push(sentence.to_string());
                }
                current.clear();
            }
        }

        if sentences.is_empty() {
            let fallback = text
                .split_whitespace()
                .take(24)
                .collect::<Vec<_>>()
                .join(" ");
            return fallback;
        }

        sentences
            .into_iter()
            .take(max_items)
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn extract_keywords(text: &str, max_items: usize) -> String {
        let stop_words = [
            "the", "and", "for", "with", "that", "this", "from", "into", "have", "are", "was",
            "were", "you", "your", "our", "not", "but", "can", "will", "all",
        ];

        let mut counts: HashMap<String, usize> = HashMap::new();
        for token in text
            .split(|c: char| !c.is_alphanumeric())
            .map(str::to_lowercase)
            .filter(|word| word.len() >= 4)
        {
            if stop_words.contains(&token.as_str()) {
                continue;
            }
            *counts.entry(token).or_insert(0) += 1;
        }

        let mut ranked = counts.into_iter().collect::<Vec<_>>();
        ranked.sort_by(|(a_word, a_count), (b_word, b_count)| {
            b_count.cmp(a_count).then_with(|| a_word.cmp(b_word))
        });

        ranked
            .into_iter()
            .take(max_items)
            .map(|(word, _)| word)
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn transform(mode: TextOpsMode, text: &str, max_items: usize) -> String {
        match mode {
            TextOpsMode::Summarize => Self::summarize(text, max_items),
            TextOpsMode::ExtractKeywords => Self::extract_keywords(text, max_items),
        }
    }

    fn build_inline_source(message: &str) -> String {
        let cleaned = message
            .replace('"', "'")
            .replace(['\n', '\r'], " ");

        format!(
            "import core from \"grapheme/core\"\n\nquery TextOps {{\n  core.echo(message: \"{}\") {{\n    state {{ current }}\n  }}\n}}\n",
            cleaned
        )
    }
}

#[async_trait]
impl JobHandler for GraphemeTextOpsJobHandler {
    fn job_type(&self) -> &'static str {
        "workflow.grapheme.textops"
    }

    async fn execute(&self, job: &Job) -> Result<JobExecutionOutcome> {
        let payload = match Self::parse_payload(&job.payload_ref) {
            Ok(payload) => payload,
            Err(message) => return Ok(Self::build_failure(message)),
        };

        let transformed = Self::transform(
            payload.mode,
            &payload.text,
            payload.max_items.unwrap_or(DEFAULT_MAX_ITEMS),
        );
        if transformed.trim().is_empty() {
            return Ok(Self::build_failure(
                "policy violation: textops transform produced empty output".to_string(),
            ));
        }

        let source = Self::build_inline_source(&transformed);
        let synthetic_job = Job {
            payload_ref: format!("grapheme:inline:{}", source),
            ..job.clone()
        };

        self.delegate.execute(&synthetic_job).await
    }
}
