#[cfg(feature = "llm-genai")]
use genai::chat::{ChatOptions, ReasoningEffort};
#[cfg(all(feature = "llm-chat", not(feature = "llm-genai")))]
use crate::ports::outbound::portable_chat::ChatOptions;

use crate::application::orchestration::prompt_pipeline::PromptExecutionContext;

const REASONING_KEYWORDS: &[&str] = &[
    "none", "minimal", "low", "medium", "high", "xhigh", "max",
];

fn validate_reasoning_effort_keyword(raw: &str) -> Result<(), String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err("reasoning_effort must be non-empty when provided".to_string());
    }

    if let Some(budget) = raw.strip_prefix("budget:") {
        budget.trim().parse::<u32>().map_err(|_| {
            format!("invalid reasoning_effort budget value in '{raw}'")
        })?;
        return Ok(());
    }

    if REASONING_KEYWORDS.contains(&raw) {
        return Ok(());
    }

    Err(format!(
        "invalid reasoning_effort '{raw}'; expected none, minimal, low, medium, high, xhigh, max, or budget:N"
    ))
}

#[cfg(feature = "llm-genai")]
pub fn parse_reasoning_effort_keyword(raw: &str) -> Result<ReasoningEffort, String> {
    validate_reasoning_effort_keyword(raw)?;
    let raw = raw.trim();

    if let Some(budget) = raw.strip_prefix("budget:") {
        let budget = budget.trim().parse::<u32>().map_err(|_| {
            format!("invalid reasoning_effort budget value in '{raw}'")
        })?;
        return Ok(ReasoningEffort::Budget(budget));
    }

    ReasoningEffort::from_keyword(raw).ok_or_else(|| {
        format!(
            "invalid reasoning_effort '{raw}'; expected none, minimal, low, medium, high, xhigh, max, or budget:N"
        )
    })
}

#[cfg(not(feature = "llm-genai"))]
pub fn parse_reasoning_effort_keyword(raw: &str) -> Result<(), String> {
    validate_reasoning_effort_keyword(raw)
}

pub fn validate_reasoning_effort(value: Option<&str>) -> Result<(), String> {
    if let Some(value) = value {
        validate_reasoning_effort_keyword(value)?;
    }
    Ok(())
}

pub fn resolve_reasoning_effort(
    branch: Option<String>,
    default: Option<String>,
) -> Option<String> {
    branch.or(default)
}

pub fn chat_options_for_context(context: &PromptExecutionContext) -> Result<Option<ChatOptions>, String> {
    let Some(raw) = context
        .reasoning_effort
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };

    #[cfg(feature = "llm-genai")]
    {
        let effort = parse_reasoning_effort_keyword(raw)?;
        return Ok(Some(ChatOptions::default().with_reasoning_effort(effort)));
    }

    #[cfg(not(feature = "llm-genai"))]
    {
        validate_reasoning_effort_keyword(raw)?;
        return Ok(Some(ChatOptions {
            reasoning_effort: Some(raw.to_string()),
        }));
    }
}

#[cfg(feature = "llm-genai")]
pub fn apply_model_reasoning_suffix(model_target: &str, options: ChatOptions) -> ChatOptions {
    if options.reasoning_effort.is_some() {
        return options;
    }

    let (Some(effort), _) = ReasoningEffort::from_model_name(model_target) else {
        return options;
    };

    options.with_reasoning_effort(effort)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_reasoning_effort_keywords() {
        validate_reasoning_effort_keyword("high").unwrap();
        validate_reasoning_effort_keyword("xhigh").unwrap();
        validate_reasoning_effort_keyword("budget:8192").unwrap();
        #[cfg(feature = "llm-genai")]
        {
            match parse_reasoning_effort_keyword("high").unwrap() {
                ReasoningEffort::High => {}
                other => panic!("expected High, got {other:?}"),
            }
        }
    }

    #[test]
    fn parse_rejects_unknown_reasoning_effort() {
        assert!(validate_reasoning_effort_keyword("fast-reasoning").is_err());
    }

    #[test]
    fn resolve_reasoning_effort_prefers_branch_override() {
        assert_eq!(
            resolve_reasoning_effort(Some("high".to_string()), Some("low".to_string())),
            Some("high".to_string())
        );
    }

    #[test]
    fn chat_options_for_context_builds_options() {
        let context = PromptExecutionContext {
            reasoning_effort: Some("medium".to_string()),
            ..Default::default()
        };
        let options = chat_options_for_context(&context)
            .expect("should parse")
            .expect("should produce options");
        #[cfg(feature = "llm-genai")]
        match options.reasoning_effort.as_ref() {
            Some(ReasoningEffort::Medium) => {}
            other => panic!("expected Medium, got {other:?}"),
        }
        #[cfg(not(feature = "llm-genai"))]
        assert_eq!(options.reasoning_effort.as_deref(), Some("medium"));
    }
}
