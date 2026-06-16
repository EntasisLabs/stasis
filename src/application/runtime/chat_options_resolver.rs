use genai::chat::{ChatOptions, ReasoningEffort};

use crate::application::orchestration::prompt_pipeline::PromptExecutionContext;

pub fn parse_reasoning_effort_keyword(raw: &str) -> Result<ReasoningEffort, String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err("reasoning_effort must be non-empty when provided".to_string());
    }

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

pub fn validate_reasoning_effort(value: Option<&str>) -> Result<(), String> {
    if let Some(value) = value {
        parse_reasoning_effort_keyword(value)?;
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

    let effort = parse_reasoning_effort_keyword(raw)?;
    Ok(Some(ChatOptions::default().with_reasoning_effort(effort)))
}

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
        match parse_reasoning_effort_keyword("high").unwrap() {
            ReasoningEffort::High => {}
            other => panic!("expected High, got {other:?}"),
        }
        match parse_reasoning_effort_keyword("xhigh").unwrap() {
            ReasoningEffort::XHigh => {}
            other => panic!("expected XHigh, got {other:?}"),
        }
        match parse_reasoning_effort_keyword("budget:8192").unwrap() {
            ReasoningEffort::Budget(8192) => {}
            other => panic!("expected Budget(8192), got {other:?}"),
        }
    }

    #[test]
    fn parse_rejects_unknown_reasoning_effort() {
        assert!(parse_reasoning_effort_keyword("fast-reasoning").is_err());
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
        match options.reasoning_effort.as_ref() {
            Some(ReasoningEffort::Medium) => {}
            other => panic!("expected Medium, got {other:?}"),
        }
    }
}
