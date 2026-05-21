use serde_json::Value;
use tokio::sync::mpsc;

use medousa::{TuiRuntime, events::TuiEvent};
use stasis::application::orchestration::tool_loop_pipeline::{
    ToolCallMode, ToolInvocation, ToolLoopExecutionRequest,
};
use stasis::ports::outbound::ai_chat_client::StreamDelta;
use stasis::prelude::{ChatMessage, PromptExecutionContext};

use super::{ConversationTurn, TuiState};

const MAX_REQUEST_PROMPT_CHARS: usize = 48_000;
const MAX_PRIOR_TOTAL_CHARS: usize = 24_000;
const MAX_SINGLE_PRIOR_MESSAGE_CHARS: usize = 4_000;
const CONTINUATION_TRIGGER_TOOL_OUTPUT_CHARS: usize = 8_000;
const CONTINUATION_TRIGGER_STDOUT_CHARS: usize = 4_000;
const CONTINUATION_MAX_DRAFT_CHARS: usize = 6_000;
const CONTINUATION_MAX_TOOL_OUTPUT_CHARS: usize = 2_000;
const CONTINUATION_MAX_TOOL_SUMMARIES: usize = 6;
const CONTINUATION_MAX_ROUNDS: usize = 4;

#[derive(Debug, Clone)]
struct ContextPackQuality {
    citation_coverage: f32,
    avg_support_strength: f32,
    supported_claim_ratio: f32,
    confidence_score: f32,
    is_usable: bool,
}

pub(crate) fn start_prompt_run(
    state: &mut TuiState,
    tui_rt: &TuiRuntime,
    event_tx: &mpsc::Sender<TuiEvent>,
    prompt: String,
    persist_user_turn: bool,
) {
    state.is_processing = true;
    state.auto_scroll = true;
    state.conv_scroll = state.conv_max_scroll;
    state.active_agent_stream_turn = None;
    state.in_thinking_tag = false;
    state.stream_tag_tail.clear();
    state.received_native_reasoning = false;

    if persist_user_turn {
        let user_turn = ConversationTurn {
            role: "user".to_string(),
            content: prompt.clone(),
            timestamp: chrono::Utc::now(),
            tool_names: vec![],
            answer_state: None,
        };
        super::append_turn(&state.session_id, &user_turn);
        state.conversation.push(user_turn);
    }

    let final_route = state.stage_routing.get("final_response").cloned();
    let verifier_route = state.stage_routing.get("verifier").cloned();

    if let Some(route) = &final_route {
        super::push_obs(
            state,
            format!(
                "◈ stage route final_response target={}:{} policy={} fallback={}",
                route.provider,
                route.model,
                route.policy_profile,
                route.fallback_chain.join(","),
            ),
        );
    }
    if let Some(route) = &verifier_route {
        super::push_obs(
            state,
            format!(
                "◈ stage route verifier target={}:{} policy={} fallback={}",
                route.provider,
                route.model,
                route.policy_profile,
                route.fallback_chain.join(","),
            ),
        );
    }

    let verifier_policy =
        verifier_policy_from_settings_and_route(&state.settings, verifier_route.as_ref());
    let (mut resolved_prompt, pack_note, verification_state) = resolve_prompt_with_context_pack(
        &state.session_id,
        &prompt,
        state.selected_context_pack_query.as_deref(),
        &verifier_policy,
    );
    state.pending_response_verified = Some(verification_state.unwrap_or(false));

    resolved_prompt = format!(
        "{resolved_prompt}\n\n[MEDOUSA_RESPONSE_DEPTH]\nmode={}\npolicy=Use concise mode for short output, standard for balanced output, deep for detailed evidence-forward explanation.",
        state.response_depth_mode
    );
    if let Some(route) = &final_route {
        resolved_prompt = format!(
            "{resolved_prompt}\n\n[MEDOUSA_STAGE_ROUTE]\nrole={}\nprovider={}\nmodel={}\npolicy_profile={}\nfallback_chain={}",
            route.role,
            route.provider,
            route.model,
            route.policy_profile,
            route.fallback_chain.join(","),
        );
    }

    if let Some(note) = pack_note {
        super::push_obs(state, note);
    }

    let prompt_len_before_budget = resolved_prompt.chars().count();
    resolved_prompt = truncate_text_for_budget(&resolved_prompt, MAX_REQUEST_PROMPT_CHARS);
    let prompt_len_after_budget = resolved_prompt.chars().count();
    if prompt_len_after_budget < prompt_len_before_budget {
        super::push_obs(
            state,
            format!(
                "◈ prompt budget applied chars={} -> {}",
                prompt_len_before_budget, prompt_len_after_budget
            ),
        );
    }

    let pipeline = if let Some(route) = &final_route {
        let route_base_url = route_base_url(route, &state.settings);
        super::push_obs(
            state,
            format!(
                "◈ stage route dispatch final_response target={}:{} base_url={}",
                route.provider,
                route.model,
                route_base_url
                    .as_deref()
                    .filter(|value| !value.is_empty())
                    .unwrap_or("(auto)"),
            ),
        );
        tui_rt.tool_loop_pipeline_for_target(
            &route.provider,
            &route.model,
            route_base_url.as_deref(),
        )
    } else {
        tui_rt.tool_loop_pipeline.clone()
    };
    let tx = event_tx.clone();
    let prompt_preview: String = resolved_prompt.chars().take(48).collect();
    let tool_call_mode = parse_tool_call_mode(&state.settings.tool_call_mode);
    let max_tool_rounds =
        super::parse_usize_with_bounds(&state.settings.max_tool_rounds, 10, 1, 50);
    let prior_messages = build_prior_messages(&state.conversation, &prompt, persist_user_turn);
    let prompt_for_request = resolved_prompt;
    let original_prompt_for_continuation = prompt.clone();
    let handle = tokio::spawn(async move {
        let (chunk_tx, mut chunk_rx) = tokio::sync::mpsc::unbounded_channel::<StreamDelta>();
        let chunk_event_tx = tx.clone();
        tokio::spawn(async move {
            while let Some(delta) = chunk_rx.recv().await {
                let event = match delta {
                    StreamDelta::Content(delta) => TuiEvent::AgentChunk { delta },
                    StreamDelta::Reasoning(delta) => TuiEvent::AgentReasoningChunk { delta },
                    StreamDelta::ThoughtSignature(delta) => TuiEvent::AgentReasoningChunk { delta },
                };
                if chunk_event_tx.send(event).await.is_err() {
                    break;
                }
            }
        });

        let _ = tx
            .send(TuiEvent::ToolInvoked {
                tool_name: "llm.chat".to_string(),
                input_summary: prompt_preview,
            })
            .await;

        let request = ToolLoopExecutionRequest {
            user_prompt: prompt_for_request,
            system_prompt: Some(super::SYSTEM_PROMPT.to_string()),
            context: PromptExecutionContext::default(),
            tool_name: String::new(),
            tool_input: Value::Null,
            tool_call_mode,
        };
        match pipeline
            .execute_with_stream_prior_messages_max_rounds(
                request,
                prior_messages,
                Some(&chunk_tx),
                max_tool_rounds,
            )
            .await
        {
            Ok(response) => {
                emit_tool_payload_events(&tx, &response.tool_invocations).await;

                let mut combined_invocations = response.tool_invocations.clone();
                let mut final_text = response.text;
                if should_run_continuation(&combined_invocations) {
                    if let Some(continuation_prompt) = build_continuation_prompt(
                        &original_prompt_for_continuation,
                        &final_text,
                        &combined_invocations,
                    ) {
                        let _ = tx
                            .send(TuiEvent::UiNotice(
                                "◈ continuation synthesis: refining draft with chunked tool context".to_string(),
                            ))
                            .await;

                        let _ = tx
                            .send(TuiEvent::ToolInvoked {
                                tool_name: "llm.chat".to_string(),
                                input_summary: "continuation synthesis".to_string(),
                            })
                            .await;

                        let continuation_request = ToolLoopExecutionRequest {
                            user_prompt: continuation_prompt,
                            system_prompt: Some(super::SYSTEM_PROMPT.to_string()),
                            context: PromptExecutionContext::default(),
                            tool_name: String::new(),
                            tool_input: Value::Null,
                            tool_call_mode: ToolCallMode::Auto,
                        };
                        let continuation_prior_messages = build_continuation_prior_messages(
                            &original_prompt_for_continuation,
                            &final_text,
                        );

                        match pipeline
                            .execute_with_stream_prior_messages_max_rounds(
                                continuation_request,
                                continuation_prior_messages,
                                Some(&chunk_tx),
                                max_tool_rounds.min(CONTINUATION_MAX_ROUNDS).max(1),
                            )
                            .await
                        {
                            Ok(continuation_response) => {
                                emit_tool_payload_events(
                                    &tx,
                                    &continuation_response.tool_invocations,
                                )
                                .await;
                                final_text = continuation_response.text;
                                combined_invocations.extend(continuation_response.tool_invocations);
                            }
                            Err(err) => {
                                let _ = tx
                                    .send(TuiEvent::UiNotice(format!(
                                        "⚠ continuation synthesis skipped: {err}"
                                    )))
                                    .await;
                            }
                        }
                    }
                }

                let tool_names = collect_tool_names(&combined_invocations);
                let _ = tx
                    .send(TuiEvent::ToolInvoked {
                        tool_name: "llm.chat".to_string(),
                        input_summary: format!(
                            "done  {} token(s)",
                            final_text.split_whitespace().count()
                        ),
                    })
                    .await;
                let _ = tx
                    .send(TuiEvent::AgentResponse {
                        text: final_text,
                        tool_names,
                    })
                    .await;
            }
            Err(err) => {
                let _ = tx.send(TuiEvent::AgentError(err.to_string())).await;
            }
        }
    });

    state.active_request_task = Some(handle);
}

fn build_prior_messages(
    turns: &[ConversationTurn],
    current_prompt: &str,
    current_user_persisted: bool,
) -> Vec<ChatMessage> {
    const MAX_TURNS: usize = 16;

    let mut selected: Vec<&ConversationTurn> = turns.iter().collect();

    if current_user_persisted {
        if let Some(last) = selected.last() {
            if last.role == "user" && last.content.trim() == current_prompt.trim() {
                selected.pop();
            }
        }
    }

    let mut remaining_chars = MAX_PRIOR_TOTAL_CHARS;
    let mut accepted: Vec<ChatMessage> = Vec::new();

    for turn in selected.into_iter().rev().take(MAX_TURNS) {
        if remaining_chars == 0 {
            break;
        }

        let bounded = truncate_text_for_budget(&turn.content, MAX_SINGLE_PRIOR_MESSAGE_CHARS);
        let bounded = truncate_text_for_budget(&bounded, remaining_chars);
        if bounded.trim().is_empty() {
            continue;
        }

        remaining_chars = remaining_chars.saturating_sub(bounded.chars().count());
        match turn.role.as_str() {
            "user" => accepted.push(ChatMessage::user(bounded)),
            "assistant" => accepted.push(ChatMessage::assistant(bounded)),
            _ => {}
        }
    }

    accepted.reverse();
    accepted
}

async fn emit_tool_payload_events(tx: &mpsc::Sender<TuiEvent>, invocations: &[ToolInvocation]) {
    for invocation in invocations {
        let safe_input = medousa::settings_guard::redact_json_value(&invocation.tool_input);
        let safe_output = medousa::settings_guard::redact_json_value(&invocation.tool_output);
        let _ = tx
            .send(TuiEvent::ToolPayload {
                tool_name: invocation.tool_name.clone(),
                tool_input: invocation.tool_input.clone(),
                tool_output: invocation.tool_output.clone(),
                input_receipt: medousa::payload_receipt::receipt_meta(
                    &safe_input,
                    medousa::payload_receipt::DEFAULT_MAX_INLINE_BYTES,
                ),
                output_receipt: medousa::payload_receipt::receipt_meta(
                    &safe_output,
                    medousa::payload_receipt::DEFAULT_MAX_INLINE_BYTES,
                ),
            })
            .await;
    }
}

fn should_run_continuation(invocations: &[ToolInvocation]) -> bool {
    for invocation in invocations {
        let output_chars = invocation.tool_output.to_string().chars().count();
        if output_chars >= CONTINUATION_TRIGGER_TOOL_OUTPUT_CHARS {
            return true;
        }

        let stdout_chars = invocation
            .tool_output
            .get("stdout")
            .and_then(|value| value.as_str())
            .map(|value| value.chars().count())
            .unwrap_or(0);
        if stdout_chars >= CONTINUATION_TRIGGER_STDOUT_CHARS {
            return true;
        }

        if invocation
            .tool_name
            .to_ascii_lowercase()
            .contains("grapheme")
            && output_chars >= 2000
        {
            return true;
        }
    }
    false
}

fn build_continuation_prompt(
    original_prompt: &str,
    draft_text: &str,
    invocations: &[ToolInvocation],
) -> Option<String> {
    if invocations.is_empty() {
        return None;
    }

    let summaries = invocations
        .iter()
        .take(CONTINUATION_MAX_TOOL_SUMMARIES)
        .map(|invocation| {
            let safe_output = medousa::settings_guard::redact_json_value(&invocation.tool_output);
            let rendered_output = truncate_text_for_budget(
                &safe_output.to_string(),
                CONTINUATION_MAX_TOOL_OUTPUT_CHARS,
            );
            format!(
                "- tool={} output={} ",
                invocation.tool_name, rendered_output
            )
        })
        .collect::<Vec<_>>();

    if summaries.is_empty() {
        return None;
    }

    let draft = truncate_text_for_budget(draft_text, CONTINUATION_MAX_DRAFT_CHARS);
    let user_request = truncate_text_for_budget(original_prompt, 3000);
    let prompt = format!(
        "You have an initial draft answer plus additional tool context that may have arrived in chunks. Rewrite one coherent final answer that integrates the tool evidence. Preserve substantiated details, remove contradictions, and mark uncertainty explicitly. Prefer concise structure with clear takeaways.\n\n[USER_REQUEST]\n{user_request}\n\n[DRAFT_ANSWER]\n{draft}\n\n[ADDITIONAL_TOOL_CONTEXT]\n{}\n\nReturn only the final answer body.",
        summaries.join("\n")
    );

    Some(truncate_text_for_budget(&prompt, MAX_REQUEST_PROMPT_CHARS))
}

fn build_continuation_prior_messages(original_prompt: &str, draft_text: &str) -> Vec<ChatMessage> {
    vec![
        ChatMessage::user(truncate_text_for_budget(original_prompt, 2000)),
        ChatMessage::assistant(truncate_text_for_budget(draft_text, 4000)),
    ]
}

fn collect_tool_names(invocations: &[ToolInvocation]) -> Vec<String> {
    let mut names = Vec::new();
    for invocation in invocations {
        if !names
            .iter()
            .any(|existing| existing == &invocation.tool_name)
        {
            names.push(invocation.tool_name.clone());
        }
    }
    names
}

pub(crate) fn stop_active_generation(state: &mut TuiState) {
    if let Some(task) = state.active_request_task.take() {
        task.abort();
        state.is_processing = false;
        state.active_agent_stream_turn = None;
        state.pending_response_verified = None;
        super::flush_thinking_buffer(state);
        super::push_obs(state, "■ generation stopped".to_string());
    }
}

fn parse_tool_call_mode(value: &str) -> ToolCallMode {
    if value.trim().eq_ignore_ascii_case("strict") {
        ToolCallMode::Strict
    } else {
        ToolCallMode::Auto
    }
}

fn resolve_prompt_with_context_pack(
    session_id: &str,
    prompt: &str,
    pack_query: Option<&str>,
    policy: &medousa::verifier::VerificationPolicy,
) -> (String, Option<String>, Option<bool>) {
    let selector = pack_query.unwrap_or("last");
    let Some(pack) = medousa::context_pack::find_context_pack(session_id, Some(selector)) else {
        return (prompt.to_string(), None, None);
    };

    let (prompt_with_pack, quality, report) = build_prompt_with_context_pack(prompt, &pack, policy);
    let verification_id = medousa::verification_store::persist_verification(
        session_id,
        selector,
        "prompt_injection",
        policy,
        &report,
    )
    .ok()
    .map(|record| record.verification_id);

    let verification_suffix = verification_id
        .map(|id| format!(" verification={id}"))
        .unwrap_or_default();
    let note = if quality.is_usable {
        format!(
            "◈ context pack verified {} selector={} artifact={} claims={} chunks={} coverage={:.2} avg_support={:.2} support_ratio={:.2} confidence={:.2}{}",
            pack.pack_id,
            selector,
            pack.artifact_id,
            pack.selected_claims.len(),
            pack.selected_chunk_refs.len(),
            quality.citation_coverage,
            quality.avg_support_strength,
            quality.supported_claim_ratio,
            quality.confidence_score,
            verification_suffix,
        )
    } else {
        format!(
            "◈ context pack verification failed {} selector={} artifact={} coverage={:.2} avg_support={:.2} support_ratio={:.2} confidence={:.2}{}",
            pack.pack_id,
            selector,
            pack.artifact_id,
            quality.citation_coverage,
            quality.avg_support_strength,
            quality.supported_claim_ratio,
            quality.confidence_score,
            verification_suffix,
        )
    };

    (prompt_with_pack, Some(note), Some(quality.is_usable))
}

fn build_prompt_with_context_pack(
    prompt: &str,
    pack: &medousa::context_pack::ContextPack,
    policy: &medousa::verifier::VerificationPolicy,
) -> (
    String,
    ContextPackQuality,
    medousa::verifier::VerificationReport,
) {
    let report = medousa::verifier::verify_context_pack(pack, policy);
    let quality = ContextPackQuality {
        citation_coverage: report.citation_coverage,
        avg_support_strength: report.avg_support_strength,
        supported_claim_ratio: report.supported_claim_ratio,
        confidence_score: report.confidence_score,
        is_usable: report.is_verified,
    };

    if !quality.is_usable {
        let fallback = format!(
            "{prompt}\n\n[MEDOUSA_CONTEXT_PACK]\nstatus=verification_failed\npack_id={}\nartifact_id={}\ncitation_coverage={:.2}\navg_support={:.2}\nsupported_claim_ratio={:.2}\nconfidence={:.2}\npolicy=Treat context pack claims as non-authoritative. If evidence is needed, call tools or request fresher data.",
            pack.pack_id,
            pack.artifact_id,
            quality.citation_coverage,
            quality.avg_support_strength,
            quality.supported_claim_ratio,
            quality.confidence_score,
        );
        return (fallback, quality, report);
    }

    let claim_lines = pack
        .selected_claims
        .iter()
        .take(8)
        .map(|claim| {
            let refs = if claim.supporting_chunk_node_ids.is_empty() {
                "none".to_string()
            } else {
                claim
                    .supporting_chunk_node_ids
                    .iter()
                    .take(3)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(",")
            };
            let statement = truncate_text_for_budget(&claim.statement, 360);
            format!(
                "- [{}] strength={:.2} refs={} {}",
                claim.claim_id, claim.support_strength, refs, statement
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    let chunk_lines = pack
        .selected_chunk_refs
        .iter()
        .take(8)
        .map(|chunk| {
            format!(
                "- {} tokens={} hash={}",
                chunk.node_id, chunk.token_estimate, chunk.hash64
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    let augmented = format!(
        "{prompt}\n\n[MEDOUSA_CONTEXT_PACK]\nstatus=verified\npack_id={}\nartifact_id={}\ntoken_estimate={}\ncitation_coverage={:.2}\navg_support={:.2}\nsupported_claim_ratio={:.2}\nconfidence={:.2}\nclaims:\n{}\nchunks:\n{}",
        pack.pack_id,
        pack.artifact_id,
        pack.total_token_estimate,
        quality.citation_coverage,
        quality.avg_support_strength,
        quality.supported_claim_ratio,
        quality.confidence_score,
        claim_lines,
        chunk_lines,
    );

    (augmented, quality, report)
}

fn truncate_text_for_budget(text: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }

    let total_chars = text.chars().count();
    if total_chars <= max_chars {
        return text.to_string();
    }

    if max_chars <= 12 {
        return text.chars().take(max_chars).collect();
    }

    let head = max_chars / 2;
    let tail = max_chars.saturating_sub(head + 5);
    let head_part = text.chars().take(head).collect::<String>();
    let tail_part = text
        .chars()
        .skip(total_chars.saturating_sub(tail))
        .collect::<String>();
    format!("{head_part}\n...\n{tail_part}")
}

pub(crate) fn verifier_policy_from_settings_and_route(
    settings: &super::RuntimeSettings,
    verifier_route: Option<&medousa::stage_routing::StageRoute>,
) -> medousa::verifier::VerificationPolicy {
    let mut policy = medousa::verifier::VerificationPolicy {
        min_citation_coverage: super::parse_f32_with_bounds(
            &settings.verifier_min_citation_coverage,
            0.60,
            0.0,
            1.0,
        ),
        min_avg_support_strength: super::parse_f32_with_bounds(
            &settings.verifier_min_avg_support_strength,
            0.70,
            0.0,
            1.0,
        ),
        min_supported_claim_ratio: super::parse_f32_with_bounds(
            &settings.verifier_min_supported_claim_ratio,
            0.60,
            0.0,
            1.0,
        ),
        min_claim_support_strength: super::parse_f32_with_bounds(
            &settings.verifier_min_claim_support_strength,
            0.65,
            0.0,
            1.0,
        ),
    };

    if let Some(route) = verifier_route {
        apply_verifier_policy_profile(&mut policy, &route.policy_profile);
    }

    policy
}

fn apply_verifier_policy_profile(
    policy: &mut medousa::verifier::VerificationPolicy,
    policy_profile: &str,
) {
    match policy_profile.trim().to_ascii_lowercase().as_str() {
        "strict" => {
            policy.min_citation_coverage = policy.min_citation_coverage.max(0.70);
            policy.min_avg_support_strength = policy.min_avg_support_strength.max(0.75);
            policy.min_supported_claim_ratio = policy.min_supported_claim_ratio.max(0.70);
            policy.min_claim_support_strength = policy.min_claim_support_strength.max(0.72);
        }
        "analytical" => {
            policy.min_citation_coverage = policy.min_citation_coverage.max(0.65);
            policy.min_avg_support_strength = policy.min_avg_support_strength.max(0.78);
            policy.min_supported_claim_ratio = policy.min_supported_claim_ratio.max(0.62);
            policy.min_claim_support_strength = policy.min_claim_support_strength.max(0.76);
        }
        "fast" => {
            policy.min_citation_coverage = policy.min_citation_coverage.min(0.50);
            policy.min_avg_support_strength = policy.min_avg_support_strength.min(0.55);
            policy.min_supported_claim_ratio = policy.min_supported_claim_ratio.min(0.50);
            policy.min_claim_support_strength = policy.min_claim_support_strength.min(0.52);
        }
        _ => {}
    }
}

fn route_base_url(
    route: &medousa::stage_routing::StageRoute,
    settings: &super::RuntimeSettings,
) -> Option<String> {
    if route
        .provider
        .eq_ignore_ascii_case(settings.provider.trim())
    {
        let candidate = settings.base_url.trim();
        if !candidate.is_empty() {
            return Some(candidate.to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{
        build_prompt_with_context_pack, should_run_continuation,
        verifier_policy_from_settings_and_route,
    };
    use chrono::Utc;
    use medousa::artifact_chunking::SttpChunkNodeRef;
    use medousa::artifact_extraction::EvidenceClaim;
    use medousa::context_pack::{ContextPack, ContextPackBudgetProfile};

    fn sample_pack() -> ContextPack {
        ContextPack {
            pack_id: "pack:test:1".to_string(),
            session_id: "session-1".to_string(),
            artifact_id: "artifact-1".to_string(),
            created_at_utc: Utc::now(),
            budget_profile: ContextPackBudgetProfile {
                max_tokens: 3200,
                max_claims: 6,
                max_chunks: 12,
            },
            selected_claims: vec![EvidenceClaim {
                claim_id: "claim-1".to_string(),
                statement: "The payload contains two result entries.".to_string(),
                supporting_chunk_node_ids: vec!["sttp:artifact-1:chunk:0".to_string()],
                support_strength: 0.88,
            }],
            selected_chunk_refs: vec![SttpChunkNodeRef {
                node_id: "sttp:artifact-1:chunk:0".to_string(),
                chunk_id: "artifact-1:chunk:0".to_string(),
                sequence: 0,
                token_estimate: 120,
                hash64: "abc123".to_string(),
            }],
            total_token_estimate: 120,
        }
    }

    #[test]
    fn prompt_includes_pack_when_quality_is_usable() {
        let pack = sample_pack();
        let policy = medousa::verifier::VerificationPolicy::default();
        let (prompt, quality, _) =
            build_prompt_with_context_pack("Summarize latest run", &pack, &policy);
        assert!(quality.is_usable);
        assert!(prompt.contains("[MEDOUSA_CONTEXT_PACK]"));
        assert!(prompt.contains("status=verified"));
        assert!(prompt.contains("claims:"));
    }

    #[test]
    fn quality_rejects_low_coverage_pack() {
        let mut pack = sample_pack();
        pack.selected_claims[0].supporting_chunk_node_ids.clear();
        pack.selected_claims[0].support_strength = 0.40;

        let policy = medousa::verifier::VerificationPolicy::default();
        let (prompt, quality, _) =
            build_prompt_with_context_pack("Summarize latest run", &pack, &policy);
        assert!(!quality.is_usable);
        assert!(prompt.contains("status=verification_failed"));
    }

    #[test]
    fn derives_policy_from_settings_values() {
        let settings = super::super::RuntimeSettings {
            backend: "in-memory".to_string(),
            provider: "openai".to_string(),
            model: "gpt-4o-mini".to_string(),
            base_url: String::new(),
            env_overrides: String::new(),
            api_key: String::new(),
            allowed_modules: String::new(),
            tool_call_mode: "auto".to_string(),
            max_tool_rounds: "10".to_string(),
            thinking_capture: "true".to_string(),
            thinking_max_lines: "300".to_string(),
            verifier_min_citation_coverage: "0.55".to_string(),
            verifier_min_avg_support_strength: "0.66".to_string(),
            verifier_min_supported_claim_ratio: "0.77".to_string(),
            verifier_min_claim_support_strength: "0.88".to_string(),
        };

        let policy = verifier_policy_from_settings_and_route(&settings, None);
        assert!((policy.min_citation_coverage - 0.55).abs() < 0.001);
        assert!((policy.min_avg_support_strength - 0.66).abs() < 0.001);
        assert!((policy.min_supported_claim_ratio - 0.77).abs() < 0.001);
        assert!((policy.min_claim_support_strength - 0.88).abs() < 0.001);
    }

    #[test]
    fn strict_route_profile_tightens_verifier_policy() {
        let settings = super::super::RuntimeSettings {
            backend: "in-memory".to_string(),
            provider: "openai".to_string(),
            model: "gpt-4o-mini".to_string(),
            base_url: String::new(),
            env_overrides: String::new(),
            api_key: String::new(),
            allowed_modules: String::new(),
            tool_call_mode: "auto".to_string(),
            max_tool_rounds: "10".to_string(),
            thinking_capture: "true".to_string(),
            thinking_max_lines: "300".to_string(),
            verifier_min_citation_coverage: "0.55".to_string(),
            verifier_min_avg_support_strength: "0.66".to_string(),
            verifier_min_supported_claim_ratio: "0.57".to_string(),
            verifier_min_claim_support_strength: "0.61".to_string(),
        };
        let route = medousa::stage_routing::StageRoute {
            role: "verifier".to_string(),
            provider: "openai".to_string(),
            model: "gpt-4o-mini".to_string(),
            policy_profile: "strict".to_string(),
            fallback_chain: vec!["verifier".to_string()],
        };

        let policy = verifier_policy_from_settings_and_route(&settings, Some(&route));
        assert!((policy.min_citation_coverage - 0.70).abs() < 0.001);
        assert!((policy.min_avg_support_strength - 0.75).abs() < 0.001);
        assert!((policy.min_supported_claim_ratio - 0.70).abs() < 0.001);
        assert!((policy.min_claim_support_strength - 0.72).abs() < 0.001);
    }

    #[test]
    fn continuation_trigger_detects_large_stdout_payload() {
        let invocations = vec![
            stasis::application::orchestration::tool_loop_pipeline::ToolInvocation {
                tool_name: "cognition.grapheme.run".to_string(),
                tool_input: serde_json::json!({"script": "noop"}),
                tool_output: serde_json::json!({
                    "stdout": "x".repeat(4500)
                }),
            },
        ];

        assert!(should_run_continuation(&invocations));
    }
}
