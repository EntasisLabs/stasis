use serde_json::{Value as JsonValue, json};

use crate::ports::outbound::memory::memory_models::{
    MemoryNode, MemoryRecallResponse, MemoryStoreResponse,
};

pub fn memory_nodes_json(nodes: &[MemoryNode]) -> JsonValue {
    JsonValue::Array(
        nodes
            .iter()
            .map(|node| {
                json!({
                    "sync_key": node.sync_key,
                    "session_id": node.session_id,
                    "tier": node.tier,
                    "raw": node.raw,
                    "context_summary": node.context_summary,
                    "semantic_tags": node.semantic_tags,
                    "semantic_links": node.semantic_links,
                    "psi": node.psi,
                    "rho": node.rho,
                    "kappa": node.kappa,
                })
            })
            .collect(),
    )
}

pub struct RuntimeMemoryDiagnosticsBundle {
    pub retrieved_count: usize,
    pub retrieval_path: Option<String>,
    pub fallback_triggered: bool,
    pub fallback_reason: Option<String>,
    pub store_valid: bool,
    pub store_node_id: Option<String>,
    pub memory_recall: JsonValue,
    pub memory_store: JsonValue,
    pub identity_context: JsonValue,
}

pub struct RuntimeMemoryRecallDiagnosticsInput {
    pub attempted: bool,
    pub response: Option<MemoryRecallResponse>,
    pub query_id: Option<String>,
    pub query_fingerprint: Option<String>,
    pub error: Option<String>,
}

pub struct RuntimeMemoryStoreDiagnosticsInput {
    pub attempted: bool,
    pub response: Option<MemoryStoreResponse>,
    pub error: Option<String>,
}

pub struct RuntimeIdentityDiagnosticsInput {
    pub attempted: bool,
    pub summary: Option<String>,
    pub error: Option<String>,
}

pub fn build_runtime_memory_diagnostics_bundle(
    memory_recall: RuntimeMemoryRecallDiagnosticsInput,
    memory_store: RuntimeMemoryStoreDiagnosticsInput,
    identity: RuntimeIdentityDiagnosticsInput,
) -> RuntimeMemoryDiagnosticsBundle {
    let retrieved_count = memory_recall
        .response
        .as_ref()
        .map(|value| value.retrieved)
        .unwrap_or_default();
    let retrieval_path = memory_recall
        .response
        .as_ref()
        .and_then(|value| value.retrieval_path.clone());
    let fallback_triggered = memory_recall
        .response
        .as_ref()
        .map(|value| value.fallback_triggered)
        .unwrap_or(false);
    let fallback_reason = memory_recall
        .response
        .as_ref()
        .and_then(|value| value.fallback_reason.clone());

    let store_valid = memory_store
        .response
        .as_ref()
        .map(|value| value.valid)
        .unwrap_or(false);
    let store_node_id = memory_store.response.as_ref().map(|value| value.node_id.clone());

    let memory_recall_section = json!({
        "attempted": memory_recall.attempted,
        "query_id": memory_recall.query_id,
        "query_fingerprint": memory_recall.query_fingerprint,
        "retrieved": retrieved_count,
        "retrieval_path": retrieval_path,
        "fallback_triggered": fallback_triggered,
        "fallback_reason": fallback_reason,
        "error": memory_recall.error,
    });

    let memory_store_section = json!({
        "attempted": memory_store.attempted,
        "node_id": store_node_id,
        "valid": store_valid,
        "error": memory_store.error,
    });

    let identity_context_section = json!({
        "attempted": identity.attempted,
        "summary": identity.summary,
        "error": identity.error,
    });

    RuntimeMemoryDiagnosticsBundle {
        retrieved_count,
        retrieval_path,
        fallback_triggered,
        fallback_reason,
        store_valid,
        store_node_id,
        memory_recall: memory_recall_section,
        memory_store: memory_store_section,
        identity_context: identity_context_section,
    }
}

pub fn build_runtime_failure_memory_recall_section(
    attempted: bool,
    error: Option<String>,
) -> JsonValue {
    json!({
        "attempted": attempted,
        "error": error,
    })
}

pub fn build_runtime_failure_identity_context_section(
    attempted: bool,
    summary: Option<String>,
    error: Option<String>,
) -> JsonValue {
    json!({
        "attempted": attempted,
        "summary": summary,
        "error": error,
    })
}

#[derive(Clone, Debug, Default)]
pub struct RuntimeDiagnosticsEnvelope {
    pub guardrail_code: Option<String>,
    pub policy_reason: Option<String>,
    pub duration_ms: Option<u64>,
    pub input_memory_query_id: Option<String>,
    pub input_memory_query_fingerprint: Option<String>,
    pub output_memory_node_id: Option<String>,
    pub retrieval_path: Option<String>,
    pub thread_id: Option<String>,
}

pub fn extract_runtime_diagnostics_envelope(diagnostics: Option<&str>) -> RuntimeDiagnosticsEnvelope {
    let Some(raw) = diagnostics else {
        return RuntimeDiagnosticsEnvelope::default();
    };

    let Ok(json) = serde_json::from_str::<JsonValue>(raw) else {
        return RuntimeDiagnosticsEnvelope::default();
    };

    RuntimeDiagnosticsEnvelope {
        guardrail_code: json
            .get("guardrail_code")
            .and_then(|v| v.as_str())
            .map(str::to_owned),
        policy_reason: json
            .get("policy_reason")
            .and_then(|v| v.as_str())
            .map(str::to_owned),
        duration_ms: json.get("duration_ms").and_then(|v| v.as_u64()),
        input_memory_query_id: json
            .get("input_memory_query_id")
            .and_then(|v| v.as_str())
            .map(str::to_owned),
        input_memory_query_fingerprint: json
            .get("input_memory_query_fingerprint")
            .and_then(|v| v.as_str())
            .map(str::to_owned)
            .or_else(|| {
                json.get("memory_recall")
                    .and_then(|v| v.get("query_fingerprint"))
                    .and_then(|v| v.as_str())
                    .map(str::to_owned)
            }),
        output_memory_node_id: json
            .get("output_memory_node_id")
            .and_then(|v| v.as_str())
            .map(str::to_owned)
            .or_else(|| {
                json.get("memory_store_node_id")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned)
            }),
        retrieval_path: json
            .get("memory_retrieval_path")
            .and_then(|v| v.as_str())
            .map(str::to_owned),
        thread_id: json
            .get("thread_id")
            .and_then(|v| v.as_str())
            .map(str::to_owned),
    }
}

pub fn extract_diagnostics_fields(
    diagnostics: Option<&str>,
) -> (Option<String>, Option<String>, Option<u64>) {
    let envelope = extract_runtime_diagnostics_envelope(diagnostics);
    (
        envelope.guardrail_code,
        envelope.policy_reason,
        envelope.duration_ms,
    )
}

pub fn extract_memory_lineage_fields(
    diagnostics: Option<&str>,
) -> (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
) {
    let envelope = extract_runtime_diagnostics_envelope(diagnostics);

    (
        envelope.input_memory_query_id,
        envelope.input_memory_query_fingerprint,
        envelope.output_memory_node_id,
        envelope.retrieval_path,
    )
}

pub fn extract_thread_id(diagnostics: Option<&str>) -> Option<String> {
    extract_runtime_diagnostics_envelope(diagnostics).thread_id
}