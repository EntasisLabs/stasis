use serde_json::Value as JsonValue;

pub fn extract_diagnostics_fields(
    diagnostics: Option<&str>,
) -> (Option<String>, Option<String>, Option<u64>) {
    let Some(raw) = diagnostics else {
        return (None, None, None);
    };

    let Ok(json) = serde_json::from_str::<JsonValue>(raw) else {
        return (None, None, None);
    };

    let guardrail_code = json
        .get("guardrail_code")
        .and_then(|v| v.as_str())
        .map(str::to_owned);
    let policy_reason = json
        .get("policy_reason")
        .and_then(|v| v.as_str())
        .map(str::to_owned);
    let duration_ms = json.get("duration_ms").and_then(|v| v.as_u64());

    (guardrail_code, policy_reason, duration_ms)
}

pub fn extract_memory_lineage_fields(
    diagnostics: Option<&str>,
) -> (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
) {
    let Some(raw) = diagnostics else {
        return (None, None, None, None);
    };

    let Ok(json) = serde_json::from_str::<JsonValue>(raw) else {
        return (None, None, None, None);
    };

    let input_memory_query_id = json
        .get("input_memory_query_id")
        .and_then(|v| v.as_str())
        .map(str::to_owned);

    let input_memory_query_fingerprint = json
        .get("input_memory_query_fingerprint")
        .and_then(|v| v.as_str())
        .map(str::to_owned)
        .or_else(|| {
            json.get("memory_recall")
                .and_then(|v| v.get("query_fingerprint"))
                .and_then(|v| v.as_str())
                .map(str::to_owned)
        });

    let output_memory_node_id = json
        .get("output_memory_node_id")
        .and_then(|v| v.as_str())
        .map(str::to_owned)
        .or_else(|| {
            json.get("memory_store_node_id")
                .and_then(|v| v.as_str())
                .map(str::to_owned)
        });

    let retrieval_path = json
        .get("memory_retrieval_path")
        .and_then(|v| v.as_str())
        .map(str::to_owned);

    (
        input_memory_query_id,
        input_memory_query_fingerprint,
        output_memory_node_id,
        retrieval_path,
    )
}

pub fn extract_thread_id(diagnostics: Option<&str>) -> Option<String> {
    let raw = diagnostics?;
    let json = serde_json::from_str::<JsonValue>(raw).ok()?;
    json.get("thread_id").and_then(|v| v.as_str()).map(str::to_owned)
}