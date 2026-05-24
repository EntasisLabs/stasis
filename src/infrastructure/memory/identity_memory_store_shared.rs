use std::collections::HashMap;

use chrono::Utc;
use serde_json::Value;

use crate::ports::outbound::memory::identity_memory_models::{
    FlattenedPolicyClaim, RelationshipEntity, RelationshipStatus,
};

pub fn render_sttp_bridge_node(
    relationship_id: &str,
    actor: &str,
    patch: &Value,
    bridge_reason: &str,
    from_status: Option<RelationshipStatus>,
    to_status: Option<RelationshipStatus>,
) -> String {
    let from_status = from_status
        .map(|value| format!("{value:?}"))
        .unwrap_or_else(|| "none".to_string());
    let to_status = to_status
        .map(|value| format!("{value:?}"))
        .unwrap_or_else(|| "none".to_string());
    let patch = patch.to_string().replace('"', "\\\"");

    format!(
        "⊕⟨ {{ trigger: identity_transition, response_format: temporal_node, origin_session: \"{}\", compression_depth: 1, parent_node: null, prime: {{ attractor_config: {{ stability: 0.88, friction: 0.32, logic: 0.86, autonomy: 0.62 }}, context_summary: \"identity bridge {}\", relevant_tier: summary, retrieval_budget: 10 }} }} ⟩\n\
⦿⟨ {{ timestamp: \"{}\", tier: summary, session_id: \"{}\", schema_version: \"sttp-1.0\" }} ⟩\n\
◈⟨ {{ relationship_id(.95): \"{}\", from_status(.88): \"{}\", to_status(.88): \"{}\", patch(.90): \"{}\" }} ⟩\n\
⍉⟨ {{ rho: 0.91, kappa: 0.90, psi: 2.66 }} ⟩",
        actor,
        bridge_reason,
        Utc::now().to_rfc3339(),
        actor,
        relationship_id,
        from_status,
        to_status,
        patch,
    )
}

pub fn compute_graph_depth_with_cap(
    relationships: &[RelationshipEntity],
    max_depth_cap: usize,
    mut next_claim_id: impl FnMut() -> String,
) -> (usize, Vec<FlattenedPolicyClaim>) {
    let map = relationships
        .iter()
        .map(|rel| (rel.relationship_id.clone(), rel))
        .collect::<HashMap<_, _>>();

    let mut max_depth = 0usize;
    for rel in relationships {
        let mut stack = vec![(rel.relationship_id.clone(), 0usize)];
        let mut seen = std::collections::HashSet::new();
        while let Some((id, depth)) = stack.pop() {
            if !seen.insert(id.clone()) {
                continue;
            }
            max_depth = max_depth.max(depth);
            if let Some(node) = map.get(&id) {
                for parent in &node.governing_relationship_ids {
                    if map.contains_key(parent) {
                        stack.push((parent.clone(), depth + 1));
                    }
                }
            }
        }
    }

    if max_depth <= max_depth_cap {
        return (max_depth, Vec::new());
    }

    let claims = relationships
        .iter()
        .filter(|rel| !rel.governing_relationship_ids.is_empty())
        .map(|rel| FlattenedPolicyClaim {
            claim_id: next_claim_id(),
            source_relationship_ids: std::iter::once(rel.relationship_id.clone())
                .chain(rel.governing_relationship_ids.clone())
                .collect::<Vec<_>>(),
            summary: "flattened governance chain due to graph depth cap".to_string(),
            confidence: rel.confidence,
            timestamp: Utc::now(),
        })
        .collect::<Vec<_>>();

    (max_depth_cap, claims)
}