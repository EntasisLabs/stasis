use crate::ports::outbound::memory::memory_models::{MemoryNode, MemoryRecallResponse};

pub fn format_memory_recall_context(nodes: &[MemoryNode]) -> Option<String> {
    if nodes.is_empty() {
        return None;
    }

    let sections: Vec<String> = nodes
        .iter()
        .enumerate()
        .map(|(index, node)| {
            let header = match node.context_summary.as_deref() {
                Some(summary) if !summary.trim().is_empty() => {
                    format!(
                        "--- memory node {} (tier={}, session={}, summary={}) ---",
                        index + 1,
                        node.tier,
                        node.session_id,
                        summary
                    )
                }
                _ => format!(
                    "--- memory node {} (tier={}, session={}) ---",
                    index + 1,
                    node.tier,
                    node.session_id
                ),
            };
            format!("{header}\n{}", node.raw.trim())
        })
        .collect();

    Some(format!("Recalled memory context:\n\n{}", sections.join("\n\n")))
}

pub fn prepend_memory_recall_context(user_prompt: &str, recall: &MemoryRecallResponse) -> String {
    let Some(context) = format_memory_recall_context(&recall.nodes) else {
        return user_prompt.to_string();
    };

    format!("{context}\n\nUser prompt:\n{user_prompt}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn sample_node(raw: &str) -> MemoryNode {
        MemoryNode {
            raw: raw.to_string(),
            session_id: "session-a".to_string(),
            tier: "raw".to_string(),
            sync_key: "sync-a".to_string(),
            timestamp: Utc::now(),
            updated_at: Utc::now(),
            ..Default::default()
        }
    }

    #[test]
    fn prepend_memory_recall_context_injects_raw_nodes() {
        let recall = MemoryRecallResponse {
            retrieved: 1,
            nodes: vec![sample_node("◈⟨ prior context ⟩")],
            node_sync_keys: vec!["sync-a".to_string()],
            ..Default::default()
        };

        let prompt = prepend_memory_recall_context("summarize rust trends", &recall);
        assert!(prompt.contains("Recalled memory context:"));
        assert!(prompt.contains("◈⟨ prior context ⟩"));
        assert!(prompt.contains("User prompt:\nsummarize rust trends"));
    }

    #[test]
    fn prepend_memory_recall_context_leaves_prompt_unchanged_when_empty() {
        let recall = MemoryRecallResponse::default();
        let prompt = prepend_memory_recall_context("summarize rust trends", &recall);
        assert_eq!(prompt, "summarize rust trends");
    }
}
