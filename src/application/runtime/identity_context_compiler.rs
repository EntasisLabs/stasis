use std::sync::Arc;

use crate::ports::outbound::memory::identity_memory_models::GetIdentityContextRequest;
use crate::ports::outbound::memory::identity_memory_store::IdentityMemoryStore;

const DEFAULT_PERSONA_ID: &str = "persona:default";
const DEFAULT_CHANNEL_ID: &str = "channel:default";

fn resolved_persona_id() -> String {
    std::env::var("STASIS_DEFAULT_PERSONA_ID").unwrap_or_else(|_| DEFAULT_PERSONA_ID.to_string())
}

fn resolved_channel_id(policy_profile: Option<&str>) -> String {
    if let Some(profile) = policy_profile {
        return format!("channel:{profile}");
    }

    std::env::var("STASIS_DEFAULT_CHANNEL_ID").unwrap_or_else(|_| DEFAULT_CHANNEL_ID.to_string())
}

pub async fn load_identity_context_summary(
    identity_memory_store: Option<&Arc<dyn IdentityMemoryStore>>,
    correlation_id: &str,
    policy_profile: Option<&str>,
) -> (Option<String>, Option<String>) {
    let Some(store) = identity_memory_store else {
        return (None, None);
    };

    let request = GetIdentityContextRequest {
        user_id: correlation_id.to_string(),
        persona_id: resolved_persona_id(),
        channel_id: resolved_channel_id(policy_profile),
        relationship_limit: 8,
    };

    match store.get_identity_context(&request).await {
        Ok(context) => {
            let continuity_links = context
                .relationships
                .iter()
                .filter(|relationship| relationship.derived_from_relationship_id.is_some())
                .count();
            let continuity_receipts = context
                .relationships
                .iter()
                .filter(|relationship| relationship.transition_receipt_id.is_some())
                .count();

            let summary = format!(
                "persona_present={} user_present={} channel_present={} relationships={} policies={} depth={} continuity_links={} continuity_receipts={}",
                context.persona.is_some(),
                context.user.is_some(),
                context.channel.is_some(),
                context.relationships.len(),
                context.policy_profiles.len(),
                context.graph_depth_used,
                continuity_links,
                continuity_receipts,
            );

            (Some(summary), None)
        }
        Err(err) => (None, Some(err.to_string())),
    }
}

pub fn prepend_identity_snapshot(user_prompt: &str, identity_summary: Option<&str>) -> String {
    if let Some(summary) = identity_summary {
        format!(
            "Identity context snapshot:\n{}\n\nUser prompt:\n{}",
            summary, user_prompt
        )
    } else {
        user_prompt.to_string()
    }
}
