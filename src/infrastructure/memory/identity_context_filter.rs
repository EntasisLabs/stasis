use crate::ports::outbound::memory::identity_memory_models::{
    ContactEntity, GetIdentityContextResponse, IdentityContextMode, RelationshipEntity,
    UserEntity,
};

pub fn collect_contact_ids(relationships: &[RelationshipEntity]) -> Vec<String> {
    let mut ids = Vec::new();
    for rel in relationships {
        for entity_ref in [&rel.source_entity_ref, &rel.target_entity_ref] {
            if entity_ref.entity_type == "ContactEntity" {
                ids.push(entity_ref.entity_id.clone());
            }
        }
    }
    ids.sort();
    ids.dedup();
    ids
}

pub fn resolve_contacts(
    contact_ids: &[String],
    lookup: impl Fn(&str) -> Option<ContactEntity>,
) -> Vec<ContactEntity> {
    contact_ids
        .iter()
        .filter_map(|id| lookup(id))
        .filter(|contact| contact.status == "active")
        .collect()
}

fn relationship_has_policy_signals(relationship: &RelationshipEntity) -> bool {
    relationship.approval_profile_id.is_some()
        || !relationship.policy_tags.is_empty()
        || !relationship.autonomy_scope.allow.is_empty()
        || !relationship.autonomy_scope.deny.is_empty()
        || !relationship.autonomy_scope.approval_required.is_empty()
        || relationship.interruption_policy.quiet_hours.is_some()
        || relationship.escalation_policy.mode.is_some()
        || relationship.escalation_policy.fallback.is_some()
}

fn strip_user_preferences(user: &mut UserEntity) {
    user.preferences.clear();
}

pub fn apply_identity_context_mode(
    mode: IdentityContextMode,
    mut response: GetIdentityContextResponse,
) -> GetIdentityContextResponse {
    match mode {
        IdentityContextMode::Full => response,
        IdentityContextMode::Policy => {
            if let Some(user) = response.user.as_mut() {
                strip_user_preferences(user);
            }
            response.contacts.clear();
            response.relationships.retain(|relationship| {
                relationship.relationship_kind.is_structural()
                    || relationship_has_policy_signals(relationship)
            });
            response
        }
        IdentityContextMode::Cognitive => {
            response.relationships
                .retain(|relationship| relationship.relationship_kind.is_social());
            response.policy_profiles.clear();
            response.flattened_claims.clear();
            response
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;

    use super::apply_identity_context_mode;
    use crate::ports::outbound::memory::identity_memory_models::{
        AutonomyScope, ContactEntity, EntityRef, GetIdentityContextResponse, IdentityContextMode,
        PersonaEntity, PolicyProfileEntity, RelationshipEntity, RelationshipKind,
        RelationshipStatus, UpdateSource, UserEntity,
    };

    fn sample_response() -> GetIdentityContextResponse {
        GetIdentityContextResponse {
            persona: Some(PersonaEntity {
                persona_id: "p1".to_string(),
                display_name: "Medousa".to_string(),
                status: "active".to_string(),
                version: 1,
                updated_at: Utc::now(),
            }),
            user: Some(UserEntity {
                user_id: "u1".to_string(),
                timezone: "UTC".to_string(),
                language_variant: None,
                preferences: [("theme".to_string(), json!("dark"))]
                    .into_iter()
                    .collect(),
                status: "active".to_string(),
                version: 1,
                updated_at: Utc::now(),
            }),
            channel: None,
            contacts: vec![ContactEntity {
                contact_id: "c1".to_string(),
                display_name: "Alex".to_string(),
                aliases: vec!["alex@example.com".to_string()],
                status: "active".to_string(),
                version: 1,
                updated_at: Utc::now(),
            }],
            relationships: vec![
                RelationshipEntity {
                    relationship_id: "rel-struct".to_string(),
                    source_entity_ref: EntityRef {
                        entity_type: "PersonaEntity".to_string(),
                        entity_id: "p1".to_string(),
                    },
                    target_entity_ref: EntityRef {
                        entity_type: "UserEntity".to_string(),
                        entity_id: "u1".to_string(),
                    },
                    relationship_kind: RelationshipKind::AssistantUser,
                    status: RelationshipStatus::Active,
                    trust_level: 0.5,
                    confidence: 0.8,
                    strength_score: 0.8,
                    recency_score: 0.8,
                    autonomy_scope: AutonomyScope::default(),
                    approval_profile_id: None,
                    interruption_policy: Default::default(),
                    escalation_policy: Default::default(),
                    policy_tags: vec![],
                    provenance: UpdateSource::UserDirect,
                    parent_relationship_id: None,
                    governing_relationship_ids: vec![],
                    derived_from_relationship_id: None,
                    last_transition_reason: None,
                    transition_receipt_id: None,
                    version: 1,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                },
                RelationshipEntity {
                    relationship_id: "rel-social".to_string(),
                    source_entity_ref: EntityRef {
                        entity_type: "UserEntity".to_string(),
                        entity_id: "u1".to_string(),
                    },
                    target_entity_ref: EntityRef {
                        entity_type: "ContactEntity".to_string(),
                        entity_id: "c1".to_string(),
                    },
                    relationship_kind: RelationshipKind::Knows,
                    status: RelationshipStatus::Active,
                    trust_level: 0.5,
                    confidence: 0.8,
                    strength_score: 0.8,
                    recency_score: 0.8,
                    autonomy_scope: AutonomyScope::default(),
                    approval_profile_id: None,
                    interruption_policy: Default::default(),
                    escalation_policy: Default::default(),
                    policy_tags: vec![],
                    provenance: UpdateSource::UserDirect,
                    parent_relationship_id: None,
                    governing_relationship_ids: vec![],
                    derived_from_relationship_id: None,
                    last_transition_reason: None,
                    transition_receipt_id: None,
                    version: 1,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                },
            ],
            policy_profiles: vec![PolicyProfileEntity {
                policy_profile_id: "policy-1".to_string(),
                graph_max_depth: 2,
                trust_delta_max_per_window: 0.03,
                status: "active".to_string(),
                version: 1,
                updated_at: Utc::now(),
            }],
            graph_depth_used: 1,
            flattened_claims: vec![],
        }
    }

    #[test]
    fn cognitive_mode_excludes_policy_payload() {
        let filtered =
            apply_identity_context_mode(IdentityContextMode::Cognitive, sample_response());
        assert_eq!(filtered.relationships.len(), 1);
        assert!(filtered.relationships[0].relationship_kind.is_social());
        assert!(filtered.policy_profiles.is_empty());
        assert!(filtered.flattened_claims.is_empty());
        assert_eq!(filtered.contacts.len(), 1);
        assert_eq!(filtered.user.expect("user").preferences.len(), 1);
    }

    #[test]
    fn policy_mode_strips_preferences_and_contacts() {
        let filtered = apply_identity_context_mode(IdentityContextMode::Policy, sample_response());
        assert!(filtered.user.expect("user").preferences.is_empty());
        assert!(filtered.contacts.is_empty());
        assert_eq!(filtered.relationships.len(), 1);
        assert!(filtered.relationships[0].relationship_kind.is_structural());
        assert_eq!(filtered.policy_profiles.len(), 1);
    }
}
