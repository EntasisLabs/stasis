use std::sync::Arc;

use crate::application::orchestration::runtime_job_payloads::MemoryPolicyPayload;
use crate::application::runtime::identity_context_compiler::{
    load_identity_context_summary, prepend_identity_snapshot,
};
use crate::application::runtime::memory_persistence_helpers::{
    SttpPromptNodeFormat, memory_query_fingerprint, memory_query_id, should_store,
    render_prompt_response_sttp_node,
};
use crate::application::runtime::memory_recall_context_compiler::prepend_memory_recall_context;
use crate::application::runtime::memory_recall_request_builder::build_memory_recall_request;
use crate::ports::outbound::memory::identity_memory_store::IdentityMemoryStore;
use crate::ports::outbound::memory::memory_context_reader::MemoryContextReader;
use crate::ports::outbound::memory::memory_context_writer::MemoryContextWriter;
use crate::ports::outbound::memory::memory_models::{
    MemoryRecallResponse, MemoryStoreRequest, MemoryStoreResponse,
};

#[derive(Clone, Debug, Default)]
pub struct PreparedConcurrentToolBranch {
    pub user_prompt: String,
    pub memory_recall: Option<MemoryRecallResponse>,
    pub memory_recall_error: Option<String>,
    pub identity_summary: Option<String>,
    pub identity_error: Option<String>,
    pub input_memory_query_id: Option<String>,
    pub input_memory_query_fingerprint: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct StoredConcurrentToolBranchMemory {
    pub memory_store: Option<MemoryStoreResponse>,
    pub memory_store_error: Option<String>,
}

pub fn branch_memory_session_id(correlation_id: &str, branch_id: &str) -> String {
    format!("{correlation_id}::concurrent-branch::{branch_id}")
}

pub async fn prepare_concurrent_tool_branch(
    memory_reader: Option<&Arc<dyn MemoryContextReader>>,
    identity_memory_store: Option<&Arc<dyn IdentityMemoryStore>>,
    correlation_id: &str,
    policy_profile: Option<&str>,
    rendered_prompt: &str,
    memory_policy: Option<&MemoryPolicyPayload>,
) -> PreparedConcurrentToolBranch {
    let (identity_summary, identity_error) = load_identity_context_summary(
        identity_memory_store,
        correlation_id,
        policy_profile,
    )
    .await;

    let mut effective_user_prompt =
        prepend_identity_snapshot(rendered_prompt, identity_summary.as_deref());

    let mut memory_recall = None;
    let mut memory_recall_error = None;
    let mut input_memory_query_id = None;
    let mut input_memory_query_fingerprint = None;

    if let Some(reader) = memory_reader {
        let recall_request = build_memory_recall_request(
            correlation_id,
            Some(&effective_user_prompt),
            memory_policy,
        );
        input_memory_query_id = Some(memory_query_id(correlation_id, &recall_request));
        input_memory_query_fingerprint = Some(memory_query_fingerprint(&recall_request));

        match reader.recall(&recall_request).await {
            Ok(response) => {
                effective_user_prompt = prepend_memory_recall_context(&effective_user_prompt, &response);
                memory_recall = Some(response);
            }
            Err(err) => memory_recall_error = Some(err.to_string()),
        }
    }

    PreparedConcurrentToolBranch {
        user_prompt: effective_user_prompt,
        memory_recall,
        memory_recall_error,
        identity_summary,
        identity_error,
        input_memory_query_id,
        input_memory_query_fingerprint,
    }
}

pub async fn store_concurrent_tool_branch_memory(
    memory_writer: Option<&Arc<dyn MemoryContextWriter>>,
    correlation_id: &str,
    branch_id: &str,
    tool_name: &str,
    response_text: &str,
    memory_policy: Option<&MemoryPolicyPayload>,
) -> StoredConcurrentToolBranchMemory {
    if !should_store(memory_policy) {
        return StoredConcurrentToolBranchMemory::default();
    }

    let Some(writer) = memory_writer else {
        return StoredConcurrentToolBranchMemory::default();
    };

    let session_id = branch_memory_session_id(correlation_id, branch_id);
    let store_request = MemoryStoreRequest {
        session_id,
        raw_node: render_prompt_response_sttp_node(
            correlation_id,
            tool_name,
            response_text,
            SttpPromptNodeFormat::TaggedSchema,
        ),
    };

    match writer.store_context(&store_request).await {
        Ok(stored) => StoredConcurrentToolBranchMemory {
            memory_store: Some(stored),
            memory_store_error: None,
        },
        Err(err) => StoredConcurrentToolBranchMemory {
            memory_store: None,
            memory_store_error: Some(err.to_string()),
        },
    }
}
