use async_trait::async_trait;

use crate::domain::errors::Result;
use crate::ports::outbound::memory::identity_memory_models::{
    CommitEntityUpdateRequest, CommitEntityUpdateResponse, GetIdentityContextRequest,
    GetIdentityContextResponse, ListEntityHistoryRequest, ListEntityHistoryResponse,
    ProposeEntityUpdateRequest, ProposeEntityUpdateResponse, RollbackEntityVersionRequest,
    RollbackEntityVersionResponse,
};

#[async_trait]
pub trait IdentityMemoryStore: Send + Sync {
    async fn get_identity_context(
        &self,
        request: &GetIdentityContextRequest,
    ) -> Result<GetIdentityContextResponse>;

    async fn propose_entity_update(
        &self,
        request: &ProposeEntityUpdateRequest,
    ) -> Result<ProposeEntityUpdateResponse>;

    async fn commit_entity_update(
        &self,
        request: &CommitEntityUpdateRequest,
    ) -> Result<CommitEntityUpdateResponse>;

    async fn list_entity_history(
        &self,
        request: &ListEntityHistoryRequest,
    ) -> Result<ListEntityHistoryResponse>;

    async fn rollback_entity_version(
        &self,
        request: &RollbackEntityVersionRequest,
    ) -> Result<RollbackEntityVersionResponse>;
}
