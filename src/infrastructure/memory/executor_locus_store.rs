//! Locus `NodeStore` and `SemanticIndexStore` over [`SqlExecutor`](crate::infrastructure::sqlite::SqlExecutor).
//!
//! The tables and ranking match `locus-sqlite-adapter` 0.1.0 and
//! `locus-durable-object` 0.1.0. Native SQLite uses that adapter. This store
//! is the wasm32 / Durable Object path: the adapter links rusqlite and does
//! not build for `wasm32`. `MIGRATION_V4` is the adapter `SCHEMA`.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use locus_core_rs::derive_tenant_id_from_session;
use locus_core_rs::domain::contracts::{
    NodeStore, NodeStoreInitializer, SemanticIndexStore, SemanticIndexStoreInitializer,
    TagEmbedding,
};
use locus_core_rs::domain::models::{
    AvecState, BatchRekeyResult, ChangeQueryResult, NodeDeleteRecord, NodeDeleteRequest,
    NodeDeleteResult, NodeDeleteStatus, NodeQuery, NodeUpsertResult, NodeUpsertStatus,
    ScopeRekeyResult, SemanticTagNodeRef, SemanticTagQueryFilter, SemanticTagRecord,
    SessionPurgeRequest, SttpNode, SyncCheckpoint, SyncCursor,
};

use crate::infrastructure::sqlite::executor::{SqlExecutor, SqlRow, SqlValue, transaction_result};
use crate::infrastructure::sqlite::schema::MIGRATION_V4;
use crate::infrastructure::sqlite::script::split_sql_script;

const DEFAULT_TENANT: &str = "default";

const NODE_COLUMNS: &str = "\
id, tenant_id, session_id, raw, tier, timestamp_ms, compression_depth, parent_node_id, \
sync_key, updated_at_ms, source_metadata, context_summary, semantic_tags, semantic_links, \
embedding, embedding_model, embedding_dimensions, embedded_at_ms, psi, rho, kappa, \
user_stability, user_friction, user_logic, user_autonomy, user_psi, \
model_stability, model_friction, model_logic, model_autonomy, model_psi, \
comp_stability, comp_friction, comp_logic, comp_autonomy, comp_psi";

#[derive(Clone)]
pub struct ExecutorLocusNodeStore {
    executor: Arc<dyn SqlExecutor>,
}

#[derive(Clone)]
pub struct ExecutorSemanticIndexStore {
    executor: Arc<dyn SqlExecutor>,
}

impl ExecutorLocusNodeStore {
    pub fn new(executor: Arc<dyn SqlExecutor>) -> Self {
        Self { executor }
    }

    pub fn initialize(&self) -> Result<()> {
        ensure_schema(self.executor.as_ref())?;
        let stale = rows(
            self.executor.as_ref(),
            "SELECT id, session_id, sync_key FROM temporal_node \
             WHERE tenant_id = '' OR sync_key = ''",
            &[],
        )?;
        if stale.is_empty() {
            return Ok(());
        }
        let executor = self.executor.clone();
        in_tx(executor.as_ref(), || {
            for row in &stale {
                let id = row.text(0).map_err(|err| anyhow!("{err}"))?;
                let session_id = row.text(1).map_err(|err| anyhow!("{err}"))?;
                let sync_key = row
                    .text_opt(2)
                    .map_err(|err| anyhow!("{err}"))?
                    .unwrap_or_default();
                let tenant_id = derive_tenant_id_from_session(&session_id);
                let sync_key = if sync_key.trim().is_empty() {
                    format!("legacy:{id}")
                } else {
                    sync_key
                };
                exec(
                    executor.as_ref(),
                    "UPDATE temporal_node SET tenant_id = ?1, sync_key = ?2 WHERE id = ?3",
                    &[
                        SqlValue::text(&tenant_id),
                        SqlValue::text(sync_key),
                        SqlValue::text(id),
                    ],
                )?;
            }
            Ok(())
        })
    }
}

impl ExecutorSemanticIndexStore {
    pub fn new(executor: Arc<dyn SqlExecutor>) -> Self {
        Self { executor }
    }

    pub fn initialize(&self) -> Result<()> {
        ensure_schema(self.executor.as_ref())
    }

    fn canonical_tag(tag: &str) -> String {
        tag.trim().to_lowercase()
    }
}

fn ensure_schema(executor: &dyn SqlExecutor) -> Result<()> {
    let statements = split_sql_script(MIGRATION_V4).context("split locus schema")?;
    for statement in statements {
        exec(executor, &statement, &[])?;
    }
    Ok(())
}

fn exec(executor: &dyn SqlExecutor, sql: &str, params: &[SqlValue]) -> Result<u64> {
    executor
        .execute(sql, params)
        .map_err(|err| anyhow!("locus sqlite: {err}"))
}

fn rows(executor: &dyn SqlExecutor, sql: &str, params: &[SqlValue]) -> Result<Vec<SqlRow>> {
    executor
        .query(sql, params)
        .map_err(|err| anyhow!("locus sqlite: {err}"))
}

fn row_opt(executor: &dyn SqlExecutor, sql: &str, params: &[SqlValue]) -> Result<Option<SqlRow>> {
    Ok(rows(executor, sql, params)?.into_iter().next())
}

fn in_tx<T>(executor: &dyn SqlExecutor, mut body: impl FnMut() -> Result<T>) -> Result<T> {
    transaction_result(executor, &mut || {
        body().map_err(|err| crate::infrastructure::sqlite::codec::sql_err("locus sqlite", err))
    })
    .map_err(|err| anyhow!("{err}"))
}

fn new_id() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    format!(
        "{:x}{:x}",
        Utc::now().timestamp_millis(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

fn sql_f32(value: f32) -> SqlValue {
    SqlValue::Text((value as f64).to_string())
}

fn sql_i64_opt(value: Option<i64>) -> SqlValue {
    match value {
        Some(value) => SqlValue::Integer(value),
        None => SqlValue::Null,
    }
}

fn text_opt(value: Option<String>) -> SqlValue {
    match value {
        Some(value) => SqlValue::Text(value),
        None => SqlValue::Null,
    }
}

fn json_text<T: serde::Serialize>(value: Option<&T>) -> Result<Option<String>> {
    value
        .map(serde_json::to_string)
        .transpose()
        .context("encode locus json column")
}

fn optional_json<T: serde::de::DeserializeOwned>(value: Option<String>) -> Result<Option<T>> {
    match value {
        None => Ok(None),
        Some(text) if text.trim().is_empty() => Ok(None),
        Some(text) => serde_json::from_str(&text)
            .map(Some)
            .context("decode locus json column"),
    }
}

fn from_millis(value: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(value).unwrap_or_else(Utc::now)
}

fn is_unique(err: &anyhow::Error) -> bool {
    err.to_string().contains("UNIQUE")
}

#[async_trait]
impl NodeStoreInitializer for ExecutorLocusNodeStore {
    async fn initialize_async(&self) -> Result<()> {
        self.initialize()
    }
}

#[async_trait]
impl NodeStore for ExecutorLocusNodeStore {
    async fn query_nodes_async(&self, query: NodeQuery) -> Result<Vec<SttpNode>> {
        query_nodes(self.executor.as_ref(), &query)
    }

    async fn upsert_node_async(&self, node: SttpNode) -> Result<NodeUpsertResult> {
        upsert_node(self.executor.as_ref(), node)
    }

    async fn get_by_resonance_async(
        &self,
        session_id: &str,
        current_avec: AvecState,
        from_utc: Option<DateTime<Utc>>,
        to_utc: Option<DateTime<Utc>>,
        tiers: Option<&[String]>,
        limit: usize,
    ) -> Result<Vec<SttpNode>> {
        resonance(
            self.executor.as_ref(),
            Some(session_id),
            current_avec,
            from_utc,
            to_utc,
            tiers,
            limit,
        )
    }

    async fn get_by_resonance_global_async(
        &self,
        current_avec: AvecState,
        from_utc: Option<DateTime<Utc>>,
        to_utc: Option<DateTime<Utc>>,
        tiers: Option<&[String]>,
        limit: usize,
    ) -> Result<Vec<SttpNode>> {
        resonance(
            self.executor.as_ref(),
            None,
            current_avec,
            from_utc,
            to_utc,
            tiers,
            limit,
        )
    }

    async fn get_by_hybrid_async(
        &self,
        session_id: &str,
        current_avec: AvecState,
        from_utc: Option<DateTime<Utc>>,
        to_utc: Option<DateTime<Utc>>,
        tiers: Option<&[String]>,
        query_embedding: Option<&[f32]>,
        alpha: f32,
        beta: f32,
        limit: usize,
    ) -> Result<Vec<SttpNode>> {
        let candidate_limit = limit.max(1).saturating_mul(5);
        let mut candidates = self
            .get_by_resonance_async(
                session_id,
                current_avec,
                from_utc,
                to_utc,
                tiers,
                candidate_limit,
            )
            .await?;
        rank_nodes_hybrid(&mut candidates, current_avec, query_embedding, alpha, beta);
        candidates.truncate(limit.max(1));
        Ok(candidates)
    }

    async fn get_by_hybrid_global_async(
        &self,
        current_avec: AvecState,
        from_utc: Option<DateTime<Utc>>,
        to_utc: Option<DateTime<Utc>>,
        tiers: Option<&[String]>,
        query_embedding: Option<&[f32]>,
        alpha: f32,
        beta: f32,
        limit: usize,
    ) -> Result<Vec<SttpNode>> {
        let candidate_limit = limit.max(1).saturating_mul(5);
        let mut candidates = self
            .get_by_resonance_global_async(current_avec, from_utc, to_utc, tiers, candidate_limit)
            .await?;
        rank_nodes_hybrid(&mut candidates, current_avec, query_embedding, alpha, beta);
        candidates.truncate(limit.max(1));
        Ok(candidates)
    }

    async fn list_nodes_async(
        &self,
        limit: usize,
        session_id: Option<&str>,
    ) -> Result<Vec<SttpNode>> {
        self.query_nodes_async(NodeQuery {
            limit: limit.clamp(1, 200),
            session_id: session_id.map(str::to_string),
            from_utc: None,
            to_utc: None,
            tiers: None,
        })
        .await
    }

    async fn get_last_avec_async(&self, session_id: &str) -> Result<Option<AvecState>> {
        let tenant_id = derive_tenant_id_from_session(session_id);
        let row = row_opt(
            self.executor.as_ref(),
            "SELECT stability, friction, logic, autonomy FROM calibration \
             WHERE session_id = ?1 AND (tenant_id = ?2 OR tenant_id IS NULL OR tenant_id = '') \
             ORDER BY created_at_ms DESC, rowid DESC LIMIT 1",
            &[SqlValue::text(session_id), SqlValue::text(tenant_id)],
        )?;
        match row {
            Some(row) => Ok(Some(AvecState {
                stability: row.f64(0).map_err(|err| anyhow!("{err}"))? as f32,
                friction: row.f64(1).map_err(|err| anyhow!("{err}"))? as f32,
                logic: row.f64(2).map_err(|err| anyhow!("{err}"))? as f32,
                autonomy: row.f64(3).map_err(|err| anyhow!("{err}"))? as f32,
            })),
            None => Ok(None),
        }
    }

    async fn get_trigger_history_async(&self, session_id: &str) -> Result<Vec<String>> {
        let tenant_id = derive_tenant_id_from_session(session_id);
        let found = rows(
            self.executor.as_ref(),
            "SELECT trigger FROM calibration \
             WHERE session_id = ?1 AND (tenant_id = ?2 OR tenant_id IS NULL OR tenant_id = '') \
             ORDER BY created_at_ms ASC, rowid ASC",
            &[SqlValue::text(session_id), SqlValue::text(tenant_id)],
        )?;
        found
            .iter()
            .map(|row| row.text(0).map_err(|err| anyhow!("{err}")))
            .collect()
    }

    async fn store_calibration_async(
        &self,
        session_id: &str,
        avec: AvecState,
        trigger: &str,
    ) -> Result<()> {
        let tenant_id = derive_tenant_id_from_session(session_id);
        exec(
            self.executor.as_ref(),
            "INSERT INTO calibration (
                id, tenant_id, session_id, stability, friction, logic, autonomy, psi, trigger, created_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            &[
                SqlValue::text(new_id()),
                SqlValue::text(tenant_id),
                SqlValue::text(session_id),
                sql_f32(avec.stability),
                sql_f32(avec.friction),
                sql_f32(avec.logic),
                sql_f32(avec.autonomy),
                sql_f32(avec.psi()),
                SqlValue::text(trigger),
                SqlValue::from(Utc::now().timestamp_millis()),
            ],
        )?;
        Ok(())
    }

    async fn query_changes_since_async(
        &self,
        session_id: &str,
        cursor: Option<SyncCursor>,
        limit: usize,
    ) -> Result<ChangeQueryResult> {
        let capped = limit.max(1);
        let tenant_id = derive_tenant_id_from_session(session_id);
        let mut params = vec![SqlValue::text(session_id), SqlValue::text(tenant_id)];
        let cursor_clause = if let Some(cursor) = cursor {
            params.push(SqlValue::from(cursor.updated_at.timestamp_millis()));
            params.push(SqlValue::text(cursor.sync_key));
            "AND (updated_at_ms > ?3 OR (updated_at_ms = ?3 AND sync_key > ?4))"
        } else {
            ""
        };
        params.push(SqlValue::from((capped + 1) as i64));
        let limit_index = params.len();
        let sql = format!(
            "SELECT {NODE_COLUMNS} FROM temporal_node \
             WHERE session_id = ?1 AND (tenant_id = ?2 OR tenant_id IS NULL OR tenant_id = '') \
             {cursor_clause} \
             ORDER BY updated_at_ms ASC, sync_key ASC \
             LIMIT ?{limit_index}"
        );
        let mut nodes = load_nodes(self.executor.as_ref(), &sql, &params)?;
        let has_more = nodes.len() > capped;
        if has_more {
            nodes.truncate(capped);
        }
        let next_cursor = nodes.last().map(|node| SyncCursor {
            updated_at: node.updated_at,
            sync_key: node.sync_key.clone(),
        });
        Ok(ChangeQueryResult {
            nodes,
            next_cursor,
            has_more,
        })
    }

    async fn get_checkpoint_async(
        &self,
        session_id: &str,
        connector_id: &str,
    ) -> Result<Option<SyncCheckpoint>> {
        let tenant_id = derive_tenant_id_from_session(session_id);
        let row = row_opt(
            self.executor.as_ref(),
            "SELECT session_id, connector_id, cursor_updated_at_ms, cursor_sync_key, metadata, updated_at_ms \
             FROM sync_checkpoint \
             WHERE session_id = ?1 AND connector_id = ?2 \
             AND (tenant_id = ?3 OR tenant_id IS NULL OR tenant_id = '') \
             LIMIT 1",
            &[
                SqlValue::text(session_id),
                SqlValue::text(connector_id),
                SqlValue::text(tenant_id),
            ],
        )?;
        row.map(map_checkpoint).transpose()
    }

    async fn put_checkpoint_async(&self, checkpoint: SyncCheckpoint) -> Result<()> {
        let tenant_id = derive_tenant_id_from_session(&checkpoint.session_id);
        let metadata = json_text(checkpoint.metadata.as_ref())?;
        let (cursor_updated_at, cursor_sync_key) = match checkpoint.cursor {
            Some(cursor) => (
                Some(cursor.updated_at.timestamp_millis()),
                Some(cursor.sync_key),
            ),
            None => (None, None),
        };
        exec(
            self.executor.as_ref(),
            "INSERT INTO sync_checkpoint (
                id, tenant_id, session_id, connector_id, cursor_updated_at_ms, cursor_sync_key, metadata, updated_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(tenant_id, session_id, connector_id) DO UPDATE SET
                cursor_updated_at_ms = excluded.cursor_updated_at_ms,
                cursor_sync_key = excluded.cursor_sync_key,
                metadata = excluded.metadata,
                updated_at_ms = excluded.updated_at_ms",
            &[
                SqlValue::text(new_id()),
                SqlValue::text(tenant_id),
                SqlValue::text(&checkpoint.session_id),
                SqlValue::text(&checkpoint.connector_id),
                sql_i64_opt(cursor_updated_at),
                text_opt(cursor_sync_key),
                text_opt(metadata),
                SqlValue::from(checkpoint.updated_at.timestamp_millis()),
            ],
        )?;
        Ok(())
    }

    async fn batch_rekey_scopes_async(
        &self,
        node_ids: Vec<String>,
        target_tenant_id: &str,
        target_session_id: &str,
        dry_run: bool,
        allow_merge: bool,
    ) -> Result<BatchRekeyResult> {
        rekey(
            self.executor.as_ref(),
            node_ids,
            &normalize_tenant_id(target_tenant_id),
            target_session_id,
            dry_run,
            allow_merge,
        )
    }

    async fn delete_nodes_async(&self, request: NodeDeleteRequest) -> Result<NodeDeleteResult> {
        delete_nodes(self.executor.as_ref(), request)
    }

    async fn purge_session_async(&self, request: SessionPurgeRequest) -> Result<NodeDeleteResult> {
        purge_session(self.executor.as_ref(), request)
    }
}

#[async_trait]
impl SemanticIndexStoreInitializer for ExecutorSemanticIndexStore {
    async fn initialize_async(&self) -> Result<()> {
        self.initialize()
    }
}

#[async_trait]
impl SemanticIndexStore for ExecutorSemanticIndexStore {
    async fn sync_node_tags_async(
        &self,
        node_ref: SemanticTagNodeRef,
        tags: &[String],
        embeddings: Option<&HashMap<String, TagEmbedding>>,
    ) -> Result<()> {
        let canonical_tags: HashSet<String> = tags
            .iter()
            .map(|tag| Self::canonical_tag(tag))
            .filter(|tag| !tag.is_empty())
            .collect();
        let embeddings = embeddings.cloned();
        let executor = self.executor.clone();
        in_tx(executor.as_ref(), || {
            let now = Utc::now().timestamp_millis();
            let existing = rows(
                executor.as_ref(),
                "SELECT tag, embedding, embedding_model, embedding_dimensions, embedded_at_ms \
                 FROM semantic_tag_index WHERE tenant_id = ?1 AND sync_key = ?2",
                &[
                    SqlValue::text(&node_ref.tenant_id),
                    SqlValue::text(&node_ref.sync_key),
                ],
            )?;
            for prior in &existing {
                let tag = prior.text(0).map_err(|err| anyhow!("{err}"))?;
                if !canonical_tags.contains(&tag) {
                    exec(
                        executor.as_ref(),
                        "DELETE FROM semantic_tag_index \
                         WHERE tenant_id = ?1 AND sync_key = ?2 AND tag = ?3",
                        &[
                            SqlValue::text(&node_ref.tenant_id),
                            SqlValue::text(&node_ref.sync_key),
                            SqlValue::text(tag),
                        ],
                    )?;
                }
            }
            for tag in &canonical_tags {
                let prior = existing
                    .iter()
                    .find(|row| row.text(0).ok().as_deref() == Some(tag.as_str()));
                let embedding_payload = embeddings
                    .as_ref()
                    .and_then(|map| lookup_embedding(map, tag));
                let embedding = match embedding_payload {
                    Some(value) => json_text(Some(&value.vector))?,
                    None => prior.and_then(|row| row.text_opt(1).ok().flatten()),
                };
                let embedding_model = embedding_payload
                    .map(|value| value.model.clone())
                    .or_else(|| prior.and_then(|row| row.text_opt(2).ok().flatten()));
                let embedding_dimensions = embedding_payload
                    .map(|value| value.vector.len() as i64)
                    .or_else(|| prior.and_then(|row| row.i64_opt(3).ok().flatten()));
                let embedded_at_ms = if embedding_payload.is_some() {
                    Some(now)
                } else {
                    prior.and_then(|row| row.i64_opt(4).ok().flatten())
                };
                exec(
                    executor.as_ref(),
                    "INSERT INTO semantic_tag_index (
                        tenant_id, session_id, node_id, sync_key, tag, embedding, embedding_model,
                        embedding_dimensions, embedded_at_ms, updated_at_ms
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                     ON CONFLICT(tenant_id, sync_key, tag) DO UPDATE SET
                        session_id = excluded.session_id,
                        node_id = excluded.node_id,
                        embedding = excluded.embedding,
                        embedding_model = excluded.embedding_model,
                        embedding_dimensions = excluded.embedding_dimensions,
                        embedded_at_ms = excluded.embedded_at_ms,
                        updated_at_ms = excluded.updated_at_ms",
                    &[
                        SqlValue::text(&node_ref.tenant_id),
                        SqlValue::text(&node_ref.session_id),
                        SqlValue::text(&node_ref.node_id),
                        SqlValue::text(&node_ref.sync_key),
                        SqlValue::text(tag),
                        text_opt(embedding),
                        text_opt(embedding_model),
                        sql_i64_opt(embedding_dimensions),
                        sql_i64_opt(embedded_at_ms),
                        SqlValue::from(now),
                    ],
                )?;
            }
            Ok(())
        })
    }

    async fn delete_node_tags_async(&self, tenant_id: &str, sync_key: &str) -> Result<()> {
        exec(
            self.executor.as_ref(),
            "DELETE FROM semantic_tag_index WHERE tenant_id = ?1 AND sync_key = ?2",
            &[SqlValue::text(tenant_id), SqlValue::text(sync_key)],
        )?;
        Ok(())
    }

    async fn find_sync_keys_by_tags_async(
        &self,
        tenant_id: &str,
        tags: &[String],
        match_all: bool,
        session_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<String>> {
        let canonical: Vec<String> = tags
            .iter()
            .map(|tag| Self::canonical_tag(tag))
            .filter(|tag| !tag.is_empty())
            .collect();
        if canonical.is_empty() {
            return Ok(Vec::new());
        }
        let mut params = vec![SqlValue::text(tenant_id)];
        let mut sql =
            String::from("SELECT sync_key, tag FROM semantic_tag_index WHERE tenant_id = ?1");
        if let Some(session_id) = session_id {
            params.push(SqlValue::text(session_id));
            sql.push_str(" AND session_id = ?2");
        }
        let found = rows(self.executor.as_ref(), &sql, &params)?;
        let mut counts: HashMap<String, usize> = HashMap::new();
        for row in found {
            let sync_key = row.text(0).map_err(|err| anyhow!("{err}"))?;
            let tag = row.text(1).map_err(|err| anyhow!("{err}"))?;
            if canonical.iter().any(|candidate| candidate == &tag) {
                *counts.entry(sync_key).or_default() += 1;
            }
        }
        let mut sync_keys = counts
            .into_iter()
            .filter(|(_, count)| {
                if match_all {
                    *count >= canonical.len()
                } else {
                    *count > 0
                }
            })
            .map(|(sync_key, _)| sync_key)
            .collect::<Vec<_>>();
        sync_keys.sort();
        sync_keys.truncate(limit);
        Ok(sync_keys)
    }

    async fn find_tags_async(
        &self,
        tenant_id: &str,
        prefix: Option<&str>,
        limit: usize,
    ) -> Result<Vec<String>> {
        let found = rows(
            self.executor.as_ref(),
            "SELECT DISTINCT tag FROM semantic_tag_index WHERE tenant_id = ?1 ORDER BY tag ASC",
            &[SqlValue::text(tenant_id)],
        )?;
        let mut tags = found
            .iter()
            .map(|row| row.text(0).map_err(|err| anyhow!("{err}")))
            .collect::<Result<Vec<_>>>()?;
        if let Some(prefix) = prefix {
            let prefix = prefix.to_lowercase();
            tags.retain(|tag| tag.starts_with(&prefix));
        }
        tags.truncate(limit);
        Ok(tags)
    }

    async fn query_tag_records_async(
        &self,
        filter: SemanticTagQueryFilter,
    ) -> Result<Vec<SemanticTagRecord>> {
        let mut clauses = Vec::new();
        let mut params = Vec::new();
        if let Some(tenant_id) = filter.tenant_id.as_ref() {
            params.push(SqlValue::text(tenant_id));
            clauses.push(format!("tenant_id = ?{}", params.len()));
        }
        if let Some(session_id) = filter.session_id.as_ref() {
            params.push(SqlValue::text(session_id));
            clauses.push(format!("session_id = ?{}", params.len()));
        }
        if let Some(tags) = filter.tags.as_ref() {
            let canonical: Vec<String> = tags
                .iter()
                .map(|tag| ExecutorSemanticIndexStore::canonical_tag(tag))
                .filter(|tag| !tag.is_empty())
                .collect();
            if canonical.is_empty() {
                return Ok(Vec::new());
            }
            let start = params.len() + 1;
            let placeholders = (0..canonical.len())
                .map(|offset| format!("?{}", start + offset))
                .collect::<Vec<_>>()
                .join(", ");
            clauses.push(format!("tag IN ({placeholders})"));
            for tag in canonical {
                params.push(SqlValue::text(tag));
            }
        }
        if let Some(prefix) = filter.tag_prefix.as_ref() {
            params.push(SqlValue::text(like_prefix(&prefix.to_lowercase())));
            clauses.push(format!("tag LIKE ?{} ESCAPE '\\'", params.len()));
        }
        match filter.has_embedding {
            Some(true) => clauses.push("embedding IS NOT NULL".to_string()),
            Some(false) => clauses.push("embedding IS NULL".to_string()),
            None => {}
        }
        if filter.missing_embedding_only {
            clauses.push("embedding IS NULL".to_string());
        }
        params.push(SqlValue::from(filter.limit.max(1) as i64));
        let where_sql = if clauses.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", clauses.join(" AND "))
        };
        let sql = format!(
            "SELECT tenant_id, session_id, node_id, sync_key, tag, embedding, embedding_model, \
             embedding_dimensions, embedded_at_ms, updated_at_ms \
             FROM semantic_tag_index {where_sql} \
             ORDER BY updated_at_ms DESC LIMIT ?{}",
            params.len()
        );
        rows(self.executor.as_ref(), &sql, &params)?
            .into_iter()
            .map(map_tag_record)
            .collect()
    }
}

#[derive(Default)]
struct NodeFilter {
    clauses: Vec<String>,
    params: Vec<SqlValue>,
}

impl NodeFilter {
    fn session(&mut self, session_id: &str) {
        let index = self.params.len() + 1;
        self.clauses.push(format!(
            "session_id = ?{index} AND (tenant_id = ?{} OR tenant_id IS NULL OR tenant_id = '')",
            index + 1
        ));
        self.params.push(SqlValue::text(session_id));
        self.params
            .push(SqlValue::text(derive_tenant_id_from_session(session_id)));
    }

    fn exact_scope(&mut self, tenant_id: &str, session_id: &str) {
        let index = self.params.len() + 1;
        self.clauses.push(format!(
            "tenant_id = ?{index} AND session_id = ?{}",
            index + 1
        ));
        self.params.push(SqlValue::text(tenant_id));
        self.params.push(SqlValue::text(session_id));
    }

    fn time_range(&mut self, from_utc: Option<DateTime<Utc>>, to_utc: Option<DateTime<Utc>>) {
        if let Some(from_utc) = from_utc {
            let index = self.params.len() + 1;
            self.clauses.push(format!("timestamp_ms >= ?{index}"));
            self.params
                .push(SqlValue::from(from_utc.timestamp_millis()));
        }
        if let Some(to_utc) = to_utc {
            let index = self.params.len() + 1;
            self.clauses.push(format!("timestamp_ms <= ?{index}"));
            self.params.push(SqlValue::from(to_utc.timestamp_millis()));
        }
    }

    fn tiers(&mut self, tiers: Option<&[String]>) {
        let Some(tiers) = normalized_tiers(tiers) else {
            return;
        };
        let start = self.params.len() + 1;
        let placeholders = (0..tiers.len())
            .map(|offset| format!("?{}", start + offset))
            .collect::<Vec<_>>()
            .join(", ");
        self.clauses
            .push(format!("lower(tier) IN ({placeholders})"));
        for tier in tiers {
            self.params.push(SqlValue::text(tier));
        }
    }

    fn finish(self) -> (String, Vec<SqlValue>) {
        if self.clauses.is_empty() {
            (String::new(), self.params)
        } else {
            (format!("WHERE {}", self.clauses.join(" AND ")), self.params)
        }
    }
}

fn query_nodes(executor: &dyn SqlExecutor, query: &NodeQuery) -> Result<Vec<SttpNode>> {
    let mut filter = NodeFilter::default();
    if let Some(session_id) = query
        .session_id
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        filter.session(session_id);
    }
    filter.time_range(query.from_utc, query.to_utc);
    filter.tiers(query.tiers.as_deref());
    let (where_sql, mut params) = filter.finish();
    params.push(SqlValue::from(query.limit.max(1) as i64));
    let limit_index = params.len();
    let sql = format!(
        "SELECT {NODE_COLUMNS} FROM temporal_node {where_sql} \
         ORDER BY timestamp_ms DESC LIMIT ?{limit_index}"
    );
    load_nodes(executor, &sql, &params)
}

fn resonance(
    executor: &dyn SqlExecutor,
    session_id: Option<&str>,
    current_avec: AvecState,
    from_utc: Option<DateTime<Utc>>,
    to_utc: Option<DateTime<Utc>>,
    tiers: Option<&[String]>,
    limit: usize,
) -> Result<Vec<SttpNode>> {
    let mut filter = NodeFilter::default();
    if let Some(session_id) = session_id {
        filter.session(session_id);
    }
    filter.time_range(from_utc, to_utc);
    filter.tiers(tiers);
    let (where_sql, mut params) = filter.finish();
    params.push(sql_f32(current_avec.stability));
    params.push(sql_f32(current_avec.friction));
    params.push(sql_f32(current_avec.logic));
    params.push(sql_f32(current_avec.autonomy));
    params.push(SqlValue::from(limit.max(1) as i64));
    let distance_start = params.len() - 4;
    let limit_index = params.len();
    let sql = format!(
        "SELECT {NODE_COLUMNS} FROM temporal_node {where_sql} \
         ORDER BY (ABS(model_stability - ?{distance_start}) + ABS(model_friction - ?{}) + \
         ABS(model_logic - ?{}) + ABS(model_autonomy - ?{})) / 4.0 ASC, \
         updated_at_ms DESC \
         LIMIT ?{limit_index}",
        distance_start + 1,
        distance_start + 2,
        distance_start + 3,
    );
    load_nodes(executor, &sql, &params)
}

fn upsert_node(executor: &dyn SqlExecutor, mut candidate: SttpNode) -> Result<NodeUpsertResult> {
    let compression = resolved_compression(&candidate);
    let sync_key = if candidate.sync_key.trim().is_empty() {
        candidate.canonical_sync_key()
    } else {
        candidate.sync_key.trim().to_string()
    };
    let updated_at = candidate.updated_at;
    candidate.sync_key = sync_key.clone();
    let tenant_id = derive_tenant_id_from_session(&candidate.session_id);
    in_tx(executor, || {
        if let Some((node_id, existing)) =
            find_existing(executor, &tenant_id, &candidate.session_id, &sync_key)?
        {
            if nodes_equivalent(&existing, &candidate, compression) {
                return Ok(NodeUpsertResult {
                    node_id,
                    sync_key: sync_key.clone(),
                    status: NodeUpsertStatus::Duplicate,
                    updated_at: existing.updated_at,
                });
            }
            update_node(executor, &node_id, &tenant_id, &candidate, compression)?;
            return Ok(NodeUpsertResult {
                node_id,
                sync_key: sync_key.clone(),
                status: NodeUpsertStatus::Updated,
                updated_at,
            });
        }
        let node_id = new_id();
        match insert_node(executor, &node_id, &tenant_id, &candidate, compression) {
            Ok(()) => Ok(NodeUpsertResult {
                node_id,
                sync_key: sync_key.clone(),
                status: NodeUpsertStatus::Created,
                updated_at,
            }),
            Err(error) if is_unique(&error) => {
                let Some((node_id, existing)) =
                    find_existing(executor, &tenant_id, &candidate.session_id, &sync_key)?
                else {
                    return Err(error);
                };
                if nodes_equivalent(&existing, &candidate, compression) {
                    return Ok(NodeUpsertResult {
                        node_id,
                        sync_key: sync_key.clone(),
                        status: NodeUpsertStatus::Duplicate,
                        updated_at: existing.updated_at,
                    });
                }
                update_node(executor, &node_id, &tenant_id, &candidate, compression)?;
                Ok(NodeUpsertResult {
                    node_id,
                    sync_key: sync_key.clone(),
                    status: NodeUpsertStatus::Updated,
                    updated_at,
                })
            }
            Err(error) => Err(error),
        }
    })
}

fn find_existing(
    executor: &dyn SqlExecutor,
    tenant_id: &str,
    session_id: &str,
    sync_key: &str,
) -> Result<Option<(String, SttpNode)>> {
    let sql = format!(
        "SELECT {NODE_COLUMNS} FROM temporal_node \
         WHERE session_id = ?1 AND sync_key = ?2 \
         ORDER BY CASE WHEN tenant_id = ?3 THEN 0 ELSE 1 END \
         LIMIT 1"
    );
    let Some(row) = row_opt(
        executor,
        &sql,
        &[
            SqlValue::text(session_id),
            SqlValue::text(sync_key),
            SqlValue::text(tenant_id),
        ],
    )?
    else {
        return Ok(None);
    };
    let id = row.text(0).map_err(|err| anyhow!("{err}"))?;
    Ok(Some((id, map_node(&row)?)))
}

fn node_values(
    node_id: &str,
    tenant_id: &str,
    node: &SttpNode,
    compression: AvecState,
) -> Result<Vec<SqlValue>> {
    Ok(vec![
        SqlValue::text(node_id),
        SqlValue::text(tenant_id),
        SqlValue::text(&node.session_id),
        SqlValue::text(&node.raw),
        SqlValue::text(&node.tier),
        SqlValue::from(node.timestamp.timestamp_millis()),
        SqlValue::from(i64::from(node.compression_depth)),
        text_opt(node.parent_node_id.clone()),
        SqlValue::text(&node.sync_key),
        SqlValue::from(node.updated_at.timestamp_millis()),
        text_opt(json_text(node.source_metadata.as_ref())?),
        text_opt(node.context_summary.clone()),
        text_opt(json_text(node.semantic_tags.as_ref())?),
        text_opt(json_text(node.semantic_links.as_ref())?),
        text_opt(json_text(node.embedding.as_ref())?),
        text_opt(node.embedding_model.clone()),
        node.embedding_dimensions
            .map(|value| SqlValue::from(value as i64))
            .unwrap_or(SqlValue::Null),
        node.embedded_at
            .map(|value| SqlValue::from(value.timestamp_millis()))
            .unwrap_or(SqlValue::Null),
        sql_f32(node.psi),
        sql_f32(node.rho),
        sql_f32(node.kappa),
        sql_f32(node.user_avec.stability),
        sql_f32(node.user_avec.friction),
        sql_f32(node.user_avec.logic),
        sql_f32(node.user_avec.autonomy),
        sql_f32(node.user_avec.psi()),
        sql_f32(node.model_avec.stability),
        sql_f32(node.model_avec.friction),
        sql_f32(node.model_avec.logic),
        sql_f32(node.model_avec.autonomy),
        sql_f32(node.model_avec.psi()),
        sql_f32(compression.stability),
        sql_f32(compression.friction),
        sql_f32(compression.logic),
        sql_f32(compression.autonomy),
        sql_f32(compression.psi()),
    ])
}

fn insert_node(
    executor: &dyn SqlExecutor,
    node_id: &str,
    tenant_id: &str,
    node: &SttpNode,
    compression: AvecState,
) -> Result<()> {
    exec(
        executor,
        "INSERT INTO temporal_node (
            id, tenant_id, session_id, raw, tier, timestamp_ms, compression_depth, parent_node_id,
            sync_key, updated_at_ms, source_metadata, context_summary, semantic_tags, semantic_links,
            embedding, embedding_model, embedding_dimensions, embedded_at_ms, psi, rho, kappa,
            user_stability, user_friction, user_logic, user_autonomy, user_psi,
            model_stability, model_friction, model_logic, model_autonomy, model_psi,
            comp_stability, comp_friction, comp_logic, comp_autonomy, comp_psi
         ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18,
            ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30, ?31, ?32, ?33, ?34, ?35, ?36
         )",
        &node_values(node_id, tenant_id, node, compression)?,
    )?;
    Ok(())
}

fn update_node(
    executor: &dyn SqlExecutor,
    node_id: &str,
    tenant_id: &str,
    node: &SttpNode,
    compression: AvecState,
) -> Result<()> {
    let mut params = node_values(node_id, tenant_id, node, compression)?;
    params.remove(0);
    params.push(SqlValue::text(node_id));
    exec(
        executor,
        "UPDATE temporal_node SET
            tenant_id = ?1, session_id = ?2, raw = ?3, tier = ?4, timestamp_ms = ?5,
            compression_depth = ?6, parent_node_id = ?7, sync_key = ?8, updated_at_ms = ?9,
            source_metadata = ?10, context_summary = ?11, semantic_tags = ?12, semantic_links = ?13,
            embedding = ?14, embedding_model = ?15, embedding_dimensions = ?16, embedded_at_ms = ?17,
            psi = ?18, rho = ?19, kappa = ?20,
            user_stability = ?21, user_friction = ?22, user_logic = ?23, user_autonomy = ?24, user_psi = ?25,
            model_stability = ?26, model_friction = ?27, model_logic = ?28, model_autonomy = ?29, model_psi = ?30,
            comp_stability = ?31, comp_friction = ?32, comp_logic = ?33, comp_autonomy = ?34, comp_psi = ?35
         WHERE id = ?36",
        &params,
    )?;
    Ok(())
}

fn load_nodes(executor: &dyn SqlExecutor, sql: &str, params: &[SqlValue]) -> Result<Vec<SttpNode>> {
    rows(executor, sql, params)?
        .iter()
        .map(map_node)
        .collect::<Result<Vec<_>>>()
        .context("read temporal nodes")
}

fn map_node(row: &SqlRow) -> Result<SttpNode> {
    let cell = |index: usize| -> Result<String> { row.text(index).map_err(|err| anyhow!("{err}")) };
    let opt = |index: usize| -> Result<Option<String>> {
        row.text_opt(index).map_err(|err| anyhow!("{err}"))
    };
    let int = |index: usize| -> Result<i64> { row.i64(index).map_err(|err| anyhow!("{err}")) };
    let int_opt = |index: usize| -> Result<Option<i64>> {
        row.i64_opt(index).map_err(|err| anyhow!("{err}"))
    };
    let real = |index: usize| -> Result<f32> {
        Ok(row.f64(index).map_err(|err| anyhow!("{err}"))? as f32)
    };
    let compression = AvecState {
        stability: real(31)?,
        friction: real(32)?,
        logic: real(33)?,
        autonomy: real(34)?,
    };
    let mut node = SttpNode {
        raw: cell(3)?,
        session_id: cell(2)?,
        tier: cell(4)?,
        timestamp: from_millis(int(5)?),
        compression_depth: i32::try_from(int(6)?).unwrap_or(0),
        parent_node_id: opt(7)?,
        sync_key: cell(8)?,
        updated_at: from_millis(int(9)?),
        source_metadata: optional_json(opt(10)?)?,
        context_summary: opt(11)?,
        semantic_tags: optional_json(opt(12)?)?,
        semantic_links: optional_json(opt(13)?)?,
        embedding: optional_json(opt(14)?)?,
        embedding_model: opt(15)?,
        embedding_dimensions: int_opt(16)?.map(|value| value as usize),
        embedded_at: int_opt(17)?.map(from_millis),
        psi: real(18)?,
        rho: real(19)?,
        kappa: real(20)?,
        user_avec: AvecState {
            stability: real(21)?,
            friction: real(22)?,
            logic: real(23)?,
            autonomy: real(24)?,
        },
        model_avec: AvecState {
            stability: real(26)?,
            friction: real(27)?,
            logic: real(28)?,
            autonomy: real(29)?,
        },
        compression_avec: Some(compression),
    };
    if node.sync_key.trim().is_empty() {
        node.sync_key = node.canonical_sync_key();
    }
    Ok(node)
}

fn map_checkpoint(row: SqlRow) -> Result<SyncCheckpoint> {
    let cursor_updated_at = row.i64_opt(2).map_err(|err| anyhow!("{err}"))?;
    let cursor_sync_key = row.text_opt(3).map_err(|err| anyhow!("{err}"))?;
    let cursor = match (cursor_updated_at, cursor_sync_key) {
        (Some(updated_at), Some(sync_key)) => Some(SyncCursor {
            updated_at: from_millis(updated_at),
            sync_key,
        }),
        _ => None,
    };
    Ok(SyncCheckpoint {
        session_id: row.text(0).map_err(|err| anyhow!("{err}"))?,
        connector_id: row.text(1).map_err(|err| anyhow!("{err}"))?,
        cursor,
        updated_at: from_millis(row.i64(5).map_err(|err| anyhow!("{err}"))?),
        metadata: optional_json(row.text_opt(4).map_err(|err| anyhow!("{err}"))?)?,
    })
}

fn map_tag_record(row: SqlRow) -> Result<SemanticTagRecord> {
    Ok(SemanticTagRecord {
        tenant_id: row.text(0).map_err(|err| anyhow!("{err}"))?,
        session_id: row.text(1).map_err(|err| anyhow!("{err}"))?,
        node_id: row.text(2).map_err(|err| anyhow!("{err}"))?,
        sync_key: row.text(3).map_err(|err| anyhow!("{err}"))?,
        tag: row.text(4).map_err(|err| anyhow!("{err}"))?,
        embedding: optional_json(row.text_opt(5).map_err(|err| anyhow!("{err}"))?)?,
        embedding_model: row.text_opt(6).map_err(|err| anyhow!("{err}"))?,
        embedding_dimensions: row
            .i64_opt(7)
            .map_err(|err| anyhow!("{err}"))?
            .map(|value| value as usize),
        embedded_at: row
            .i64_opt(8)
            .map_err(|err| anyhow!("{err}"))?
            .map(from_millis),
        updated_at: from_millis(row.i64(9).map_err(|err| anyhow!("{err}"))?),
    })
}

fn nodes_equivalent(existing: &SttpNode, candidate: &SttpNode, compression: AvecState) -> bool {
    existing.raw == candidate.raw
        && existing.tier == candidate.tier
        && existing.timestamp == candidate.timestamp
        && existing.compression_depth == candidate.compression_depth
        && existing.parent_node_id == candidate.parent_node_id
        && existing.context_summary == candidate.context_summary
        && existing.semantic_tags == candidate.semantic_tags
        && existing.semantic_links == candidate.semantic_links
        && vectors_equivalent(
            existing.embedding.as_deref(),
            candidate.embedding.as_deref(),
        )
        && existing.embedding_model == candidate.embedding_model
        && existing.embedding_dimensions == candidate.embedding_dimensions
        && existing.embedded_at == candidate.embedded_at
        && avec_equivalent(&existing.user_avec, &candidate.user_avec)
        && avec_equivalent(&existing.model_avec, &candidate.model_avec)
        && existing
            .compression_avec
            .is_some_and(|stored| avec_equivalent(&stored, &compression))
        && floats_equivalent(existing.rho, candidate.rho)
        && floats_equivalent(existing.kappa, candidate.kappa)
        && floats_equivalent(existing.psi, candidate.psi)
        && json_text(existing.source_metadata.as_ref()).ok()
            == json_text(candidate.source_metadata.as_ref()).ok()
}

fn resolved_compression(node: &SttpNode) -> AvecState {
    match node.compression_avec {
        Some(avec) if avec.psi() != 0.0 => avec,
        _ => node.model_avec,
    }
}

fn avec_equivalent(left: &AvecState, right: &AvecState) -> bool {
    floats_equivalent(left.stability, right.stability)
        && floats_equivalent(left.friction, right.friction)
        && floats_equivalent(left.logic, right.logic)
        && floats_equivalent(left.autonomy, right.autonomy)
}

fn vectors_equivalent(left: Option<&[f32]>, right: Option<&[f32]>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| floats_equivalent(*left, *right))
        }
        _ => false,
    }
}

fn floats_equivalent(left: f32, right: f32) -> bool {
    (left - right).abs() <= f32::EPSILON.mul_add(8.0, 1e-5)
}

fn normalized_tiers(tiers: Option<&[String]>) -> Option<Vec<String>> {
    let tiers = tiers
        .unwrap_or(&[])
        .iter()
        .map(|tier| tier.trim().to_ascii_lowercase())
        .filter(|tier| !tier.is_empty())
        .collect::<Vec<_>>();
    if tiers.is_empty() { None } else { Some(tiers) }
}

fn normalize_tenant_id(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        DEFAULT_TENANT.to_string()
    } else {
        trimmed.to_string()
    }
}

fn normalize_temporal_node_id(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Some((table, id)) = trimmed.split_once(':') {
        if table != "temporal_node" {
            return None;
        }
        let id = id.trim();
        if id.is_empty() {
            None
        } else {
            Some(id.to_string())
        }
    } else {
        Some(trimmed.to_string())
    }
}

fn delete_nodes(
    executor: &dyn SqlExecutor,
    request: NodeDeleteRequest,
) -> Result<NodeDeleteResult> {
    let tenant_id = normalize_tenant_id(&request.tenant_id);
    let session_id = request.session_id.trim();
    if session_id.is_empty() {
        return Err(anyhow!("session id cannot be empty"));
    }
    if request.sync_keys.is_empty() && request.node_ids.is_empty() {
        return Err(anyhow!("at least one sync key or node id is required"));
    }
    let mut records = Vec::new();
    let mut target_ids = BTreeSet::new();
    for sync_key in &request.sync_keys {
        let sync_key = sync_key.trim();
        if sync_key.is_empty() {
            continue;
        }
        let found = row_opt(
            executor,
            "SELECT id, sync_key FROM temporal_node \
             WHERE tenant_id = ?1 AND session_id = ?2 AND sync_key = ?3 LIMIT 1",
            &[
                SqlValue::text(&tenant_id),
                SqlValue::text(session_id),
                SqlValue::text(sync_key),
            ],
        )?;
        match found {
            Some(row) => {
                target_ids.insert(row.text(0).map_err(|err| anyhow!("{err}"))?);
            }
            None => records.push(NodeDeleteRecord {
                node_id: String::new(),
                sync_key: sync_key.to_string(),
                status: NodeDeleteStatus::NotFound,
                reason: Some("sync key not found in session scope".to_string()),
            }),
        }
    }
    for node_id in &request.node_ids {
        let Some(normalized) = normalize_temporal_node_id(node_id) else {
            records.push(NodeDeleteRecord {
                node_id: node_id.clone(),
                sync_key: String::new(),
                status: NodeDeleteStatus::NotFound,
                reason: Some("invalid node id".to_string()),
            });
            continue;
        };
        let found = row_opt(
            executor,
            "SELECT id, sync_key, session_id FROM temporal_node WHERE id = ?1",
            &[SqlValue::text(&normalized)],
        )?;
        match found {
            Some(row) if row.text(2).map_err(|err| anyhow!("{err}"))? == session_id => {
                target_ids.insert(row.text(0).map_err(|err| anyhow!("{err}"))?);
            }
            Some(row) => records.push(NodeDeleteRecord {
                node_id: normalized,
                sync_key: row.text(1).unwrap_or_default(),
                status: NodeDeleteStatus::NotFound,
                reason: Some("node id not found in session scope".to_string()),
            }),
            None => records.push(NodeDeleteRecord {
                node_id: normalized,
                sync_key: String::new(),
                status: NodeDeleteStatus::NotFound,
                reason: Some("node id not found".to_string()),
            }),
        }
    }
    for node_id in &target_ids {
        if let Some(row) = row_opt(
            executor,
            "SELECT sync_key FROM temporal_node WHERE id = ?1",
            &[SqlValue::text(node_id)],
        )? {
            let status = if request.dry_run {
                NodeDeleteStatus::Skipped
            } else {
                NodeDeleteStatus::Deleted
            };
            records.push(NodeDeleteRecord {
                node_id: node_id.clone(),
                sync_key: row.text(0).unwrap_or_default(),
                status,
                reason: request.dry_run.then(|| "would delete".to_string()),
            });
        }
    }
    if !request.dry_run {
        let ids = target_ids.clone();
        in_tx(executor, || {
            for node_id in &ids {
                exec(
                    executor,
                    "DELETE FROM temporal_node WHERE id = ?1",
                    &[SqlValue::text(node_id)],
                )?;
            }
            Ok(())
        })?;
    }
    Ok(summarize_delete(request.dry_run, 0, 0, records))
}

fn purge_session(
    executor: &dyn SqlExecutor,
    request: SessionPurgeRequest,
) -> Result<NodeDeleteResult> {
    let tenant_id = normalize_tenant_id(&request.tenant_id);
    let session_id = request.session_id.trim().to_string();
    if session_id.is_empty() {
        return Err(anyhow!("session id cannot be empty"));
    }
    let mut filter = NodeFilter::default();
    filter.exact_scope(&tenant_id, &session_id);
    filter.tiers(request.tiers.as_deref());
    let (where_sql, params) = filter.finish();
    let sql = format!("SELECT id, sync_key FROM temporal_node {where_sql} ORDER BY sync_key ASC");
    let targets = rows(executor, &sql, &params)?;
    let records = targets
        .iter()
        .map(|row| {
            Ok(NodeDeleteRecord {
                node_id: row.text(0).map_err(|err| anyhow!("{err}"))?,
                sync_key: row.text(1).map_err(|err| anyhow!("{err}"))?,
                status: if request.dry_run {
                    NodeDeleteStatus::Skipped
                } else {
                    NodeDeleteStatus::Deleted
                },
                reason: request.dry_run.then(|| "would delete".to_string()),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let calibrations_deleted = if request.include_calibration {
        count_scope(executor, "calibration", &tenant_id, &session_id)?
    } else {
        0
    };
    let checkpoints_deleted = if request.include_checkpoints {
        count_scope(executor, "sync_checkpoint", &tenant_id, &session_id)?
    } else {
        0
    };
    if !request.dry_run {
        in_tx(executor, || {
            exec(
                executor,
                &format!("DELETE FROM temporal_node {where_sql}"),
                &params,
            )?;
            exec(
                executor,
                "DELETE FROM semantic_tag_index WHERE tenant_id = ?1 AND session_id = ?2",
                &[SqlValue::text(&tenant_id), SqlValue::text(&session_id)],
            )?;
            if request.include_calibration {
                exec(
                    executor,
                    "DELETE FROM calibration WHERE tenant_id = ?1 AND session_id = ?2",
                    &[SqlValue::text(&tenant_id), SqlValue::text(&session_id)],
                )?;
            }
            if request.include_checkpoints {
                exec(
                    executor,
                    "DELETE FROM sync_checkpoint WHERE tenant_id = ?1 AND session_id = ?2",
                    &[SqlValue::text(&tenant_id), SqlValue::text(&session_id)],
                )?;
            }
            Ok(())
        })?;
    }
    Ok(summarize_delete(
        request.dry_run,
        calibrations_deleted,
        checkpoints_deleted,
        records,
    ))
}

fn summarize_delete(
    dry_run: bool,
    calibrations_deleted: usize,
    checkpoints_deleted: usize,
    records: Vec<NodeDeleteRecord>,
) -> NodeDeleteResult {
    let deleted = records
        .iter()
        .filter(|record| record.status == NodeDeleteStatus::Deleted)
        .count();
    let skipped = records
        .iter()
        .filter(|record| record.status == NodeDeleteStatus::Skipped)
        .count();
    let not_found = records
        .iter()
        .filter(|record| record.status == NodeDeleteStatus::NotFound)
        .count();
    NodeDeleteResult {
        dry_run,
        deleted: if dry_run { skipped } else { deleted },
        blocked: 0,
        not_found,
        skipped: if dry_run { 0 } else { skipped },
        calibrations_deleted,
        checkpoints_deleted,
        records,
    }
}

fn count_scope(
    executor: &dyn SqlExecutor,
    table: &str,
    tenant_id: &str,
    session_id: &str,
) -> Result<usize> {
    let row = row_opt(
        executor,
        &format!("SELECT COUNT(*) FROM {table} WHERE tenant_id = ?1 AND session_id = ?2"),
        &[SqlValue::text(tenant_id), SqlValue::text(session_id)],
    )?
    .context("count returned no row")?;
    Ok(row.i64(0).map_err(|err| anyhow!("{err}"))? as usize)
}

fn rekey(
    executor: &dyn SqlExecutor,
    node_ids: Vec<String>,
    target_tenant_id: &str,
    target_session_id: &str,
    dry_run: bool,
    allow_merge: bool,
) -> Result<BatchRekeyResult> {
    if node_ids.is_empty() {
        return Err(anyhow!("at least one node id is required"));
    }
    if target_session_id.trim().is_empty() {
        return Err(anyhow!("target session id cannot be empty"));
    }
    let normalized_node_ids = node_ids
        .iter()
        .filter_map(|node_id| normalize_temporal_node_id(node_id))
        .collect::<Vec<_>>();
    if normalized_node_ids.is_empty() {
        return Err(anyhow!("no valid node ids were provided"));
    }
    let mut missing_node_ids = Vec::new();
    let mut scopes = Vec::new();
    for node_id in &normalized_node_ids {
        let found = row_opt(
            executor,
            "SELECT tenant_id, session_id FROM temporal_node WHERE id = ?1",
            &[SqlValue::text(node_id)],
        )?;
        let Some(row) = found else {
            missing_node_ids.push(node_id.clone());
            continue;
        };
        let mut tenant_id = row.text(0).map_err(|err| anyhow!("{err}"))?;
        let session_id = row.text(1).map_err(|err| anyhow!("{err}"))?;
        if tenant_id.trim().is_empty() {
            tenant_id = derive_tenant_id_from_session(&session_id);
        }
        if !scopes.iter().any(|(tenant, session): &(String, String)| {
            tenant == &tenant_id && session == &session_id
        }) {
            scopes.push((tenant_id, session_id));
        }
    }
    let mut scope_results = Vec::new();
    let mut temporal_nodes_updated = 0usize;
    let mut calibrations_updated = 0usize;
    in_tx(executor, || {
        for (source_tenant_id, source_session_id) in &scopes {
            let same_scope =
                source_tenant_id == target_tenant_id && source_session_id == target_session_id;
            let include_legacy = source_tenant_id == DEFAULT_TENANT;
            let temporal_nodes = count_rekey_scope(
                executor,
                "temporal_node",
                source_tenant_id,
                source_session_id,
                include_legacy,
            )?;
            let calibrations = count_rekey_scope(
                executor,
                "calibration",
                source_tenant_id,
                source_session_id,
                include_legacy,
            )?;
            let (target_temporal_nodes, target_calibrations) = if same_scope {
                (0, 0)
            } else {
                (
                    count_rekey_scope(
                        executor,
                        "temporal_node",
                        target_tenant_id,
                        target_session_id,
                        target_tenant_id == DEFAULT_TENANT,
                    )?,
                    count_rekey_scope(
                        executor,
                        "calibration",
                        target_tenant_id,
                        target_session_id,
                        target_tenant_id == DEFAULT_TENANT,
                    )?,
                )
            };
            let conflict = !allow_merge
                && !same_scope
                && (target_temporal_nodes > 0 || target_calibrations > 0);
            let mut applied = false;
            let message = if same_scope {
                Some("source and target scopes are identical".to_string())
            } else if conflict {
                Some(
                    "target scope already contains rows; set allow_merge=true to override"
                        .to_string(),
                )
            } else if dry_run {
                None
            } else {
                apply_rekey_scope(
                    executor,
                    "temporal_node",
                    source_tenant_id,
                    source_session_id,
                    target_tenant_id,
                    target_session_id,
                    include_legacy,
                )?;
                apply_rekey_scope(
                    executor,
                    "calibration",
                    source_tenant_id,
                    source_session_id,
                    target_tenant_id,
                    target_session_id,
                    include_legacy,
                )?;
                applied = true;
                temporal_nodes_updated += temporal_nodes;
                calibrations_updated += calibrations;
                None
            };
            scope_results.push(ScopeRekeyResult {
                source_tenant_id: source_tenant_id.clone(),
                source_session_id: source_session_id.clone(),
                target_tenant_id: target_tenant_id.to_string(),
                target_session_id: target_session_id.to_string(),
                temporal_nodes,
                calibrations,
                target_temporal_nodes,
                target_calibrations,
                applied,
                conflict,
                message,
            });
        }
        Ok(())
    })?;
    Ok(BatchRekeyResult {
        dry_run,
        requested_node_ids: normalized_node_ids.len(),
        resolved_node_ids: normalized_node_ids
            .len()
            .saturating_sub(missing_node_ids.len()),
        missing_node_ids,
        scopes: scope_results,
        temporal_nodes_updated,
        calibrations_updated,
    })
}

fn count_rekey_scope(
    executor: &dyn SqlExecutor,
    table: &str,
    tenant_id: &str,
    session_id: &str,
    include_legacy: bool,
) -> Result<usize> {
    let row = row_opt(
        executor,
        &format!(
            "SELECT COUNT(*) FROM {table} \
             WHERE session_id = ?1 AND (tenant_id = ?2 OR (?3 = 1 AND (tenant_id IS NULL OR tenant_id = '')))"
        ),
        &[
            SqlValue::text(session_id),
            SqlValue::text(tenant_id),
            SqlValue::from(i64::from(include_legacy)),
        ],
    )?
    .context("rekey count returned no row")?;
    Ok(row.i64(0).map_err(|err| anyhow!("{err}"))? as usize)
}

fn apply_rekey_scope(
    executor: &dyn SqlExecutor,
    table: &str,
    source_tenant_id: &str,
    source_session_id: &str,
    target_tenant_id: &str,
    target_session_id: &str,
    include_legacy: bool,
) -> Result<()> {
    exec(
        executor,
        &format!(
            "UPDATE {table} SET tenant_id = ?1, session_id = ?2 \
             WHERE session_id = ?3 AND (tenant_id = ?4 OR (?5 = 1 AND (tenant_id IS NULL OR tenant_id = '')))"
        ),
        &[
            SqlValue::text(target_tenant_id),
            SqlValue::text(target_session_id),
            SqlValue::text(source_session_id),
            SqlValue::text(source_tenant_id),
            SqlValue::from(i64::from(include_legacy)),
        ],
    )?;
    Ok(())
}

fn lookup_embedding<'a>(
    embeddings: &'a HashMap<String, TagEmbedding>,
    tag: &str,
) -> Option<&'a TagEmbedding> {
    embeddings
        .get(tag)
        .or_else(|| embeddings.get(&tag.to_lowercase()))
        .or_else(|| {
            embeddings
                .iter()
                .find(|(candidate, _)| candidate.trim().eq_ignore_ascii_case(tag))
                .map(|(_, value)| value)
        })
}

fn like_prefix(prefix: &str) -> String {
    let mut escaped = String::new();
    for ch in prefix.chars() {
        if matches!(ch, '%' | '_' | '\\') {
            escaped.push('\\');
        }
        escaped.push(ch);
    }
    escaped.push('%');
    escaped
}

fn rank_nodes_hybrid(
    nodes: &mut [SttpNode],
    current_avec: AvecState,
    query_embedding: Option<&[f32]>,
    alpha: f32,
    beta: f32,
) {
    let alpha = alpha.clamp(0.0, 1.0);
    let beta = beta.clamp(0.0, 1.0);
    nodes.sort_by(|left, right| {
        let left_score = hybrid_score(left, current_avec, query_embedding, alpha, beta);
        let right_score = hybrid_score(right, current_avec, query_embedding, alpha, beta);
        right_score
            .total_cmp(&left_score)
            .then_with(|| right.updated_at.cmp(&left.updated_at))
    });
}

fn hybrid_score(
    node: &SttpNode,
    target_avec: AvecState,
    query_embedding: Option<&[f32]>,
    alpha: f32,
    beta: f32,
) -> f32 {
    let resonance = (1.0 - avec_distance(&node.model_avec, &target_avec)).clamp(0.0, 1.0);
    let semantic = match (query_embedding, node.embedding.as_deref()) {
        (Some(query), Some(node_vec)) => {
            cosine_similarity(query, node_vec).map(|value| (value + 1.0) / 2.0)
        }
        _ => None,
    };
    match semantic {
        Some(value) => (alpha * resonance + beta * value).clamp(0.0, 1.0),
        None => resonance,
    }
}

fn avec_distance(left: &AvecState, right: &AvecState) -> f32 {
    ((left.stability - right.stability).abs()
        + (left.friction - right.friction).abs()
        + (left.logic - right.logic).abs()
        + (left.autonomy - right.autonomy).abs())
        / 4.0
}

fn cosine_similarity(left: &[f32], right: &[f32]) -> Option<f32> {
    if left.is_empty() || right.is_empty() || left.len() != right.len() {
        return None;
    }
    let mut dot = 0.0f32;
    let mut left_norm = 0.0f32;
    let mut right_norm = 0.0f32;
    for (left, right) in left.iter().zip(right.iter()) {
        dot += left * right;
        left_norm += left * left;
        right_norm += right * right;
    }
    if left_norm <= f32::EPSILON || right_norm <= f32::EPSILON {
        return None;
    }
    Some(dot / (left_norm.sqrt() * right_norm.sqrt()))
}
