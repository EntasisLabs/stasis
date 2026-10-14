//! Versioned SQLite DDL for the Stasis runtime.
//!
//! This module is intentionally free of `rusqlite` (and of any SQLite driver).
//! Native stores execute these statements through `rusqlite`. The Durable Object
//! backend applies the same strings one statement at a time: Workers SQLite
//! rejects `BEGIN` / `PRAGMA`, and integer columns are read with `CAST(... AS TEXT)`
//! so nanosecond timestamps survive JavaScript's number type. Those adjustments
//! live in `script` and the executor stores, not in this DDL.
//!
//! Timestamps are INTEGER nanoseconds since the Unix epoch (UTC) so ordering
//! does not depend on RFC3339 formatting. Documents that are awkward to flatten
//! (identity rows, string arrays, placement) keep a `payload` JSON column plus
//! the columns queries filter on.

/// `(version, sql)` migrations applied in order. Each statement batch is idempotent
/// only through the version table; do not edit a shipped version in place.
pub const MIGRATIONS: &[(i64, &str)] = &[
    (1, MIGRATION_V1),
    (2, MIGRATION_V2),
    (3, MIGRATION_V3),
    (4, MIGRATION_V4),
];

/// One row payload, not the whole runtime. Durable Object SQLite rejects larger rows.
pub const MAX_ROW_PAYLOAD_BYTES: usize = 1024 * 1024;

/// Row-level native state. Migration code copies v1 tables and a format-1 ledger
/// into these tables inside the same transaction, then deletes those sources.
pub const MIGRATION_V3: &str = r#"
CREATE TABLE runtime_job (
    job_id TEXT PRIMARY KEY,
    queue TEXT NOT NULL,
    state TEXT NOT NULL,
    priority INTEGER NOT NULL,
    scheduled_at INTEGER NOT NULL,
    lease_expires_at INTEGER,
    finished_at INTEGER,
    execution_epoch INTEGER NOT NULL,
    version INTEGER NOT NULL,
    incarnation INTEGER NOT NULL,
    terminal_cycle INTEGER NOT NULL,
    payload TEXT NOT NULL
);
CREATE INDEX idx_runtime_job_due ON runtime_job (queue, state, scheduled_at, priority, job_id);
CREATE INDEX idx_runtime_job_lease ON runtime_job (state, lease_expires_at);
CREATE INDEX idx_runtime_job_finished ON runtime_job (state, finished_at);

CREATE TABLE runtime_epoch (
    job_id TEXT PRIMARY KEY,
    generation INTEGER NOT NULL
);

CREATE TABLE runtime_attempt (
    attempt_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    attempt_number INTEGER NOT NULL,
    guardrail_code TEXT,
    execution_id TEXT,
    finished_at INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    payload TEXT NOT NULL
);
CREATE INDEX idx_runtime_attempt_job ON runtime_attempt (job_id, seq);
CREATE INDEX idx_runtime_attempt_guard ON runtime_attempt (guardrail_code);
CREATE INDEX idx_runtime_attempt_exec ON runtime_attempt (execution_id);
CREATE INDEX idx_runtime_attempt_finished ON runtime_attempt (finished_at);

CREATE TABLE runtime_event (
    event_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    status TEXT NOT NULL,
    thread_id TEXT,
    execution_id TEXT,
    occurred_at INTEGER NOT NULL,
    next_attempt_at INTEGER,
    payload TEXT NOT NULL
);
CREATE INDEX idx_runtime_event_pending ON runtime_event (status, occurred_at, event_id);
CREATE INDEX idx_runtime_event_job ON runtime_event (job_id, occurred_at);
CREATE INDEX idx_runtime_event_thread ON runtime_event (thread_id, occurred_at);
CREATE INDEX idx_runtime_event_exec ON runtime_event (execution_id, occurred_at);

CREATE TABLE runtime_receipt (
    job_id TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    delivered INTEGER NOT NULL,
    retained_at INTEGER,
    digest TEXT NOT NULL,
    snapshot_version INTEGER NOT NULL,
    snapshot_incarnation INTEGER NOT NULL,
    snapshot_cycle INTEGER NOT NULL,
    snapshot_state TEXT NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY (job_id, epoch)
);
CREATE INDEX idx_runtime_receipt_open ON runtime_receipt (delivered);
CREATE INDEX idx_runtime_receipt_retain ON runtime_receipt (retained_at);
CREATE INDEX idx_runtime_receipt_identity ON runtime_receipt (
    job_id, snapshot_version, epoch, snapshot_incarnation, snapshot_cycle, snapshot_state
);

CREATE TABLE runtime_cancellation (
    cancel_key TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    delivered INTEGER NOT NULL,
    retained_at INTEGER,
    payload TEXT NOT NULL
);
CREATE INDEX idx_runtime_cancel_open ON runtime_cancellation (delivered);
CREATE INDEX idx_runtime_cancel_job ON runtime_cancellation (job_id);
CREATE INDEX idx_runtime_cancel_retain ON runtime_cancellation (retained_at);

CREATE TABLE runtime_cleanup (
    job_id TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    retained_at INTEGER,
    payload TEXT NOT NULL,
    PRIMARY KEY (job_id, epoch)
);
CREATE INDEX idx_runtime_cleanup_retain ON runtime_cleanup (retained_at);

CREATE TABLE runtime_staged (
    parent_id TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    version INTEGER NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY (parent_id, epoch, version)
);

CREATE TABLE runtime_wait (
    wait_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    incarnation INTEGER NOT NULL,
    signal_type TEXT NOT NULL,
    correlation_key TEXT NOT NULL,
    status TEXT NOT NULL,
    payload TEXT NOT NULL
);
CREATE INDEX idx_runtime_wait_signal ON runtime_wait (signal_type, correlation_key, status);
CREATE INDEX idx_runtime_wait_job ON runtime_wait (job_id, incarnation, status);

CREATE TABLE runtime_signal (
    signal_id TEXT PRIMARY KEY,
    signal_type TEXT NOT NULL,
    correlation_key TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    payload TEXT NOT NULL
);
CREATE INDEX idx_runtime_signal_key ON runtime_signal (signal_type, correlation_key, created_at, signal_id);

CREATE TABLE runtime_continuation (
    continuation_id TEXT PRIMARY KEY,
    parent_job_id TEXT NOT NULL,
    child_job_id TEXT,
    status TEXT NOT NULL,
    payload TEXT NOT NULL
);
CREATE INDEX idx_runtime_continuation_parent ON runtime_continuation (parent_job_id, status);
CREATE INDEX idx_runtime_continuation_child ON runtime_continuation (child_job_id);

CREATE TABLE runtime_recurring (
    recurring_id TEXT PRIMARY KEY,
    enabled INTEGER NOT NULL,
    next_run_at INTEGER NOT NULL,
    lease_expires_at INTEGER,
    payload TEXT NOT NULL
);
CREATE INDEX idx_runtime_recurring_due ON runtime_recurring (enabled, next_run_at, lease_expires_at);

CREATE TABLE runtime_id_alloc (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    next INTEGER NOT NULL
);
"#;

/// Historical snapshot ledger (format 1). Version 3 copies one row into the
/// `runtime_*` tables and deletes it. New runtime state is not written here.
pub const MIGRATION_V2: &str = r#"
CREATE TABLE runtime_safety_ledger (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    format INTEGER NOT NULL,
    payload TEXT NOT NULL
);
"#;

pub const MIGRATION_V1: &str = r#"
CREATE TABLE job (
    job_id TEXT PRIMARY KEY,
    queue TEXT NOT NULL,
    job_type TEXT NOT NULL,
    payload_ref TEXT NOT NULL,
    state TEXT NOT NULL,
    priority INTEGER NOT NULL,
    attempts INTEGER NOT NULL,
    max_attempts INTEGER NOT NULL,
    backoff_base_delay_seconds INTEGER NOT NULL,
    backoff_max_delay_seconds INTEGER NOT NULL,
    idempotency_key TEXT NOT NULL,
    correlation_id TEXT NOT NULL,
    causation_id TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    sttp_input_node_id TEXT NOT NULL,
    sttp_output_node_id TEXT,
    input_provenance_json TEXT,
    output_provenance_json TEXT,
    placement_json TEXT,
    lease_owner TEXT,
    lease_expires_at INTEGER,
    heartbeat_at INTEGER,
    scheduled_at INTEGER NOT NULL,
    started_at INTEGER,
    finished_at INTEGER,
    last_error TEXT,
    progress_json TEXT
);
CREATE INDEX idx_job_due ON job (queue, state, scheduled_at, priority);
CREATE INDEX idx_job_lease_expiry ON job (state, lease_expires_at);

CREATE TABLE outbox_event (
    event_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    publish_attempts INTEGER NOT NULL,
    published_at INTEGER,
    next_attempt_at INTEGER,
    last_publish_error TEXT,
    event_type TEXT NOT NULL,
    job_id TEXT NOT NULL,
    thread_id TEXT,
    correlation_id TEXT NOT NULL,
    causation_id TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    sttp_input_node_id TEXT NOT NULL,
    sttp_output_node_id TEXT,
    input_provenance_json TEXT,
    output_provenance_json TEXT,
    execution_id TEXT,
    input_memory_query_id TEXT,
    input_memory_query_fingerprint TEXT,
    output_memory_node_id TEXT,
    retrieval_path TEXT,
    occurred_at INTEGER NOT NULL,
    message TEXT
);
CREATE INDEX idx_outbox_status_due ON outbox_event (status, next_attempt_at, occurred_at);
CREATE INDEX idx_outbox_job ON outbox_event (job_id, occurred_at);
CREATE INDEX idx_outbox_thread ON outbox_event (thread_id, occurred_at);
CREATE INDEX idx_outbox_execution ON outbox_event (execution_id, occurred_at);

CREATE TABLE job_attempt (
    attempt_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    attempt_number INTEGER NOT NULL,
    worker_id TEXT NOT NULL,
    started_at INTEGER NOT NULL,
    finished_at INTEGER NOT NULL,
    outcome TEXT NOT NULL,
    error_message TEXT,
    sttp_output_node_id TEXT,
    output_provenance_json TEXT,
    execution_id TEXT,
    guardrail_code TEXT,
    policy_reason TEXT,
    duration_ms INTEGER,
    diagnostics TEXT
);
CREATE INDEX idx_job_attempt_job ON job_attempt (job_id, attempt_number);
CREATE INDEX idx_job_attempt_guardrail ON job_attempt (guardrail_code);
CREATE INDEX idx_job_attempt_execution ON job_attempt (execution_id);
CREATE INDEX idx_job_attempt_finished ON job_attempt (finished_at);

CREATE TABLE durable_wait (
    wait_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    signal_type TEXT NOT NULL,
    correlation_key TEXT NOT NULL,
    status TEXT NOT NULL,
    deadline_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    signal_payload TEXT,
    consumed_signal_ids_json TEXT NOT NULL
);
CREATE INDEX idx_durable_wait_signal ON durable_wait (signal_type, correlation_key, status);
CREATE INDEX idx_durable_wait_job ON durable_wait (job_id, status);

CREATE TABLE durable_signal (
    signal_id TEXT PRIMARY KEY,
    signal_type TEXT NOT NULL,
    correlation_key TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE INDEX idx_durable_signal_lookup ON durable_signal (signal_type, correlation_key, created_at);

CREATE TABLE job_continuation (
    continuation_id TEXT PRIMARY KEY,
    parent_job_id TEXT NOT NULL,
    trigger TEXT NOT NULL,
    status TEXT NOT NULL,
    child_spec_json TEXT NOT NULL,
    child_job_id TEXT,
    parent_output_json TEXT,
    claim_token TEXT,
    created_at INTEGER NOT NULL
);
CREATE INDEX idx_job_continuation_parent ON job_continuation (parent_job_id, status);
CREATE INDEX idx_job_continuation_child ON job_continuation (child_job_id);

CREATE TABLE recurring_definition (
    recurring_id TEXT PRIMARY KEY,
    queue TEXT NOT NULL,
    job_type TEXT NOT NULL,
    payload_template_ref TEXT NOT NULL,
    cron_expr TEXT NOT NULL,
    timezone TEXT NOT NULL,
    jitter_seconds INTEGER NOT NULL,
    enabled INTEGER NOT NULL,
    max_attempts INTEGER NOT NULL,
    next_run_at INTEGER NOT NULL,
    last_run_at INTEGER,
    lease_owner TEXT,
    lease_expires_at INTEGER
);
CREATE INDEX idx_recurring_due ON recurring_definition (enabled, next_run_at, lease_expires_at);

CREATE TABLE inbound_trigger (
    idempotency_key TEXT PRIMARY KEY,
    protocol TEXT NOT NULL,
    job_id TEXT NOT NULL,
    job_type TEXT NOT NULL,
    accepted_at INTEGER NOT NULL
);

CREATE TABLE resource_lease (
    resource TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    generation INTEGER NOT NULL,
    fencing_token INTEGER NOT NULL,
    expires_at INTEGER NOT NULL
);

CREATE TABLE thread (
    thread_id TEXT PRIMARY KEY,
    parent_thread_id TEXT,
    branch_label TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE thread_event (
    event_id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL,
    event_kind TEXT NOT NULL,
    payload_ref TEXT NOT NULL,
    occurred_at INTEGER NOT NULL
);
CREATE INDEX idx_thread_event_thread ON thread_event (thread_id, occurred_at);

CREATE TABLE cluster_node (
    node_id TEXT PRIMARY KEY,
    role TEXT NOT NULL,
    region TEXT NOT NULL,
    queue_ownership_json TEXT NOT NULL,
    capability_tags_json TEXT NOT NULL,
    heartbeat_at INTEGER NOT NULL,
    lease_expires_at INTEGER NOT NULL,
    metadata TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX idx_cluster_node_lease ON cluster_node (lease_expires_at);

CREATE TABLE delivery_endpoint (
    endpoint_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    protocol TEXT NOT NULL,
    target TEXT NOT NULL,
    metadata TEXT,
    enabled INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE endpoint_delivery_status (
    endpoint_id TEXT PRIMARY KEY,
    success_count INTEGER NOT NULL,
    failure_count INTEGER NOT NULL,
    last_event_id TEXT,
    last_error TEXT,
    last_success_at INTEGER,
    last_failure_at INTEGER,
    updated_at INTEGER NOT NULL
);
CREATE INDEX idx_endpoint_delivery_status_updated ON endpoint_delivery_status (updated_at);

CREATE TABLE workflow_definition (
    workflow_id TEXT PRIMARY KEY,
    queue TEXT NOT NULL,
    latest_revision_id TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE workflow_revision (
    revision_key TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    revision_id TEXT NOT NULL,
    source TEXT NOT NULL,
    graph_state_json TEXT,
    compiler_metadata_json TEXT,
    graph_modules_csv TEXT NOT NULL,
    graph_function_steps_csv TEXT NOT NULL,
    graph_function_inputs_json TEXT NOT NULL,
    reflected_at_utc INTEGER NOT NULL,
    executable_count INTEGER NOT NULL,
    reflection_receipt_json TEXT NOT NULL
);
CREATE INDEX idx_workflow_revision_workflow ON workflow_revision (workflow_id, reflected_at_utc);

CREATE TABLE cluster_forward_outcome (
    outcome_id INTEGER PRIMARY KEY AUTOINCREMENT,
    target_region TEXT NOT NULL,
    command_name TEXT NOT NULL,
    correlation_id TEXT,
    accepted INTEGER NOT NULL,
    attempts INTEGER NOT NULL,
    error TEXT,
    completed_at INTEGER NOT NULL
);
CREATE INDEX idx_cluster_forward_outcome_completed_at ON cluster_forward_outcome (completed_at);
CREATE INDEX idx_cluster_forward_outcome_corr ON cluster_forward_outcome (correlation_id);

CREATE TABLE identity_persona (
    persona_id TEXT PRIMARY KEY,
    payload TEXT NOT NULL
);
CREATE TABLE identity_user (
    user_id TEXT PRIMARY KEY,
    payload TEXT NOT NULL
);
CREATE TABLE identity_contact (
    contact_id TEXT PRIMARY KEY,
    payload TEXT NOT NULL
);
CREATE TABLE identity_channel_profile (
    channel_id TEXT PRIMARY KEY,
    payload TEXT NOT NULL
);
CREATE TABLE identity_policy_profile (
    policy_profile_id TEXT PRIMARY KEY,
    payload TEXT NOT NULL
);
CREATE TABLE identity_relationship (
    relationship_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    source_entity_type TEXT NOT NULL,
    source_entity_id TEXT NOT NULL,
    target_entity_type TEXT NOT NULL,
    target_entity_id TEXT NOT NULL,
    relationship_kind TEXT NOT NULL,
    payload TEXT NOT NULL
);
CREATE INDEX idx_identity_relationship_status ON identity_relationship (status);
CREATE INDEX idx_identity_relationship_endpoints ON identity_relationship (
    source_entity_type, source_entity_id, target_entity_type, target_entity_id
);
CREATE INDEX idx_identity_relationship_kind_status ON identity_relationship (relationship_kind, status);

CREATE TABLE identity_relationship_version (
    version_id TEXT PRIMARY KEY,
    relationship_id TEXT NOT NULL,
    payload TEXT NOT NULL
);
CREATE INDEX idx_identity_relationship_version_rel ON identity_relationship_version (relationship_id);

CREATE TABLE identity_entity_update_proposal (
    proposal_id TEXT PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    payload TEXT NOT NULL
);
CREATE INDEX idx_identity_proposal_lookup ON identity_entity_update_proposal (entity_type, entity_id, state);

CREATE TABLE identity_relationship_transition (
    event_id TEXT PRIMARY KEY,
    relationship_id TEXT NOT NULL,
    occurred_at INTEGER NOT NULL,
    payload TEXT NOT NULL
);
CREATE INDEX idx_identity_transition_rel_time ON identity_relationship_transition (relationship_id, occurred_at);
"#;

/// Locus node store and semantic index. The columns match `locus-sqlite-adapter`
/// 0.1.0 and `locus-durable-object` 0.1.0 (`temporal_node`, `calibration`,
/// `sync_checkpoint`, `semantic_tag_index`). Timestamps are unix milliseconds.
pub const MIGRATION_V4: &str = r#"
CREATE TABLE IF NOT EXISTS temporal_node (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL DEFAULT '',
    session_id TEXT NOT NULL,
    raw TEXT NOT NULL,
    tier TEXT NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    compression_depth INTEGER NOT NULL,
    parent_node_id TEXT,
    sync_key TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    source_metadata TEXT,
    context_summary TEXT,
    semantic_tags TEXT,
    semantic_links TEXT,
    embedding TEXT,
    embedding_model TEXT,
    embedding_dimensions INTEGER,
    embedded_at_ms INTEGER,
    psi REAL NOT NULL,
    rho REAL NOT NULL,
    kappa REAL NOT NULL,
    user_stability REAL NOT NULL,
    user_friction REAL NOT NULL,
    user_logic REAL NOT NULL,
    user_autonomy REAL NOT NULL,
    user_psi REAL NOT NULL,
    model_stability REAL NOT NULL,
    model_friction REAL NOT NULL,
    model_logic REAL NOT NULL,
    model_autonomy REAL NOT NULL,
    model_psi REAL NOT NULL,
    comp_stability REAL NOT NULL,
    comp_friction REAL NOT NULL,
    comp_logic REAL NOT NULL,
    comp_autonomy REAL NOT NULL,
    comp_psi REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS calibration (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL DEFAULT '',
    session_id TEXT NOT NULL,
    stability REAL NOT NULL,
    friction REAL NOT NULL,
    logic REAL NOT NULL,
    autonomy REAL NOT NULL,
    psi REAL NOT NULL,
    trigger TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS sync_checkpoint (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL DEFAULT '',
    session_id TEXT NOT NULL,
    connector_id TEXT NOT NULL,
    cursor_updated_at_ms INTEGER,
    cursor_sync_key TEXT,
    metadata TEXT,
    updated_at_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS semantic_tag_index (
    tenant_id TEXT NOT NULL DEFAULT '',
    session_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    sync_key TEXT NOT NULL,
    tag TEXT NOT NULL,
    embedding TEXT,
    embedding_model TEXT,
    embedding_dimensions INTEGER,
    embedded_at_ms INTEGER,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, sync_key, tag)
);
CREATE INDEX IF NOT EXISTS idx_node_session ON temporal_node (session_id);
CREATE INDEX IF NOT EXISTS idx_node_tenant_session ON temporal_node (tenant_id, session_id);
CREATE INDEX IF NOT EXISTS idx_node_tier ON temporal_node (tier);
CREATE INDEX IF NOT EXISTS idx_node_timestamp ON temporal_node (timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_node_change_cursor ON temporal_node (tenant_id, session_id, updated_at_ms, sync_key);
CREATE UNIQUE INDEX IF NOT EXISTS idx_node_sync_identity ON temporal_node (tenant_id, session_id, sync_key);
CREATE INDEX IF NOT EXISTS idx_tag_lookup ON semantic_tag_index (tenant_id, tag);
CREATE INDEX IF NOT EXISTS idx_tag_node ON semantic_tag_index (tenant_id, node_id);
CREATE INDEX IF NOT EXISTS idx_cal_session ON calibration (session_id);
CREATE INDEX IF NOT EXISTS idx_cal_tenant_session ON calibration (tenant_id, session_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_checkpoint_scope ON sync_checkpoint (tenant_id, session_id, connector_id);
"#;
