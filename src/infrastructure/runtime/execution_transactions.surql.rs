// Every canonical execution write is conditional in ONE native database transaction.
// The statement clock is native DB time, not the worker's process timestamp.
pub const COMMIT_EXECUTION: &str = r#"
BEGIN TRANSACTION;
LET $job = SELECT * FROM ONLY type::record($table, $job_id);
LET $receipt = SELECT * FROM ONLY type::record($receipt_table, $key);
LET $decision = IF $receipt != NONE {
    IF $receipt.owner = $owner AND $receipt.epoch = $epoch AND $receipt.version = $version AND $receipt.digest = $digest { 'same' } ELSE { 'conflict' }
} ELSE IF $canonical_base_valid AND $job != NONE AND $job.lease_owner = $owner AND $job.execution_epoch = $epoch
    AND $job.version = $version AND $job.state IN ['leased', 'running']
    AND (($recovery = false AND $job.lease_expires_at > time::now())
      OR ($recovery = true AND $job.lease_expires_at <= time::now())) {
    'committed'
} ELSE { 'stale' };
IF $decision = 'committed' {
    IF $recovery = false {
        LET $staged = SELECT * FROM type::table($staged_table) WHERE parent_id = $job_id AND epoch = $epoch AND version = $version;
        FOR $effect IN $staged {
            IF $effect.kind = 'child' {
                IF $effect.child_data.state != 'enqueued' OR $effect.child_data.attempts != 0 OR $effect.child_data.started_at != NONE OR $effect.child_data.finished_at != NONE OR $effect.child_data.last_error != NONE OR $effect.child_data.progress_json != NONE OR $effect.child_data.output_provenance_json != NONE OR $effect.child_data.sttp_output_node_id != NONE { THROW 'materialization requires new enqueued specification'; };
                LET $generation = UPSERT type::record($generation_table,$effect.canonical_id) SET epoch = (epoch ?? 0) + 1 RETURN AFTER;
                CREATE type::record($table, $effect.canonical_id) CONTENT $effect.child_data;
                UPDATE type::record($table,$effect.canonical_id) SET incarnation = $generation[0].epoch, terminal_cycle = 1, wakeup_pending = false, execution_epoch = 0, version = 0, lease_owner = NONE, lease_expires_at = NONE, heartbeat_at = NONE;
            };
            IF $effect.kind = 'continuation' {
                IF $effect.continuation_data.parent_job_id != $job_id OR $effect.continuation_data.parent_incarnation != $job.incarnation OR ($job.terminal_cycle ?? 0) = 0 OR ($effect.continuation_data.parent_terminal_cycle ?? 0) != $job.terminal_cycle OR $effect.continuation_data.parent_version_floor != $version { THROW 'stale continuation cycle binding'; };
                CREATE type::record('job_continuation', $effect.canonical_id) CONTENT $effect.continuation_data;
            };
            IF $effect.kind = 'event' { CREATE type::record('outbox_event', $effect.canonical_id) CONTENT $effect.event_data; };
        };
    };
    DELETE type::table($staged_table) WHERE parent_id = $job_id AND epoch = $epoch;
    LET $wakeup_at = time::now();
    -- Never replace canonical identity/specification with a caller-derived object.
    -- Rust derives transition counters and artifacts from the canonical base;
    -- owner/epoch/version above fence that base against any control-plane save.
    UPDATE type::record($table, $job_id) SET
        state = $job_data.state, attempts = $job_data.attempts,
        output_provenance_json = $job_data.output_provenance_json,
        sttp_output_node_id = $job_data.sttp_output_node_id,
        finished_at = $job_data.finished_at, last_error = $job_data.last_error,
        scheduled_at = IF $job_data.state = 'enqueued' { $job_data.scheduled_at } ELSE { $job.scheduled_at },
        version = $job.version + 1, lease_owner = NONE, lease_expires_at = NONE,
        heartbeat_at = NONE, wakeup_pending = false;
    IF $deferred AND ($job.wakeup_pending ?? false) {
        UPDATE type::record($table,$job_id) SET scheduled_at = $wakeup_at;
    };
    CREATE type::record('job_attempt', $key) CONTENT $attempt_data;
    CREATE type::record('outbox_event', $key) CONTENT $outbox_data;
    CREATE type::record($receipt_table, $key) CONTENT $receipt_data;
    UPDATE type::record($receipt_table,$key) SET job_snapshot = (SELECT * FROM ONLY type::record($table,$job_id));
    IF $deferred AND ($job.wakeup_pending ?? false) {
        UPDATE type::record($receipt_table,$key) SET effective_scheduled_at = $wakeup_at;
    };
};
IF $decision = 'committed' AND $recovery = false AND $job.lease_expires_at <= time::now() {
    THROW 'execution expired at native commit boundary';
};
RETURN $decision;
COMMIT TRANSACTION;
"#;

pub const ACQUIRE_EXECUTION: &str = r#"
BEGIN TRANSACTION;
LET $candidate = SELECT * FROM ONLY type::record($table, $job_id);
LET $leased = IF $candidate != NONE AND $candidate.state = 'enqueued'
    AND ($candidate.version ?? 0) = $version AND $candidate.queue = $queue
    AND $candidate.scheduled_at <= $now
    AND ($candidate.lease_expires_at = NONE OR $candidate.lease_expires_at <= time::now()) {
    LET $generation = UPSERT type::record($generation_table, $job_id)
        SET epoch = math::max([epoch ?? 0, $candidate.execution_epoch ?? 0]) + 1 RETURN AFTER;
    UPDATE type::record($table, $job_id)
        SET state = 'leased', lease_owner = $owner, execution_epoch = $generation[0].epoch,
            version = ($candidate.version ?? 0) + 1, heartbeat_at = time::now(),
            lease_expires_at = time::now() + $ttl RETURN AFTER;
} ELSE { [] };
RETURN $leased;
COMMIT TRANSACTION;
"#;

pub const CANCEL_EXECUTION: &str = r#"
BEGIN TRANSACTION;
LET $current = SELECT * FROM ONLY type::record($table, $job_id);
LET $committed = $current != NONE AND $current.version = $version
    AND $current.execution_epoch = $epoch AND ($current.incarnation ?? 0) = $job_data.incarnation AND $current.state IN ['enqueued', 'leased', 'running'];
IF $committed {
    UPDATE durable_wait SET status = 'cancelled', updated_at = time::now() WHERE job_id = $job_id AND incarnation = $job_data.incarnation AND status = 'pending';
    DELETE type::table($staged_table) WHERE parent_id = $job_id AND epoch = $epoch;
    UPDATE type::record($table, $job_id) CONTENT $job_data;
    CREATE type::record('outbox_event', $key) CONTENT $outbox_data;
    CREATE type::record($receipt_table, $key) CONTENT $receipt_data;
    IF $attempt_data != NONE { CREATE type::record('job_attempt',$key) CONTENT $attempt_data; };
    IF $cleanup_data != NONE {
        CREATE type::record($cleanup_table, $cleanup_key) CONTENT $cleanup_data;
    };
};
RETURN IF $committed { 'committed' } ELSE { 'stale' };
COMMIT TRANSACTION;
"#;
