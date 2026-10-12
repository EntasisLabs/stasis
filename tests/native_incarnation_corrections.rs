// Run the byte-identical independent production-path probe against fresh product sources.
include!("support/native_review_fixtures.rs");
review_fixtures!(
    stasis::application::runtime::in_memory_runtime::InMemoryRuntime,
    stasis::application::runtime::in_memory_runtime::InMemoryRuntime::new()
);

use stasis::domain::runtime::{
    durable_wait::{DurableSignalRecord, DurableWaitStatus},
    execution::CommitResult,
    job_continuation::ContinuationStatus,
};

async fn authority_matrix(rt: Runtime) {
    let s = rt.services();
    let parent = running(&s, "authority-matrix").await;
    for kind in ["running", "terminal", "output", "attempt", "progress"] {
        let mut child = job(&format!("bad-{kind}"));
        match kind {
            "running" => child.state = JobState::Running,
            "terminal" => child.state = JobState::Succeeded,
            "output" => child.set_sttp_output_node_id("forged"),
            "attempt" => child.attempts = 1,
            _ => child.progress_json = Some("forged".into()),
        }
        assert!(
            s.job_store
                .stage_child(&ExecutionGrant::from_job(&parent).unwrap(), child.clone())
                .await
                .is_err()
        );
        assert!(s.job_store.insert(child).await.is_err());
    }
    let mut child = job("normalized-child");
    child.execution_epoch = 500;
    child.version = 7;
    child.incarnation = 999;
    child.lease_owner = Some("caller".into());
    child.lease_expires_at = Some(Utc::now() + Duration::days(1));
    child.heartbeat_at = Some(Utc::now());
    child.wakeup_pending = true;
    s.job_store
        .stage_child(&ExecutionGrant::from_job(&parent).unwrap(), child)
        .await
        .unwrap();
    let edge = context(&parent, "A", s.clone())
        .continue_with(Child)
        .send()
        .await
        .unwrap();
    s.job_store
        .commit_execution(&parent, success())
        .await
        .unwrap();
    let terminal: Job = serde_json::from_str(
        &s.job_store
            .execution_receipt(&parent.id, parent.execution_epoch)
            .await
            .unwrap()
            .unwrap()
            .job_json,
    )
    .unwrap();
    let record = s
        .continuation_store
        .get(&edge.continuation_id)
        .await
        .unwrap()
        .unwrap();
    let mut forged = job("continuation-boundary-child");
    forged.state = JobState::Running;
    forged.execution_epoch = 500;
    forged.version = 7;
    forged.lease_owner = Some("caller".into());
    forged.lease_expires_at = Some(Utc::now() + Duration::days(1));
    assert!(
        s.job_store
            .settle_continuation(&terminal, record.clone(), Some(forged.clone()), None)
            .await
            .is_err()
    );
    assert_eq!(
        s.continuation_store
            .get(&record.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ContinuationStatus::Pending
    );
    forged.state = JobState::Enqueued;
    assert!(
        s.job_store
            .settle_continuation(&terminal, record.clone(), Some(forged), None)
            .await
            .unwrap()
    );
    let continuation_child = s
        .job_store
        .get("continuation-boundary-child")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            continuation_child.execution_epoch,
            continuation_child.version
        ),
        (0, 0)
    );
    assert!(
        continuation_child.lease_owner.is_none() && continuation_child.lease_expires_at.is_none()
    );
    s.job_store
        .cancel_execution(&continuation_child.id)
        .await
        .unwrap();
    let mut producer = job("normalized-producer");
    producer.execution_epoch = 500;
    producer.version = 7;
    producer.incarnation = 999;
    producer.lease_owner = Some("caller".into());
    producer.lease_expires_at = Some(Utc::now() + Duration::days(1));
    s.job_store.insert(producer).await.unwrap();
    let producer = s
        .job_store
        .get("normalized-producer")
        .await
        .unwrap()
        .unwrap();
    assert_eq!((producer.execution_epoch, producer.version), (0, 0));
    assert!(producer.incarnation != 999 && producer.lease_owner.is_none());
    s.job_store.cancel_execution(&producer.id).await.unwrap();
    let child = s.job_store.get("normalized-child").await.unwrap().unwrap();
    assert_eq!(child.state, JobState::Enqueued);
    assert_eq!((child.execution_epoch, child.version), (0, 0));
    assert!(child.incarnation > 0 && child.incarnation != 999);
    assert!(
        child.lease_owner.is_none()
            && child.lease_expires_at.is_none()
            && child.heartbeat_at.is_none()
            && !child.wakeup_pending
    );
    assert_eq!(
        s.job_store
            .commit_execution(&child, success())
            .await
            .unwrap(),
        CommitResult::Stale
    );
    let leased = s
        .job_store
        .lease_due("q", "native", Utc::now(), 120, &WorkerCapabilities::any())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(leased.id, child.id);
    assert_eq!(
        s.job_store
            .commit_execution(&leased, success())
            .await
            .unwrap(),
        CommitResult::Committed
    );
}

async fn legacy_identity_matrix(rt: Runtime) {
    let s = rt.services();
    let a = running(&s, "legacy-identity").await;
    assert!(
        context(&a, "A", s.clone())
            .wait_for::<Signal>()
            .await
            .is_err()
    );
    let wait = s
        .wait_store
        .list_pending_by_job(&a.id)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let mut legacy_wait = wait.clone();
    legacy_wait.incarnation = 0;
    s.wait_store.save_wait(legacy_wait).await.unwrap();
    assert!(matches!(
        context(&a, "A", s.clone()).wait_for::<Signal>().await,
        Err(stasis::application::runtime::job_context::JobConsumeError::Port(_))
    ));
    s.wait_store.save_wait(wait).await.unwrap();
    assert!(matches!(
        context(&a, "A", s.clone()).wait_for::<Signal>().await,
        Err(stasis::application::runtime::job_context::JobConsumeError::Deferred { .. })
    ));
    let edge = context(&a, "A", s.clone())
        .continue_with(Child)
        .send()
        .await
        .unwrap();
    s.job_store.commit_execution(&a, success()).await.unwrap();
    let original = s
        .continuation_store
        .get(&edge.continuation_id)
        .await
        .unwrap()
        .unwrap();
    let mut legacy = original.clone();
    legacy.parent_incarnation = 0;
    s.continuation_store.save(legacy).await.unwrap();
    let replay = match &rt {
        Runtime::Memory(r) => r.replay_lifecycle().await,
        #[cfg(feature = "surreal-native")]
        Runtime::Surreal(r) => r.replay_lifecycle().await,
    };
    assert!(replay.is_err(), "ambiguous edge must keep delivery pending");
    assert!(
        !s.job_store
            .execution_receipt(&a.id, a.execution_epoch)
            .await
            .unwrap()
            .unwrap()
            .delivered
    );
    assert_eq!(
        s.continuation_store
            .get(&edge.continuation_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ContinuationStatus::Pending
    );
    // Test operator reconciliation uses the original proved identity, never a current-name guess.
    s.continuation_store.save(original).await.unwrap();
    assert_eq!(rt.replay().await, 1);
    assert_eq!(rt.replay().await, 0);
}
#[tokio::test]
async fn memory_legacy_identity_fails_closed() {
    legacy_identity_matrix(Runtime::memory()).await;
}
#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn surreal_legacy_identity_fails_closed() {
    legacy_identity_matrix(Runtime::surreal().await).await;
}

#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn surreal_revalidates_persisted_child_before_atomic_materialization() {
    use stasis::application::runtime::surreal_runtime::SurrealRuntime;
    let db = surrealdb::Surreal::<surrealdb::engine::any::Any>::init();
    db.connect("mem://").await.unwrap();
    db.use_ns("legacy-child").use_db("atomic").await.unwrap();
    let rt = SurrealRuntime::new(db.clone());
    let s = rt.job_context_services();
    let a = running(&s, "persisted-child").await;
    s.job_store
        .stage_child(
            &ExecutionGrant::from_job(&a).unwrap(),
            job("malformed-child"),
        )
        .await
        .unwrap();
    // Model a staged row retained from the rejected writer, not authority granted to DB callers.
    db.query("UPDATE job_execution_staged SET child_data.state = 'running', child_data.execution_epoch = 500, child_data.version = 7, child_data.lease_owner = 'caller', child_data.lease_expires_at = time::now() + 1d WHERE parent_id = $parent").bind(("parent",a.id.clone())).await.unwrap().check().unwrap();
    assert!(s.job_store.commit_execution(&a, success()).await.is_err());
    assert_eq!(
        s.job_store.get(&a.id).await.unwrap().unwrap().state,
        JobState::Running
    );
    assert!(s.job_store.get("malformed-child").await.unwrap().is_none());
    assert!(
        s.job_store
            .execution_receipt(&a.id, a.execution_epoch)
            .await
            .unwrap()
            .is_none()
    );
    assert!(s.outbox_store.list_pending(10).await.unwrap().is_empty());
    db.query(
        "UPDATE job_execution_staged SET child_data.state = 'enqueued' WHERE parent_id = $parent",
    )
    .bind(("parent", a.id.clone()))
    .await
    .unwrap()
    .check()
    .unwrap();
    assert_eq!(
        s.job_store.commit_execution(&a, success()).await.unwrap(),
        CommitResult::Committed
    );
    let child = s.job_store.get("malformed-child").await.unwrap().unwrap();
    assert_eq!(child.state, JobState::Enqueued);
    assert_eq!((child.execution_epoch, child.version), (0, 0));
    assert!(child.lease_owner.is_none() && child.lease_expires_at.is_none());
}

async fn wait_state_matrix(rt: Runtime) {
    let s = rt.services();
    for mode in ["cancel", "fail", "signal", "timeout", "pending"] {
        let id = format!("wait-retained-{mode}");
        let a = running(&s, &id).await;
        let ctx = context(&a, "A", s.clone());
        if mode == "timeout" {
            assert!(
                ctx.wait_for::<Signal>()
                    .timeout(std::time::Duration::ZERO)
                    .await
                    .is_err()
            );
            assert!(matches!(
                ctx.wait_for::<Signal>().await,
                Err(stasis::application::runtime::job_context::JobConsumeError::Timeout(_))
            ));
        } else {
            assert!(ctx.wait_for::<Signal>().await.is_err());
        }
        if mode == "signal" {
            runtime_job_dx::signal_event(
                s.job_store.as_ref(),
                s.wait_store.as_ref(),
                s.clock.as_ref(),
                id.clone(),
                Signal,
            )
            .await
            .unwrap();
            ctx.wait_for::<Signal>().await.unwrap();
        }
        let old = s
            .wait_store
            .list_by_signal(Signal::NAME, &id)
            .await
            .unwrap()
            .into_iter()
            .find(|w| w.incarnation == a.incarnation)
            .unwrap();
        if mode == "cancel" {
            s.job_store.cancel_execution(&id).await.unwrap();
        } else if mode == "fail" {
            s.job_store.fail_execution(&id).await.unwrap();
        } else {
            s.job_store.commit_execution(&a, success()).await.unwrap();
        }
        assert!(s.job_store.delete(&id).await.unwrap());
        let b = running(&s, &id).await;
        let ctx = context(&b, "A", s.clone());
        assert!(
            matches!(
                ctx.wait_for::<Signal>().await,
                Err(stasis::application::runtime::job_context::JobConsumeError::Deferred { .. })
            ),
            "retired {mode} contaminated successor"
        );
        let fresh = s
            .wait_store
            .list_pending_by_job(&id)
            .await
            .unwrap()
            .into_iter()
            .find(|w| w.incarnation == b.incarnation)
            .unwrap();
        assert_ne!(fresh.wait_id, old.wait_id);
        // Delayed dispatcher has captured A. Both stored signal and wake remain A-scoped.
        let signal_id = format!("delayed-{id}");
        s.wait_store
            .insert_signal(DurableSignalRecord {
                signal_id: signal_id.clone(),
                target_incarnations: Some(vec![old.target_identity()]),
                signal_type: Signal::NAME.into(),
                correlation_key: id.clone(),
                payload_json: "null".into(),
                created_at: Utc::now(),
            })
            .await
            .unwrap();
        s.wait_store
            .complete_wait(
                &old.wait_id,
                DurableWaitStatus::Signaled,
                Some("null".into()),
                Some(signal_id),
                Utc::now(),
            )
            .await
            .unwrap();
        s.job_store
            .wake_incarnation(&id, old.incarnation)
            .await
            .unwrap();
        let current = s.job_store.get(&id).await.unwrap().unwrap();
        assert_eq!(current.scheduled_at, b.scheduled_at);
        assert_eq!(current.version, b.version);
        assert!(matches!(
            ctx.wait_for::<Signal>().await,
            Err(stasis::application::runtime::job_context::JobConsumeError::Deferred { .. })
        ));
        rt.replay().await; // Late cancellation/failure hooks must not cancel B.
        assert!(matches!(
            ctx.wait_for::<Signal>().await,
            Err(stasis::application::runtime::job_context::JobConsumeError::Deferred { .. })
        ));
        assert!(
            runtime_job_dx::signal_event(
                s.job_store.as_ref(),
                s.wait_store.as_ref(),
                s.clock.as_ref(),
                id.clone(),
                Signal
            )
            .await
            .unwrap()
        );
        ctx.wait_for::<Signal>().await.unwrap();
        assert!(
            !runtime_job_dx::signal_event(
                s.job_store.as_ref(),
                s.wait_store.as_ref(),
                s.clock.as_ref(),
                id.clone(),
                Signal
            )
            .await
            .unwrap()
        );
        s.job_store.cancel_execution(&id).await.unwrap();
    }
}

async fn continuation_order_matrix(rt: Runtime) {
    let s = rt.services();
    for b_success in [false, true] {
        for old_first in [false, true] {
            let id = format!("edges-{b_success}-{old_first}");
            let a = running(&s, &id).await;
            let aedge = context(&a, "A", s.clone())
                .continue_with(Child)
                .send()
                .await
                .unwrap();
            context(&a, "A", s.clone())
                .progress("A native progress")
                .await
                .unwrap();
            let submitted = success();
            s.job_store
                .commit_execution(&a, submitted.clone())
                .await
                .unwrap();
            let areceipt = s
                .job_store
                .execution_receipt(&id, a.execution_epoch)
                .await
                .unwrap()
                .unwrap();
            let aterminal: Job = serde_json::from_str(&areceipt.job_json).unwrap();
            assert_eq!(
                serde_json::to_value(&aterminal).unwrap(),
                serde_json::to_value(s.job_store.get(&id).await.unwrap().unwrap()).unwrap()
            );
            assert_eq!(
                s.job_store.commit_execution(&a, submitted).await.unwrap(),
                CommitResult::AlreadyCommittedSameDigest
            );
            assert_eq!(
                s.job_store.commit_execution(&a, deferred()).await.unwrap(),
                CommitResult::Conflict
            );
            s.job_store.delete(&id).await.unwrap();
            let b = running(&s, &id).await;
            let bedge = context(&b, "A", s.clone())
                .continue_with(Child)
                .send()
                .await
                .unwrap();
            let outcome = if b_success {
                success()
            } else {
                JobExecutionOutcome::FatalFailure {
                    message: "B failed".into(),
                    execution_id: None,
                    diagnostics: None,
                }
            };
            s.job_store.commit_execution(&b, outcome).await.unwrap();
            let breceipt = s
                .job_store
                .execution_receipt(&id, b.execution_epoch)
                .await
                .unwrap()
                .unwrap();
            let bterminal: Job = serde_json::from_str(&breceipt.job_json).unwrap();
            s.job_store.delete(&id).await.unwrap();
            let order = if old_first {
                vec![(&aterminal, &areceipt), (&bterminal, &breceipt)]
            } else {
                vec![(&bterminal, &breceipt), (&aterminal, &areceipt)]
            };
            for (parent, receipt) in order {
                stasis::application::runtime::job_continuation::settle_continuations(
                    s.continuation_store.as_ref(),
                    s.job_store.as_ref(),
                    s.clock.as_ref(),
                    s.id_generator.as_ref(),
                    parent,
                    &receipt.lifecycle,
                    receipt.output_json.clone(),
                )
                .await
                .unwrap();
            }
            let ar = s
                .continuation_store
                .get(&aedge.continuation_id)
                .await
                .unwrap()
                .unwrap();
            let br = s
                .continuation_store
                .get(&bedge.continuation_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(ar.status, ContinuationStatus::Materialized);
            assert_eq!(
                br.status,
                if b_success {
                    ContinuationStatus::Materialized
                } else {
                    ContinuationStatus::Discarded
                }
            );
            let child = s
                .job_store
                .get(ar.child_job_id.as_ref().unwrap())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(child.state, JobState::Enqueued);
            assert_eq!(child.execution_epoch, 0);
            assert!(child.lease_owner.is_none());
            rt.replay().await;
            assert_eq!(rt.replay().await, 0);
            assert_eq!(
                s.continuation_store
                    .get(&ar.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .child_job_id,
                ar.child_job_id
            );
            // Retire materialized children so next iteration leases only its own parent.
            for edge in [ar, br] {
                if let Some(child_id) = edge.child_job_id {
                    s.job_store.cancel_execution(&child_id).await.unwrap();
                }
            }
        }
    }
}

#[tokio::test]
async fn memory_authority_matrix() {
    authority_matrix(Runtime::memory()).await;
}
#[tokio::test]
async fn memory_retained_wait_states() {
    wait_state_matrix(Runtime::memory()).await;
}
#[tokio::test]
async fn memory_continuation_replay_orders() {
    continuation_order_matrix(Runtime::memory()).await;
}
#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn surreal_authority_matrix() {
    authority_matrix(Runtime::surreal().await).await;
}
#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn surreal_retained_wait_states() {
    wait_state_matrix(Runtime::surreal().await).await;
}
#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn surreal_continuation_replay_orders() {
    continuation_order_matrix(Runtime::surreal().await).await;
}

#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn actual_kv_retired_wait_and_continuation_restart() {
    use stasis::application::runtime::surreal_runtime::SurrealRuntime;
    async fn open(endpoint: &str) -> surrealdb::Surreal<surrealdb::engine::any::Any> {
        let db = surrealdb::Surreal::<surrealdb::engine::any::Any>::init();
        db.connect(endpoint).await.unwrap();
        db.use_ns("incarnation").use_db("reopen").await.unwrap();
        db
    }
    let root = std::env::temp_dir().join("stasis-store-runtime-safety-regressions")
        .join(format!(
            "incarnation-kv-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
    std::fs::create_dir_all(&root).unwrap();
    let endpoint = format!("surrealkv://{}", root.display());
    let (old_wait, edge, a) = {
        let rt = SurrealRuntime::new(open(&endpoint).await);
        let s = rt.job_context_services();
        let a = running(&s, "kv-retired").await;
        assert!(
            context(&a, "A", s.clone())
                .wait_for::<Signal>()
                .await
                .is_err()
        );
        let wait = s
            .wait_store
            .list_pending_by_job(&a.id)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let edge = context(&a, "A", s.clone())
            .continue_with(Child)
            .send()
            .await
            .unwrap();
        context(&a, "A", s.clone())
            .progress("persisted effective progress")
            .await
            .unwrap();
        s.job_store.commit_execution(&a, success()).await.unwrap();
        s.job_store.delete(&a.id).await.unwrap();
        let b = running(&s, &a.id).await;
        assert!(
            context(&b, "A", s.clone())
                .wait_for::<Signal>()
                .await
                .is_err()
        );
        (wait, edge, a)
    };
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    {
        let rt = Runtime::Surreal(SurrealRuntime::new(open(&endpoint).await));
        let s = rt.services();
        let b = s.job_store.get(&a.id).await.unwrap().unwrap();
        assert!(b.incarnation > a.incarnation);
        s.wait_store
            .complete_wait(
                &old_wait.wait_id,
                DurableWaitStatus::Cancelled,
                None,
                None,
                Utc::now(),
            )
            .await
            .unwrap();
        s.job_store
            .wake_incarnation(&b.id, a.incarnation)
            .await
            .unwrap();
        assert!(matches!(
            context(&b, "A", s.clone()).wait_for::<Signal>().await,
            Err(stasis::application::runtime::job_context::JobConsumeError::Deferred { .. })
        ));
        let r = s
            .job_store
            .execution_receipt(&a.id, a.execution_epoch)
            .await
            .unwrap()
            .unwrap();
        let j: Job = serde_json::from_str(&r.job_json).unwrap();
        assert_eq!(
            j.progress_json,
            Some("\"persisted effective progress\"".into())
        );
        assert_eq!(
            s.job_store.commit_execution(&a, success()).await.unwrap(),
            CommitResult::AlreadyCommittedSameDigest
        );
        s.job_store.fail_execution(&b.id).await.unwrap();
        s.job_store.delete(&b.id).await.unwrap();
        rt.replay().await;
        assert_eq!(rt.replay().await, 0);
        let e = s
            .continuation_store
            .get(&edge.continuation_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(e.status, ContinuationStatus::Materialized);
        assert!(
            s.job_store
                .get(e.child_job_id.as_ref().unwrap())
                .await
                .unwrap()
                .is_some()
        );
    }
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    std::fs::remove_dir_all(root).unwrap();
}
