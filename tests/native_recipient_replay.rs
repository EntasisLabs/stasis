// Reuse immutable independent fixtures; run this target with the supplement_ filter.
include!("audit_probe.rs");
use stasis::application::runtime::job_continuation::settle_continuations;
use stasis::domain::runtime::job_continuation::ContinuationStatus;

async fn settle(s: &JobContextServices, p: &Job, event: &JobLifecycleEvent) -> Result<()> {
    settle_continuations(
        s.continuation_store.as_ref(),
        s.job_store.as_ref(),
        s.clock.as_ref(),
        s.id_generator.as_ref(),
        p,
        event,
        p.last_error.clone(),
    )
    .await
}
async fn reacquire(s: &JobContextServices) -> Job {
    let b = s
        .job_store
        .lease_due("q", "A", Utc::now(), 120, &WorkerCapabilities::any())
        .await
        .unwrap()
        .unwrap();
    transition::running_snapshot(s.job_store.as_ref(), b)
        .await
        .unwrap()
        .unwrap()
}
async fn reopen(rt: &Runtime, id: &str) {
    let ok = match rt {
        Runtime::Memory(r) => r.replay_dead_letter(id, Utc::now()).await.unwrap(),
        #[cfg(feature = "surreal-native")]
        Runtime::Surreal(r) => r.replay_dead_letter(id, Utc::now()).await.unwrap(),
    };
    assert!(ok);
}
async fn producer(s: &JobContextServices, id: &str) -> String {
    ContinuationBuilder::new(
        id.into(),
        Child,
        s.clock.clone(),
        s.id_generator.clone(),
        s.job_store.clone(),
        s.continuation_store.clone(),
    )
    .send()
    .await
    .unwrap()
    .continuation_id
}
async fn materialized(s: &JobContextServices, id: &str) {
    let e = s.continuation_store.get(id).await.unwrap().unwrap();
    assert_eq!(e.status, ContinuationStatus::Materialized);
    let child = s
        .job_store
        .get(e.child_job_id.as_deref().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(child.terminal_cycle, 1);
}

async fn recipient_signal_matrix(rt: Runtime) {
    let s = rt.services();
    // Real custom correlation capture, paused at direct-key lookup after A is sampled.
    let a = running(&s, "custom-signal").await;
    let ctx = context(&a, "A", s.clone());
    assert!(
        ctx.wait_for::<Signal>()
            .correlated_by("external-key")
            .await
            .is_err()
    );
    let reached = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let store = Arc::new(SignalCaptureBarrier {
        inner: s.job_store.clone(),
        gets: AtomicUsize::new(0),
        reached: reached.clone(),
        release: release.clone(),
    });
    let ss = s.clone();
    let dispatch = tokio::spawn(async move {
        runtime_job_dx::signal_event(
            store.as_ref(),
            ss.wait_store.as_ref(),
            ss.clock.as_ref(),
            "external-key".into(),
            Signal,
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), reached.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    s.job_store.cancel_execution(&a.id).await.unwrap();
    assert!(s.job_store.delete(&a.id).await.unwrap());
    let b = running(&s, &a.id).await;
    let ctx = context(&b, "A", s.clone());
    assert!(
        ctx.wait_for::<Signal>()
            .correlated_by("external-key")
            .await
            .is_err()
    );
    release.add_permits(1);
    assert!(dispatch.await.unwrap().unwrap());
    assert!(matches!(
        ctx.wait_for::<Signal>().correlated_by("external-key").await,
        Err(stasis::application::runtime::job_context::JobConsumeError::Deferred { .. })
    ));
    // A fresh dispatch may reach B; custom keys are not disabled by the fence.
    assert!(
        runtime_job_dx::signal_event(
            s.job_store.as_ref(),
            s.wait_store.as_ref(),
            s.clock.as_ref(),
            "external-key".into(),
            Signal
        )
        .await
        .unwrap()
    );
    assert!(
        ctx.wait_for::<Signal>()
            .correlated_by("external-key")
            .await
            .is_ok()
    );
    s.job_store.commit_execution(&b, success()).await.unwrap();

    // Buffered direct-key event, emitted before wait registration, stays A-scoped.
    let a = running(&s, "buffered-signal").await;
    assert!(
        runtime_job_dx::signal_event(
            s.job_store.as_ref(),
            s.wait_store.as_ref(),
            s.clock.as_ref(),
            a.id.clone(),
            Signal
        )
        .await
        .unwrap()
    );
    s.job_store.cancel_execution(&a.id).await.unwrap();
    s.job_store.delete(&a.id).await.unwrap();
    let b = running(&s, &a.id).await;
    assert!(matches!(
        context(&b, "A", s.clone()).wait_for::<Signal>().await,
        Err(stasis::application::runtime::job_context::JobConsumeError::Deferred { .. })
    ));
    s.job_store.commit_execution(&b, success()).await.unwrap();
    let c = running(&s, "buffered-current").await;
    runtime_job_dx::signal_event(
        s.job_store.as_ref(),
        s.wait_store.as_ref(),
        s.clock.as_ref(),
        c.id.clone(),
        Signal,
    )
    .await
    .unwrap();
    assert!(
        context(&c, "A", s.clone())
            .wait_for::<Signal>()
            .await
            .is_ok()
    );
}

async fn replay_matrix(rt: Runtime) {
    let s = rt.services();
    for control in [false, true] {
        for old_first in [false, true] {
            for defer in [false, true] {
                let id = format!("cycle-{control}-{old_first}-{defer}");
                s.job_store.insert(job(&id)).await.unwrap();
                let initial_producer = producer(&s, &id).await;
                let a = reacquire(&s).await;
                let (ap, ae) = if control {
                    let p = s.job_store.fail_execution(&id).await.unwrap().unwrap();
                    let event = JobLifecycleEvent::DeadLettered {
                        message: p.last_error.clone().unwrap(),
                    };
                    (p, event)
                } else {
                    s.job_store
                        .commit_execution(
                            &a,
                            JobExecutionOutcome::FatalFailure {
                                message: "old A failure".into(),
                                execution_id: None,
                                diagnostics: None,
                            },
                        )
                        .await
                        .unwrap();
                    let r = s
                        .job_store
                        .execution_receipt(&id, a.execution_epoch)
                        .await
                        .unwrap()
                        .unwrap();
                    (
                        serde_json::from_str::<Job>(&r.job_json).unwrap(),
                        r.lifecycle,
                    )
                };
                // A producer registering against an already-terminal parent still settles immediately.
                let terminal = producer(&s, &id).await;
                assert_eq!(
                    s.continuation_store
                        .get(&terminal)
                        .await
                        .unwrap()
                        .unwrap()
                        .status,
                    ContinuationStatus::Discarded
                );
                reopen(&rt, &id).await;
                // Before B even has a lease, its producer edge must not belong to A's old commit.
                let pe = producer(&s, &id).await;
                let persisted = s.continuation_store.get(&pe).await.unwrap().unwrap();
                assert!(persisted.parent_version_floor.unwrap() > ap.version);
                // Bypass the application filter: the native transaction must independently
                // reject old genuine failure/control authority for the new persisted edge.
                assert!(
                    !s.job_store
                        .settle_continuation(&ap, persisted, None, None)
                        .await
                        .unwrap()
                );
                let b = reacquire(&s).await;
                assert_eq!(a.incarnation, b.incarnation);
                assert_eq!(b.terminal_cycle, a.terminal_cycle + 1);
                let mut forged_control = b.clone();
                forged_control.terminal_cycle += 10;
                assert!(s.job_store.save(forged_control).await.is_err());
                let we = context(&b, "A", s.clone())
                    .continue_with(Child)
                    .send()
                    .await
                    .unwrap()
                    .continuation_id;
                let current = if defer {
                    s.job_store
                        .commit_execution(
                            &b,
                            JobExecutionOutcome::Deferred {
                                scheduled_at: Utc::now(),
                                message: "ordinary deferral".into(),
                                execution_id: None,
                                diagnostics: None,
                            },
                        )
                        .await
                        .unwrap();
                    reacquire(&s).await
                } else {
                    s.job_store
                        .commit_execution(
                            &b,
                            JobExecutionOutcome::RetryableFailure {
                                message: "ordinary retry".into(),
                                execution_id: None,
                                diagnostics: None,
                            },
                        )
                        .await
                        .unwrap();
                    reacquire(&s).await
                };
                assert!(current.execution_epoch > b.execution_epoch);
                assert_eq!(current.terminal_cycle, b.terminal_cycle);
                let mut forged_snapshot = current.clone();
                forged_snapshot.terminal_cycle += 20;
                s.job_store
                    .commit_execution(&forged_snapshot, success())
                    .await
                    .unwrap();
                let br = s
                    .job_store
                    .execution_receipt(&id, current.execution_epoch)
                    .await
                    .unwrap()
                    .unwrap();
                let bp = serde_json::from_str::<Job>(&br.job_json).unwrap();
                assert_eq!(bp.terminal_cycle, current.terminal_cycle);
                let ordered = if old_first {
                    [(&ap, &ae), (&bp, &br.lifecycle)]
                } else {
                    [(&bp, &br.lifecycle), (&ap, &ae)]
                };
                for (p, e) in ordered {
                    settle(&s, p, e).await.unwrap();
                }
                materialized(&s, &pe).await;
                materialized(&s, &we).await;
                // Re-delivery of both terminal outcomes is idempotent, not a second child.
                settle(&s, &ap, &ae).await.unwrap();
                settle(&s, &bp, &br.lifecycle).await.unwrap();
                materialized(&s, &we).await;
                assert_eq!(
                    s.continuation_store
                        .get(&initial_producer)
                        .await
                        .unwrap()
                        .unwrap()
                        .status,
                    ContinuationStatus::Discarded
                );
                let late_producer = producer(&s, &id).await;
                materialized(&s, &late_producer).await;
            }
        }
    }
}

async fn legacy_floor(rt: Runtime) {
    let s = rt.services();
    for missing_cycle in [false, true] {
        let a = running(&s, &format!("legacy-commit-binding-{missing_cycle}")).await;
        let edge = producer(&s, &a.id).await;
        let mut record = s.continuation_store.get(&edge).await.unwrap().unwrap();
        if missing_cycle {
            record.parent_terminal_cycle = 0;
        } else {
            record.parent_version_floor = None;
        }
        s.continuation_store.save(record).await.unwrap();
        s.job_store.commit_execution(&a, success()).await.unwrap();
        let r = s
            .job_store
            .execution_receipt(&a.id, a.execution_epoch)
            .await
            .unwrap()
            .unwrap();
        let p = serde_json::from_str::<Job>(&r.job_json).unwrap();
        assert!(settle(&s, &p, &r.lifecycle).await.is_err());
        let record = s.continuation_store.get(&edge).await.unwrap().unwrap();
        assert_eq!(record.status, ContinuationStatus::Pending);
        assert!(
            !s.job_store
                .settle_continuation(&p, record, None, None)
                .await
                .unwrap()
        );
    }
}

#[tokio::test]
async fn supplement_signal_memory() {
    recipient_signal_matrix(Runtime::memory()).await;
}
#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn supplement_signal_mem() {
    recipient_signal_matrix(Runtime::surreal().await).await;
}
#[tokio::test]
async fn supplement_cycles_memory() {
    replay_matrix(Runtime::memory()).await;
}
#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn supplement_cycles_mem() {
    replay_matrix(Runtime::surreal().await).await;
}
#[tokio::test]
async fn supplement_legacy_memory() {
    legacy_floor(Runtime::memory()).await;
}
#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn supplement_legacy_mem() {
    legacy_floor(Runtime::surreal().await).await;
}

#[cfg(feature = "surreal-native")]
#[tokio::test]
async fn supplement_kv_close_reopen() {
    use stasis::application::runtime::surreal_runtime::SurrealRuntime;
    async fn open(endpoint: &str) -> surrealdb::Surreal<surrealdb::engine::any::Any> {
        let db = surrealdb::Surreal::<surrealdb::engine::any::Any>::init();
        db.connect(endpoint).await.unwrap();
        db.use_ns("recipient-replay")
            .use_db("restart")
            .await
            .unwrap();
        db
    }
    let root = std::env::temp_dir().join(format!(
        "stasis-commit-floor-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let endpoint = format!("surrealkv://{}", root.display());
    let (old, new, edges, legacy) = {
        let db = open(&endpoint).await;
        let rt = Runtime::Surreal(SurrealRuntime::new(db.clone()));
        let s = rt.services();
        let mut old = Vec::new();
        let mut new = Vec::new();
        let mut edges = Vec::new();
        for control in [false, true] {
            for old_first in [false, true] {
                let id = format!("kv-cycle-{control}-{old_first}");
                let a = running(&s, &id).await;
                let (p, e) = if control {
                    let p = s.job_store.fail_execution(&id).await.unwrap().unwrap();
                    let e = JobLifecycleEvent::DeadLettered {
                        message: p.last_error.clone().unwrap(),
                    };
                    (p, e)
                } else {
                    s.job_store
                        .commit_execution(
                            &a,
                            JobExecutionOutcome::FatalFailure {
                                message: "durable old failure".into(),
                                execution_id: None,
                                diagnostics: None,
                            },
                        )
                        .await
                        .unwrap();
                    let r = s
                        .job_store
                        .execution_receipt(&id, a.execution_epoch)
                        .await
                        .unwrap()
                        .unwrap();
                    (serde_json::from_str(&r.job_json).unwrap(), r.lifecycle)
                };
                old.push((p, e, old_first));
                reopen(&rt, &id).await;
                edges.push(producer(&s, &id).await);
                let b = reacquire(&s).await;
                edges.push(
                    context(&b, "A", s.clone())
                        .continue_with(Child)
                        .send()
                        .await
                        .unwrap()
                        .continuation_id,
                );
                s.job_store.commit_execution(&b, success()).await.unwrap();
                let r = s
                    .job_store
                    .execution_receipt(&id, b.execution_epoch)
                    .await
                    .unwrap()
                    .unwrap();
                new.push((
                    serde_json::from_str::<Job>(&r.job_json).unwrap(),
                    r.lifecycle,
                ));
            }
        }
        let a = running(&s, "kv-legacy").await;
        let edge = producer(&s, &a.id).await;
        // Actual pre-binding row: field absent, not just a Rust None constructed decoder.
        db.query("UPDATE type::record('job_continuation',$id) UNSET parent_version_floor, parent_terminal_cycle")
            .bind(("id", edge.clone())).await.unwrap().check().unwrap();
        s.job_store.commit_execution(&a, success()).await.unwrap();
        let r = s
            .job_store
            .execution_receipt(&a.id, a.execution_epoch)
            .await
            .unwrap()
            .unwrap();
        (
            old,
            new,
            edges,
            (
                edge,
                serde_json::from_str::<Job>(&r.job_json).unwrap(),
                r.lifecycle,
            ),
        )
    };
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    {
        let rt = Runtime::Surreal(SurrealRuntime::new(open(&endpoint).await));
        let s = rt.services();
        for ((ap, ae, old_first), (bp, be)) in old.iter().zip(&new) {
            if *old_first {
                settle(&s, ap, ae).await.unwrap();
            }
            settle(&s, bp, be).await.unwrap();
            settle(&s, ap, ae).await.unwrap();
        }
        for edge in &edges {
            materialized(&s, edge).await;
        }
        assert!(settle(&s, &legacy.1, &legacy.2).await.is_err());
        assert_eq!(
            s.continuation_store
                .get(&legacy.0)
                .await
                .unwrap()
                .unwrap()
                .status,
            ContinuationStatus::Pending
        );
        // The same real capture/buffered matrices also run against the reopened KV backend.
        recipient_signal_matrix(rt).await;
    }
    // Keep backend files as graceful-restart evidence; no physical cleanup claim.
}
