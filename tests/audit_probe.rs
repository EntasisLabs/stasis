include!("../foundation-evidence/review-75eec/reviewer_probe.rs");
struct SignalCaptureBarrier {
    inner: Arc<dyn JobStore>,
    gets: AtomicUsize,
    reached: Arc<Semaphore>,
    release: Arc<Semaphore>,
}
#[async_trait]
impl JobStore for SignalCaptureBarrier {
    async fn insert(&self, j: Job) -> Result<()> {
        self.inner.insert(j).await
    }
    async fn save(&self, j: Job) -> Result<()> {
        self.inner.save(j).await
    }
    async fn get(&self, id: &str) -> Result<Option<Job>> {
        if self.gets.fetch_add(1, Ordering::SeqCst) == 1 {
            self.reached.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
        self.inner.get(id).await
    }
    async fn lease_due(
        &self,
        q: &str,
        o: &str,
        n: DateTime<Utc>,
        t: i64,
        w: &WorkerCapabilities,
    ) -> Result<Option<Job>> {
        self.inner.lease_due(q, o, n, t, w).await
    }
    async fn heartbeat(&self, id: &str, o: &str, n: DateTime<Utc>, t: i64) -> Result<()> {
        self.inner.heartbeat(id, o, n, t).await
    }
    async fn list_by_state(&self, s: JobState) -> Result<Vec<Job>> {
        self.inner.list_by_state(s).await
    }
    async fn list_expired_leases(&self, n: DateTime<Utc>) -> Result<Vec<Job>> {
        self.inner.list_expired_leases(n).await
    }
    async fn delete(&self, id: &str) -> Result<bool> {
        self.inner.delete(id).await
    }
    async fn wake_incarnation(&self, id: &str, inc: u64) -> Result<()> { self.inner.wake_incarnation(id, inc).await }
    async fn prune_terminal_before(&self, n: DateTime<Utc>) -> Result<usize> {
        self.inner.prune_terminal_before(n).await
    }
}

async fn capture_race(rt: Runtime) {
 let s=rt.services(); let id="capture-race"; let a=running(&s,id).await;
 assert!(matches!(context(&a,"A",s.clone()).wait_for::<Signal>().await,Err(stasis::application::runtime::job_context::JobConsumeError::Deferred{..})));
 let reached=Arc::new(Semaphore::new(0)); let release=Arc::new(Semaphore::new(0));
 let store=Arc::new(SignalCaptureBarrier { inner:s.job_store.clone(), gets:AtomicUsize::new(0), reached:reached.clone(),release:release.clone() });
 let ss=s.clone();
 let task=tokio::spawn(async move {runtime_job_dx::signal_event(store.as_ref(),ss.wait_store.as_ref(),ss.clock.as_ref(),id.into(),Signal).await});
 tokio::time::timeout(std::time::Duration::from_secs(10),reached.acquire()).await.unwrap().unwrap().forget();
 // Production dispatcher has accepted A's pending wait and captured A. Pause only its second lookup.
 s.job_store.cancel_execution(id).await.unwrap(); assert!(s.job_store.delete(id).await.unwrap());
 let b=running(&s,id).await;
 assert!(matches!(context(&b,"A",s.clone()).wait_for::<Signal>().await,Err(stasis::application::runtime::job_context::JobConsumeError::Deferred{..})));
 release.add_permits(1); assert!(task.await.unwrap().unwrap());
 let got=context(&b,"A",s.clone()).wait_for::<Signal>().await;
 println!("A incarnation {}, B incarnation {}, successor consumed old signal={}",a.incarnation,b.incarnation,got.is_ok());
 assert!(matches!(got,Err(stasis::application::runtime::job_context::JobConsumeError::Deferred{..})),"delayed A dispatcher must not authorize B's wait");
}
#[tokio::test] async fn audit_capture_race_memory(){capture_race(Runtime::memory()).await;}
#[cfg(feature="surreal-native")]
#[tokio::test] async fn audit_capture_race_surreal(){capture_race(Runtime::surreal().await).await;}
async fn replay_edge_race(rt: Runtime) {
 let s=rt.services(); let id="retry-edge-race"; let a=running(&s,id).await;
 s.job_store.commit_execution(&a,JobExecutionOutcome::FatalFailure {message:"A dead".into(),execution_id:None,diagnostics:None}).await.unwrap();
 let ar=s.job_store.execution_receipt(id,a.execution_epoch).await.unwrap().unwrap();
 let ap:Job=serde_json::from_str(&ar.job_json).unwrap();
 match &rt { Runtime::Memory(r)=>assert!(r.replay_dead_letter(id,Utc::now()).await.unwrap()), #[cfg(feature="surreal-native")] Runtime::Surreal(r)=>assert!(r.replay_dead_letter(id,Utc::now()).await.unwrap()) }
 let b=s.job_store.lease_due("q","A",Utc::now(),120,&WorkerCapabilities::any()).await.unwrap().unwrap();
 let b=transition::running_snapshot(s.job_store.as_ref(),b).await.unwrap().unwrap();
 assert_eq!(a.incarnation,b.incarnation); assert_ne!(a.execution_epoch,b.execution_epoch);
 let edge=context(&b,"A",s.clone()).continue_with(Child).send().await.unwrap();
 s.job_store.commit_execution(&b,success()).await.unwrap();
 let br=s.job_store.execution_receipt(id,b.execution_epoch).await.unwrap().unwrap();
 let bp:Job=serde_json::from_str(&br.job_json).unwrap();
 for (p,r) in [(&ap,&ar),(&bp,&br)] {
 stasis::application::runtime::job_continuation::settle_continuations(s.continuation_store.as_ref(),s.job_store.as_ref(),s.clock.as_ref(),s.id_generator.as_ref(),p,&r.lifecycle,r.output_json.clone()).await.unwrap();
 }
 let record=s.continuation_store.get(&edge.continuation_id).await.unwrap().unwrap();
 println!("same-incarnation A epoch {}, B epoch {}, B success edge status={:?}",a.execution_epoch,b.execution_epoch,record.status);
 assert_eq!(record.status,stasis::domain::runtime::job_continuation::ContinuationStatus::Materialized,"A's old dead-letter receipt must not discard B's success edge");
}
#[tokio::test] async fn audit_replay_edge_memory(){replay_edge_race(Runtime::memory()).await;}
#[cfg(feature="surreal-native")]
#[tokio::test] async fn audit_replay_edge_surreal(){replay_edge_race(Runtime::surreal().await).await;}
