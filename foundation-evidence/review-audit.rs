use chrono::Utc;
use stasis::application::runtime::{in_memory_runtime::InMemoryRuntime,surreal_runtime::SurrealRuntime,job_context::{JobContext,JobContextServices},runtime_job_dx,execution_commit::CommitResult};
use stasis::domain::runtime::{job::{Job,NewJob,JobState},placement::WorkerCapabilities,execution::{JobExecutionOutcome,ExecutionGrant},typed_contract::{StasisJob,StasisEvent}};
use stasis::ports::outbound::runtime::job_store::JobStore;
#[derive(serde::Serialize,serde::Deserialize)] struct Child;
impl StasisJob for Child { const NAME:&'static str="review.child"; const VERSION:u32=1; type Output=String; }
#[derive(serde::Serialize,serde::Deserialize)] struct Signal;
impl StasisEvent for Signal { const NAME:&'static str="review.signal"; const VERSION:u32=1; }
fn job(id:&str)->Job { NewJob{id:id.into(),queue:"q".into(),job_type:"inert".into(),payload_ref:"{}".into(),priority:0,max_attempts:4,idempotency_key:id.into(),correlation_id:id.into(),causation_id:id.into(),trace_id:id.into(),input_provenance:None,placement:Default::default(),scheduled_at:Utc::now(),backoff_policy:Default::default()}.into_job() }
async fn checks(label:&str,s:JobContextServices) {
 let js=s.job_store.clone();
 js.insert(job("recreated-cancel")).await.unwrap(); js.cancel_execution("recreated-cancel").await.unwrap().unwrap(); js.delete("recreated-cancel").await.unwrap(); js.insert(job("recreated-cancel")).await.unwrap();
 let second=js.cancel_execution("recreated-cancel").await;
 println!("{label} repeated queued cancellation: {second:?}");
 if label=="surreal" { assert!(second.is_err()); assert_eq!(js.get("recreated-cancel").await.unwrap().unwrap().state,JobState::Enqueued); js.fail_execution("recreated-cancel").await.unwrap(); }
 js.insert(job("signal-active")).await.unwrap(); let mut j=js.lease_due("q","owner",Utc::now(),120,&WorkerCapabilities::any()).await.unwrap().unwrap(); assert_eq!(j.id,"signal-active"); j.state=JobState::Running; js.save(j).await.unwrap(); let j=js.get("signal-active").await.unwrap().unwrap();
 let (_tx,rx)=tokio::sync::watch::channel(false); let ctx=JobContext::new(&j,"owner",rx,s.clone(),120);
 assert!(ctx.wait_for::<Signal>().await.is_err());
 runtime_job_dx::signal_event(js.as_ref(),s.wait_store.as_ref(),s.clock.as_ref(),j.id.clone(),Signal).await.unwrap();
 let current=js.get(&j.id).await.unwrap().unwrap(); assert!(current.version>j.version);
 let result=js.commit_execution(&j,JobExecutionOutcome::Deferred{scheduled_at:Utc::now(),message:"waiting".into(),execution_id:None,diagnostics:None}).await.unwrap();
 println!("{label} signal changes active version {} -> {}; subsequent native outcome {result:?}",j.version,current.version); assert_eq!(result,CommitResult::Stale);
 js.cancel_execution(&j.id).await.unwrap();
 js.insert(job("no-grant-cont")).await.unwrap(); let j=js.lease_due("q","owner",Utc::now(),120,&WorkerCapabilities::any()).await.unwrap().unwrap();
 let (_tx,rx)=tokio::sync::watch::channel(false); let ctx=JobContext::new(&j,"wrong-owner",rx,s.clone(),120); assert!(ctx.execution_grant.is_none());
 let r=ctx.continue_with(Child).send().await.unwrap(); assert!(s.continuation_store.get(&r.continuation_id).await.unwrap().is_some());
 println!("{label} wrong-owner no-grant context registered canonical continuation {}",r.continuation_id);
 let grant=ExecutionGrant::from_job(&j).unwrap(); js.cancel_execution(&j.id).await.unwrap(); let _=grant;
}
fn main(){tokio::runtime::Runtime::new().unwrap().block_on(async {
 let rt=InMemoryRuntime::new(); checks("memory",rt.job_context_services()).await;
 let db=surrealdb::Surreal::<surrealdb::engine::any::Any>::init(); db.connect("mem://").await.unwrap(); db.use_ns("review").use_db("review").await.unwrap(); let rt=SurrealRuntime::new(db); checks("surreal",rt.job_context_services()).await;
});}
