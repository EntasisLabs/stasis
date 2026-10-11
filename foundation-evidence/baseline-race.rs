use stasis::application::runtime::{in_memory_runtime::InMemoryRuntime,typed_job::JobConsumer,job_context::{JobContext,JobResult}};
use stasis::domain::runtime::{typed_contract::StasisJob,job::{NewJob,BackoffPolicy,JobState},resource_lease::{ResourceKey,OwnerId}};
use stasis::ports::outbound::runtime::{job_store::JobStore,job_attempt_store::JobAttemptStore,resource_lease_store::ResourceLeaseStore};
use std::sync::{Arc,atomic::{AtomicUsize,Ordering}};
use tokio::sync::Notify;
use chrono::{Utc,Duration};
#[derive(serde::Serialize,serde::Deserialize)]struct Payload{}
impl StasisJob for Payload{const NAME:&'static str="proof.inert";const VERSION:u32=1;type Output=String;}
struct Consumer{entered:Arc<Notify>,release:Arc<Notify>,calls:Arc<AtomicUsize>}
#[async_trait::async_trait]impl JobConsumer<Payload> for Consumer{
 async fn consume(&self,_:Payload,ctx:JobContext)->JobResult<String>{
  assert!(ctx.fencing_token.is_none());
  let i=self.calls.fetch_add(1,Ordering::SeqCst);
  if i==0{self.entered.notify_one();self.release.notified().await;Ok("OLD_A_OUTPUT".into())}else{Ok("CURRENT_B_OUTPUT".into())}
 }
}
#[tokio::main]async fn main(){
 let rt=InMemoryRuntime::new();let entered=Arc::new(Notify::new());let release=Arc::new(Notify::new());let calls=Arc::new(AtomicUsize::new(0));
 rt.register_consumer::<Payload,_>(Consumer{entered:entered.clone(),release:release.clone(),calls}).unwrap();
 let now=Utc::now();let key=ResourceKey("proof-job".into());
 let old_fence=rt.lease_store.acquire(key.clone(),OwnerId("A".into()),Duration::seconds(60),now,false).await.unwrap().fencing_token;
 rt.enqueue(NewJob{id:"proof-job".into(),queue:"proof".into(),job_type:Payload::NAME.into(),payload_ref:r#"{"version":1,"payload":{}}"#.into(),priority:100,max_attempts:3,idempotency_key:"proof-idem".into(),correlation_id:"proof-corr".into(),causation_id:"proof-cause".into(),trace_id:"proof-trace".into(),input_provenance:None,placement:Default::default(),scheduled_at:now,backoff_policy:BackoffPolicy{base_delay_seconds:1,max_delay_seconds:1}}).await.unwrap();
 let a=rt.clone();let handle=tokio::spawn(async move{a.process_once("proof","A",now).await.unwrap()});
 entered.notified().await;
 assert!(rt.lease_store.validate_fence(&key,old_fence,now).await.unwrap());
 let later=now+Duration::seconds(1000);
 rt.recover_stale(later).await.unwrap();
 let reassigned=rt.lease_store.acquire(key.clone(),OwnerId("B".into()),Duration::seconds(60),later,true).await.unwrap();
 assert_ne!(reassigned.fencing_token,old_fence);
 assert!(!rt.lease_store.validate_fence(&key,old_fence,later).await.unwrap());
 assert_eq!(rt.process_once("proof","B",later+Duration::seconds(2)).await.unwrap(),Some("proof-job".into()));
 let before=rt.job_store.get("proof-job").await.unwrap().unwrap();assert_eq!(before.state,JobState::Succeeded);
 let before_attempts=rt.job_attempt_store.list_by_job_id("proof-job").await.unwrap();
 release.notify_one();handle.await.unwrap();
 let after=rt.job_store.get("proof-job").await.unwrap().unwrap();
 let attempts=rt.job_attempt_store.list_by_job_id("proof-job").await.unwrap();
 assert_ne!(before.output_provenance,after.output_provenance,"stale A overwrites B output");
 assert_eq!(after.attempts,0,"old local job snapshot overwrites retry count");
 assert_eq!(attempts.len(),before_attempts.len()+1,"automatic stale completion appended another attempt");
 assert!(attempts.iter().any(|a|a.worker_id=="A"));
 println!("NATIVE_IN_MEMORY_RACE_REPRODUCED old_fence={} current_fence={} before_attempts={} after_attempts={} after_retry_count={}",old_fence.0,reassigned.fencing_token.0,before_attempts.len(),attempts.len(),after.attempts);
 println!("No CI process, durable database, Linux containment, or safe terminal composition proven.");
}
