//! Federated job contract: provenance, placement leasing, fenced handoff, blobs, cross-runtime delivery.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{Duration, Utc};
use stasis::application::runtime::in_memory_runtime::InMemoryJobStore;
use stasis::application::runtime::in_memory_runtime::{JobExecutionOutcome, JobHandler};
use stasis::application::runtime::runtime_factory::RuntimeComposition;
use stasis::domain::runtime::blob_descriptor::BlobDescriptor;
use stasis::domain::runtime::federation::{
    FEDERATED_SIGNAL_SCHEMA_VERSION_V1, FEDERATED_TERMINAL_RESULT_SCHEMA_VERSION_V1,
    FederatedSignalEnvelope, FederatedTerminalResult, sign_federated_signal,
    sign_federated_terminal_result,
};
use stasis::domain::runtime::job::{BackoffPolicy, NewJob};
use stasis::domain::runtime::ownership_handoff::OwnershipHandoffPhase;
use stasis::domain::runtime::placement::{PlacementConstraints, WorkerCapabilities};
use stasis::domain::runtime::provenance::{ProvenanceRef, ProvenanceScheme, SttpProvenanceAdapter};
use stasis::domain::runtime::remote_job_envelope::{
    EnvelopeSignature, OriginAuthority, REMOTE_JOB_ENVELOPE_SCHEMA_VERSION_V1, RemoteJobEnvelope,
    TerminalDeliveryEndpoint, sign_remote_job_envelope, verify_remote_job_envelope,
};
use stasis::domain::runtime::resource_lease::{OwnerId, ResourceKey};
use stasis::infrastructure::runtime::in_memory_blob_transfer::InMemoryBlobTransfer;
use stasis::infrastructure::runtime::in_memory_federated_bus::InMemoryFederatedBus;
use stasis::infrastructure::runtime::in_memory_ownership_handoff_store::InMemoryOwnershipHandoffStore;
use stasis::infrastructure::runtime::in_memory_resource_lease_store::InMemoryResourceLeaseStore;
use stasis::ports::outbound::runtime::blob_transfer::BlobTransferPort;
use stasis::ports::outbound::runtime::federated_delivery::{
    FederatedDeliveryPort, FederatedIngressPort,
};
use stasis::ports::outbound::runtime::job_store::JobStore;
use stasis::ports::outbound::runtime::ownership_handoff_store::{
    OwnershipHandoffStore, ReserveOwnershipHandoff,
};
use stasis::ports::outbound::runtime::resource_lease_store::ResourceLeaseStore;
use stasis::sdk::runtime_sdk::RuntimeSdk;

#[test]
fn sttp_adapter_preserves_optional_provenance() {
    assert!(SttpProvenanceAdapter::from_compat_string("").is_none());
    let prov = SttpProvenanceAdapter::to_provenance("sttp:in:1");
    assert_eq!(prov.scheme, ProvenanceScheme::Sttp);
    assert_eq!(
        SttpProvenanceAdapter::to_compat_string(Some(&prov)),
        "sttp:in:1"
    );
    let cas = ProvenanceRef::cas(
        stasis::domain::runtime::provenance::ContentDigest::sha256_bytes(b"payload"),
    );
    assert!(SttpProvenanceAdapter::from_provenance(&cas).is_none());
}

#[tokio::test]
async fn placement_constraints_applied_atomically_during_lease() {
    let store = InMemoryJobStore::default();
    let now = Utc::now();

    store
        .insert(
            NewJob {
                id: "job-gpu".into(),
                queue: "default".into(),
                job_type: "federated.gpu".into(),
                payload_ref: "cas:abc".into(),
                priority: 10,
                max_attempts: 1,
                idempotency_key: "idem-gpu".into(),
                correlation_id: "corr".into(),
                causation_id: "cause".into(),
                trace_id: "trace".into(),
                input_provenance: None,
                placement: PlacementConstraints::unrestricted()
                    .require_capability("gpu")
                    .region("us-west")
                    .target_node("node-a"),
                scheduled_at: now,
                backoff_policy: BackoffPolicy::default(),
            }
            .into_job(),
        )
        .await
        .unwrap();

    let cpu_only = WorkerCapabilities::any()
        .with_capability("cpu")
        .region("us-west")
        .node_id("node-a");
    assert!(
        store
            .lease_due("default", "worker-cpu", now, 30, &cpu_only)
            .await
            .unwrap()
            .is_none()
    );

    let gpu_worker = WorkerCapabilities::any()
        .with_capability("gpu")
        .region("us-west")
        .node_id("node-a");
    let leased = store
        .lease_due("default", "worker-gpu", now, 30, &gpu_worker)
        .await
        .unwrap()
        .expect("gpu worker should lease matching job");
    assert_eq!(leased.id, "job-gpu");
    assert_eq!(leased.lease_owner.as_deref(), Some("worker-gpu"));
}

#[tokio::test]
async fn fenced_ownership_handoff_prevents_generation_conflict() {
    let leases: Arc<dyn ResourceLeaseStore> = Arc::new(InMemoryResourceLeaseStore::default());
    let handoffs = InMemoryOwnershipHandoffStore::new(leases.clone());
    let now = Utc::now();
    let resource = ResourceKey("res-1".into());

    let lease = leases
        .acquire(
            resource.clone(),
            OwnerId("node-a".into()),
            Duration::seconds(60),
            now,
            false,
        )
        .await
        .unwrap();

    let reservation = handoffs
        .reserve(ReserveOwnershipHandoff {
            resource: resource.clone(),
            from_owner: OwnerId("node-a".into()),
            to_owner: OwnerId("node-b".into()),
            fencing_token: lease.fencing_token,
            ttl: Duration::seconds(30),
            now,
            handoff_id: "ho-1".into(),
        })
        .await
        .unwrap();
    assert_eq!(reservation.handoff.phase, OwnershipHandoffPhase::Reserved);
    assert_eq!(reservation.handoff.generation, lease.generation);

    // Second reserve for same generation must fail while active.
    let conflict = handoffs
        .reserve(ReserveOwnershipHandoff {
            resource: resource.clone(),
            from_owner: OwnerId("node-a".into()),
            to_owner: OwnerId("node-c".into()),
            fencing_token: lease.fencing_token,
            ttl: Duration::seconds(30),
            now,
            handoff_id: "ho-2".into(),
        })
        .await;
    assert!(conflict.is_err());

    let transferred = handoffs
        .commit(
            "ho-1",
            lease.fencing_token,
            Duration::seconds(60),
            now + Duration::seconds(1),
        )
        .await
        .unwrap();
    assert_eq!(transferred.owner, OwnerId("node-b".into()));
    assert!(transferred.generation > lease.generation);

    // Stale fence from the pre-handoff generation cannot execute.
    assert!(
        !leases
            .validate_fence(&resource, lease.fencing_token, now + Duration::seconds(2))
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn concurrent_handoff_reservations_have_one_winner() {
    let leases: Arc<dyn ResourceLeaseStore> = Arc::new(InMemoryResourceLeaseStore::default());
    let handoffs = InMemoryOwnershipHandoffStore::new(leases.clone());
    let now = Utc::now();
    let resource = ResourceKey("res-race".into());
    let lease = leases
        .acquire(
            resource.clone(),
            OwnerId("node-a".into()),
            Duration::seconds(60),
            now,
            false,
        )
        .await
        .unwrap();
    let first = handoffs.reserve(ReserveOwnershipHandoff {
        resource: resource.clone(),
        from_owner: OwnerId("node-a".into()),
        to_owner: OwnerId("node-b".into()),
        fencing_token: lease.fencing_token,
        ttl: Duration::seconds(30),
        now,
        handoff_id: "ho-race-1".into(),
    });
    let second = handoffs.reserve(ReserveOwnershipHandoff {
        resource: resource.clone(),
        from_owner: OwnerId("node-a".into()),
        to_owner: OwnerId("node-c".into()),
        fencing_token: lease.fencing_token,
        ttl: Duration::seconds(30),
        now,
        handoff_id: "ho-race-2".into(),
    });
    let (first, second) = tokio::join!(first, second);
    assert_ne!(first.is_ok(), second.is_ok());
}

#[tokio::test]
async fn blob_transfer_port_keeps_artifacts_out_of_band() {
    let blobs = InMemoryBlobTransfer::new();
    let descriptor = blobs
        .put(b"large-artifact", Some("application/json"))
        .await
        .unwrap();
    assert!(descriptor.verify(b"large-artifact"));
    assert!(blobs.exists(&descriptor).await.unwrap());
    let fetched = blobs.get(&descriptor).await.unwrap();
    assert_eq!(fetched, b"large-artifact");
}

#[tokio::test]
async fn signed_remote_job_and_signals_cross_runtimes_without_shared_db() {
    let bus = InMemoryFederatedBus::new();
    bus.ensure_runtime("rt-a").unwrap();
    bus.ensure_runtime("rt-b").unwrap();

    let key = b"federation-shared-secret";
    bus.register_verification_key("key-1", key.to_vec())
        .unwrap();
    let payload = BlobDescriptor::from_bytes(br#"{"task":"echo"}"#);
    let mut envelope = RemoteJobEnvelope {
        schema_version: REMOTE_JOB_ENVELOPE_SCHEMA_VERSION_V1,
        envelope_id: "env-1".into(),
        job_type: "federated.echo".into(),
        payload: payload.clone(),
        idempotency_key: "idem-fed-1".into(),
        correlation_id: "corr-fed".into(),
        causation_id: "cause-fed".into(),
        deadline: Utc::now() + Duration::minutes(10),
        origin_authority: OriginAuthority {
            runtime_id: "rt-a".into(),
            authority_id: "auth-a".into(),
            realm: Some("prod".into()),
        },
        terminal_delivery: TerminalDeliveryEndpoint {
            endpoint_id: "ep-a".into(),
            protocol: "memory-bus".into(),
            address: "rt-a://terminal".into(),
        },
        placement: PlacementConstraints::unrestricted().require_capability("cpu"),
        signature: EnvelopeSignature {
            algorithm: EnvelopeSignature::HMAC_SHA256.into(),
            key_id: String::new(),
            signature_hex: String::new(),
        },
    };
    sign_remote_job_envelope(&mut envelope, "key-1", key).unwrap();
    verify_remote_job_envelope(&envelope, key).unwrap();

    let delivery_to_b = bus.delivery_port("rt-b");
    delivery_to_b
        .submit_remote_job(envelope.clone())
        .await
        .unwrap();
    delivery_to_b
        .submit_remote_job(envelope.clone())
        .await
        .unwrap();

    let mut signal = FederatedSignalEnvelope {
        schema_version: FEDERATED_SIGNAL_SCHEMA_VERSION_V1,
        signal_id: "sig-1".into(),
        signal_type: "ApprovalGranted".into(),
        correlation_key: "corr-fed".into(),
        payload: BlobDescriptor::from_bytes(br#"{"ok":true}"#),
        origin_authority: envelope.origin_authority.clone(),
        destination_runtime_id: "rt-a".into(),
        causation_id: "env-1".into(),
        correlation_id: "corr-fed".into(),
        occurred_at: Utc::now(),
        signature: EnvelopeSignature {
            algorithm: EnvelopeSignature::HMAC_SHA256.into(),
            key_id: String::new(),
            signature_hex: String::new(),
        },
    };
    sign_federated_signal(&mut signal, "key-1", key).unwrap();
    let mut tampered_signal = signal.clone();
    tampered_signal.signal_type = "Tampered".into();
    assert!(delivery_to_b.deliver_signal(tampered_signal).await.is_err());
    delivery_to_b.deliver_signal(signal.clone()).await.unwrap();

    let mut result = FederatedTerminalResult {
        schema_version: FEDERATED_TERMINAL_RESULT_SCHEMA_VERSION_V1,
        result_id: "res-1".into(),
        envelope_id: envelope.envelope_id.clone(),
        job_id: "job-on-b".into(),
        job_type: envelope.job_type.clone(),
        succeeded: true,
        output: Some(payload),
        output_provenance: Some(ProvenanceRef::cas(
            stasis::domain::runtime::provenance::ContentDigest::sha256_bytes(b"out"),
        )),
        error_message: None,
        origin_authority: envelope.origin_authority.clone(),
        terminal_delivery: envelope.terminal_delivery.clone(),
        correlation_id: envelope.correlation_id.clone(),
        causation_id: envelope.causation_id.clone(),
        occurred_at: Utc::now(),
        signature: EnvelopeSignature {
            algorithm: EnvelopeSignature::HMAC_SHA256.into(),
            key_id: String::new(),
            signature_hex: String::new(),
        },
    };
    sign_federated_terminal_result(&mut result, "key-1", key).unwrap();
    let mut tampered_result = result.clone();
    tampered_result.succeeded = false;
    assert!(
        delivery_to_b
            .deliver_terminal_result(tampered_result)
            .await
            .is_err()
    );
    delivery_to_b
        .deliver_terminal_result(result.clone())
        .await
        .unwrap();

    let inbox_b = bus.inbox("rt-b").unwrap();
    assert_eq!(inbox_b.remote_jobs.len(), 1);
    assert_eq!(inbox_b.remote_jobs[0].envelope_id, "env-1");

    // Signal and terminal result land on origin runtime rt-a without sharing a DB.
    let inbox_a = bus.inbox("rt-a").unwrap();
    assert_eq!(inbox_a.signals.len(), 1);
    assert_eq!(inbox_a.terminal_results.len(), 1);
    assert!(inbox_a.terminal_results[0].succeeded);

    let ingress_a = bus.ingress_port("rt-a");
    ingress_a.accept_signal(signal).await.unwrap();
    assert_eq!(bus.inbox("rt-a").unwrap().signals.len(), 1);
}

struct GenericCapabilityHandler;

#[async_trait]
impl JobHandler for GenericCapabilityHandler {
    fn job_type(&self) -> &'static str {
        "generic.capability"
    }

    async fn execute(
        &self,
        _job: &stasis::domain::runtime::job::Job,
    ) -> stasis::domain::errors::Result<JobExecutionOutcome> {
        Ok(JobExecutionOutcome::Success {
            output_provenance: Some(ProvenanceRef::uri("artifact://generic-output")),
            execution_id: Some("generic-execution".into()),
            diagnostics: None,
        })
    }
}

#[tokio::test]
async fn runtime_sdk_processes_capability_matched_job_without_sttp() {
    let runtime = RuntimeSdk::in_memory().await.unwrap();
    runtime.register_handler(GenericCapabilityHandler).unwrap();
    let now = Utc::now();
    runtime
        .enqueue(NewJob {
            id: "job-generic".into(),
            queue: "generic".into(),
            job_type: "generic.capability".into(),
            payload_ref: "payload://generic".into(),
            priority: 10,
            max_attempts: 1,
            idempotency_key: "idem-generic".into(),
            correlation_id: "corr-generic".into(),
            causation_id: "cause-generic".into(),
            trace_id: "trace-generic".into(),
            input_provenance: None,
            placement: PlacementConstraints::unrestricted().require_capability("gpu"),
            scheduled_at: now,
            backoff_policy: BackoffPolicy::default(),
        })
        .await
        .unwrap();

    assert!(
        runtime
            .process_once_with_capabilities(
                "generic",
                "cpu-worker",
                &WorkerCapabilities::any().with_capability("cpu"),
            )
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        runtime
            .process_once_with_capabilities(
                "generic",
                "gpu-worker",
                &WorkerCapabilities::any().with_capability("gpu"),
            )
            .await
            .unwrap()
            .as_deref(),
        Some("job-generic")
    );

    let RuntimeComposition::InMemory(inner) = runtime.runtime() else {
        unreachable!();
    };
    let job = inner.job_store.get("job-generic").await.unwrap().unwrap();
    assert_eq!(job.input_provenance, None);
    assert_eq!(
        job.output_provenance,
        Some(ProvenanceRef::uri("artifact://generic-output"))
    );
    assert!(job.sttp_output_node_id().is_none());

    let attempts = inner.list_job_attempts("job-generic").await.unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].output_provenance, job.output_provenance);
}

#[tokio::test]
async fn surreal_runtime_round_trips_generic_provenance_and_placement() {
    let runtime = RuntimeSdk::surreal_mem("federated-contract", "generic-provenance")
        .await
        .unwrap();
    runtime.register_handler(GenericCapabilityHandler).unwrap();
    runtime
        .enqueue(NewJob {
            id: "job-surreal-generic".into(),
            queue: "generic".into(),
            job_type: "generic.capability".into(),
            payload_ref: "payload://surreal-generic".into(),
            priority: 10,
            max_attempts: 1,
            idempotency_key: "idem-surreal-generic".into(),
            correlation_id: "corr-surreal-generic".into(),
            causation_id: "cause-surreal-generic".into(),
            trace_id: "trace-surreal-generic".into(),
            input_provenance: Some(ProvenanceRef::uri("artifact://generic-input")),
            placement: PlacementConstraints::unrestricted().require_capability("gpu"),
            scheduled_at: Utc::now(),
            backoff_policy: BackoffPolicy::default(),
        })
        .await
        .unwrap();

    runtime
        .process_once_with_capabilities(
            "generic",
            "gpu-worker",
            &WorkerCapabilities::any().with_capability("gpu"),
        )
        .await
        .unwrap()
        .expect("matching worker should process the generic job");

    let RuntimeComposition::Surreal(inner) = runtime.runtime() else {
        unreachable!();
    };
    let job = inner
        .job_store
        .get("job-surreal-generic")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        job.input_provenance,
        Some(ProvenanceRef::uri("artifact://generic-input"))
    );
    assert_eq!(
        job.output_provenance,
        Some(ProvenanceRef::uri("artifact://generic-output"))
    );

    let attempts = inner
        .list_job_attempts("job-surreal-generic")
        .await
        .unwrap();
    assert_eq!(attempts[0].output_provenance, job.output_provenance);

    let events = inner
        .list_lineage_events("job-surreal-generic")
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event.input_provenance, job.input_provenance);
    assert_eq!(events[0].event.output_provenance, job.output_provenance);
}

#[tokio::test]
async fn federated_ingress_rejects_tampered_and_expired_envelopes() {
    let bus = InMemoryFederatedBus::new();
    bus.ensure_runtime("rt-b").unwrap();
    let key = b"federation-shared-secret";
    bus.register_verification_key("key-1", key.to_vec())
        .unwrap();

    let mut envelope = RemoteJobEnvelope {
        schema_version: REMOTE_JOB_ENVELOPE_SCHEMA_VERSION_V1,
        envelope_id: "env-reject".into(),
        job_type: "federated.echo".into(),
        payload: BlobDescriptor::from_bytes(b"payload"),
        idempotency_key: "idem-reject".into(),
        correlation_id: "corr-reject".into(),
        causation_id: "cause-reject".into(),
        deadline: Utc::now() + Duration::minutes(1),
        origin_authority: OriginAuthority {
            runtime_id: "rt-a".into(),
            authority_id: "auth-a".into(),
            realm: None,
        },
        terminal_delivery: TerminalDeliveryEndpoint {
            endpoint_id: "ep-a".into(),
            protocol: "memory-bus".into(),
            address: "rt-a://terminal".into(),
        },
        placement: PlacementConstraints::unrestricted(),
        signature: EnvelopeSignature {
            algorithm: EnvelopeSignature::HMAC_SHA256.into(),
            key_id: String::new(),
            signature_hex: String::new(),
        },
    };
    sign_remote_job_envelope(&mut envelope, "key-1", key).unwrap();

    let ingress = bus.ingress_port("rt-b");
    let mut tampered = envelope.clone();
    tampered.job_type = "federated.tampered".into();
    assert!(ingress.accept_remote_job(tampered).await.is_err());

    envelope.deadline = Utc::now() - Duration::seconds(1);
    sign_remote_job_envelope(&mut envelope, "key-1", key).unwrap();
    assert!(ingress.accept_remote_job(envelope).await.is_err());
}
