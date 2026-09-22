use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use stasis::application::runtime::job_context::{JobContext, JobResult};
use stasis::application::runtime::typed_job::JobConsumer;
use stasis::domain::runtime::inbound_trigger::{InboundDisposition, InboundProtocol};
use stasis::domain::runtime::job::JobState;
use stasis::domain::runtime::typed_contract::{JobDeclaration, StasisJob};
use stasis::sdk::runtime_sdk::RuntimeSdk;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct InvoicePaid {
    invoice_id: String,
}

impl StasisJob for InvoicePaid {
    const NAME: &'static str = "billing.invoice_paid";
    const VERSION: u32 = 1;
    type Output = String;

    fn declaration() -> JobDeclaration {
        JobDeclaration::default().queue("billing")
    }
}

struct InvoiceConsumer;

#[async_trait]
impl JobConsumer<InvoicePaid> for InvoiceConsumer {
    async fn consume(&self, job: InvoicePaid, _ctx: JobContext) -> JobResult<String> {
        Ok(job.invoice_id)
    }
}

fn webhook_body(key: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "schema_version": 1,
        "idempotency_key": key,
        "job_type": "billing.invoice_paid",
        "payload": { "invoice_id": "inv-9" },
        "queue": "billing",
        "correlation_id": "stripe:evt-9"
    }))
    .unwrap()
}

async fn sdk(name: &str) -> RuntimeSdk {
    if name == "memory" {
        RuntimeSdk::in_memory().await.expect("memory sdk")
    } else {
        RuntimeSdk::surreal_mem("stasis", name)
            .await
            .expect("surreal sdk")
    }
}

async fn webhook_enqueues_typed_job_and_dedupes(name: &str) {
    let sdk = sdk(name).await;
    sdk.register_consumer(InvoiceConsumer).unwrap();
    let body = webhook_body("stripe:evt-9");
    let first = sdk
        .accept_inbound_json(InboundProtocol::HttpWebhook, &body)
        .await
        .unwrap();
    assert_eq!(first.disposition, InboundDisposition::Accepted);

    let second = sdk
        .accept_inbound_json(InboundProtocol::Kafka, &body)
        .await
        .unwrap();
    assert_eq!(second.disposition, InboundDisposition::Duplicate);
    assert_eq!(second.job_id, first.job_id);

    let job = sdk.get_job(&first.job_id).await.unwrap().unwrap();
    assert_eq!(job.queue, "billing");
    assert_eq!(job.job_type, "billing.invoice_paid");
    assert_eq!(job.causation_id, "inbound:http_webhook");
    assert_eq!(job.correlation_id, "stripe:evt-9");
    assert_eq!(job.idempotency_key, "stripe:evt-9");

    let processed = sdk
        .process_once("billing", "worker")
        .await
        .unwrap()
        .expect("job processed");
    assert_eq!(processed, first.job_id);
    let done = sdk.get_job(&first.job_id).await.unwrap().unwrap();
    assert_eq!(done.state, JobState::Succeeded);
    assert!(
        sdk.process_once("billing", "worker")
            .await
            .unwrap()
            .is_none()
    );
}

async fn each_protocol_can_start_a_job(name: &str) {
    let sdk = sdk(name).await;
    for (protocol, key) in [
        (InboundProtocol::HttpWebhook, "wh-1"),
        (InboundProtocol::Tcp, "tcp-1"),
        (InboundProtocol::Kafka, "kafka-1"),
        (InboundProtocol::RabbitMq, "queue-1"),
    ] {
        let body = webhook_body(key);
        let accepted = sdk.accept_inbound_json(protocol, &body).await.unwrap();
        assert_eq!(accepted.disposition, InboundDisposition::Accepted);
        let job = sdk.get_job(&accepted.job_id).await.unwrap().unwrap();
        assert_eq!(job.causation_id, format!("inbound:{}", protocol.as_str()));
    }
}

async fn typed_accept_uses_declaration(name: &str) {
    let sdk = sdk(name).await;
    let accepted = sdk
        .accept_inbound_job(
            InboundProtocol::Tcp,
            "tcp:line-1",
            InvoicePaid {
                invoice_id: "inv-typed".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(accepted.disposition, InboundDisposition::Accepted);
    let job = sdk.get_job(&accepted.job_id).await.unwrap().unwrap();
    assert_eq!(job.queue, "billing");
    assert_eq!(job.causation_id, "inbound:tcp");
    assert!(job.payload_ref.contains("inv-typed"));
}

#[tokio::test]
async fn memory_webhook_enqueues_typed_job_and_dedupes() {
    webhook_enqueues_typed_job_and_dedupes("memory").await;
}

#[tokio::test]
async fn surreal_webhook_enqueues_typed_job_and_dedupes() {
    webhook_enqueues_typed_job_and_dedupes("inbound_webhook").await;
}

#[tokio::test]
async fn memory_each_protocol_can_start_a_job() {
    each_protocol_can_start_a_job("memory").await;
}

#[tokio::test]
async fn surreal_each_protocol_can_start_a_job() {
    each_protocol_can_start_a_job("inbound_protocols").await;
}

#[tokio::test]
async fn memory_typed_accept_uses_declaration() {
    typed_accept_uses_declaration("memory").await;
}

#[tokio::test]
async fn rejects_malformed_body_without_enqueue() {
    let sdk = RuntimeSdk::in_memory().await.unwrap();
    let err = sdk
        .accept_inbound_json(InboundProtocol::HttpWebhook, b"not-json")
        .await
        .expect_err("malformed");
    assert!(err.to_string().contains("decode inbound job trigger"));
}

#[tokio::test]
async fn rejects_empty_idempotency_key() {
    let sdk = RuntimeSdk::in_memory().await.unwrap();
    let body = serde_json::to_vec(&serde_json::json!({
        "schema_version": 1,
        "idempotency_key": "  ",
        "job_type": "billing.invoice_paid",
        "payload": {}
    }))
    .unwrap();
    let err = sdk
        .accept_inbound_json(InboundProtocol::RabbitMq, &body)
        .await
        .expect_err("empty key");
    assert!(err.to_string().contains("idempotency_key"));
}
