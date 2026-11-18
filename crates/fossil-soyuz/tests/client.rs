use fossil_core::ids::{ClaimCallbackId, EventId, RunId, WorkerId};
use fossil_soyuz::{
    OperationalEvent, SoyuzApi, SoyuzClient, SoyuzClientConfig, SoyuzClientError, SoyuzRunRead,
    StartAuthorizationSource, StartedRequest, WorkerClaimRequest,
};
use secrecy::SecretString;
use serde_json::json;
use std::time::Duration;
use wiremock::matchers::{body_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const RUN_ID: &str = "f8a0c4c5-d628-7b93-a7cf-4b4629a28853";
const WORKER_ID: &str = "fossil-host-01";
const CLAIM_ID: &str = "d744c5a2-1d8f-4a2e-bc4d-6813d6fc0133";
const EVENT_ID: &str = "15870068-6723-4b70-9e9f-53b8271e032d";
const TOKEN: &str = "secret-worker-token";

fn run_id() -> RunId {
    RunId::parse(RUN_ID).unwrap()
}

fn worker_id() -> WorkerId {
    WorkerId::parse(WORKER_ID).unwrap()
}

fn claim_id() -> ClaimCallbackId {
    ClaimCallbackId::parse(CLAIM_ID).unwrap()
}

fn event_id() -> EventId {
    EventId::parse(EVENT_ID).unwrap()
}

fn client_config(server: &MockServer, timeout: Duration) -> SoyuzClientConfig {
    SoyuzClientConfig::new(
        url::Url::parse(&server.uri()).unwrap(),
        SecretString::from(TOKEN.to_owned()),
        timeout,
    )
    .unwrap()
}

#[tokio::test]
// Soyuz requirements: AUTH-REST-01 and WORKER-READ-01.
async fn worker_read_deserializes_canonical_and_display_ids_and_cancellation() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/v1/worker/runs/{RUN_ID}")))
        .and(header("authorization", format!("Bearer {TOKEN}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": {
                "runId": RUN_ID,
                "displayId": "soyuz-213",
                "status": "cancel_requested",
                "cancelRequestedAt": "2026-10-02T12:00:00.000Z",
                "claimExpiresAt": null,
                "workerId": WORKER_ID,
                "claimCallbackId": CLAIM_ID,
                "stage": "test",
                "updatedAt": "2026-10-02T12:00:01.000Z",
                "lastHeartbeatAt": null,
                "operationalHealth": "unknown"
            },
            "requestId": "request-1"
        })))
        .mount(&server)
        .await;

    let client = SoyuzClient::new(client_config(&server, Duration::from_secs(1))).unwrap();
    let run: SoyuzRunRead = client.get_run(run_id()).await.unwrap();
    assert_eq!(run.run_id.to_string(), RUN_ID);
    assert_eq!(run.display_id.as_str(), "soyuz-213");
    assert!(run.cancellation_requested());
}

#[tokio::test]
// Soyuz requirement: WORKER-CLAIM-01.
async fn claim_request_uses_stable_worker_and_claim_fencing_identifiers() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("/v1/worker/runs/{RUN_ID}/claim")))
        .and(header("authorization", format!("Bearer {TOKEN}")))
        .and(body_json(json!({
            "contractVersion": "1",
            "callbackId": CLAIM_ID,
            "workerId": WORKER_ID,
            "leaseSeconds": 120
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": {"accepted": true},
            "requestId": "request-2"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = SoyuzClient::new(client_config(&server, Duration::from_secs(1))).unwrap();
    client
        .claim(WorkerClaimRequest {
            run_id: run_id(),
            callback_id: claim_id(),
            worker_id: worker_id(),
            lease_seconds: Some(120),
        })
        .await
        .unwrap();
}

#[tokio::test]
// Soyuz requirement: WORKER-CLAIM-FENCE-02.
async fn stale_claim_errors_are_classified_without_exposing_remote_messages() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("/v1/worker/runs/{RUN_ID}/claim")))
        .respond_with(ResponseTemplate::new(409).set_body_json(json!({
            "error": {
                "code": "STALE_CLAIM",
                "message": "private server details",
                "requestId": "request-3"
            }
        })))
        .mount(&server)
        .await;

    let client = SoyuzClient::new(client_config(&server, Duration::from_secs(1))).unwrap();
    let error = client
        .claim(WorkerClaimRequest {
            run_id: run_id(),
            callback_id: claim_id(),
            worker_id: worker_id(),
            lease_seconds: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(error, SoyuzClientError::StaleClaim));
    assert!(!error.to_string().contains("private server details"));
}

#[tokio::test]
// Soyuz requirement: WORKER-START-02.
async fn timeout_is_a_non_authorizing_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("/v1/worker/runs/{RUN_ID}/started")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(300))
                .set_body_json(json!({"data": {}, "requestId": "request-4"})),
        )
        .mount(&server)
        .await;

    let client = SoyuzClient::new(client_config(&server, Duration::from_millis(100))).unwrap();
    let error = client
        .started(fossil_soyuz::StartedRequest {
            run_id: run_id(),
            callback_id: event_id(),
            worker_id: worker_id(),
            claim_callback_id: Some(claim_id()),
            started_at: None,
            stage: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(error, SoyuzClientError::Timeout));
}

#[tokio::test]
// Soyuz requirement: WORKER-START-01.
async fn successful_started_callback_returns_a_non_forgeable_authorization_proof() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("/v1/worker/runs/{RUN_ID}/started")))
        .and(body_json(json!({
            "contractVersion": "1",
            "callbackId": EVENT_ID,
            "workerId": WORKER_ID,
            "claimCallbackId": CLAIM_ID
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": {},
            "requestId": "request-started"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = SoyuzClient::new(client_config(&server, Duration::from_secs(1))).unwrap();
    let proof = client
        .started(StartedRequest {
            run_id: run_id(),
            callback_id: event_id(),
            worker_id: worker_id(),
            claim_callback_id: Some(claim_id()),
            started_at: None,
            stage: None,
        })
        .await
        .unwrap();
    assert_eq!(proof.run_id(), run_id());
    assert_eq!(proof.claim_callback_id(), claim_id());
    assert_eq!(
        proof.source(),
        StartAuthorizationSource::StartedCallbackAccepted
    );
}

#[tokio::test]
// Soyuz requirements: WORKER-CLAIM-RECOVERY-01 and WORKER-START-02.
async fn uncertain_start_reconciliation_requires_matching_canonical_worker_and_claim() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/v1/worker/runs/{RUN_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": {
                "runId": RUN_ID,
                "displayId": "soyuz-213",
                "status": "running",
                "cancelRequestedAt": null,
                "claimExpiresAt": "2026-10-02T12:02:00.000Z",
                "workerId": WORKER_ID,
                "claimCallbackId": CLAIM_ID,
                "stage": "running",
                "updatedAt": "2026-10-02T12:00:00.000Z",
                "lastHeartbeatAt": "2026-10-02T12:00:00.000Z",
                "operationalHealth": "healthy"
            },
            "requestId": "request-read"
        })))
        .expect(2)
        .mount(&server)
        .await;

    let client = SoyuzClient::new(client_config(&server, Duration::from_secs(1))).unwrap();
    let proof = client
        .reconcile_started(run_id(), event_id(), worker_id(), claim_id())
        .await
        .unwrap();
    assert_eq!(
        proof.source(),
        StartAuthorizationSource::CanonicalStateReconciled
    );

    let stale_claim = ClaimCallbackId::parse("b267af18-7536-48f9-9047-1e420f9b50d8").unwrap();
    assert!(matches!(
        client
            .reconcile_started(run_id(), event_id(), worker_id(), stale_claim)
            .await,
        Err(SoyuzClientError::Conflict)
    ));
}

#[tokio::test]
// Soyuz requirements: WORKER-EVENT-01 and CALLBACK-IDEMPOTENCY-01.
async fn callback_retry_reuses_event_id_and_event_payload() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("/v1/worker/runs/{RUN_ID}/events")))
        .and(body_json(json!({
            "contractVersion": "1",
            "eventId": EVENT_ID,
            "workerId": WORKER_ID,
            "claimCallbackId": CLAIM_ID,
            "type": "progress_update",
            "stage": "validation",
            "timestamp": "2026-10-02T12:00:00.000Z"
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": {},
            "requestId": "request-5"
        })))
        .expect(2)
        .mount(&server)
        .await;

    let client = SoyuzClient::new(client_config(&server, Duration::from_secs(1))).unwrap();
    let event = OperationalEvent {
        run_id: run_id(),
        event_id: event_id(),
        worker_id: worker_id(),
        claim_callback_id: Some(claim_id()),
        event_type: fossil_soyuz::OperationalEventType::ProgressUpdate,
        stage: Some("validation".to_owned()),
        step: None,
        timestamp: Some("2026-10-02T12:00:00.000Z".to_owned()),
        payload: None,
    };
    client.event(event.clone()).await.unwrap();
    client.event(event).await.unwrap();
}
