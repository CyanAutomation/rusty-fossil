use fossil_core::ids::{ClaimCallbackId, EventId, RunId, WorkerId};
use fossil_soyuz::{SoyuzClient, SoyuzClientConfig};
use fossil_supervisor::{
    CancellationOutcome, ExecutionSupervisor, MockSupervisor, SyntheticScenario, TerminalReason,
};
use secrecy::SecretString;
use serde_json::json;
use std::time::Duration;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const RUN_ID: &str = "f8a0c4c5-d628-7b93-a7cf-4b4629a28853";
const WORKER_ID: &str = "fossil-host-01";
const CLAIM_ID: &str = "d744c5a2-1d8f-4a2e-bc4d-6813d6fc0133";
const START_ID: &str = "15870068-6723-4b70-9e9f-53b8271e032d";

#[tokio::test]
async fn duplicate_proof_prepares_one_stable_execution_and_terminal_result() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/v1/worker/runs/{RUN_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": {
                "runId": RUN_ID,
                "displayId": "soyuz-213",
                "status": "running",
                "cancelRequestedAt": null,
                "claimExpiresAt": null,
                "workerId": WORKER_ID,
                "claimCallbackId": CLAIM_ID,
                "stage": "test",
                "updatedAt": "2026-10-02T12:00:01.000Z",
                "lastHeartbeatAt": null,
                "operationalHealth": "healthy"
            },
            "requestId": "request-1"
        })))
        .expect(2)
        .mount(&server)
        .await;
    let client = SoyuzClient::new(
        SoyuzClientConfig::new(
            url::Url::parse(&server.uri()).unwrap(),
            SecretString::from("test-token".to_owned()),
            Duration::from_secs(1),
        )
        .unwrap(),
    )
    .unwrap();
    let started = client
        .reconcile_started(
            RunId::parse(RUN_ID).unwrap(),
            EventId::parse(START_ID).unwrap(),
            WorkerId::parse(WORKER_ID).unwrap(),
            ClaimCallbackId::parse(CLAIM_ID).unwrap(),
        )
        .await
        .unwrap();
    let proof = client.verify_execution_gate(&started).await.unwrap();
    let supervisor = MockSupervisor::new(WORKER_ID);
    let prepared = supervisor
        .prepare(proof.clone(), SyntheticScenario::Success)
        .await
        .unwrap();
    let duplicate = supervisor
        .prepare(proof, SyntheticScenario::ControlledFailure)
        .await
        .unwrap();
    assert_eq!(prepared.container_name(), duplicate.container_name());

    let result = supervisor.execute(prepared.clone()).await.unwrap();
    assert_eq!(result.reason, TerminalReason::Completed);
    assert_eq!(result.exit_code, Some(0));
    assert_eq!(supervisor.execute(prepared).await.unwrap(), result);
    assert_eq!(
        supervisor
            .cancel(RunId::parse(RUN_ID).unwrap())
            .await
            .unwrap(),
        CancellationOutcome::AlreadyTerminal
    );
}

#[tokio::test]
async fn supervisor_rejects_a_valid_gate_for_another_worker() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/v1/worker/runs/{RUN_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": {
                "runId": RUN_ID,
                "displayId": "soyuz-213",
                "status": "running",
                "cancelRequestedAt": null,
                "claimExpiresAt": null,
                "workerId": WORKER_ID,
                "claimCallbackId": CLAIM_ID,
                "stage": "test",
                "updatedAt": "2026-10-02T12:00:01.000Z",
                "lastHeartbeatAt": null,
                "operationalHealth": "healthy"
            },
            "requestId": "request-1"
        })))
        .expect(2)
        .mount(&server)
        .await;
    let client = SoyuzClient::new(
        SoyuzClientConfig::new(
            url::Url::parse(&server.uri()).unwrap(),
            SecretString::from("test-token".to_owned()),
            Duration::from_secs(1),
        )
        .unwrap(),
    )
    .unwrap();
    let started = client
        .reconcile_started(
            RunId::parse(RUN_ID).unwrap(),
            EventId::parse(START_ID).unwrap(),
            WorkerId::parse(WORKER_ID).unwrap(),
            ClaimCallbackId::parse(CLAIM_ID).unwrap(),
        )
        .await
        .unwrap();
    let proof = client.verify_execution_gate(&started).await.unwrap();
    let supervisor = MockSupervisor::new("different-worker");
    assert!(
        supervisor
            .prepare(proof, SyntheticScenario::Success)
            .await
            .is_err()
    );
}
