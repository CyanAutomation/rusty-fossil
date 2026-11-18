use fossil_core::contract::SoyuzRunStatus;
use fossil_core::ids::{ClaimCallbackId, EventId, RunId, WorkerId};
use fossil_core::lifecycle::{CallbackDeliveryState, LocalLifecycleState};
use fossil_soyuz::{
    SoyuzApi, SoyuzClient, SoyuzClientConfig, StartedAuthorization, StartedRequest,
};
use fossil_storage::{
    CallbackOutboxRecord, CallbackPayload, RecoveryAction, SqliteStore, StoreError,
};
use secrecy::SecretString;
use std::fs;
use std::time::Duration;
use tempfile::tempdir;
use wiremock::matchers::{body_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const RUN_ID: &str = "f8a0c4c5-d628-7b93-a7cf-4b4629a28853";

fn worker_id() -> WorkerId {
    WorkerId::parse("fossil-host-01").unwrap()
}

fn run_id() -> RunId {
    RunId::parse(RUN_ID).unwrap()
}

async fn authorization_for(
    run_id: RunId,
    worker_id: WorkerId,
    claim_callback_id: ClaimCallbackId,
    callback_id: EventId,
) -> StartedAuthorization {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("/v1/worker/runs/{run_id}/started")))
        .and(body_json(serde_json::json!({
            "contractVersion": "1",
            "callbackId": callback_id.to_string(),
            "workerId": worker_id.as_str(),
            "claimCallbackId": claim_callback_id.to_string()
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": {},
            "requestId": "start-request-1"
        })))
        .mount(&server)
        .await;
    let config = SoyuzClientConfig::new(
        url::Url::parse(&server.uri()).unwrap(),
        SecretString::from("test-worker-token".to_owned()),
        Duration::from_secs(1),
    )
    .unwrap();
    SoyuzClient::new(config)
        .unwrap()
        .started(StartedRequest {
            run_id,
            callback_id,
            worker_id,
            claim_callback_id: Some(claim_callback_id),
            started_at: None,
            stage: None,
        })
        .await
        .unwrap()
}

async fn authorized_store(path: &std::path::Path) -> SqliteStore {
    let store = SqliteStore::open(path).unwrap();
    store.observe_run(run_id(), worker_id()).unwrap();
    store
        .transition(run_id(), LocalLifecycleState::MessageValidated)
        .unwrap();
    store
        .transition(run_id(), LocalLifecycleState::CanonicalRunChecked)
        .unwrap();
    store
        .begin_claim_intent(
            run_id(),
            ClaimCallbackId::parse("d744c5a2-1d8f-4a2e-bc4d-6813d6fc0133").unwrap(),
        )
        .unwrap();
    store
        .transition(run_id(), LocalLifecycleState::ClaimAuthorized)
        .unwrap();
    let authorization = authorization_for(
        run_id(),
        worker_id(),
        ClaimCallbackId::parse("d744c5a2-1d8f-4a2e-bc4d-6813d6fc0133").unwrap(),
        EventId::parse("15870068-6723-4b70-9e9f-53b8271e032d").unwrap(),
    )
    .await;
    store
        .record_start_authorization(run_id(), authorization)
        .unwrap();
    store
}

#[tokio::test]
// Soyuz requirements: WORKER-CLAIM-RECOVERY-01 and WORKER-START-01.
async fn records_survive_store_reinitialization_and_partial_work_is_recoverable() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("state.sqlite3");
    let store = authorized_store(&path).await;
    drop(store);

    let reopened = SqliteStore::open(&path).unwrap();
    let record = reopened.get_run(run_id()).unwrap();
    assert_eq!(
        record.lifecycle_state,
        LocalLifecycleState::StartAuthorizationPersisted
    );
    assert!(record.start_authorized);
    assert_eq!(
        record.claim_callback_id.unwrap().as_str(),
        "d744c5a2-1d8f-4a2e-bc4d-6813d6fc0133"
    );
    assert_eq!(reopened.recovery_candidates().unwrap().len(), 1);
}

#[test]
// Soyuz requirement: QUEUE-MESSAGE-01.
fn duplicate_messages_return_the_existing_record_without_creating_another() {
    let directory = tempdir().unwrap();
    let store = SqliteStore::open(directory.path().join("state.sqlite3")).unwrap();

    let first = store.observe_run(run_id(), worker_id()).unwrap();
    let second = store.observe_run(run_id(), worker_id()).unwrap();

    assert_eq!(first.run_id, second.run_id);
    assert_eq!(store.run_count().unwrap(), 1);
}

#[test]
fn duplicate_run_identity_with_a_different_worker_is_rejected() {
    let directory = tempdir().unwrap();
    let store = SqliteStore::open(directory.path().join("state.sqlite3")).unwrap();
    store.observe_run(run_id(), worker_id()).unwrap();

    let other_worker = WorkerId::parse("fossil-host-02").unwrap();
    assert!(matches!(
        store.observe_run(run_id(), other_worker),
        Err(StoreError::WorkerIdentityConflict)
    ));
}

#[test]
// Soyuz requirement: WORKER-CLAIM-FENCE-01.
fn claim_fencing_identity_is_idempotent_but_cannot_be_silently_replaced() {
    let directory = tempdir().unwrap();
    let store = SqliteStore::open(directory.path().join("state.sqlite3")).unwrap();
    store.observe_run(run_id(), worker_id()).unwrap();
    store
        .transition(run_id(), LocalLifecycleState::MessageValidated)
        .unwrap();
    store
        .transition(run_id(), LocalLifecycleState::CanonicalRunChecked)
        .unwrap();
    let first_id = ClaimCallbackId::parse("d744c5a2-1d8f-4a2e-bc4d-6813d6fc0133").unwrap();
    store.begin_claim_intent(run_id(), first_id).unwrap();
    store.begin_claim_intent(run_id(), first_id).unwrap();

    let replacement = ClaimCallbackId::parse("b267af18-7536-48f9-9047-1e420f9b50d8").unwrap();
    assert!(matches!(
        store.begin_claim_intent(run_id(), replacement),
        Err(StoreError::ClaimFenceConflict)
    ));
}

#[test]
fn invalid_transition_rolls_back_and_does_not_advance_state() {
    let directory = tempdir().unwrap();
    let store = SqliteStore::open(directory.path().join("state.sqlite3")).unwrap();
    store.observe_run(run_id(), worker_id()).unwrap();

    assert!(matches!(
        store.transition(run_id(), LocalLifecycleState::ExecutionActive),
        Err(StoreError::Lifecycle(_))
    ));
    assert_eq!(
        store.get_run(run_id()).unwrap().lifecycle_state,
        LocalLifecycleState::QueueObserved
    );
    assert!(!store.get_run(run_id()).unwrap().start_authorized);
}

#[tokio::test]
// Soyuz requirement: CALLBACK-IDEMPOTENCY-01.
async fn callback_outbox_reuses_event_ids_and_tracks_delivery() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("state.sqlite3");
    let store = authorized_store(&path).await;
    store
        .transition(run_id(), LocalLifecycleState::ExecutionActive)
        .unwrap();
    store
        .transition(run_id(), LocalLifecycleState::ExecutionTerminal)
        .unwrap();
    let event_id =
        fossil_core::ids::EventId::parse("15870068-6723-4b70-9e9f-53b8271e032d").unwrap();
    let callback = CallbackOutboxRecord::new(
        event_id,
        run_id(),
        CallbackPayload::Terminal {
            status: SoyuzRunStatus::Completed,
            completed_at_ms: 1_798_000_000_000,
            exit_code: Some(0),
        },
    );

    store.enqueue_callback(callback.clone()).unwrap();
    store.enqueue_callback(callback).unwrap();
    assert_eq!(store.pending_callbacks().unwrap().len(), 1);
    assert_eq!(
        store.get_run(run_id()).unwrap().callback_delivery,
        CallbackDeliveryState::Pending
    );

    store.mark_callback_delivered(event_id).unwrap();
    assert!(store.pending_callbacks().unwrap().is_empty());
    assert_eq!(
        store.get_run(run_id()).unwrap().callback_delivery,
        CallbackDeliveryState::Delivered
    );
}

#[test]
// Soyuz requirement: WORKER-START-01.
fn start_authorization_cannot_be_written_without_a_soyuz_proof() {
    let directory = tempdir().unwrap();
    let store = SqliteStore::open(directory.path().join("state.sqlite3")).unwrap();
    store.observe_run(run_id(), worker_id()).unwrap();
    store
        .transition(run_id(), LocalLifecycleState::MessageValidated)
        .unwrap();
    store
        .transition(run_id(), LocalLifecycleState::CanonicalRunChecked)
        .unwrap();
    store
        .begin_claim_intent(
            run_id(),
            ClaimCallbackId::parse("d744c5a2-1d8f-4a2e-bc4d-6813d6fc0133").unwrap(),
        )
        .unwrap();
    store
        .transition(run_id(), LocalLifecycleState::ClaimAuthorized)
        .unwrap();

    assert!(matches!(
        store.transition(run_id(), LocalLifecycleState::StartAuthorizationPersisted),
        Err(StoreError::AuthorizationProofRequired)
    ));
    assert!(!store.get_run(run_id()).unwrap().start_authorized);
}

#[test]
fn incompatible_schema_and_corrupt_database_fail_closed() {
    let directory = tempdir().unwrap();
    let incompatible_path = directory.path().join("future.sqlite3");
    let connection = rusqlite::Connection::open(&incompatible_path).unwrap();
    connection
        .execute_batch("PRAGMA user_version = 999;")
        .unwrap();
    drop(connection);
    assert!(matches!(
        SqliteStore::open(&incompatible_path),
        Err(StoreError::UnsupportedSchemaVersion(999))
    ));

    let corrupt_path = directory.path().join("corrupt.sqlite3");
    fs::write(&corrupt_path, b"not a sqlite database").unwrap();
    assert!(SqliteStore::open(&corrupt_path).is_err());
}

#[test]
fn recovery_metadata_is_bounded_and_durable() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("state.sqlite3");
    let store = SqliteStore::open(&path).unwrap();
    store.observe_run(run_id(), worker_id()).unwrap();
    store
        .record_recovery_action(run_id(), RecoveryAction::ReconcileWithSoyuz)
        .unwrap();
    drop(store);

    let reopened = SqliteStore::open(path).unwrap();
    let record = reopened.get_run(run_id()).unwrap();
    assert_eq!(record.recovery_attempts, 1);
    assert_eq!(record.recovery_action, RecoveryAction::ReconcileWithSoyuz);
}
