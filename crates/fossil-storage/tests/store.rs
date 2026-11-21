use fossil_core::contract::OperationalEventType;
use fossil_core::execution::SyntheticScenario;
use fossil_core::ids::{ClaimCallbackId, EventId, RunId, WorkerId};
use fossil_core::lifecycle::{CallbackDeliveryState, LocalLifecycleState};
use fossil_core::queue::QueueEnvelope;
use fossil_soyuz::{
    CallbackDeliveryAuthorization, ClaimAuthorization, CompletedCallback, SoyuzApi, SoyuzClient,
    SoyuzClientConfig, StartedAuthorization, StartedRequest, WorkerClaimRequest,
};
use fossil_storage::{
    CallbackOutboxRecord, CallbackPayload, ContainerLaunchState, ExecutionDescriptor,
    RecoveryAction, SqliteStore, StoreError, callback_retry_deadline_ms,
};
use secrecy::SecretString;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tempfile::tempdir;
use wiremock::matchers::{body_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const RUN_ID: &str = "f8a0c4c5-d628-7b93-a7cf-4b4629a28853";

fn state_path(root: &Path) -> PathBuf {
    root.join("private-state").join("state.sqlite3")
}

fn prepare_state_path(path: &Path) {
    let parent = path.parent().unwrap();
    fs::create_dir_all(parent).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(parent, fs::Permissions::from_mode(0o700)).unwrap();
    }
}

fn worker_id() -> WorkerId {
    WorkerId::parse("fossil-host-01").unwrap()
}

fn run_id() -> RunId {
    RunId::parse(RUN_ID).unwrap()
}

fn execution_descriptor() -> ExecutionDescriptor {
    let envelope = QueueEnvelope::parse(include_bytes!(
        "../../fossil-core/tests/fixtures/queue-v1-valid.json"
    ))
    .unwrap();
    ExecutionDescriptor::from(&envelope)
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

async fn claim_authorization_for(
    run_id: RunId,
    worker_id: WorkerId,
    claim_callback_id: ClaimCallbackId,
) -> ClaimAuthorization {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("/v1/worker/runs/{run_id}/claim")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": {"accepted": true},
            "requestId": "claim-request-1"
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
        .claim(WorkerClaimRequest {
            run_id,
            callback_id: claim_callback_id,
            worker_id,
            lease_seconds: Some(120),
        })
        .await
        .unwrap()
}

async fn execution_gate_authorization_for(
    authorization: &StartedAuthorization,
) -> fossil_soyuz::ExecutionGateAuthorization {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/v1/worker/runs/{}", authorization.run_id())))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": {
                "runId": authorization.run_id().to_string(),
                "displayId": "soyuz-213",
                "status": "running",
                "cancelRequestedAt": null,
                "claimExpiresAt": "2026-10-02T12:02:00.000Z",
                "workerId": authorization.worker_id().as_str(),
                "claimCallbackId": authorization.claim_callback_id().to_string(),
                "stage": "running",
                "updatedAt": "2026-10-02T12:00:00.000Z",
                "lastHeartbeatAt": "2026-10-02T12:00:00.000Z",
                "operationalHealth": "healthy"
            },
            "requestId": "gate-request-1"
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
        .verify_execution_gate(authorization)
        .await
        .unwrap()
}

async fn callback_delivery_authorization_for(
    run_id: RunId,
    event_id: EventId,
    worker_id: WorkerId,
    claim_callback_id: ClaimCallbackId,
) -> CallbackDeliveryAuthorization {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("/v1/worker/runs/{run_id}/completed")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": {},
            "requestId": "callback-delivery-1"
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
        .completed(CompletedCallback {
            run_id,
            callback_id: event_id,
            worker_id,
            claim_callback_id: Some(claim_callback_id),
            completed_at: Some("2026-12-23T04:26:40Z".to_owned()),
            exit_code: Some(0),
            summary: None,
            published_url: None,
            changed_file_count: None,
            outcome_summary: None,
        })
        .await
        .unwrap()
}

async fn authorized_store(path: &std::path::Path) -> (SqliteStore, StartedAuthorization) {
    let store = SqliteStore::open(path).unwrap();
    store.observe_run(run_id(), worker_id()).unwrap();
    store
        .persist_execution_descriptor(execution_descriptor())
        .unwrap();
    store
        .transition(run_id(), LocalLifecycleState::MessageValidated)
        .unwrap();
    store
        .transition(run_id(), LocalLifecycleState::CanonicalRunChecked)
        .unwrap();
    let claim_callback_id = ClaimCallbackId::parse("d744c5a2-1d8f-4a2e-bc4d-6813d6fc0133").unwrap();
    store
        .begin_claim_intent(run_id(), claim_callback_id)
        .unwrap();
    let claim_authorization =
        claim_authorization_for(run_id(), worker_id(), claim_callback_id).await;
    store
        .record_claim_authorization(run_id(), claim_authorization)
        .unwrap();
    let start_callback_id = EventId::parse("15870068-6723-4b70-9e9f-53b8271e032d").unwrap();
    store
        .begin_start_intent(run_id(), start_callback_id)
        .unwrap();
    let authorization =
        authorization_for(run_id(), worker_id(), claim_callback_id, start_callback_id).await;
    store
        .record_start_authorization(run_id(), authorization.clone())
        .unwrap();
    (store, authorization)
}

#[tokio::test]
// Soyuz requirements: WORKER-CLAIM-RECOVERY-01 and WORKER-START-01.
async fn records_survive_store_reinitialization_and_partial_work_is_recoverable() {
    let directory = tempdir().unwrap();
    let path = state_path(directory.path());
    let (store, _) = authorized_store(&path).await;
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

#[tokio::test]
// Soyuz requirements: WORKER-START-01 and CANCEL-RUNNING-01.
async fn single_execution_slot_requires_a_fresh_matching_canonical_proof() {
    let directory = tempdir().unwrap();
    let path = state_path(directory.path());
    let (store, started_authorization) = authorized_store(&path).await;

    assert!(store.reserve_execution_slot(run_id()).unwrap());
    assert!(!store.reserve_execution_slot(run_id()).unwrap());
    let gate_authorization = execution_gate_authorization_for(&started_authorization).await;
    let active = store
        .mark_execution_active(run_id(), gate_authorization.clone())
        .unwrap();
    assert_eq!(active.lifecycle_state, LocalLifecycleState::ExecutionActive);
    assert!(matches!(
        store.mark_execution_active(run_id(), gate_authorization),
        Err(StoreError::ExecutionGateProofRequired)
    ));
    assert!(matches!(
        store.release_execution_slot(run_id()),
        Err(StoreError::ExecutionSlotReleaseDenied)
    ));

    store
        .transition(run_id(), LocalLifecycleState::ExecutionTerminal)
        .unwrap();
    store.release_execution_slot(run_id()).unwrap();
    assert!(
        store
            .remove_terminal_execution_descriptor(run_id())
            .unwrap()
    );
    assert_eq!(store.execution_descriptor(run_id()).unwrap(), None);
}

#[tokio::test]
async fn docker_launch_intent_is_proof_gated_stable_and_durable() {
    let directory = tempdir().unwrap();
    let path = state_path(directory.path());
    let (store, started_authorization) = authorized_store(&path).await;
    let gate = execution_gate_authorization_for(&started_authorization).await;

    assert!(matches!(
        store.begin_container_launch(run_id(), SyntheticScenario::Success, gate.clone()),
        Err(StoreError::ExecutionSlotNotAuthorized)
    ));
    store.reserve_execution_slot(run_id()).unwrap();

    let intent = store
        .begin_container_launch(run_id(), SyntheticScenario::Success, gate.clone())
        .unwrap();
    assert_eq!(intent.state, ContainerLaunchState::LaunchIntentPersisted);
    assert!(intent.container_name.starts_with("rusty-fossil-synth-"));
    assert_eq!(
        intent,
        store
            .begin_container_launch(run_id(), SyntheticScenario::Success, gate.clone())
            .unwrap()
    );
    assert!(matches!(
        store.begin_container_launch(run_id(), SyntheticScenario::ControlledFailure, gate),
        Err(StoreError::ContainerLaunchIdentityConflict)
    ));

    drop(store);
    let reopened = SqliteStore::open(path).unwrap();
    let recovered = reopened.container_launch(run_id()).unwrap().unwrap();
    assert_eq!(recovered.state, ContainerLaunchState::LaunchIntentPersisted);
    assert_eq!(recovered.container_name, intent.container_name);
}

#[tokio::test]
async fn cancellation_before_container_creation_is_terminal_and_keeps_cleanup_fenced() {
    let directory = tempdir().unwrap();
    let path = state_path(directory.path());
    let (store, started_authorization) = authorized_store(&path).await;
    let gate = execution_gate_authorization_for(&started_authorization).await;
    store.reserve_execution_slot(run_id()).unwrap();
    store
        .begin_container_launch(run_id(), SyntheticScenario::Success, gate)
        .unwrap();

    store
        .mark_container_stop_requested(run_id(), "cancelled")
        .unwrap();
    let terminal = store
        .record_container_exit(
            run_id(),
            None,
            "cancelled",
            "synthetic workload cancelled before start",
            false,
            1_798_000_001_000,
        )
        .unwrap();
    let callback = CallbackOutboxRecord::new(
        terminal.terminal_callback_id.unwrap(),
        run_id(),
        CallbackPayload::Cancelled {
            worker_id: worker_id(),
            claim_callback_id: ClaimCallbackId::parse("d744c5a2-1d8f-4a2e-bc4d-6813d6fc0133")
                .unwrap(),
            completed_at_ms: 1_798_000_001_000,
            exit_code: None,
            reason: Some("synthetic workload cancelled before start".to_owned()),
            outcome_summary: None,
        },
    );
    store
        .transition_terminal_with_callback(run_id(), callback.clone())
        .unwrap();

    assert_eq!(store.pending_callbacks().unwrap(), vec![callback]);
    assert_eq!(store.execution_slot_owner().unwrap(), Some(run_id()));
    assert_eq!(
        store.container_launch(run_id()).unwrap().unwrap().state,
        ContainerLaunchState::CleanupPending
    );
}

#[tokio::test]
async fn restarted_supervisor_recovers_running_state_from_durable_start_proof() {
    let directory = tempdir().unwrap();
    let path = state_path(directory.path());
    let (store, started_authorization) = authorized_store(&path).await;
    let gate = execution_gate_authorization_for(&started_authorization).await;
    store.reserve_execution_slot(run_id()).unwrap();
    store
        .begin_container_launch(run_id(), SyntheticScenario::Success, gate)
        .unwrap();
    store.request_container_create(run_id()).unwrap();
    store
        .record_container_created(run_id(), "container-id-123")
        .unwrap();
    store.mark_container_start_requested(run_id()).unwrap();

    let recovered = store
        .recover_container_running(run_id(), 1_798_000_000_000)
        .unwrap();
    assert_eq!(recovered.state, ContainerLaunchState::ContainerRunning);
    assert_eq!(recovered.started_at_ms, Some(1_798_000_000_000));
    assert_eq!(
        store.get_run(run_id()).unwrap().lifecycle_state,
        LocalLifecycleState::ExecutionActive
    );
    assert_eq!(store.execution_slot_owner().unwrap(), Some(run_id()));
}

#[tokio::test]
async fn docker_lifecycle_persists_before_each_side_effect_and_terminal_callback() {
    let directory = tempdir().unwrap();
    let path = state_path(directory.path());
    let (store, started_authorization) = authorized_store(&path).await;
    let gate = execution_gate_authorization_for(&started_authorization).await;
    store.reserve_execution_slot(run_id()).unwrap();
    store
        .begin_container_launch(run_id(), SyntheticScenario::Success, gate.clone())
        .unwrap();

    assert!(store.request_container_create(run_id()).unwrap());
    assert!(!store.request_container_create(run_id()).unwrap());
    store
        .record_container_created(run_id(), "container-id-123")
        .unwrap();
    store.mark_container_start_requested(run_id()).unwrap();
    store
        .mark_container_running(run_id(), gate.clone(), 1_798_000_000_000)
        .unwrap();
    let exited = store
        .record_container_exit(
            run_id(),
            Some(0),
            "success",
            "synthetic workload completed",
            false,
            1_798_000_001_000,
        )
        .unwrap();

    let callback_id = exited.terminal_callback_id.unwrap();
    let callback = CallbackOutboxRecord::new(
        callback_id,
        run_id(),
        CallbackPayload::Completed {
            worker_id: worker_id(),
            claim_callback_id: ClaimCallbackId::parse("d744c5a2-1d8f-4a2e-bc4d-6813d6fc0133")
                .unwrap(),
            completed_at_ms: 1_798_000_000_000,
            exit_code: Some(0),
            summary: Some("synthetic workload completed".to_owned()),
            published_url: None,
            changed_file_count: None,
            outcome_summary: None,
        },
    );
    store
        .transition_terminal_with_callback(run_id(), callback.clone())
        .unwrap();
    assert_eq!(
        store.container_launch(run_id()).unwrap().unwrap().state,
        ContainerLaunchState::CleanupPending
    );
    assert_eq!(store.pending_callbacks().unwrap(), vec![callback]);

    store.complete_container_cleanup(run_id()).unwrap();
    assert_eq!(
        store.container_launch(run_id()).unwrap().unwrap().state,
        ContainerLaunchState::CleanupCompleted
    );
    assert_eq!(store.execution_slot_owner().unwrap(), None);
}

#[test]
// Soyuz requirements: WORKER-CLAIM-01 and WORKER-START-01.
fn intake_slot_serializes_different_runs_before_network_authorization() {
    let directory = tempdir().unwrap();
    let store = SqliteStore::open(state_path(directory.path())).unwrap();
    let first = run_id();
    let second = RunId::parse("f8a0c4c5-d628-7b93-a7cf-4b4629a28854").unwrap();
    store.observe_run(first, worker_id()).unwrap();
    store
        .persist_execution_descriptor(execution_descriptor())
        .unwrap();
    assert!(store.reserve_intake_slot(first).unwrap());

    store.observe_run(second, worker_id()).unwrap();
    let mut second_descriptor = execution_descriptor();
    second_descriptor.run_id = second;
    store
        .persist_execution_descriptor(second_descriptor)
        .unwrap();

    assert!(!store.reserve_intake_slot(first).unwrap());
    assert!(matches!(
        store.reserve_intake_slot(second),
        Err(StoreError::ExecutionSlotOccupied)
    ));

    store
        .transition(first, LocalLifecycleState::ExecutionTerminal)
        .unwrap();
    store.release_execution_slot(first).unwrap();
    assert!(store.reserve_intake_slot(second).unwrap());
}

#[test]
// Soyuz requirement: QUEUE-MESSAGE-01.
fn duplicate_messages_return_the_existing_record_without_creating_another() {
    let directory = tempdir().unwrap();
    let store = SqliteStore::open(state_path(directory.path())).unwrap();

    let first = store.observe_run(run_id(), worker_id()).unwrap();
    let second = store.observe_run(run_id(), worker_id()).unwrap();

    assert_eq!(first.run_id, second.run_id);
    assert_eq!(store.run_count().unwrap(), 1);
}

#[test]
fn duplicate_run_identity_with_a_different_worker_is_rejected() {
    let directory = tempdir().unwrap();
    let store = SqliteStore::open(state_path(directory.path())).unwrap();
    store.observe_run(run_id(), worker_id()).unwrap();

    let other_worker = WorkerId::parse("fossil-host-02").unwrap();
    assert!(matches!(
        store.observe_run(run_id(), other_worker),
        Err(StoreError::WorkerIdentityConflict)
    ));
}

#[test]
// Soyuz requirement: QUEUE-MESSAGE-01.
fn execution_descriptor_is_durable_bounded_and_immutable_per_run() {
    let directory = tempdir().unwrap();
    let path = state_path(directory.path());
    let store = SqliteStore::open(&path).unwrap();
    store.observe_run(run_id(), worker_id()).unwrap();
    let descriptor = execution_descriptor();
    store
        .persist_execution_descriptor(descriptor.clone())
        .unwrap();
    store
        .persist_execution_descriptor(descriptor.clone())
        .unwrap();
    assert_eq!(
        store.execution_descriptor(run_id()).unwrap(),
        Some(descriptor)
    );

    let mut replacement = execution_descriptor();
    replacement.request.task_prompt = Some("A different bounded task prompt.".to_owned());
    assert!(matches!(
        store.persist_execution_descriptor(replacement),
        Err(StoreError::ExecutionDescriptorConflict)
    ));
    drop(store);

    let reopened = SqliteStore::open(&path).unwrap();
    assert!(reopened.execution_descriptor(run_id()).unwrap().is_some());
}

#[test]
// Recovery invariant: corrupt or missing task input must not be executable.
fn corrupt_execution_descriptor_fails_closed() {
    let directory = tempdir().unwrap();
    let path = state_path(directory.path());
    let store = SqliteStore::open(&path).unwrap();
    store.observe_run(run_id(), worker_id()).unwrap();
    store
        .persist_execution_descriptor(execution_descriptor())
        .unwrap();
    drop(store);

    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute(
            "UPDATE execution_handoffs SET descriptor_json = '{}' WHERE run_id = ?1",
            [RUN_ID],
        )
        .unwrap();
    drop(connection);

    let reopened = SqliteStore::open(&path).unwrap();
    assert!(matches!(
        reopened.execution_descriptor(run_id()),
        Err(StoreError::InvalidExecutionDescriptor)
    ));
}

#[cfg(unix)]
#[test]
fn sensitive_sqlite_state_uses_private_directory_and_database_modes() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempdir().unwrap();
    let state_dir = directory.path().join("private-state");
    let path = state_dir.join("state.sqlite3");
    let _store = SqliteStore::open(&path).unwrap();
    assert_eq!(
        std::fs::metadata(&state_dir).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
}

#[test]
// Soyuz requirement: WORKER-CLAIM-FENCE-01.
fn claim_fencing_identity_is_idempotent_but_cannot_be_silently_replaced() {
    let directory = tempdir().unwrap();
    let store = SqliteStore::open(state_path(directory.path())).unwrap();
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
    let store = SqliteStore::open(state_path(directory.path())).unwrap();
    store.observe_run(run_id(), worker_id()).unwrap();

    assert!(matches!(
        store.transition(run_id(), LocalLifecycleState::ExecutionActive),
        Err(StoreError::ExecutionGateProofRequired)
    ));
    assert_eq!(
        store.get_run(run_id()).unwrap().lifecycle_state,
        LocalLifecycleState::QueueObserved
    );
    assert!(!store.get_run(run_id()).unwrap().start_authorized);
}

#[test]
// Soyuz requirement: WORKER-CLAIM-01.
fn claim_authorization_cannot_be_forged_by_a_generic_lifecycle_transition() {
    let directory = tempdir().unwrap();
    let store = SqliteStore::open(state_path(directory.path())).unwrap();
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

    assert!(matches!(
        store.transition(run_id(), LocalLifecycleState::ClaimAuthorized),
        Err(StoreError::ClaimAuthorizationProofRequired)
    ));
    assert_eq!(
        store.get_run(run_id()).unwrap().lifecycle_state,
        LocalLifecycleState::ClaimIntentPersisted
    );
}

#[tokio::test]
// Soyuz requirement: CALLBACK-IDEMPOTENCY-01.
async fn callback_outbox_reuses_event_ids_and_tracks_delivery() {
    let directory = tempdir().unwrap();
    let path = state_path(directory.path());
    let (store, _) = authorized_store(&path).await;
    let event_id =
        fossil_core::ids::EventId::parse("fa870068-6723-4b70-9e9f-53b8271e032d").unwrap();
    let callback = CallbackOutboxRecord::new(
        event_id,
        run_id(),
        CallbackPayload::Completed {
            worker_id: worker_id(),
            claim_callback_id: ClaimCallbackId::parse("d744c5a2-1d8f-4a2e-bc4d-6813d6fc0133")
                .unwrap(),
            completed_at_ms: 1_798_000_000_000,
            exit_code: Some(0),
            summary: Some("Finished safely".to_owned()),
            published_url: Some("https://example.test/pr/42".to_owned()),
            changed_file_count: Some(2),
            outcome_summary: Some(serde_json::json!({
                "schemaVersion": 1,
                "evaluation": {
                    "assessment": "good",
                    "reviewerConfidence": "high",
                    "improvementCategories": []
                }
            })),
        },
    );

    store
        .transition_terminal_with_callback(run_id(), callback.clone())
        .unwrap();
    store.enqueue_callback(callback.clone()).unwrap();
    assert_eq!(store.pending_callbacks().unwrap(), vec![callback.clone()]);
    assert_eq!(
        store.get_run(run_id()).unwrap().callback_delivery,
        CallbackDeliveryState::Pending
    );

    assert_eq!(
        store.begin_callback_attempt(event_id, 100, 1_100).unwrap(),
        1
    );
    assert!(matches!(
        store.begin_callback_attempt(event_id, 101, 1_200),
        Err(StoreError::CallbackNotEligible)
    ));
    store
        .defer_callback(event_id, "soyuz_unavailable", 5_000)
        .unwrap();
    let pending = store.pending_callbacks().unwrap();
    assert_eq!(pending[0].attempts, 1);
    assert_eq!(pending[0].next_eligible_at_ms, 5_000);
    assert_eq!(
        pending[0].failure_class.as_deref(),
        Some("soyuz_unavailable")
    );

    let delivered = callback_delivery_authorization_for(
        run_id(),
        event_id,
        worker_id(),
        ClaimCallbackId::parse("d744c5a2-1d8f-4a2e-bc4d-6813d6fc0133").unwrap(),
    )
    .await;
    store.mark_callback_delivered(delivered).unwrap();
    assert!(store.pending_callbacks().unwrap().is_empty());
    assert_eq!(
        store.get_run(run_id()).unwrap().callback_delivery,
        CallbackDeliveryState::Delivered
    );
}

#[test]
fn outbox_backpressure_reserves_terminal_callback_capacity_before_new_intake() {
    let directory = tempdir().unwrap();
    let store = SqliteStore::open(state_path(directory.path())).unwrap();
    store.observe_run(run_id(), worker_id()).unwrap();
    store
        .transition(run_id(), LocalLifecycleState::MessageValidated)
        .unwrap();
    store
        .transition(run_id(), LocalLifecycleState::CanonicalRunChecked)
        .unwrap();
    let claim_callback_id = ClaimCallbackId::parse("d744c5a2-1d8f-4a2e-bc4d-6813d6fc0133").unwrap();
    store
        .begin_claim_intent(run_id(), claim_callback_id)
        .unwrap();
    for _ in 0..999 {
        store
            .enqueue_callback(CallbackOutboxRecord::new(
                EventId::new(),
                run_id(),
                CallbackPayload::Event {
                    event_type: OperationalEventType::ProgressUpdate,
                    worker_id: worker_id(),
                    claim_callback_id,
                    stage: None,
                    step: None,
                    timestamp_ms: 1_798_000_000_000,
                    payload: serde_json::json!({}),
                },
            ))
            .unwrap();
    }

    let status = store.callback_outbox_status().unwrap();
    assert_eq!(status.pending, 999);
    assert!(status.intake_backpressured);

    let next_run = RunId::parse("f8a0c4c5-d628-7b93-a7cf-4b4629a28854").unwrap();
    store.observe_run(next_run, worker_id()).unwrap();
    let mut descriptor = execution_descriptor();
    descriptor.run_id = next_run;
    store.persist_execution_descriptor(descriptor).unwrap();
    assert!(matches!(
        store.reserve_intake_slot(next_run),
        Err(StoreError::OutboxBackpressure)
    ));
}

#[tokio::test]
async fn marking_a_missing_callback_as_delivered_reports_callback_ineligibility() {
    let directory = tempdir().unwrap();
    let (store, _) = authorized_store(&state_path(directory.path())).await;
    let event_id = EventId::parse("fa870068-6723-4b70-9e9f-53b8271e032d").unwrap();
    let authorization = callback_delivery_authorization_for(
        run_id(),
        event_id,
        worker_id(),
        ClaimCallbackId::parse("d744c5a2-1d8f-4a2e-bc4d-6813d6fc0133").unwrap(),
    )
    .await;

    assert!(matches!(
        store.mark_callback_delivered(authorization),
        Err(StoreError::CallbackNotEligible)
    ));
}

#[tokio::test]
async fn callback_attempt_counts_are_atomic_across_store_handles() {
    let directory = tempdir().unwrap();
    let path = state_path(directory.path());
    let (store, _) = authorized_store(&path).await;
    let event_id = EventId::parse("fa870068-6723-4b70-9e9f-53b8271e032d").unwrap();
    store
        .enqueue_callback(CallbackOutboxRecord::new(
            event_id,
            run_id(),
            CallbackPayload::Completed {
                worker_id: worker_id(),
                claim_callback_id: ClaimCallbackId::parse("d744c5a2-1d8f-4a2e-bc4d-6813d6fc0133")
                    .unwrap(),
                completed_at_ms: 1_798_000_000_000,
                exit_code: Some(0),
                summary: None,
                published_url: None,
                changed_file_count: None,
                outcome_summary: None,
            },
        ))
        .unwrap();

    const THREADS: usize = 6;
    const ATTEMPTS_PER_THREAD: usize = 100;
    let stores = (0..THREADS)
        .map(|_| SqliteStore::open(&path).unwrap())
        .collect::<Vec<_>>();
    let start = std::sync::Arc::new(std::sync::Barrier::new(THREADS));
    let workers = stores
        .into_iter()
        .map(|store| {
            let start = std::sync::Arc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                (0..ATTEMPTS_PER_THREAD)
                    .map(|_| store.begin_callback_attempt(event_id, 0, 0).unwrap())
                    .collect::<Vec<_>>()
            })
        })
        .collect::<Vec<_>>();
    let mut returned_attempts = workers
        .into_iter()
        .flat_map(|worker| worker.join().unwrap())
        .collect::<Vec<_>>();
    returned_attempts.sort_unstable();

    assert_eq!(
        returned_attempts,
        (1..=(THREADS * ATTEMPTS_PER_THREAD) as u32).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn callback_outbox_blocks_permanent_failures_and_rejects_mismatched_fences() {
    let directory = tempdir().unwrap();
    let path = state_path(directory.path());
    let (store, _) = authorized_store(&path).await;
    let event_id = EventId::parse("eb870068-6723-4b70-9e9f-53b8271e032d").unwrap();
    let mismatched = CallbackOutboxRecord::new(
        event_id,
        run_id(),
        CallbackPayload::Failed {
            worker_id: worker_id(),
            claim_callback_id: ClaimCallbackId::parse("e744c5a2-1d8f-4a2e-bc4d-6813d6fc0133")
                .unwrap(),
            completed_at_ms: 1_798_000_000_000,
            exit_code: None,
            failure_class: "worker_runtime".to_owned(),
            failure_message: "The worker stopped unexpectedly".to_owned(),
            outcome_summary: None,
        },
    );
    assert!(matches!(
        store.enqueue_callback(mismatched),
        Err(StoreError::CallbackAuthorizationMismatch)
    ));

    let callback = CallbackOutboxRecord::new(
        event_id,
        run_id(),
        CallbackPayload::Failed {
            worker_id: worker_id(),
            claim_callback_id: ClaimCallbackId::parse("d744c5a2-1d8f-4a2e-bc4d-6813d6fc0133")
                .unwrap(),
            completed_at_ms: 1_798_000_000_000,
            exit_code: None,
            failure_class: "worker_runtime".to_owned(),
            failure_message: "The worker stopped unexpectedly".to_owned(),
            outcome_summary: None,
        },
    );
    store.enqueue_callback(callback).unwrap();
    store.block_callback(event_id, "stale_claim").unwrap();
    assert_eq!(store.callback_outbox_counts().unwrap(), (0, 1));
    assert_eq!(
        store.get_run(run_id()).unwrap().callback_delivery,
        CallbackDeliveryState::Blocked
    );
    assert_eq!(store.recovery_candidates().unwrap().len(), 1);
}

#[test]
fn schema_v2_callback_outbox_migrates_retry_state_without_losing_rows() {
    let directory = tempdir().unwrap();
    let path = state_path(directory.path());
    prepare_state_path(&path);
    {
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE schema_migrations (
                    version INTEGER PRIMARY KEY NOT NULL,
                    applied_at_ms INTEGER NOT NULL
                );
                CREATE TABLE run_records (
                    run_id TEXT PRIMARY KEY NOT NULL,
                    worker_id TEXT NOT NULL,
                    lifecycle_state TEXT NOT NULL,
                    claim_callback_id TEXT UNIQUE,
                    start_callback_id TEXT UNIQUE,
                    start_authorization_source TEXT,
                    start_authorized INTEGER NOT NULL DEFAULT 0,
                    callback_delivery TEXT NOT NULL DEFAULT 'not_required',
                    recovery_attempts INTEGER NOT NULL DEFAULT 0,
                    recovery_action TEXT NOT NULL DEFAULT 'none',
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL
                );
                CREATE TABLE callback_outbox (
                    event_id TEXT PRIMARY KEY NOT NULL,
                    run_id TEXT NOT NULL REFERENCES run_records(run_id) ON DELETE CASCADE,
                    callback_kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    delivery_state TEXT NOT NULL CHECK (delivery_state IN ('pending', 'delivered')),
                    attempts INTEGER NOT NULL DEFAULT 0,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL
                );
                CREATE INDEX callback_outbox_pending_idx
                    ON callback_outbox(delivery_state, created_at_ms);
                CREATE TABLE execution_handoffs (
                    run_id TEXT PRIMARY KEY NOT NULL REFERENCES run_records(run_id) ON DELETE CASCADE,
                    descriptor_json TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL
                );
                CREATE TABLE execution_slot (
                    slot_id INTEGER PRIMARY KEY NOT NULL CHECK (slot_id = 1),
                    run_id TEXT NOT NULL UNIQUE REFERENCES run_records(run_id) ON DELETE CASCADE,
                    reserved_at_ms INTEGER NOT NULL
                );
                INSERT INTO run_records (
                    run_id, worker_id, lifecycle_state, created_at_ms, updated_at_ms
                ) VALUES (
                    'f8a0c4c5-d628-7b93-a7cf-4b4629a28853',
                    'fossil-host-01', 'execution_terminal', 10, 10
                );
                INSERT INTO callback_outbox (
                    event_id, run_id, callback_kind, payload_json, delivery_state,
                    attempts, created_at_ms, updated_at_ms
                ) VALUES (
                    'ea870068-6723-4b70-9e9f-53b8271e032d',
                    'f8a0c4c5-d628-7b93-a7cf-4b4629a28853', 'terminal', '{}',
                    'pending', 3, 11, 12
                );
                INSERT INTO schema_migrations(version, applied_at_ms) VALUES (1, 0);
                INSERT INTO schema_migrations(version, applied_at_ms) VALUES (2, 0);
                PRAGMA user_version = 2;",
            )
            .unwrap();
    }

    let _store = SqliteStore::open(&path).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    let version: u32 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, 4);
    let columns: Vec<String> = connection
        .prepare("PRAGMA table_info(callback_outbox)")
        .unwrap()
        .query_map([], |row| row.get(1))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert!(columns.contains(&"next_eligible_at_ms".to_owned()));
    assert!(columns.contains(&"failure_class".to_owned()));
    let (attempts, state): (u32, String) = connection
        .query_row(
            "SELECT attempts, delivery_state FROM callback_outbox WHERE event_id = ?1",
            ["ea870068-6723-4b70-9e9f-53b8271e032d"],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(attempts, 3);
    assert_eq!(state, "pending");
}

#[test]
fn callback_retry_deadlines_are_stable_and_capped() {
    let event = EventId::parse("eb870068-6723-4b70-9e9f-53b8271e032d").unwrap();
    let first = callback_retry_deadline_ms(1_000, 5, event);
    assert_eq!(first, callback_retry_deadline_ms(1_000, 5, event));
    assert!((17_000..=25_000).contains(&first));
    let capped = callback_retry_deadline_ms(10_000, 100, event);
    assert!(capped <= 910_000);
    assert!(capped > 10_000);
}

#[tokio::test]
// Soyuz requirement: WORKER-START-01.
async fn start_authorization_cannot_be_written_without_a_soyuz_proof() {
    let directory = tempdir().unwrap();
    let store = SqliteStore::open(state_path(directory.path())).unwrap();
    store.observe_run(run_id(), worker_id()).unwrap();
    store
        .persist_execution_descriptor(execution_descriptor())
        .unwrap();
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
    let claim_authorization = claim_authorization_for(
        run_id(),
        worker_id(),
        ClaimCallbackId::parse("d744c5a2-1d8f-4a2e-bc4d-6813d6fc0133").unwrap(),
    )
    .await;
    store
        .record_claim_authorization(run_id(), claim_authorization)
        .unwrap();
    store
        .begin_start_intent(
            run_id(),
            EventId::parse("15870068-6723-4b70-9e9f-53b8271e032d").unwrap(),
        )
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
    let incompatible_path = state_path(directory.path()).with_file_name("future.sqlite3");
    prepare_state_path(&incompatible_path);
    let connection = rusqlite::Connection::open(&incompatible_path).unwrap();
    connection
        .execute_batch("PRAGMA user_version = 999;")
        .unwrap();
    drop(connection);
    assert!(matches!(
        SqliteStore::open(&incompatible_path),
        Err(StoreError::UnsupportedSchemaVersion(999))
    ));

    let corrupt_path = state_path(directory.path()).with_file_name("corrupt.sqlite3");
    fs::write(&corrupt_path, b"not a sqlite database").unwrap();
    assert!(SqliteStore::open(&corrupt_path).is_err());
}

#[test]
fn recovery_metadata_is_bounded_and_durable() {
    let directory = tempdir().unwrap();
    let path = state_path(directory.path());
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

#[test]
fn recovery_outcomes_are_durable_without_counting_an_extra_attempt() {
    let directory = tempdir().unwrap();
    let store = SqliteStore::open(state_path(directory.path())).unwrap();
    store.observe_run(run_id(), worker_id()).unwrap();
    store
        .record_recovery_action(run_id(), RecoveryAction::AwaitingSoyuz)
        .unwrap();

    store
        .set_recovery_action(run_id(), RecoveryAction::Recovered)
        .unwrap();

    let record = store.get_run(run_id()).unwrap();
    let diagnostics = store.recovery_diagnostics().unwrap();
    assert_eq!(record.recovery_attempts, 1);
    assert_eq!(record.recovery_action, RecoveryAction::Recovered);
    assert_eq!(diagnostics.recovered_runs, 1);
    assert_eq!(diagnostics.awaiting_soyuz_runs, 0);
}
