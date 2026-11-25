mod support;

use fossil_core::contract::SoyuzRunStatus;
use fossil_core::ids::{ClaimCallbackId, EventId, RunId, WorkerId};
use fossil_core::lifecycle::LocalLifecycleState;
use fossil_core::queue::CloudflarePullResponse;
use fossil_queue::QueueDisposition;
use fossil_soyuz::{
    FailedCallback, OperationalEvent, SoyuzApi, SoyuzClient, SoyuzClientConfig, SoyuzClientError,
    StartedRequest, WorkerClaimRequest,
};
use fossil_storage::{CallbackOutboxRecord, CallbackPayload, SqliteStore};
use rusty_fossil::coordinator::{AuthorizationCoordinator, CallbackReplaySummary};
use secrecy::SecretString;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use url::Url;

use support::soyuz_simulator::{SoyuzSimulator, SoyuzSimulatorSnapshot};

const WORKER_ID: &str = "fossil-simulator-test";
const TOKEN: &str = "soyuz-simulator-worker-token";

struct Harness {
    _directory: TempDir,
    simulator: SoyuzSimulator,
    store: SqliteStore,
    coordinator: AuthorizationCoordinator,
    run_id: RunId,
}

impl Harness {
    async fn new() -> Self {
        let message = queue_message();
        let run_id = message.decode_envelope().unwrap().run_id;
        let directory = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(directory.path().join("fossil.sqlite3")).unwrap();
        let simulator = SoyuzSimulator::start(TOKEN).await.unwrap();
        simulator.seed_queued_run(run_id, "soyuz-sim-1").await;
        let worker_id = WorkerId::parse(WORKER_ID).unwrap();
        let client = SoyuzClient::new(
            SoyuzClientConfig::new(
                Url::parse(&simulator.uri()).unwrap(),
                SecretString::from(simulator.token().to_owned()),
                Duration::from_secs(2),
            )
            .unwrap(),
        )
        .unwrap();
        let coordinator =
            AuthorizationCoordinator::new(Arc::new(client), store.clone(), worker_id.clone());

        Self {
            _directory: directory,
            simulator,
            store,
            coordinator,
            run_id,
        }
    }

    async fn canonical(&self) -> SoyuzSimulatorSnapshot {
        self.simulator.snapshot(self.run_id).await.unwrap()
    }

    async fn deliver(&self) -> QueueDisposition {
        self.coordinator.process(queue_message()).await
    }
}

fn queue_message() -> fossil_core::queue::CloudflareQueueMessage {
    serde_json::from_slice::<CloudflarePullResponse>(include_bytes!(
        "../../fossil-core/tests/fixtures/cloudflare-pull-v1.json"
    ))
    .unwrap()
    .result
    .unwrap()
    .messages
    .into_iter()
    .next()
    .unwrap()
}

fn client(simulator: &SoyuzSimulator) -> SoyuzClient {
    SoyuzClient::new(
        SoyuzClientConfig::new(
            Url::parse(&simulator.uri()).unwrap(),
            SecretString::from(simulator.token().to_owned()),
            Duration::from_secs(2),
        )
        .unwrap(),
    )
    .unwrap()
}

fn run_id() -> RunId {
    queue_message().decode_envelope().unwrap().run_id
}

#[tokio::test]
// Soyuz requirements: WORKER-START-02 and WORKER-CLAIM-RECOVERY-01.
async fn committed_started_response_loss_reconciles_and_opens_the_durable_gate() {
    let harness = Harness::new().await;
    harness
        .simulator
        .fail_after_commit_once(harness.run_id, "started")
        .await;

    assert_eq!(harness.deliver().await, QueueDisposition::Acknowledge);

    let canonical = harness.canonical().await;
    assert_eq!(canonical.status, SoyuzRunStatus::Running);
    assert_eq!(canonical.worker_id.as_deref(), Some(WORKER_ID));
    assert!(canonical.claim_callback_id.is_some());
    assert_eq!(canonical.callback_count("claim"), 1);
    assert_eq!(canonical.callback_count("started"), 1);
    assert_eq!(canonical.request_count("started"), 1);

    let local = harness.store.get_run(harness.run_id).unwrap();
    assert_eq!(
        local.lifecycle_state,
        LocalLifecycleState::StartAuthorizationPersisted
    );
    assert!(local.start_authorized);
    assert_eq!(
        harness.store.execution_slot_owner().unwrap(),
        Some(harness.run_id)
    );
}

#[tokio::test]
// Soyuz requirements: CANCEL-RUNNING-01 and WORKER-CANCEL-01.
async fn cancellation_after_started_commit_prevents_execution_and_replays_terminal_callback() {
    let harness = Harness::new().await;
    harness
        .simulator
        .cancel_after_started_once(harness.run_id)
        .await;

    assert_eq!(harness.deliver().await, QueueDisposition::Acknowledge);

    let canonical = harness.canonical().await;
    assert_eq!(canonical.status, SoyuzRunStatus::CancelRequested);
    assert!(canonical.cancel_requested_at.is_some());
    let local = harness.store.get_run(harness.run_id).unwrap();
    assert_eq!(
        local.lifecycle_state,
        LocalLifecycleState::ExecutionTerminal
    );
    assert_eq!(harness.store.execution_slot_owner().unwrap(), None);
    assert_eq!(harness.store.pending_callbacks().unwrap().len(), 1);

    assert_eq!(
        harness.coordinator.replay_pending_callbacks(1).await,
        CallbackReplaySummary {
            attempted: 1,
            delivered: 1,
            retried: 0,
            blocked: 0,
        }
    );
    assert_eq!(harness.canonical().await.status, SoyuzRunStatus::Cancelled);
}

#[tokio::test]
// Soyuz requirements: CALLBACK-IDEMPOTENCY-01 and WORKER-COMPLETE-01.
async fn terminal_callback_replay_uses_the_same_id_after_commit_response_loss() {
    let harness = Harness::new().await;
    assert_eq!(harness.deliver().await, QueueDisposition::Acknowledge);
    let claim_callback_id = harness
        .store
        .get_run(harness.run_id)
        .unwrap()
        .claim_callback_id
        .unwrap();
    let event_id = EventId::parse("a3be4bf3-d14a-4e51-8cb5-c0b33d7e2afd").unwrap();
    let completed_at_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    harness
        .store
        .transition_terminal_with_callback(
            harness.run_id,
            CallbackOutboxRecord::new(
                event_id,
                harness.run_id,
                CallbackPayload::Completed {
                    worker_id: WorkerId::parse(WORKER_ID).unwrap(),
                    claim_callback_id,
                    completed_at_ms,
                    exit_code: Some(0),
                    summary: Some("simulated completion".to_owned()),
                    published_url: None,
                    changed_file_count: Some(1),
                    outcome_summary: None,
                },
            ),
        )
        .unwrap();
    harness
        .simulator
        .fail_after_commit_once(harness.run_id, "completed")
        .await;

    let first = harness.coordinator.replay_pending_callbacks(1).await;
    assert_eq!(first.attempted, 1);
    assert_eq!(first.retried, 1);
    assert_eq!(harness.canonical().await.status, SoyuzRunStatus::Completed);
    assert_eq!(harness.canonical().await.callback_count("completed"), 1);

    harness
        .store
        .defer_callback(event_id, "soyuz_unavailable", 0)
        .unwrap();
    let replay = harness.coordinator.replay_pending_callbacks(1).await;
    assert_eq!(replay.delivered, 1);
    let canonical = harness.canonical().await;
    assert_eq!(canonical.status, SoyuzRunStatus::Completed);
    assert_eq!(canonical.callback_count("completed"), 1);
    assert_eq!(canonical.request_count("completed"), 2);
}

#[tokio::test]
// Soyuz requirements: WORKER-CLAIM-RACE-01, WORKER-START-01, WORKER-EVENT-01, and CALLBACK-IDEMPOTENCY-01.
async fn concurrent_workers_have_one_claim_winner_and_callbacks_are_idempotent() {
    let simulator = SoyuzSimulator::start(TOKEN).await.unwrap();
    let run_id = run_id();
    simulator.seed_queued_run(run_id, "soyuz-sim-race").await;
    let client_a = client(&simulator);
    let client_b = client(&simulator);
    let worker_a = WorkerId::parse("worker-a").unwrap();
    let worker_b = WorkerId::parse("worker-b").unwrap();
    let claim_a = ClaimCallbackId::parse("d744c5a2-1d8f-4a2e-bc4d-6813d6fc0133").unwrap();
    let claim_b = ClaimCallbackId::parse("b267af18-7536-48f9-9047-1e420f9b50d8").unwrap();

    let (result_a, result_b) = tokio::join!(
        client_a.claim(WorkerClaimRequest {
            run_id,
            callback_id: claim_a,
            worker_id: worker_a.clone(),
            lease_seconds: Some(120),
        }),
        client_b.claim(WorkerClaimRequest {
            run_id,
            callback_id: claim_b,
            worker_id: worker_b.clone(),
            lease_seconds: Some(120),
        }),
    );
    let (winning_client, winning_worker, winning_claim) = match (result_a, result_b) {
        (Ok(_), Err(SoyuzClientError::Conflict)) => (&client_a, worker_a, claim_a),
        (Err(SoyuzClientError::Conflict), Ok(_)) => (&client_b, worker_b, claim_b),
        other => panic!("expected exactly one claim winner, got {other:?}"),
    };

    let started_id = EventId::parse("15870068-6723-4b70-9e9f-53b8271e032d").unwrap();
    let started = StartedRequest {
        run_id,
        callback_id: started_id,
        worker_id: winning_worker.clone(),
        claim_callback_id: Some(winning_claim),
        started_at: None,
        stage: Some("prepare".to_owned()),
    };
    winning_client.started(started.clone()).await.unwrap();
    winning_client.started(started).await.unwrap();
    let after_start = simulator.snapshot(run_id).await.unwrap();
    assert_eq!(after_start.stage.as_deref(), Some("prepare"));
    assert_eq!(after_start.operational_health, "healthy");

    let event_id = EventId::parse("a3be4bf3-d14a-4e51-8cb5-c0b33d7e2afd").unwrap();
    let event = OperationalEvent {
        run_id,
        event_id,
        worker_id: winning_worker.clone(),
        claim_callback_id: Some(winning_claim),
        event_type: fossil_soyuz::OperationalEventType::ProgressUpdate,
        stage: None,
        step: None,
        timestamp: None,
        payload: Some(serde_json::json!({ "percent": 25 })),
    };
    winning_client.event(event.clone()).await.unwrap();
    winning_client.event(event.clone()).await.unwrap();
    let reused = OperationalEvent {
        payload: Some(serde_json::json!({ "percent": 50 })),
        ..event
    };
    assert!(matches!(
        winning_client.event(reused).await,
        Err(SoyuzClientError::Conflict)
    ));

    let snapshot = simulator.snapshot(run_id).await.unwrap();
    assert_eq!(snapshot.status, SoyuzRunStatus::Running);
    assert_eq!(snapshot.callback_count("started"), 1);
    assert_eq!(snapshot.callback_count("events"), 1);
}

#[tokio::test]
// Soyuz requirement: WORKER-START-02.
async fn started_is_rejected_when_the_claim_lease_has_expired() {
    let simulator = SoyuzSimulator::start(TOKEN).await.unwrap();
    let run_id = run_id();
    simulator.seed_queued_run(run_id, "soyuz-sim-expiry").await;
    let client = client(&simulator);
    let worker_id = WorkerId::parse("worker-expiry").unwrap();
    let claim_callback_id = ClaimCallbackId::parse("d744c5a2-1d8f-4a2e-bc4d-6813d6fc0133").unwrap();
    client
        .claim(WorkerClaimRequest {
            run_id,
            callback_id: claim_callback_id,
            worker_id: worker_id.clone(),
            lease_seconds: Some(30),
        })
        .await
        .unwrap();
    simulator.advance_time(Duration::from_secs(31)).await;

    assert!(matches!(
        client
            .started(StartedRequest {
                run_id,
                callback_id: EventId::parse("15870068-6723-4b70-9e9f-53b8271e032d").unwrap(),
                worker_id,
                claim_callback_id: Some(claim_callback_id),
                started_at: None,
                stage: None,
            })
            .await,
        Err(SoyuzClientError::Conflict)
    ));
    assert_eq!(
        simulator.snapshot(run_id).await.unwrap().status,
        SoyuzRunStatus::Claimed
    );
    assert!(
        simulator
            .snapshot(run_id)
            .await
            .unwrap()
            .claim_expires_at
            .is_some()
    );
}

#[tokio::test]
// Soyuz requirement: WORKER-CLAIM-FENCE-01.
async fn replacement_claim_fences_the_previous_attempt() {
    let simulator = SoyuzSimulator::start(TOKEN).await.unwrap();
    let run_id = run_id();
    simulator.seed_queued_run(run_id, "soyuz-sim-fence").await;
    let client = client(&simulator);
    let worker_id = WorkerId::parse("worker-reused-host-id").unwrap();
    let old_claim_id = ClaimCallbackId::parse("d744c5a2-1d8f-4a2e-bc4d-6813d6fc0133").unwrap();
    let new_claim_id = ClaimCallbackId::parse("b267af18-7536-48f9-9047-1e420f9b50d8").unwrap();
    client
        .claim(WorkerClaimRequest {
            run_id,
            callback_id: old_claim_id,
            worker_id: worker_id.clone(),
            lease_seconds: Some(30),
        })
        .await
        .unwrap();
    simulator.advance_time(Duration::from_secs(31)).await;
    assert!(simulator.reconcile_expired_claim(run_id).await.unwrap());
    client
        .claim(WorkerClaimRequest {
            run_id,
            callback_id: new_claim_id,
            worker_id: worker_id.clone(),
            lease_seconds: Some(120),
        })
        .await
        .unwrap();

    let stale_start = client
        .started(StartedRequest {
            run_id,
            callback_id: EventId::parse("15870068-6723-4b70-9e9f-53b8271e032d").unwrap(),
            worker_id: worker_id.clone(),
            claim_callback_id: Some(old_claim_id),
            started_at: None,
            stage: None,
        })
        .await;
    assert!(matches!(stale_start, Err(SoyuzClientError::StaleClaim)));

    client
        .started(StartedRequest {
            run_id,
            callback_id: EventId::parse("a3be4bf3-d14a-4e51-8cb5-c0b33d7e2afd").unwrap(),
            worker_id,
            claim_callback_id: Some(new_claim_id),
            started_at: None,
            stage: None,
        })
        .await
        .unwrap();
    let snapshot = simulator.snapshot(run_id).await.unwrap();
    assert_eq!(snapshot.status, SoyuzRunStatus::Running);
    let expected_claim_id = new_claim_id.as_str();
    assert_eq!(
        snapshot.claim_callback_id.as_deref(),
        Some(expected_claim_id.as_str())
    );
}

#[tokio::test]
// Soyuz requirement: CANCEL-QUEUED-01.
async fn queued_cancellation_is_terminal_and_prevents_a_late_claim() {
    let simulator = SoyuzSimulator::start(TOKEN).await.unwrap();
    let run_id = run_id();
    simulator.seed_queued_run(run_id, "soyuz-sim-cancel").await;
    simulator.request_cancel(run_id).await.unwrap();

    let response = client(&simulator)
        .claim(WorkerClaimRequest {
            run_id,
            callback_id: ClaimCallbackId::parse("d744c5a2-1d8f-4a2e-bc4d-6813d6fc0133").unwrap(),
            worker_id: WorkerId::parse("worker-after-cancel").unwrap(),
            lease_seconds: None,
        })
        .await;

    assert!(matches!(response, Err(SoyuzClientError::Conflict)));
    let snapshot = simulator.snapshot(run_id).await.unwrap();
    assert_eq!(snapshot.status, SoyuzRunStatus::Cancelled);
    assert!(snapshot.cancel_requested_at.is_some());
}

#[tokio::test]
// Soyuz requirements: WORKER-PRESTART-FAIL-01, WORKER-FAIL-01, and CALLBACK-IDEMPOTENCY-01.
async fn claimed_run_accepts_an_idempotent_prestart_failure_callback() {
    let simulator = SoyuzSimulator::start(TOKEN).await.unwrap();
    let run_id = run_id();
    simulator
        .seed_queued_run(run_id, "soyuz-sim-prestart-failure")
        .await;
    let client = client(&simulator);
    let worker_id = WorkerId::parse("worker-prestart-failure").unwrap();
    let claim_callback_id = ClaimCallbackId::parse("d744c5a2-1d8f-4a2e-bc4d-6813d6fc0133").unwrap();
    client
        .claim(WorkerClaimRequest {
            run_id,
            callback_id: claim_callback_id,
            worker_id: worker_id.clone(),
            lease_seconds: None,
        })
        .await
        .unwrap();

    let callback_id = EventId::parse("15870068-6723-4b70-9e9f-53b8271e032d").unwrap();
    let callback = FailedCallback {
        run_id,
        callback_id,
        worker_id,
        claim_callback_id: Some(claim_callback_id),
        completed_at: None,
        exit_code: None,
        failure_class: "admission_failed".to_owned(),
        failure_message: "Worker setup was rejected before execution.".to_owned(),
        outcome_summary: None,
    };
    client.failed(callback.clone()).await.unwrap();
    client.failed(callback.clone()).await.unwrap();
    let changed = FailedCallback {
        failure_message: "Different failure for the same callback ID.".to_owned(),
        ..callback
    };
    assert!(matches!(
        client.failed(changed).await,
        Err(SoyuzClientError::Conflict)
    ));

    let snapshot = simulator.snapshot(run_id).await.unwrap();
    assert_eq!(snapshot.status, SoyuzRunStatus::Failed);
    assert_eq!(snapshot.callback_count("failed"), 1);
}
