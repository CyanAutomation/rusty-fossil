#![cfg(feature = "test-support")]

use fossil_core::execution::SyntheticScenario;
use fossil_core::ids::{ClaimCallbackId, EventId, RunId, WorkerId};
use fossil_core::lifecycle::LocalLifecycleState;
use fossil_core::queue::QueueEnvelope;
use fossil_soyuz::{SoyuzApi, SoyuzClient, SoyuzClientConfig, StartedRequest, WorkerClaimRequest};
use fossil_storage::{CallbackPayload, ContainerLaunchState, ExecutionDescriptor, SqliteStore};
use fossil_supervisor::{
    DockerRuntimeConfig, DockerSupervisor, ExecutionOutcome, SupervisorLimitSettings,
    SupervisorLimits, TerminalReason,
};
use secrecy::SecretString;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tempfile::{TempDir, tempdir};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use url::Url;
use uuid::Uuid;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

const WORKER: &str = "fossil-docker-integration";
const CLAIM: &str = "d744c5a2-1d8f-4a2e-bc4d-6813d6fc0133";
const START: &str = "15870068-6723-4b70-9e9f-53b8271e032d";

struct DockerHarness {
    _directory: TempDir,
    _soyuz_mock: MockServer,
    pub store: SqliteStore,
    pub run_id: RunId,
    pub supervisor: DockerSupervisor,
    pub cancel_requested: Arc<AtomicBool>,
    pub docker_socket: String,
}

#[tokio::test]
#[ignore = "requires a Docker Engine and a locally built synthetic worker image ID"]
async fn soyuz_authorized_synthetic_container_runs_and_is_cleaned_up() {
    let harness = authorized_harness(SupervisorLimits::default()).await;
    let outcome = launch(&harness, SyntheticScenario::Success).await;

    assert_eq!(outcome.reason, TerminalReason::Completed);
    assert_eq!(outcome.workload_id, "synthetic-v1");
    assert!(outcome.started_at.is_some());
    assert!(outcome.completed_at.is_some());
    assert_eq!(harness.store.execution_slot_owner().unwrap(), None);
    assert_eq!(
        harness
            .store
            .container_launch(harness.run_id)
            .unwrap()
            .unwrap()
            .state,
        ContainerLaunchState::CleanupCompleted
    );
    assert_eq!(
        harness
            .store
            .get_run(harness.run_id)
            .unwrap()
            .lifecycle_state,
        LocalLifecycleState::ExecutionTerminal
    );
    let callback = harness.store.pending_callbacks().unwrap().remove(0);
    assert!(matches!(
        callback.payload,
        CallbackPayload::Completed { .. }
    ));
}

#[tokio::test]
#[ignore = "requires a Docker Engine and a locally built synthetic worker image ID"]
async fn nonzero_worker_exit_is_persisted_as_failure() {
    let harness = authorized_harness(SupervisorLimits::default()).await;
    let outcome = launch(&harness, SyntheticScenario::ControlledFailure).await;

    assert_eq!(outcome.reason, TerminalReason::NonzeroExit);
    assert_eq!(outcome.exit_code, Some(17));
    let callback = harness.store.pending_callbacks().unwrap().remove(0);
    assert!(matches!(
        callback.payload,
        CallbackPayload::Failed {
            failure_class,
            ..
        } if failure_class == "nonzero_exit"
    ));
}

#[tokio::test]
#[ignore = "requires a Docker Engine and a locally built synthetic worker image ID"]
async fn timeout_stops_the_container_and_persists_timeout_callback() {
    let limits = limits_with_runtime(Duration::from_secs(1));
    let harness = authorized_harness(limits).await;
    let outcome = launch(&harness, SyntheticScenario::Timeout).await;

    assert_eq!(outcome.reason, TerminalReason::TimedOut);
    let callback = harness.store.pending_callbacks().unwrap().remove(0);
    assert!(matches!(
        callback.payload,
        CallbackPayload::Failed {
            failure_class,
            ..
        } if failure_class == "timeout"
    ));
}

#[tokio::test]
#[ignore = "requires a Docker Engine and a locally built synthetic worker image ID"]
async fn soyuz_cancellation_stops_the_running_container_before_callback() {
    let harness = authorized_harness(SupervisorLimits::default()).await;
    let supervisor = harness.supervisor.clone();
    let run_id = harness.run_id;
    let launch_task = tokio::spawn(async move {
        supervisor
            .launch(run_id, SyntheticScenario::Cancellation)
            .await
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        harness
            .store
            .container_launch(run_id)
            .unwrap()
            .unwrap()
            .state,
        ContainerLaunchState::ContainerRunning,
        "cancellation must race with a confirmed running worker"
    );
    harness.cancel_requested.store(true, Ordering::SeqCst);
    let outcome = tokio::time::timeout(Duration::from_secs(20), launch_task)
        .await
        .expect("cancellation did not drain")
        .unwrap()
        .unwrap();

    assert_eq!(outcome.reason, TerminalReason::Cancelled);
    assert_eq!(harness.store.execution_slot_owner().unwrap(), None);
    let callback = harness.store.pending_callbacks().unwrap().remove(0);
    assert!(matches!(
        callback.payload,
        CallbackPayload::Cancelled { .. }
    ));
}

#[tokio::test]
#[ignore = "requires a Docker Engine and a locally built synthetic worker image ID"]
async fn output_stress_is_truncated_before_terminal_callback() {
    let harness = authorized_harness(SupervisorLimits::default()).await;
    let outcome = launch(&harness, SyntheticScenario::BoundedOutput).await;

    assert_eq!(outcome.reason, TerminalReason::Completed);
    assert!(outcome.output_truncated);
    assert!(outcome.diagnostic_summary.len() <= 512);
}

#[tokio::test]
#[ignore = "requires a Docker Engine and a locally built synthetic worker image ID"]
async fn abrupt_worker_termination_is_not_reported_as_success() {
    let harness = authorized_harness(SupervisorLimits::default()).await;
    let outcome = launch(&harness, SyntheticScenario::UnexpectedTermination).await;

    assert_eq!(outcome.reason, TerminalReason::UnexpectedTermination);
    assert_ne!(outcome.exit_code, Some(0));
}

async fn launch(harness: &DockerHarness, scenario: SyntheticScenario) -> ExecutionOutcome {
    let result: Result<ExecutionOutcome, _> =
        harness.supervisor.launch(harness.run_id, scenario).await;
    if let Err(error) = &result {
        eprintln!("Docker integration launch failed: {error:?}");
        diagnose_container(harness).await;
        cleanup_owned_container(&harness.store, harness.run_id, &harness.docker_socket).await;
    }
    result.unwrap()
}

async fn diagnose_container(harness: &DockerHarness) {
    use bollard::Docker;

    let Ok(docker) =
        Docker::connect_with_socket(&harness.docker_socket, 30, bollard::API_DEFAULT_VERSION)
    else {
        return;
    };
    let Ok(docker) = docker.negotiate_version().await else {
        return;
    };
    let Ok(Some(launch)) = harness.store.container_launch(harness.run_id) else {
        return;
    };
    let Ok(inspect) = docker
        .inspect_container(
            &launch.container_name,
            None::<bollard::query_parameters::InspectContainerOptions>,
        )
        .await
    else {
        return;
    };
    eprintln!(
        "Docker integration inspect state: launch={:?}, config={:?}, host_config={:?}, state={:?}",
        launch.state, inspect.config, inspect.host_config, inspect.state
    );
}

async fn authorized_harness(limits: SupervisorLimits) -> DockerHarness {
    let image = std::env::var("FOSSIL_SUPERVISOR_TEST_IMAGE")
        .expect("CI builds the test worker and supplies its immutable image ID");
    let docker_socket = std::env::var("FOSSIL_SUPERVISOR_TEST_DOCKER_SOCKET")
        .unwrap_or_else(|_| "/var/run/docker.sock".to_owned());
    let directory = private_tempdir();
    let store = SqliteStore::open(directory.path().join("state.sqlite3")).unwrap();
    let run_id = RunId::parse(&Uuid::now_v7().to_string()).unwrap();
    let worker_id = WorkerId::parse(WORKER).unwrap();
    let claim_id = ClaimCallbackId::parse(CLAIM).unwrap();
    let start_id = EventId::parse(START).unwrap();
    let mut envelope = QueueEnvelope::parse(include_bytes!(
        "../../fossil-core/tests/fixtures/queue-v1-valid.json"
    ))
    .unwrap();
    envelope.run_id = run_id;
    store.observe_run(run_id, worker_id.clone()).unwrap();
    store
        .persist_execution_descriptor(ExecutionDescriptor::from(&envelope))
        .unwrap();
    store
        .transition(run_id, LocalLifecycleState::MessageValidated)
        .unwrap();
    store
        .transition(run_id, LocalLifecycleState::CanonicalRunChecked)
        .unwrap();
    store.begin_claim_intent(run_id, claim_id).unwrap();

    let soyuz_mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("/v1/worker/runs/{run_id}/claim")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": {"accepted": true},
            "requestId": "claim-request"
        })))
        .mount(&soyuz_mock)
        .await;
    Mock::given(method("POST"))
        .and(path(format!("/v1/worker/runs/{run_id}/started")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": {},
            "requestId": "started-request"
        })))
        .mount(&soyuz_mock)
        .await;
    let cancel_requested = Arc::new(AtomicBool::new(false));
    let cancel_response = cancel_requested.clone();
    Mock::given(method("GET"))
        .and(path(format!("/v1/worker/runs/{run_id}")))
        .respond_with(move |_request: &Request| {
            let now = OffsetDateTime::now_utc();
            let future = (now + time::Duration::minutes(2))
                .format(&Rfc3339)
                .unwrap();
            let body = serde_json::json!({
                "data": {
                    "runId": run_id.to_string(),
                    "displayId": "soyuz-docker-integration",
                    "status": if cancel_response.load(Ordering::SeqCst) { "cancel_requested" } else { "running" },
                    "cancelRequestedAt": if cancel_response.load(Ordering::SeqCst) { Some(now.format(&Rfc3339).unwrap()) } else { None::<String> },
                    "claimExpiresAt": future,
                    "workerId": WORKER,
                    "claimCallbackId": CLAIM,
                    "stage": "running",
                    "updatedAt": now.format(&Rfc3339).unwrap(),
                    "lastHeartbeatAt": now.format(&Rfc3339).unwrap(),
                    "operationalHealth": "healthy"
                },
                "requestId": "canonical-read"
            });
            ResponseTemplate::new(200).set_body_json(body)
        })
        .mount(&soyuz_mock)
        .await;
    let soyuz = Arc::new(
        SoyuzClient::new(
            SoyuzClientConfig::new(
                Url::parse(&soyuz_mock.uri()).unwrap(),
                SecretString::from("test-worker-token".to_owned()),
                Duration::from_secs(2),
            )
            .unwrap(),
        )
        .unwrap(),
    );
    let claim = soyuz
        .claim(WorkerClaimRequest {
            run_id,
            callback_id: claim_id,
            worker_id: worker_id.clone(),
            lease_seconds: Some(120),
        })
        .await
        .unwrap();
    store.record_claim_authorization(run_id, claim).unwrap();
    store.begin_start_intent(run_id, start_id).unwrap();
    let started = soyuz
        .started(StartedRequest {
            run_id,
            callback_id: start_id,
            worker_id: worker_id.clone(),
            claim_callback_id: Some(claim_id),
            started_at: None,
            stage: None,
        })
        .await
        .unwrap();
    store.record_start_authorization(run_id, started).unwrap();
    store.reserve_execution_slot(run_id).unwrap();

    let docker_config =
        DockerRuntimeConfig::with_test_image_id(image, &docker_socket, limits).unwrap();
    let supervisor = DockerSupervisor::connect(docker_config, store.clone(), soyuz, worker_id)
        .await
        .unwrap();
    DockerHarness {
        _directory: directory,
        _soyuz_mock: soyuz_mock,
        store,
        run_id,
        supervisor,
        cancel_requested,
        docker_socket,
    }
}

fn private_tempdir() -> TempDir {
    let directory = tempdir().unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    directory
}

#[test]
fn integration_state_directory_is_owner_only() {
    let directory = private_tempdir();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(directory.path())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
    }
}

fn limits_with_runtime(max_runtime: Duration) -> SupervisorLimits {
    SupervisorLimits::new(SupervisorLimitSettings {
        max_runtime,
        cancellation_grace: Duration::from_secs(1),
        monitor_cadence: Duration::from_millis(100),
        ..SupervisorLimitSettings::default()
    })
    .unwrap()
}

async fn cleanup_owned_container(store: &SqliteStore, run_id: RunId, docker_socket: &str) {
    use bollard::Docker;
    use bollard::query_parameters::{RemoveContainerOptionsBuilder, StopContainerOptionsBuilder};

    let Ok(Some(launch)) = store.container_launch(run_id) else {
        return;
    };
    let Ok(docker) = Docker::connect_with_socket(docker_socket, 30, bollard::API_DEFAULT_VERSION)
    else {
        return;
    };
    let Ok(docker) = docker.negotiate_version().await else {
        return;
    };
    let Ok(container) = docker
        .inspect_container(
            &launch.container_name,
            None::<bollard::query_parameters::InspectContainerOptions>,
        )
        .await
    else {
        return;
    };
    let owned = container
        .config
        .as_ref()
        .and_then(|config| config.labels.as_ref())
        .is_some_and(|labels| {
            labels
                .get("io.rusty-fossil.managed")
                .is_some_and(|value| value == "true")
                && labels
                    .get("io.rusty-fossil.run-id")
                    .is_some_and(|value| value == &run_id.to_string())
        });
    if !owned {
        return;
    }
    let _ = docker
        .stop_container(
            &launch.container_name,
            Some(StopContainerOptionsBuilder::default().t(2).build()),
        )
        .await;
    let _ = docker
        .remove_container(
            &launch.container_name,
            Some(
                RemoveContainerOptionsBuilder::default()
                    .force(true)
                    .v(false)
                    .link(false)
                    .build(),
            ),
        )
        .await;
}
