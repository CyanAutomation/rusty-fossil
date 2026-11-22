use bollard::Docker;
use bollard::models::ContainerStateStatusEnum;
use fossil_core::ids::{RunId, WorkerId};
use fossil_core::lifecycle::{CallbackDeliveryState, LocalLifecycleState};
use fossil_core::queue::CloudflarePullResponse;
use fossil_queue::QueueDisposition;
use fossil_soyuz::{SoyuzClient, SoyuzClientConfig};
use fossil_storage::{CallbackPayload, ContainerLaunchRecord, ContainerLaunchState, SqliteStore};
use fossil_supervisor::SupervisorIpcClient;
use rusty_fossil::coordinator::AuthorizationCoordinator;
use secrecy::SecretString;
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::process::{Command as StdCommand, Stdio};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::{TempDir, tempdir};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::net::UnixStream;
use tokio::process::{Child, Command};
use tokio::time::timeout;
use url::Url;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

const WORKER_ID: &str = "fossil-e2e-worker";
const IPC_TOKEN: &str = "integration-test-supervisor-token-32bytes";
const STATUS_QUEUED: u8 = 0;
const STATUS_CLAIMED: u8 = 1;
const STATUS_RUNNING: u8 = 2;
const STATUS_CANCEL_REQUESTED: u8 = 3;

struct ProcessHarness {
    _directory: TempDir,
    _soyuz: MockServer,
    state: Arc<AtomicU8>,
    delivered_callback_id: Arc<Mutex<Option<String>>>,
    pub store: SqliteStore,
    pub coordinator: Arc<AuthorizationCoordinator>,
    pub run_id: RunId,
    pub worker_id: WorkerId,
    pub image: String,
    barrier_path: PathBuf,
    pub socket_path: PathBuf,
    pub token_path: PathBuf,
    pub soyuz_token_path: PathBuf,
    pub state_path: PathBuf,
    pub docker_socket: String,
    pub docker: Docker,
}

impl ProcessHarness {
    async fn new() -> Self {
        let image = std::env::var("FOSSIL_SUPERVISOR_TEST_IMAGE")
            .expect("Docker CI supplies an immutable local worker image ID");
        let docker_socket = std::env::var("FOSSIL_SUPERVISOR_TEST_DOCKER_SOCKET")
            .unwrap_or_else(|_| "/var/run/docker.sock".to_owned());
        let directory = private_tempdir();
        let state_path = directory.path().join("state.sqlite3");
        let socket_path = directory.path().join("supervisor.sock");
        let token_path = directory.path().join("supervisor-token");
        let soyuz_token_path = directory.path().join("soyuz-token");
        let barrier_path = directory.path().join("supervisor-barrier");
        write_private_file(&token_path, IPC_TOKEN);
        write_private_file(&soyuz_token_path, "test-worker-token");

        let run_id = queue_message().decode_envelope().unwrap().run_id;
        let worker_id = WorkerId::parse(WORKER_ID).unwrap();
        let store = SqliteStore::open(&state_path).unwrap();
        let soyuz = MockServer::start().await;
        let state = Arc::new(AtomicU8::new(STATUS_QUEUED));
        let claim_callback_id = Arc::new(Mutex::new(None::<String>));
        let delivered_callback_id = Arc::new(Mutex::new(None::<String>));

        let run_state = Arc::clone(&state);
        let run_claim = Arc::clone(&claim_callback_id);
        Mock::given(method("GET"))
            .and(path(format!("/v1/worker/runs/{run_id}")))
            .respond_with(move |_request: &Request| {
                let now = OffsetDateTime::now_utc();
                let future = (now + time::Duration::minutes(30))
                    .format(&Rfc3339)
                    .unwrap();
                let current = run_state.load(Ordering::SeqCst);
                let claimed = current >= STATUS_CLAIMED;
                let cancel_requested = current == STATUS_CANCEL_REQUESTED;
                let claim_id = run_claim.lock().unwrap().clone();
                let body = json!({
                    "data": {
                        "runId": run_id.to_string(),
                        "displayId": "synthetic-process-integration",
                        "status": match current {
                            STATUS_QUEUED => "queued",
                            STATUS_CLAIMED => "claimed",
                            STATUS_RUNNING => "running",
                            STATUS_CANCEL_REQUESTED => "cancel_requested",
                            _ => "unknown"
                        },
                        "cancelRequestedAt": if cancel_requested { Some(now.format(&Rfc3339).unwrap()) } else { None::<String> },
                        "claimExpiresAt": if claimed { Some(future) } else { None::<String> },
                        "workerId": if claimed { Some(WORKER_ID) } else { None::<&str> },
                        "claimCallbackId": if claimed { claim_id } else { None },
                        "stage": if current == STATUS_QUEUED { "queued" } else { "running" },
                        "updatedAt": now.format(&Rfc3339).unwrap(),
                        "lastHeartbeatAt": if claimed { Some(now.format(&Rfc3339).unwrap()) } else { None::<String> },
                        "operationalHealth": "healthy"
                    },
                    "requestId": "synthetic-process-canonical-read"
                });
                ResponseTemplate::new(200).set_body_json(body)
            })
            .mount(&soyuz)
            .await;

        let claim_state = Arc::clone(&state);
        let claim_id_state = Arc::clone(&claim_callback_id);
        Mock::given(method("POST"))
            .and(path(format!("/v1/worker/runs/{run_id}/claim")))
            .respond_with(move |request: &Request| {
                let body: Value = serde_json::from_slice(&request.body).unwrap();
                *claim_id_state.lock().unwrap() = body
                    .get("callbackId")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                claim_state.store(STATUS_CLAIMED, Ordering::SeqCst);
                ResponseTemplate::new(200).set_body_json(json!({
                    "data": { "accepted": true },
                    "requestId": "synthetic-process-claim"
                }))
            })
            .expect(1)
            .mount(&soyuz)
            .await;

        let start_state = Arc::clone(&state);
        Mock::given(method("POST"))
            .and(path(format!("/v1/worker/runs/{run_id}/started")))
            .respond_with(move |_request: &Request| {
                start_state.store(STATUS_RUNNING, Ordering::SeqCst);
                ResponseTemplate::new(200).set_body_json(json!({
                    "data": {},
                    "requestId": "synthetic-process-start"
                }))
            })
            .expect(1)
            .mount(&soyuz)
            .await;

        for terminal_path in ["cancelled", "failed", "completed"] {
            let captured = Arc::clone(&delivered_callback_id);
            Mock::given(method("POST"))
                .and(path(format!("/v1/worker/runs/{run_id}/{terminal_path}")))
                .respond_with(move |request: &Request| {
                    let body: Value = serde_json::from_slice(&request.body).unwrap();
                    *captured.lock().unwrap() = body
                        .get("callbackId")
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                    ResponseTemplate::new(200).set_body_json(json!({
                        "data": {},
                        "requestId": "synthetic-process-terminal"
                    }))
                })
                .mount(&soyuz)
                .await;
        }

        let soyuz_client = SoyuzClient::new(
            SoyuzClientConfig::new(
                Url::parse(&soyuz.uri()).unwrap(),
                SecretString::from("test-worker-token".to_owned()),
                Duration::from_secs(2),
            )
            .unwrap(),
        )
        .unwrap();
        let ipc_client = SupervisorIpcClient::new(&socket_path, IPC_TOKEN).unwrap();
        let coordinator = Arc::new(
            AuthorizationCoordinator::new(Arc::new(soyuz_client), store.clone(), worker_id.clone())
                .with_supervisor_client(ipc_client),
        );

        let docker = Docker::connect_with_socket(&docker_socket, 30, bollard::API_DEFAULT_VERSION)
            .unwrap()
            .negotiate_version()
            .await
            .unwrap();

        Self {
            _directory: directory,
            _soyuz: soyuz,
            state,
            delivered_callback_id,
            store,
            coordinator,
            run_id,
            worker_id,
            image,
            barrier_path,
            socket_path,
            token_path,
            soyuz_token_path,
            state_path,
            docker_socket,
            docker,
        }
    }

    async fn start_supervisor(&self) -> Child {
        self.start_supervisor_configured(None, "cancellation").await
    }

    async fn start_supervisor_at_barrier(&self, checkpoint: &str, scenario: &str) -> Child {
        self.start_supervisor_configured(Some(checkpoint), scenario)
            .await
    }

    async fn start_supervisor_configured(&self, barrier: Option<&str>, scenario: &str) -> Child {
        let _ = std::fs::remove_file(&self.barrier_path);
        let binary = std::env::var_os("FOSSIL_SUPERVISORD_BIN")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("../..")
                    .join("target/debug/fossil-supervisord")
            });
        let mut command = Command::new(binary);
        command
            .env("FOSSIL_SUPERVISOR_SOCKET", &self.socket_path)
            .env("FOSSIL_SUPERVISOR_STATE_DB", &self.state_path)
            .env("FOSSIL_SUPERVISOR_IMAGE", &self.image)
            .env("FOSSIL_SUPERVISOR_TEST_IMAGE_ID", &self.image)
            .env("FOSSIL_SUPERVISOR_IPC_TOKEN_FILE", &self.token_path)
            .env("FOSSIL_SUPERVISOR_DOCKER_SOCKET", &self.docker_socket)
            .env("FOSSIL_SUPERVISOR_SCENARIO", scenario)
            .env("FOSSIL_SUPERVISOR_MONITOR_CADENCE_MILLIS", "100")
            .env("FOSSIL_SUPERVISOR_MAX_RUNTIME_SECONDS", "120")
            .env("FOSSIL_SUPERVISOR_TEST_BARRIER_MARKER", &self.barrier_path)
            .env("SOYUZ_API_ORIGIN", self._soyuz.uri())
            .env("SOYUZ_WORKER_ID", self.worker_id.as_str())
            .env("SOYUZ_WORKER_API_TOKEN_FILE", &self.soyuz_token_path)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        if let Some(barrier) = barrier {
            command.env("FOSSIL_SUPERVISOR_TEST_BARRIER", barrier);
        }
        let mut child = command
            .spawn()
            .expect("start actual fossil-supervisord process");
        self.wait_for_socket(&mut child).await;
        child
    }

    async fn wait_for_barrier(&self, child: &mut Child, expected: &str) {
        timeout(Duration::from_secs(30), async {
            loop {
                if let Some(status) = child.try_wait().unwrap() {
                    panic!("fossil-supervisord exited before {expected}: {status}");
                }
                if std::fs::read_to_string(&self.barrier_path)
                    .is_ok_and(|marker| marker == expected)
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("supervisor did not reach deterministic barrier {expected}"));
    }

    async fn wait_for_socket(&self, child: &mut Child) {
        timeout(Duration::from_secs(30), async {
            loop {
                if let Some(status) = child.try_wait().unwrap() {
                    panic!("fossil-supervisord exited before binding IPC: {status}");
                }
                if UnixStream::connect(&self.socket_path).await.is_ok() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("fossil-supervisord did not bind its socket");
    }

    async fn wait_for_launch_state(&self, expected: ContainerLaunchState) -> ContainerLaunchRecord {
        timeout(Duration::from_secs(45), async {
            loop {
                let launch = self
                    .store
                    .container_launch(self.run_id)
                    .unwrap()
                    .expect("authorized coordinator request should create launch state");
                if launch.state == expected {
                    return launch;
                }
                assert_ne!(
                    launch.state,
                    ContainerLaunchState::ManualReviewRequired,
                    "unexpected ambiguous supervisor state"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("supervisor did not reach expected durable state")
    }

    async fn inspect_running(&self, launch: &ContainerLaunchRecord) -> String {
        let inspect = self
            .docker
            .inspect_container(
                &launch.container_name,
                None::<bollard::query_parameters::InspectContainerOptions>,
            )
            .await
            .unwrap();
        assert_eq!(
            inspect.state.as_ref().and_then(|state| state.status),
            Some(ContainerStateStatusEnum::RUNNING)
        );
        let id = inspect.id.clone().unwrap();
        assert!(
            launch
                .container_id
                .as_deref()
                .is_none_or(|persisted| persisted == id)
        );
        let labels = inspect
            .config
            .as_ref()
            .and_then(|config| config.labels.as_ref())
            .unwrap();
        assert_eq!(
            labels.get("io.rusty-fossil.managed").map(String::as_str),
            Some("true")
        );
        assert_eq!(
            labels.get("io.rusty-fossil.run-id").map(String::as_str),
            Some(self.run_id.to_string().as_str())
        );
        assert_eq!(
            labels.get("io.rusty-fossil.worker-id").map(String::as_str),
            Some(self.worker_id.as_str())
        );
        assert_eq!(
            labels
                .get("io.rusty-fossil.claim-callback-id")
                .map(String::as_str),
            Some(launch.claim_callback_id.to_string().as_str())
        );
        assert_eq!(
            labels
                .get("io.rusty-fossil.start-callback-id")
                .map(String::as_str),
            Some(launch.start_callback_id.to_string().as_str())
        );
        let config = inspect.config.as_ref().unwrap();
        assert!(config.cmd.as_ref().is_none_or(Vec::is_empty));
        let environment = config.env.as_ref().unwrap();
        assert!(environment.contains(&"FOSSIL_SCENARIO=cancellation".to_owned()));
        assert!(
            environment.iter().all(|value| {
                value.starts_with("PATH=") || value.starts_with("FOSSIL_SCENARIO=")
            })
        );
        id
    }

    async fn inspect_container_identity(&self, launch: &ContainerLaunchRecord) -> String {
        let inspect = self
            .docker
            .inspect_container(
                &launch.container_name,
                None::<bollard::query_parameters::InspectContainerOptions>,
            )
            .await
            .expect("expected deterministic container to exist");
        let id = inspect
            .id
            .clone()
            .expect("Docker returns a full container ID");
        assert!(
            launch
                .container_id
                .as_deref()
                .is_none_or(|persisted| persisted == id)
        );
        let labels = inspect
            .config
            .as_ref()
            .and_then(|config| config.labels.as_ref())
            .expect("managed container has labels");
        for (key, expected) in [
            ("io.rusty-fossil.managed", "true".to_owned()),
            ("io.rusty-fossil.run-id", self.run_id.to_string()),
            ("io.rusty-fossil.worker-id", self.worker_id.to_string()),
            (
                "io.rusty-fossil.claim-callback-id",
                launch.claim_callback_id.to_string(),
            ),
            (
                "io.rusty-fossil.start-callback-id",
                launch.start_callback_id.to_string(),
            ),
            (
                "io.rusty-fossil.workload-version",
                "synthetic-v1".to_owned(),
            ),
        ] {
            assert_eq!(labels.get(key), Some(&expected));
        }
        id
    }

    async fn terminate_gracefully(&self, child: &mut Child) {
        let pid = child.id().expect("supervisor child has a process ID");
        let status = tokio::process::Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .status()
            .await
            .unwrap();
        assert!(status.success(), "sending SIGTERM to child failed");
        let status = timeout(Duration::from_secs(70), child.wait())
            .await
            .expect("graceful supervisor shutdown timed out")
            .unwrap();
        assert!(
            status.success(),
            "supervisor did not exit cleanly after SIGTERM"
        );
    }
}

#[tokio::test]
#[ignore = "requires Docker Engine, a local worker image, and the test-support fossil-supervisord binary"]
async fn queue_redelivery_recovers_supervisor_crash_before_launch_intent_commit() {
    let harness = ProcessHarness::new().await;
    let mut cleanup = TestContainerCleanup::new(&harness);
    let mut interrupted = harness
        .start_supervisor_at_barrier("before_launch_intent_commit", "success")
        .await;
    let coordinator = Arc::clone(&harness.coordinator);
    let first_delivery = tokio::spawn(async move { coordinator.process(queue_message()).await });

    harness
        .wait_for_barrier(&mut interrupted, "before_launch_intent_commit")
        .await;
    assert!(
        harness
            .store
            .container_launch(harness.run_id)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        harness.store.execution_slot_owner().unwrap(),
        Some(harness.run_id),
        "the durable coordinator handoff still owns the execution slot"
    );

    interrupted.start_kill().unwrap();
    timeout(Duration::from_secs(10), interrupted.wait())
        .await
        .expect("interrupted supervisor did not exit")
        .unwrap();
    assert!(matches!(
        timeout(Duration::from_secs(10), first_delivery)
            .await
            .expect("failed IPC handoff did not return")
            .unwrap(),
        QueueDisposition::Retry { .. }
    ));

    let mut restarted = harness.start_supervisor_configured(None, "success").await;
    assert_eq!(
        harness.coordinator.process(queue_message()).await,
        QueueDisposition::Acknowledge
    );
    let record_after_redelivery = harness.store.get_run(harness.run_id).unwrap();
    assert!(
        harness
            .store
            .container_launch(harness.run_id)
            .unwrap()
            .is_some(),
        "acknowledged redelivery has no supervisor launch row; lifecycle={:?}, recovery={:?}",
        record_after_redelivery.lifecycle_state,
        record_after_redelivery.recovery_action
    );
    let terminal = harness
        .wait_for_launch_state(ContainerLaunchState::CleanupCompleted)
        .await;
    assert!(terminal.container_id.is_some());
    assert_eq!(harness.store.execution_slot_owner().unwrap(), None);
    assert!(matches!(
        harness.store.pending_callbacks().unwrap().as_slice(),
        [fossil_storage::CallbackOutboxRecord {
            payload: CallbackPayload::Completed { .. },
            ..
        }]
    ));
    let callback = harness.store.pending_callbacks().unwrap().remove(0);
    let replay = harness.coordinator.replay_pending_callbacks(10).await;
    assert_eq!(replay.delivered, 1);
    assert_eq!(
        harness.delivered_callback_id.lock().unwrap().as_deref(),
        Some(callback.event_id.to_string().as_str())
    );
    assert!(harness.store.pending_callbacks().unwrap().is_empty());
    harness.terminate_gracefully(&mut restarted).await;
    cleanup.disarm();
}

#[tokio::test]
#[ignore = "requires Docker Engine, a local worker image, and the test-support fossil-supervisord binary"]
async fn supervisor_restart_reconciles_create_and_start_crash_boundaries() {
    for (
        checkpoint,
        expected_launch_state,
        expected_docker_state,
        recovers_automatically,
        first_delivery_retries,
    ) in [
        (
            "after_launch_intent_commit",
            ContainerLaunchState::LaunchIntentPersisted,
            None,
            true,
            true,
        ),
        (
            "after_create_requested_commit",
            ContainerLaunchState::CreateRequested,
            None,
            false,
            false,
        ),
        (
            "after_docker_create_before_identity_commit",
            ContainerLaunchState::CreateRequested,
            Some(ContainerStateStatusEnum::CREATED),
            true,
            false,
        ),
        (
            "after_container_identity_commit",
            ContainerLaunchState::ContainerCreated,
            Some(ContainerStateStatusEnum::CREATED),
            true,
            false,
        ),
        (
            "after_start_requested_commit",
            ContainerLaunchState::StartRequested,
            Some(ContainerStateStatusEnum::CREATED),
            true,
            false,
        ),
        (
            "after_docker_start_before_running_commit",
            ContainerLaunchState::StartRequested,
            Some(ContainerStateStatusEnum::RUNNING),
            true,
            false,
        ),
    ] {
        let harness = ProcessHarness::new().await;
        let mut cleanup = TestContainerCleanup::new(&harness);
        let mut interrupted = harness
            .start_supervisor_at_barrier(checkpoint, "cancellation")
            .await;
        let coordinator = Arc::clone(&harness.coordinator);
        let first_delivery =
            tokio::spawn(async move { coordinator.process(queue_message()).await });

        harness.wait_for_barrier(&mut interrupted, checkpoint).await;
        let before_restart = harness
            .store
            .container_launch(harness.run_id)
            .unwrap()
            .unwrap();
        assert_eq!(before_restart.state, expected_launch_state);
        let container_id = if let Some(expected_status) = expected_docker_state {
            let container_id = harness.inspect_container_identity(&before_restart).await;
            let inspect = harness
                .docker
                .inspect_container(
                    &before_restart.container_name,
                    None::<bollard::query_parameters::InspectContainerOptions>,
                )
                .await
                .unwrap();
            assert_eq!(
                inspect.state.as_ref().and_then(|state| state.status),
                Some(expected_status)
            );
            Some(container_id)
        } else {
            assert!(before_restart.container_id.is_none());
            assert!(
                harness
                    .docker
                    .inspect_container(
                        &before_restart.container_name,
                        None::<bollard::query_parameters::InspectContainerOptions>,
                    )
                    .await
                    .is_err()
            );
            None
        };

        interrupted.start_kill().unwrap();
        timeout(Duration::from_secs(10), interrupted.wait())
            .await
            .expect("crashed supervisor process did not exit")
            .unwrap();
        let first_disposition = timeout(Duration::from_secs(10), first_delivery)
            .await
            .expect("initial queue delivery did not return after the crash")
            .unwrap();
        if first_delivery_retries {
            assert!(matches!(first_disposition, QueueDisposition::Retry { .. }));
        } else {
            assert_eq!(first_disposition, QueueDisposition::Acknowledge);
        }

        if !recovers_automatically {
            let mut restarted = harness
                .start_supervisor_at_barrier(
                    "after_create_requested_missing_container",
                    "cancellation",
                )
                .await;
            harness
                .wait_for_barrier(&mut restarted, "after_create_requested_missing_container")
                .await;
            assert_eq!(
                harness.coordinator.process(queue_message()).await,
                QueueDisposition::Acknowledge
            );
            let fenced = harness
                .store
                .container_launch(harness.run_id)
                .unwrap()
                .unwrap();
            assert_eq!(fenced.state, ContainerLaunchState::CreateRequested);
            assert!(fenced.container_id.is_none());
            assert_eq!(
                harness.store.execution_slot_owner().unwrap(),
                Some(harness.run_id),
                "an uncertain create remains fenced for manual review"
            );
            assert!(
                harness
                    .docker
                    .inspect_container(
                        &fenced.container_name,
                        None::<bollard::query_parameters::InspectContainerOptions>,
                    )
                    .await
                    .is_err(),
                "the supervisor must not retry a create without proving the first request had no effect"
            );
            restarted.start_kill().unwrap();
            timeout(Duration::from_secs(10), restarted.wait())
                .await
                .expect("restarted supervisor did not exit")
                .unwrap();
            cleanup.disarm();
            continue;
        }

        let mut restarted = harness
            .start_supervisor_configured(None, "cancellation")
            .await;
        assert_eq!(
            harness.coordinator.process(queue_message()).await,
            QueueDisposition::Acknowledge
        );
        let recovered = harness
            .wait_for_launch_state(ContainerLaunchState::ContainerRunning)
            .await;
        if let Some(container_id) = container_id {
            assert_eq!(
                recovered.container_id.as_deref(),
                Some(container_id.as_str())
            );
            assert_eq!(harness.inspect_running(&recovered).await, container_id);
        } else {
            let recovered_id = harness.inspect_running(&recovered).await;
            assert_eq!(
                recovered.container_id.as_deref(),
                Some(recovered_id.as_str())
            );
        }

        harness
            .state
            .store(STATUS_CANCEL_REQUESTED, Ordering::SeqCst);
        let terminal = harness
            .wait_for_launch_state(ContainerLaunchState::CleanupCompleted)
            .await;
        assert_eq!(harness.store.execution_slot_owner().unwrap(), None);
        let callback = harness.store.pending_callbacks().unwrap().remove(0);
        assert_eq!(callback.event_id, terminal.terminal_callback_id.unwrap());
        let replay = harness.coordinator.replay_pending_callbacks(10).await;
        assert_eq!(replay.delivered, 1);
        assert_eq!(
            harness.delivered_callback_id.lock().unwrap().as_deref(),
            Some(callback.event_id.to_string().as_str())
        );
        harness.terminate_gracefully(&mut restarted).await;
        cleanup.disarm();
    }
}

#[tokio::test]
#[ignore = "requires Docker Engine, a local worker image, and the test-support fossil-supervisord binary"]
async fn supervisor_restart_finalizes_exit_and_cleanup_crash_boundaries() {
    for (checkpoint, expected_state) in [
        (
            "after_docker_exit_before_terminal_persist",
            ContainerLaunchState::StartRequested,
        ),
        (
            "after_terminal_persist_before_cleanup",
            ContainerLaunchState::CleanupPending,
        ),
        (
            "after_docker_remove_before_cleanup_commit",
            ContainerLaunchState::CleanupPending,
        ),
    ] {
        let harness = ProcessHarness::new().await;
        let mut cleanup = TestContainerCleanup::new(&harness);
        let mut interrupted = harness
            .start_supervisor_at_barrier(checkpoint, "success")
            .await;
        let coordinator = Arc::clone(&harness.coordinator);
        let delivery = tokio::spawn(async move { coordinator.process(queue_message()).await });

        harness.wait_for_barrier(&mut interrupted, checkpoint).await;
        let launch = harness
            .store
            .container_launch(harness.run_id)
            .unwrap()
            .unwrap();
        assert_eq!(launch.state, expected_state);
        let container_id = launch.container_id.clone().unwrap();
        if checkpoint == "after_docker_remove_before_cleanup_commit" {
            assert!(
                harness
                    .docker
                    .inspect_container(
                        &launch.container_name,
                        None::<bollard::query_parameters::InspectContainerOptions>,
                    )
                    .await
                    .is_err()
            );
        } else {
            let inspect = harness
                .docker
                .inspect_container(
                    &launch.container_name,
                    None::<bollard::query_parameters::InspectContainerOptions>,
                )
                .await
                .unwrap();
            assert_eq!(inspect.id.as_deref(), Some(container_id.as_str()));
            assert_eq!(
                inspect.state.as_ref().and_then(|state| state.status),
                Some(ContainerStateStatusEnum::EXITED)
            );
        }
        if checkpoint != "after_docker_exit_before_terminal_persist" {
            assert_eq!(
                harness.store.pending_callbacks().unwrap().len(),
                1,
                "terminal result is durable before cleanup resumes"
            );
        }
        assert_eq!(
            timeout(Duration::from_secs(10), delivery)
                .await
                .expect("coordinator did not receive IPC acceptance")
                .unwrap(),
            QueueDisposition::Acknowledge
        );

        interrupted.start_kill().unwrap();
        timeout(Duration::from_secs(10), interrupted.wait())
            .await
            .expect("crashed supervisor process did not exit")
            .unwrap();
        let mut restarted = harness.start_supervisor_configured(None, "success").await;
        assert_eq!(
            harness.coordinator.process(queue_message()).await,
            QueueDisposition::Acknowledge
        );
        let completed = harness
            .wait_for_launch_state(ContainerLaunchState::CleanupCompleted)
            .await;
        assert_eq!(
            completed.container_id.as_deref(),
            Some(container_id.as_str())
        );
        assert_eq!(harness.store.execution_slot_owner().unwrap(), None);
        let callback = harness.store.pending_callbacks().unwrap().remove(0);
        assert_eq!(callback.event_id, completed.terminal_callback_id.unwrap());
        assert!(matches!(
            callback.payload,
            CallbackPayload::Completed { .. }
        ));
        assert_eq!(
            harness
                .coordinator
                .replay_pending_callbacks(10)
                .await
                .delivered,
            1
        );
        assert_eq!(
            harness.delivered_callback_id.lock().unwrap().as_deref(),
            Some(callback.event_id.to_string().as_str())
        );
        assert!(harness.store.pending_callbacks().unwrap().is_empty());
        assert!(
            harness
                .docker
                .inspect_container(
                    &completed.container_name,
                    None::<bollard::query_parameters::InspectContainerOptions>,
                )
                .await
                .is_err()
        );
        harness.terminate_gracefully(&mut restarted).await;
        cleanup.disarm();
    }
}

#[tokio::test]
#[ignore = "requires Docker Engine, a local worker image, and the built fossil-supervisord binary"]
async fn coordinator_duplicate_delivery_recovers_one_container_after_supervisor_crash() {
    let harness = ProcessHarness::new().await;
    let mut cleanup = TestContainerCleanup::new(&harness);
    let mut first = harness.start_supervisor().await;

    assert_eq!(
        harness.coordinator.process(queue_message()).await,
        QueueDisposition::Acknowledge
    );
    let launch = harness
        .wait_for_launch_state(ContainerLaunchState::ContainerRunning)
        .await;
    let container_id = harness.inspect_running(&launch).await;

    assert_eq!(
        harness.coordinator.process(queue_message()).await,
        QueueDisposition::Acknowledge
    );
    assert_eq!(
        harness
            .store
            .container_launch(harness.run_id)
            .unwrap()
            .unwrap()
            .container_id
            .as_deref(),
        Some(container_id.as_str()),
        "duplicate Queue delivery must keep the first container identity"
    );

    first.start_kill().unwrap();
    let _ = timeout(Duration::from_secs(10), first.wait())
        .await
        .expect("crashed supervisor process did not exit")
        .unwrap();
    assert_eq!(
        harness.store.execution_slot_owner().unwrap(),
        Some(harness.run_id)
    );

    let mut restarted = harness.start_supervisor().await;
    assert_eq!(
        harness.coordinator.process(queue_message()).await,
        QueueDisposition::Acknowledge
    );
    let recovered = harness
        .wait_for_launch_state(ContainerLaunchState::ContainerRunning)
        .await;
    assert_eq!(
        recovered.container_id.as_deref(),
        Some(container_id.as_str())
    );
    assert_eq!(harness.inspect_running(&recovered).await, container_id);

    harness
        .state
        .store(STATUS_CANCEL_REQUESTED, Ordering::SeqCst);
    let terminal = harness
        .wait_for_launch_state(ContainerLaunchState::CleanupCompleted)
        .await;
    assert_eq!(
        harness
            .store
            .get_run(harness.run_id)
            .unwrap()
            .lifecycle_state,
        LocalLifecycleState::ExecutionTerminal
    );
    assert_eq!(harness.store.execution_slot_owner().unwrap(), None);
    let callback = harness.store.pending_callbacks().unwrap().remove(0);
    assert_eq!(callback.event_id, terminal.terminal_callback_id.unwrap());
    assert!(matches!(
        callback.payload,
        CallbackPayload::Cancelled { .. }
    ));

    let replay = harness.coordinator.replay_pending_callbacks(10).await;
    assert_eq!(replay.delivered, 1);
    assert_eq!(
        harness.delivered_callback_id.lock().unwrap().as_deref(),
        Some(callback.event_id.to_string().as_str())
    );
    assert!(harness.store.pending_callbacks().unwrap().is_empty());
    assert_eq!(
        harness
            .store
            .get_run(harness.run_id)
            .unwrap()
            .callback_delivery,
        CallbackDeliveryState::Delivered
    );
    assert!(
        harness
            .docker
            .inspect_container(
                &recovered.container_name,
                None::<bollard::query_parameters::InspectContainerOptions>,
            )
            .await
            .is_err()
    );
    harness.terminate_gracefully(&mut restarted).await;
    cleanup.disarm();
}

#[tokio::test]
#[ignore = "requires Docker Engine, a local worker image, and the built fossil-supervisord binary"]
async fn supervisor_sigterm_stops_active_synthetic_work_and_persists_uncertainty() {
    let harness = ProcessHarness::new().await;
    let mut cleanup = TestContainerCleanup::new(&harness);
    let mut supervisor = harness.start_supervisor().await;

    assert_eq!(
        harness.coordinator.process(queue_message()).await,
        QueueDisposition::Acknowledge
    );
    let launch = harness
        .wait_for_launch_state(ContainerLaunchState::ContainerRunning)
        .await;
    let container_id = harness.inspect_running(&launch).await;

    harness.terminate_gracefully(&mut supervisor).await;
    let terminal = harness
        .wait_for_launch_state(ContainerLaunchState::CleanupCompleted)
        .await;
    assert_eq!(
        terminal.container_id.as_deref(),
        Some(container_id.as_str())
    );
    assert_eq!(harness.store.execution_slot_owner().unwrap(), None);
    let callback = harness.store.pending_callbacks().unwrap().remove(0);
    assert!(matches!(
        callback.payload,
        CallbackPayload::Failed {
            failure_class,
            ..
        } if failure_class == "execution_uncertain"
    ));
    let replay = harness.coordinator.replay_pending_callbacks(10).await;
    assert_eq!(replay.delivered, 1);
    assert_eq!(
        harness.delivered_callback_id.lock().unwrap().as_deref(),
        Some(callback.event_id.to_string().as_str())
    );
    cleanup.disarm();
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

fn private_tempdir() -> TempDir {
    let directory = tempdir().unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    directory
}

fn write_private_file(path: &Path, value: &str) {
    std::fs::write(path, value).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o400)).unwrap();
    }
}

struct TestContainerCleanup {
    store: SqliteStore,
    run_id: RunId,
    docker_socket: String,
    armed: bool,
}

impl TestContainerCleanup {
    fn new(harness: &ProcessHarness) -> Self {
        Self {
            store: harness.store.clone(),
            run_id: harness.run_id,
            docker_socket: harness.docker_socket.clone(),
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TestContainerCleanup {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Ok(Some(launch)) = self.store.container_launch(self.run_id) else {
            return;
        };
        let container_reference = launch
            .container_id
            .as_deref()
            .unwrap_or(launch.container_name.as_str());
        let host = format!("unix://{}", self.docker_socket);
        let inspected = StdCommand::new("docker")
            .arg("inspect")
            .arg(container_reference)
            .env("DOCKER_HOST", &host)
            .output();
        let Ok(inspected) = inspected else {
            return;
        };
        if !inspected.status.success() {
            return;
        }
        let Ok(value) = serde_json::from_slice::<Value>(&inspected.stdout) else {
            return;
        };
        let Some(container) = value.as_array().and_then(|values| values.first()) else {
            return;
        };
        let labels = container
            .get("Config")
            .and_then(|config| config.get("Labels"))
            .and_then(Value::as_object);
        let expected_labels = [
            ("io.rusty-fossil.managed", "true".to_owned()),
            ("io.rusty-fossil.run-id", self.run_id.to_string()),
            ("io.rusty-fossil.worker-id", launch.worker_id.to_string()),
            (
                "io.rusty-fossil.claim-callback-id",
                launch.claim_callback_id.to_string(),
            ),
            (
                "io.rusty-fossil.start-callback-id",
                launch.start_callback_id.to_string(),
            ),
            (
                "io.rusty-fossil.workload-version",
                "synthetic-v1".to_owned(),
            ),
        ];
        let Some(actual_id) = container.get("Id").and_then(Value::as_str) else {
            return;
        };
        let identity_matches = launch
            .container_id
            .as_deref()
            .is_none_or(|stored| stored == actual_id)
            && labels.is_some_and(|labels| {
                expected_labels.iter().all(|(key, value)| {
                    labels.get(*key).and_then(Value::as_str) == Some(value.as_str())
                })
            });
        if identity_matches {
            let _ = StdCommand::new("docker")
                .args(["rm", "--force", actual_id])
                .env("DOCKER_HOST", host)
                .status();
        }
    }
}
