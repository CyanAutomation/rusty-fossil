use crate::{
    CancellationOutcome, DockerSupervisor, ExecutionSupervisor, ReconciliationOutcome,
    SyntheticScenario,
};
use fossil_core::ids::RunId;
use fossil_storage::{ContainerLaunchState, SqliteStore};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::future::Future;
use std::io;
#[cfg(unix)]
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{Duration, timeout};
use tracing::{error, warn};

const MAX_REQUEST_BYTES: usize = 4 * 1024;
const MAX_TOKEN_BYTES: usize = 256;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_CONNECTIONS: usize = 16;
const RECOVERY_CADENCE: Duration = Duration::from_secs(5);

#[derive(Debug, Error)]
pub enum SupervisorIpcError {
    #[error("supervisor IPC configuration is invalid")]
    InvalidConfiguration,
    #[error("supervisor IPC authentication failed")]
    Unauthorized,
    #[error("supervisor IPC connection or protocol failed")]
    Protocol,
    #[error("supervisor IPC service failed")]
    Service,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Request {
    version: u8,
    token: String,
    #[serde(flatten)]
    command: Command,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    tag = "command",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
enum Command {
    Launch { run_id: RunId },
    Cancel { run_id: RunId },
    Reconcile { run_id: RunId },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Response {
    ok: bool,
    result: String,
}

/// Narrow authenticated client for the supervisor. It accepts only a
/// canonical run ID; image, scenario, Docker settings, commands, and mounts
/// are not part of this protocol.
#[derive(Clone)]
pub struct SupervisorIpcClient {
    socket_path: PathBuf,
    token: Arc<str>,
}

impl SupervisorIpcClient {
    pub fn new(
        socket_path: impl Into<PathBuf>,
        token: impl Into<String>,
    ) -> Result<Self, SupervisorIpcError> {
        let socket_path = socket_path.into();
        let token = token.into();
        if !socket_path.is_absolute() || !valid_token(&token) {
            return Err(SupervisorIpcError::InvalidConfiguration);
        }
        Ok(Self {
            socket_path,
            token: Arc::from(token),
        })
    }

    pub fn from_token_file(
        socket_path: impl Into<PathBuf>,
        token_path: impl AsRef<Path>,
    ) -> Result<Self, SupervisorIpcError> {
        Self::new(socket_path, read_ipc_token_file(token_path)?)
    }

    pub async fn launch(&self, run_id: RunId) -> Result<String, SupervisorIpcError> {
        self.request(Command::Launch { run_id }).await
    }

    pub async fn cancel(&self, run_id: RunId) -> Result<String, SupervisorIpcError> {
        self.request(Command::Cancel { run_id }).await
    }

    pub async fn reconcile(&self, run_id: RunId) -> Result<String, SupervisorIpcError> {
        self.request(Command::Reconcile { run_id }).await
    }

    async fn request(&self, command: Command) -> Result<String, SupervisorIpcError> {
        timeout(REQUEST_TIMEOUT, async {
            let mut stream = UnixStream::connect(&self.socket_path)
                .await
                .map_err(|_| SupervisorIpcError::Protocol)?;
            let mut body = serde_json::to_vec(&Request {
                version: 1,
                token: self.token.to_string(),
                command,
            })
            .map_err(|_| SupervisorIpcError::Protocol)?;
            body.push(b'\n');
            if body.len() > MAX_REQUEST_BYTES {
                return Err(SupervisorIpcError::Protocol);
            }
            stream
                .write_all(&body)
                .await
                .map_err(|_| SupervisorIpcError::Protocol)?;
            stream
                .shutdown()
                .await
                .map_err(|_| SupervisorIpcError::Protocol)?;
            let mut reader = BufReader::new(stream).take((MAX_REQUEST_BYTES + 1) as u64);
            let mut response = Vec::new();
            reader
                .read_until(b'\n', &mut response)
                .await
                .map_err(|_| SupervisorIpcError::Protocol)?;
            if response.len() > MAX_REQUEST_BYTES || response.last() != Some(&b'\n') {
                return Err(SupervisorIpcError::Protocol);
            }
            let response: Response = serde_json::from_slice(&response[..response.len() - 1])
                .map_err(|_| SupervisorIpcError::Protocol)?;
            if !response.ok {
                return Err(SupervisorIpcError::Service);
            }
            Ok(response.result)
        })
        .await
        .map_err(|_| SupervisorIpcError::Protocol)?
    }
}

pub fn read_ipc_token_file(path: impl AsRef<Path>) -> Result<String, SupervisorIpcError> {
    use std::os::unix::fs::PermissionsExt;

    let path = path.as_ref();
    let metadata =
        std::fs::symlink_metadata(path).map_err(|_| SupervisorIpcError::InvalidConfiguration)?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() > MAX_TOKEN_BYTES as u64
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(SupervisorIpcError::InvalidConfiguration);
    }
    let mut token =
        std::fs::read_to_string(path).map_err(|_| SupervisorIpcError::InvalidConfiguration)?;
    while token.ends_with('\n') || token.ends_with('\r') {
        token.pop();
    }
    if !valid_token(&token) {
        return Err(SupervisorIpcError::InvalidConfiguration);
    }
    Ok(token)
}

/// Docker authority process endpoint. The listener is private to the current
/// Unix account, and every operation is re-authorized against SQLite and
/// canonical Soyuz state inside `DockerSupervisor`.
pub struct SupervisorIpcServer {
    socket_path: PathBuf,
    token: Arc<str>,
    scenario: SyntheticScenario,
    store: SqliteStore,
    supervisor: DockerSupervisor,
}

impl SupervisorIpcServer {
    pub fn new(
        socket_path: impl Into<PathBuf>,
        token: impl Into<String>,
        scenario: SyntheticScenario,
        store: SqliteStore,
        supervisor: DockerSupervisor,
    ) -> Result<Self, SupervisorIpcError> {
        let socket_path = socket_path.into();
        let token = token.into();
        if !socket_path.is_absolute() || !valid_token(&token) {
            return Err(SupervisorIpcError::InvalidConfiguration);
        }
        Ok(Self {
            socket_path,
            token: Arc::from(token),
            scenario,
            store,
            supervisor,
        })
    }

    pub async fn serve_until<F>(self, shutdown: F) -> Result<(), SupervisorIpcError>
    where
        F: Future<Output = ()> + Send,
    {
        prepare_socket_directory(&self.socket_path).map_err(|_| SupervisorIpcError::Service)?;
        remove_stale_socket(&self.socket_path).await?;
        let listener =
            UnixListener::bind(&self.socket_path).map_err(|_| SupervisorIpcError::Service)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&self.socket_path, std::fs::Permissions::from_mode(0o600))
                .map_err(|_| SupervisorIpcError::Service)?;
        }
        let _socket_guard =
            SocketPathGuard::new(&self.socket_path).map_err(|_| SupervisorIpcError::Service)?;
        let active = Arc::new(Mutex::new(HashSet::new()));
        let recovery_supervisor = self.supervisor.clone();
        let recovery_store = self.store.clone();
        let recovery_active = active.clone();
        let recovery = tokio::spawn(async move {
            loop {
                recover_pending(
                    &recovery_store,
                    &recovery_supervisor,
                    recovery_active.clone(),
                )
                .await;
                tokio::time::sleep(RECOVERY_CADENCE).await;
            }
        });
        let semaphore = Arc::new(Semaphore::new(MAX_CONNECTIONS));
        let mut handlers = JoinSet::new();
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                accepted = listener.accept() => {
                    let (stream, _) = accepted.map_err(|_| SupervisorIpcError::Service)?;
                    let Ok(permit) = semaphore.clone().try_acquire_owned() else {
                        reject_connection(stream).await;
                        continue;
                    };
                    let token = self.token.clone();
                    let scenario = self.scenario;
                    let store = self.store.clone();
                    let supervisor = self.supervisor.clone();
                    let active = active.clone();
                    handlers.spawn(async move {
                        let _permit = permit;
                        handle_connection(stream, token, scenario, store, supervisor, active).await;
                    });
                }
                Some(_) = handlers.join_next(), if !handlers.is_empty() => {}
            }
        }
        recovery.abort();
        let _ = recovery.await;
        while handlers.join_next().await.is_some() {}
        if !stop_supervisor_work(&self.store, &self.supervisor, active).await {
            return Err(SupervisorIpcError::Service);
        }
        Ok(())
    }
}

async fn stop_supervisor_work(
    store: &SqliteStore,
    supervisor: &DockerSupervisor,
    active: Arc<Mutex<HashSet<RunId>>>,
) -> bool {
    let launches = match store.container_launches_requiring_reconciliation() {
        Ok(launches) => launches,
        Err(error) => {
            error!(%error, "could not load supervisor shutdown state");
            return false;
        }
    };
    for launch in launches {
        if matches!(
            launch.state,
            ContainerLaunchState::CleanupCompleted | ContainerLaunchState::ManualReviewRequired
        ) {
            continue;
        }
        let was_active = active.lock().await.contains(&launch.run_id);
        if supervisor.request_stop_for_shutdown(launch.run_id).is_err() {
            warn!(run_id = %launch.run_id, "could not persist stop intent during shutdown");
            return false;
        }
        if !was_active && supervisor.resume_existing(launch.run_id).await.is_err() {
            warn!(run_id = %launch.run_id, "could not finalize supervisor work during shutdown");
            return false;
        }
    }

    let drained = timeout(Duration::from_secs(60), async {
        loop {
            if active.lock().await.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .is_ok();
    if !drained {
        error!("supervisor work did not stop before the shutdown deadline");
        return false;
    }
    let pending = match store.container_launches_requiring_reconciliation() {
        Ok(launches) => launches,
        Err(error) => {
            error!(%error, "could not reload supervisor shutdown state");
            return false;
        }
    };
    for launch in pending {
        if matches!(
            launch.state,
            ContainerLaunchState::CleanupCompleted | ContainerLaunchState::ManualReviewRequired
        ) {
            continue;
        }
        if supervisor.resume_existing(launch.run_id).await.is_err() {
            warn!(run_id = %launch.run_id, "supervisor work remains for recovery after shutdown");
            return false;
        }
    }
    true
}

async fn handle_connection(
    mut stream: UnixStream,
    expected_token: Arc<str>,
    scenario: SyntheticScenario,
    store: SqliteStore,
    supervisor: DockerSupervisor,
    active: Arc<Mutex<HashSet<RunId>>>,
) {
    let response = match read_request(&mut stream).await {
        Ok(request) if request.version == 1 && secure_eq(&request.token, &expected_token) => {
            match request.command {
                Command::Launch { run_id } => {
                    let existing = store.container_launch(run_id).ok().flatten();
                    if let Some(launch) = existing {
                        if launch.state == ContainerLaunchState::ManualReviewRequired {
                            Response {
                                ok: true,
                                result: "manual_review_required".to_owned(),
                            }
                        } else if launch.state == ContainerLaunchState::CleanupCompleted {
                            Response {
                                ok: true,
                                result: "already_terminal".to_owned(),
                            }
                        } else {
                            spawn_resume(run_id, supervisor, active).await;
                            Response {
                                ok: true,
                                result: "accepted".to_owned(),
                            }
                        }
                    } else {
                        match supervisor.prepare_run(run_id, scenario).await {
                            Ok(prepared) => {
                                spawn_execute(run_id, prepared, supervisor, active).await;
                                Response {
                                    ok: true,
                                    result: "accepted".to_owned(),
                                }
                            }
                            Err(_) => Response {
                                ok: false,
                                result: "authorization_or_state_rejected".to_owned(),
                            },
                        }
                    }
                }
                Command::Cancel { run_id } => match supervisor.cancel(run_id).await {
                    Ok(CancellationOutcome::Requested) => {
                        spawn_resume(run_id, supervisor, active).await;
                        Response {
                            ok: true,
                            result: "cancellation_requested".to_owned(),
                        }
                    }
                    Ok(CancellationOutcome::AlreadyTerminal) => Response {
                        ok: true,
                        result: "already_terminal".to_owned(),
                    },
                    Ok(CancellationOutcome::NotFound) => Response {
                        ok: true,
                        result: "not_found".to_owned(),
                    },
                    Err(_) => Response {
                        ok: false,
                        result: "cancellation_rejected".to_owned(),
                    },
                },
                Command::Reconcile { run_id } => match supervisor.reconcile(run_id).await {
                    Ok(outcome) => {
                        if matches!(
                            outcome,
                            ReconciliationOutcome::Prepared | ReconciliationOutcome::Running
                        ) {
                            spawn_resume(run_id, supervisor, active).await;
                        }
                        Response {
                            ok: true,
                            result: reconcile_name(outcome).to_owned(),
                        }
                    }
                    Err(_) => Response {
                        ok: false,
                        result: "reconciliation_failed".to_owned(),
                    },
                },
            }
        }
        _ => Response {
            ok: false,
            result: "unauthorized_or_invalid_request".to_owned(),
        },
    };
    write_response(&mut stream, response).await;
}

async fn spawn_execute(
    run_id: RunId,
    prepared: crate::PreparedExecution,
    supervisor: DockerSupervisor,
    active: Arc<Mutex<HashSet<RunId>>>,
) {
    let mut active_runs = active.lock().await;
    if !active_runs.insert(run_id) {
        return;
    }
    drop(active_runs);
    tokio::spawn(async move {
        if let Err(error) = supervisor.execute_prepared(prepared).await {
            warn!(%run_id, %error, "supervisor execution paused for reconciliation");
        }
        active.lock().await.remove(&run_id);
    });
}

async fn spawn_resume(
    run_id: RunId,
    supervisor: DockerSupervisor,
    active: Arc<Mutex<HashSet<RunId>>>,
) {
    let mut active_runs = active.lock().await;
    if !active_runs.insert(run_id) {
        return;
    }
    drop(active_runs);
    tokio::spawn(async move {
        if let Err(error) = supervisor.resume_existing(run_id).await {
            warn!(%run_id, %error, "supervisor reconciliation paused");
        }
        active.lock().await.remove(&run_id);
    });
}

async fn recover_pending(
    store: &SqliteStore,
    supervisor: &DockerSupervisor,
    active: Arc<Mutex<HashSet<RunId>>>,
) {
    let launches = match store.container_launches_requiring_reconciliation() {
        Ok(launches) => launches,
        Err(error) => {
            error!(%error, "could not load durable supervisor recovery state");
            return;
        }
    };
    for launch in launches {
        if should_resume(launch.state) {
            spawn_resume(launch.run_id, supervisor.clone(), active.clone()).await;
        }
    }
}

fn should_resume(state: ContainerLaunchState) -> bool {
    !matches!(
        state,
        ContainerLaunchState::CleanupCompleted | ContainerLaunchState::ManualReviewRequired
    )
}

async fn read_request(stream: &mut UnixStream) -> Result<Request, SupervisorIpcError> {
    timeout(REQUEST_TIMEOUT, async {
        let mut reader = BufReader::new(stream).take((MAX_REQUEST_BYTES + 1) as u64);
        let mut bytes = Vec::new();
        reader
            .read_until(b'\n', &mut bytes)
            .await
            .map_err(|_| SupervisorIpcError::Protocol)?;
        if bytes.len() > MAX_REQUEST_BYTES || bytes.last() != Some(&b'\n') {
            return Err(SupervisorIpcError::Protocol);
        }
        serde_json::from_slice(&bytes[..bytes.len() - 1]).map_err(|_| SupervisorIpcError::Protocol)
    })
    .await
    .map_err(|_| SupervisorIpcError::Protocol)?
}

async fn write_response(stream: &mut UnixStream, response: Response) {
    if let Ok(mut bytes) = serde_json::to_vec(&response) {
        bytes.push(b'\n');
        let _ = stream.write_all(&bytes).await;
        let _ = stream.shutdown().await;
    }
}

async fn reject_connection(mut stream: UnixStream) {
    let _ = stream
        .write_all(b"{\"ok\":false,\"result\":\"busy\"}\n")
        .await;
}

fn reconcile_name(value: ReconciliationOutcome) -> &'static str {
    match value {
        ReconciliationOutcome::NeverPrepared => "never_prepared",
        ReconciliationOutcome::Prepared => "prepared",
        ReconciliationOutcome::Running => "running",
        ReconciliationOutcome::Terminal => "terminal",
        ReconciliationOutcome::Ambiguous => "manual_review_required",
    }
}

fn valid_token(token: &str) -> bool {
    (32..=MAX_TOKEN_BYTES).contains(&token.len())
        && token.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
}

fn secure_eq(left: &str, right: &str) -> bool {
    let width = left.len().max(right.len());
    let mut difference = left.len() ^ right.len();
    for index in 0..width {
        difference |= usize::from(
            left.as_bytes().get(index).copied().unwrap_or(0)
                ^ right.as_bytes().get(index).copied().unwrap_or(0),
        );
    }
    difference == 0
}

fn prepare_socket_directory(socket_path: &Path) -> io::Result<()> {
    let parent = socket_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing socket parent"))?;
    if !parent.exists() {
        std::fs::create_dir_all(parent)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
        }
    }
    let metadata = std::fs::symlink_metadata(parent)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "unsafe socket parent",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "socket parent is not private",
            ));
        }
    }
    Ok(())
}

async fn remove_stale_socket(path: &Path) -> Result<(), SupervisorIpcError> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(_) => return Err(SupervisorIpcError::Service),
    };
    if !metadata.file_type().is_socket() {
        return Err(SupervisorIpcError::Service);
    }
    if matches!(
        timeout(Duration::from_millis(250), UnixStream::connect(path)).await,
        Ok(Ok(_))
    ) {
        return Err(SupervisorIpcError::Service);
    }
    std::fs::remove_file(path).map_err(|_| SupervisorIpcError::Service)
}

struct SocketPathGuard {
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl SocketPathGuard {
    fn new(path: &Path) -> io::Result<Self> {
        use std::os::unix::fs::MetadataExt;
        let metadata = std::fs::symlink_metadata(path)?;
        Ok(Self {
            path: path.to_owned(),
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
}

impl Drop for SocketPathGuard {
    fn drop(&mut self) {
        use std::os::unix::fs::MetadataExt;
        if std::fs::symlink_metadata(&self.path).is_ok_and(|metadata| {
            metadata.file_type().is_socket()
                && metadata.dev() == self.device
                && metadata.ino() == self.inode
        }) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ipc_protocol_accepts_only_run_id_operations() {
        let request: Request = serde_json::from_str(&format!(
            "{{\"version\":1,\"token\":\"{}\",\"command\":\"launch\",\"runId\":\"f8a0c4c5-d628-7b93-a7cf-4b4629a28853\"}}",
            "a".repeat(32)
        ))
        .unwrap();
        assert!(matches!(request.command, Command::Launch { .. }));

        let invalid = format!(
            "{{\"version\":1,\"token\":\"{}\",\"command\":\"launch\",\"runId\":\"f8a0c4c5-d628-7b93-a7cf-4b4629a28853\",\"image\":\"evil\"}}",
            "a".repeat(32)
        );
        assert!(serde_json::from_str::<Request>(&invalid).is_err());
    }

    #[test]
    fn ipc_authentication_compares_tokens_and_validates_bounds() {
        assert!(secure_eq(
            "same-secret-token-value-123456",
            "same-secret-token-value-123456"
        ));
        assert!(!secure_eq(
            "same-secret-token-value-123456",
            "other-secret-token-value-123456"
        ));
        assert!(!valid_token("too-short"));
        assert!(valid_token(&"x".repeat(32)));
        assert!(!secure_eq("x", &format!("x{}", "\0".repeat(256))));
    }

    #[test]
    fn recovery_skips_completed_and_manual_review_launches() {
        assert!(!should_resume(ContainerLaunchState::CleanupCompleted));
        assert!(!should_resume(ContainerLaunchState::ManualReviewRequired));
        assert!(should_resume(ContainerLaunchState::ContainerRunning));
    }

    #[test]
    fn ipc_token_file_must_be_private_and_not_a_symlink() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("ipc.token");
        std::fs::write(&path, format!("{}\n", "t".repeat(32))).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(read_ipc_token_file(&path).unwrap(), "t".repeat(32));

        let linked = directory.path().join("linked.token");
        std::os::unix::fs::symlink(&path, &linked).unwrap();
        assert!(read_ipc_token_file(linked).is_err());

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(read_ipc_token_file(&path).is_err());
    }

    #[tokio::test]
    async fn ipc_client_sends_only_a_run_identity_and_reads_the_service_ack() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("supervisor.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let token = "z".repeat(32);
        let server_token = token.clone();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_request(&mut stream).await.unwrap();
            assert_eq!(request.token, server_token);
            assert!(matches!(request.command, Command::Launch { .. }));
            write_response(
                &mut stream,
                Response {
                    ok: true,
                    result: "accepted".to_owned(),
                },
            )
            .await;
        });
        let client = SupervisorIpcClient::new(&socket, token).unwrap();
        let run_id = RunId::parse("f8a0c4c5-d628-7b93-a7cf-4b4629a28853").unwrap();
        assert_eq!(client.launch(run_id).await.unwrap(), "accepted");
        task.await.unwrap();
    }
}
