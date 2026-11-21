use fossil_core::execution::SyntheticScenario;
use fossil_core::ids::WorkerId;
use fossil_soyuz::{SoyuzClient, SoyuzClientConfig};
use fossil_storage::SqliteStore;
use fossil_supervisor::{
    DockerRuntimeConfig, DockerSupervisor, SupervisorIpcServer, SupervisorLimitSettings,
    SupervisorLimits, read_ipc_token_file,
};
use secrecy::SecretString;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::EnvFilter;
use url::Url;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let socket_path = required_path("FOSSIL_SUPERVISOR_SOCKET")?;
    let state_database = required_path("FOSSIL_SUPERVISOR_STATE_DB")?;
    let image = required("FOSSIL_SUPERVISOR_IMAGE")?;
    let worker_id = WorkerId::parse(required("SOYUZ_WORKER_ID")?)?;
    let origin = Url::parse(&required("SOYUZ_API_ORIGIN")?)?;
    let soyuz_token = read_secret_file("SOYUZ_WORKER_API_TOKEN_FILE")?;
    let ipc_token = read_ipc_token_file(required_path("FOSSIL_SUPERVISOR_IPC_TOKEN_FILE")?)?;
    let scenario = parse_scenario(&required("FOSSIL_SUPERVISOR_SCENARIO")?)?;
    let docker_socket = std::env::var_os("FOSSIL_SUPERVISOR_DOCKER_SOCKET")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/var/run/docker.sock"));
    let defaults = SupervisorLimitSettings::default();
    let settings = SupervisorLimitSettings {
        cpu_millicores: optional_u64("FOSSIL_SUPERVISOR_CPU_MILLICORES", defaults.cpu_millicores)?,
        memory_mib: optional_u64("FOSSIL_SUPERVISOR_MEMORY_MIB", defaults.memory_mib)?,
        pid_limit: optional_u64("FOSSIL_SUPERVISOR_PID_LIMIT", defaults.pid_limit)?,
        temporary_storage_mib: optional_u64(
            "FOSSIL_SUPERVISOR_TEMPORARY_STORAGE_MIB",
            defaults.temporary_storage_mib,
        )?,
        max_runtime: Duration::from_secs(optional_u64(
            "FOSSIL_SUPERVISOR_MAX_RUNTIME_SECONDS",
            defaults.max_runtime.as_secs(),
        )?),
        cancellation_grace: Duration::from_secs(optional_u64(
            "FOSSIL_SUPERVISOR_CANCELLATION_GRACE_SECONDS",
            defaults.cancellation_grace.as_secs(),
        )?),
        monitor_cadence: Duration::from_millis(optional_u64(
            "FOSSIL_SUPERVISOR_MONITOR_CADENCE_MILLIS",
            defaults.monitor_cadence.as_millis() as u64,
        )?),
        soyuz_unavailable_grace: Duration::from_secs(optional_u64(
            "FOSSIL_SUPERVISOR_SOYUZ_OUTAGE_GRACE_SECONDS",
            defaults.soyuz_unavailable_grace.as_secs(),
        )?),
        max_output_bytes: usize::try_from(optional_u64(
            "FOSSIL_SUPERVISOR_MAX_OUTPUT_BYTES",
            defaults.max_output_bytes as u64,
        )?)
        .map_err(|_| "FOSSIL_SUPERVISOR_MAX_OUTPUT_BYTES is too large")?,
    };
    let limits = SupervisorLimits::new(settings)?;

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let store = SqliteStore::open(state_database)?;
    let soyuz_config = SoyuzClientConfig::new(origin, soyuz_token, Duration::from_secs(10))?;
    let soyuz = Arc::new(SoyuzClient::new(soyuz_config)?);
    let docker_config = DockerRuntimeConfig::with_limits(image, docker_socket, limits)?;
    let supervisor =
        DockerSupervisor::connect(docker_config, store.clone(), soyuz, worker_id).await?;
    let server =
        SupervisorIpcServer::new(socket_path.clone(), ipc_token, scenario, store, supervisor)?;
    tracing::info!(socket = %socket_path.display(), "restricted supervisor IPC listening");
    server.serve_until(shutdown_signal()).await?;
    Ok(())
}

fn required(name: &'static str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("missing required configuration {name}").into())
}

fn required_path(name: &'static str) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let path = PathBuf::from(required(name)?);
    if !path.is_absolute() {
        return Err(format!("{name} must be an absolute path").into());
    }
    Ok(path)
}

fn optional_u64(name: &'static str, default: u64) -> Result<u64, Box<dyn std::error::Error>> {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<u64>()
            .map_err(|_| format!("{name} must be an integer").into()),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(std::env::VarError::NotUnicode(_)) => {
            Err(format!("{name} must be valid Unicode").into())
        }
    }
}

fn parse_scenario(value: &str) -> Result<SyntheticScenario, Box<dyn std::error::Error>> {
    match value {
        "success" => Ok(SyntheticScenario::Success),
        "controlled_failure" => Ok(SyntheticScenario::ControlledFailure),
        "timeout" => Ok(SyntheticScenario::Timeout),
        "cancellation" => Ok(SyntheticScenario::Cancellation),
        "bounded_output" => Ok(SyntheticScenario::BoundedOutput),
        "unexpected_termination" => Ok(SyntheticScenario::UnexpectedTermination),
        _ => Err("FOSSIL_SUPERVISOR_SCENARIO is not a supported synthetic scenario".into()),
    }
}

fn read_secret_file(variable: &'static str) -> Result<SecretString, Box<dyn std::error::Error>> {
    use std::os::unix::fs::PermissionsExt;

    let path = PathBuf::from(required(variable)?);
    let metadata = std::fs::symlink_metadata(&path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(format!("{variable} must reference a regular file").into());
    }
    if metadata.permissions().mode() & 0o077 != 0 || metadata.len() > 4096 {
        return Err(format!("{variable} must be private and no larger than 4 KiB").into());
    }
    let mut value = std::fs::read_to_string(path)?;
    while value.ends_with('\n') || value.ends_with('\r') {
        value.pop();
    }
    if !(1..=4096).contains(&value.len())
        || !value.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
    {
        return Err(format!("{variable} contains an invalid token").into());
    }
    Ok(SecretString::from(value))
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        let mut signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("register SIGTERM handler");
        signal.recv().await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
