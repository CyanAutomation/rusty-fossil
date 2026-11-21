use bollard::models::{ContainerCreateBody, HostConfig, HostConfigLogConfig};
use fossil_core::execution::SyntheticScenario;
use fossil_storage::ContainerLaunchRecord;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use thiserror::Error;

const MEMORY_MIB_MIN: u64 = 64;
const MEMORY_MIB_MAX: u64 = 512;
const CPU_MILLICORES_MIN: u64 = 100;
const CPU_MILLICORES_MAX: u64 = 1_000;
const PID_LIMIT_MIN: u64 = 8;
const PID_LIMIT_MAX: u64 = 64;
const TMPFS_MIB_MIN: u64 = 1;
const TMPFS_MIB_MAX: u64 = 64;
const TRUSTED_WORKER_IMAGE: &str = "ghcr.io/cyanautomation/rusty-fossil-synthetic-worker";

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DockerConfigError {
    #[error("synthetic worker image must be pinned by a sha256 digest")]
    ImageNotPinned,
    #[error("Docker socket must be an absolute Unix socket path")]
    InvalidSocketPath,
    #[error("Docker resource limits are outside the supported safe bounds")]
    InvalidLimits,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SupervisorLimits {
    pub(crate) cpu_millicores: u64,
    pub(crate) memory_mib: u64,
    pub(crate) pid_limit: u64,
    pub(crate) temporary_storage_mib: u64,
    pub(crate) max_runtime: Duration,
    pub(crate) cancellation_grace: Duration,
    pub(crate) monitor_cadence: Duration,
    pub(crate) soyuz_unavailable_grace: Duration,
    pub(crate) max_output_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SupervisorLimitSettings {
    pub cpu_millicores: u64,
    pub memory_mib: u64,
    pub pid_limit: u64,
    pub temporary_storage_mib: u64,
    pub max_runtime: Duration,
    pub cancellation_grace: Duration,
    pub monitor_cadence: Duration,
    pub soyuz_unavailable_grace: Duration,
    pub max_output_bytes: usize,
}

impl Default for SupervisorLimitSettings {
    fn default() -> Self {
        Self {
            cpu_millicores: 500,
            memory_mib: 256,
            pid_limit: 32,
            temporary_storage_mib: 16,
            max_runtime: Duration::from_secs(15 * 60),
            cancellation_grace: Duration::from_secs(5),
            monitor_cadence: Duration::from_millis(500),
            soyuz_unavailable_grace: Duration::from_secs(30),
            max_output_bytes: 64 * 1024,
        }
    }
}

impl SupervisorLimits {
    pub fn new(settings: SupervisorLimitSettings) -> Result<Self, DockerConfigError> {
        if !(CPU_MILLICORES_MIN..=CPU_MILLICORES_MAX).contains(&settings.cpu_millicores)
            || !(MEMORY_MIB_MIN..=MEMORY_MIB_MAX).contains(&settings.memory_mib)
            || !(PID_LIMIT_MIN..=PID_LIMIT_MAX).contains(&settings.pid_limit)
            || !(TMPFS_MIB_MIN..=TMPFS_MIB_MAX).contains(&settings.temporary_storage_mib)
            || !(Duration::from_secs(1)..=Duration::from_secs(30 * 60))
                .contains(&settings.max_runtime)
            || !(Duration::from_secs(1)..=Duration::from_secs(30))
                .contains(&settings.cancellation_grace)
            || !(Duration::from_millis(100)..=Duration::from_secs(5))
                .contains(&settings.monitor_cadence)
            || !(Duration::from_secs(1)..=Duration::from_secs(120))
                .contains(&settings.soyuz_unavailable_grace)
            || !(1024..=256 * 1024).contains(&settings.max_output_bytes)
        {
            return Err(DockerConfigError::InvalidLimits);
        }
        Ok(Self {
            cpu_millicores: settings.cpu_millicores,
            memory_mib: settings.memory_mib,
            pid_limit: settings.pid_limit,
            temporary_storage_mib: settings.temporary_storage_mib,
            max_runtime: settings.max_runtime,
            cancellation_grace: settings.cancellation_grace,
            monitor_cadence: settings.monitor_cadence,
            soyuz_unavailable_grace: settings.soyuz_unavailable_grace,
            max_output_bytes: settings.max_output_bytes,
        })
    }
}

impl Default for SupervisorLimits {
    fn default() -> Self {
        let settings = SupervisorLimitSettings::default();
        Self {
            cpu_millicores: settings.cpu_millicores,
            memory_mib: settings.memory_mib,
            pid_limit: settings.pid_limit,
            temporary_storage_mib: settings.temporary_storage_mib,
            max_runtime: settings.max_runtime,
            cancellation_grace: settings.cancellation_grace,
            monitor_cadence: settings.monitor_cadence,
            soyuz_unavailable_grace: settings.soyuz_unavailable_grace,
            max_output_bytes: settings.max_output_bytes,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DockerRuntimeConfig {
    image: String,
    socket_path: PathBuf,
    limits: SupervisorLimits,
}

impl DockerRuntimeConfig {
    pub fn new(
        image: impl Into<String>,
        socket_path: impl Into<PathBuf>,
    ) -> Result<Self, DockerConfigError> {
        Self::with_limits(image, socket_path, SupervisorLimits::default())
    }

    pub fn with_limits(
        image: impl Into<String>,
        socket_path: impl Into<PathBuf>,
        limits: SupervisorLimits,
    ) -> Result<Self, DockerConfigError> {
        let image = image.into();
        let socket_path = socket_path.into();
        if !is_digest_pinned_image(&image) {
            return Err(DockerConfigError::ImageNotPinned);
        }
        if !socket_path.is_absolute()
            || socket_path.to_string_lossy().contains('\0')
            || !matches!(socket_path.to_string_lossy().as_ref(), path if path.starts_with('/'))
        {
            return Err(DockerConfigError::InvalidSocketPath);
        }
        Ok(Self {
            image,
            socket_path,
            limits,
        })
    }

    pub fn image(&self) -> &str {
        &self.image
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub fn limits(&self) -> SupervisorLimits {
        self.limits
    }

    pub fn container_config(&self, launch: &ContainerLaunchRecord) -> ContainerCreateBody {
        let labels = HashMap::from([
            ("io.rusty-fossil.managed".to_owned(), "true".to_owned()),
            (
                "io.rusty-fossil.run-id".to_owned(),
                launch.run_id.to_string(),
            ),
            (
                "io.rusty-fossil.worker-id".to_owned(),
                launch.worker_id.to_string(),
            ),
            (
                "io.rusty-fossil.claim-callback-id".to_owned(),
                launch.claim_callback_id.to_string(),
            ),
            (
                "io.rusty-fossil.start-callback-id".to_owned(),
                launch.start_callback_id.to_string(),
            ),
            (
                "io.rusty-fossil.workload-version".to_owned(),
                "synthetic-v1".to_owned(),
            ),
        ]);
        let mut temporary_filesystems = HashMap::new();
        temporary_filesystems.insert(
            "/tmp".to_owned(),
            format!(
                "rw,noexec,nosuid,nodev,size={}",
                self.limits
                    .temporary_storage_mib
                    .saturating_mul(1024 * 1024)
            ),
        );
        let mut logging_options = HashMap::new();
        logging_options.insert("max-size".to_owned(), "1m".to_owned());
        logging_options.insert("max-file".to_owned(), "1".to_owned());
        let memory_bytes = (self.limits.memory_mib * 1024 * 1024) as i64;
        let nano_cpus = (self.limits.cpu_millicores * 1_000_000) as i64;
        let host_config = HostConfig {
            auto_remove: Some(false),
            cap_drop: Some(vec!["ALL".to_owned()]),
            devices: None,
            binds: None,
            memory: Some(memory_bytes),
            memory_swap: Some(memory_bytes),
            nano_cpus: Some(nano_cpus),
            oom_kill_disable: Some(false),
            pids_limit: Some(self.limits.pid_limit as i64),
            privileged: Some(false),
            readonly_rootfs: Some(true),
            security_opt: Some(vec!["no-new-privileges:true".to_owned()]),
            tmpfs: Some(temporary_filesystems),
            log_config: Some(HostConfigLogConfig {
                typ: Some("json-file".to_owned()),
                config: Some(logging_options),
            }),
            network_mode: Some("none".to_owned()),
            ..Default::default()
        };
        ContainerCreateBody {
            image: Some(self.image.clone()),
            user: Some("65532:65532".to_owned()),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            attach_stdin: Some(false),
            open_stdin: Some(false),
            tty: Some(false),
            env: Some(vec![format!(
                "FOSSIL_SCENARIO={}",
                launch.scenario.as_str()
            )]),
            labels: Some(labels),
            stop_timeout: Some(self.limits.cancellation_grace.as_secs() as i64),
            host_config: Some(host_config),
            ..Default::default()
        }
    }
}

fn is_digest_pinned_image(image: &str) -> bool {
    if let Some(digest) = image.strip_prefix("sha256:") {
        return digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit());
    }
    let Some((repository, digest)) = image.rsplit_once("@sha256:") else {
        return false;
    };
    repository == TRUSTED_WORKER_IMAGE
        && digest.len() == 64
        && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub(crate) fn outcome_for_exit(exit_code: i32) -> (Option<i32>, &'static str, &'static str) {
    if exit_code == 0 {
        (Some(0), "success", "synthetic workload completed")
    } else {
        (Some(exit_code), "nonzero_exit", "synthetic workload failed")
    }
}

pub(crate) fn classify_status(
    scenario: SyntheticScenario,
    exit_code: i32,
    oom_killed: bool,
) -> (Option<i32>, &'static str, &'static str) {
    if oom_killed {
        return (
            Some(exit_code),
            "resource_limit_failure",
            "synthetic workload exceeded its resource limit",
        );
    }
    match scenario {
        SyntheticScenario::Timeout | SyntheticScenario::Cancellation => (
            Some(exit_code),
            "unexpected_termination",
            "synthetic workload terminated before its supervisor stop request",
        ),
        SyntheticScenario::UnexpectedTermination => (
            Some(exit_code),
            "unexpected_termination",
            "synthetic workload terminated unexpectedly",
        ),
        _ => outcome_for_exit(exit_code),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeout_and_cancellation_require_a_durable_supervisor_stop_intent() {
        assert_eq!(
            classify_status(SyntheticScenario::Timeout, 137, false).1,
            "unexpected_termination"
        );
        assert_eq!(
            classify_status(SyntheticScenario::Cancellation, 137, false).1,
            "unexpected_termination"
        );
        assert_eq!(
            classify_status(SyntheticScenario::Timeout, 137, true).1,
            "resource_limit_failure"
        );
    }

    #[test]
    fn runtime_limits_reject_values_outside_the_supported_bounds() {
        assert_eq!(
            SupervisorLimits::new(SupervisorLimitSettings {
                cpu_millicores: 1_001,
                ..SupervisorLimitSettings::default()
            })
            .unwrap_err(),
            DockerConfigError::InvalidLimits
        );
        assert!(SupervisorLimits::new(SupervisorLimitSettings::default()).is_ok());
    }

    #[test]
    fn only_the_fixed_worker_repository_or_immutable_ci_image_id_is_accepted() {
        let digest = "a".repeat(64);
        let trusted = format!("{TRUSTED_WORKER_IMAGE}@sha256:{digest}");
        assert!(DockerRuntimeConfig::new(trusted, "/var/run/docker.sock").is_ok());
        assert!(
            DockerRuntimeConfig::new(format!("sha256:{digest}"), "/var/run/docker.sock").is_ok()
        );
        assert_eq!(
            DockerRuntimeConfig::new("alpine:latest", "/var/run/docker.sock").unwrap_err(),
            DockerConfigError::ImageNotPinned
        );
    }
}
