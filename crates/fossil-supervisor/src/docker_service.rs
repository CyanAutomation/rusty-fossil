use crate::docker::{DockerRuntimeConfig, classify_status};
use crate::pi_events::{PiEventParser, PiStreamError, classify_pi_execution};
use crate::{
    CancellationOutcome, ExecutionOutcome, ExecutionSupervisor, PreparedExecution,
    ReconciliationOutcome, SupervisorError, SyntheticScenario, TerminalReason,
};
use async_trait::async_trait;
use bollard::container::LogOutput;
use bollard::errors::Error as BollardError;
use bollard::models::{ContainerInspectResponse, ContainerStateStatusEnum, SystemInfo};
use bollard::query_parameters::{
    CreateContainerOptionsBuilder, LogsOptionsBuilder, RemoveContainerOptionsBuilder,
    StartContainerOptions, StopContainerOptionsBuilder,
};
use bollard::{API_DEFAULT_VERSION, Docker};
use fossil_core::contract::SoyuzRunStatus;
use fossil_core::ids::{EventId, RunId, WorkerId};
use fossil_soyuz::{ExecutionGateAuthorization, SoyuzApi};
use fossil_storage::{
    CallbackOutboxRecord, CallbackPayload, ContainerExitTimes, ContainerLaunchProfile,
    ContainerLaunchRecord, ContainerLaunchState, SqliteStore, WorkloadKind,
};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

const MANAGED_LABEL: &str = "io.rusty-fossil.managed";
const LABEL_RUN_ID: &str = "io.rusty-fossil.run-id";
const LABEL_WORKER_ID: &str = "io.rusty-fossil.worker-id";
const LABEL_CLAIM_ID: &str = "io.rusty-fossil.claim-callback-id";
const LABEL_START_ID: &str = "io.rusty-fossil.start-callback-id";
const LABEL_WORKLOAD_VERSION: &str = "io.rusty-fossil.workload-version";
const WORKLOAD_VERSION: &str = "synthetic-v1";
const MIN_DOCKER_API_MINOR: usize = 41;
const CREATE_RESPONSE_GRACE: Duration = Duration::from_secs(30);
const MAX_PI_DOCKER_LOG_BYTES: usize = 20 * 1024 * 1024;

#[derive(Clone)]
pub struct DockerSupervisor {
    docker: Docker,
    config: DockerRuntimeConfig,
    store: SqliteStore,
    soyuz: Arc<dyn SoyuzApi>,
    worker_id: WorkerId,
    metrics: Arc<SupervisorMetrics>,
}

#[derive(Default)]
struct SupervisorMetrics {
    optional_callbacks_enqueued: AtomicU64,
    heartbeats_suppressed_capacity: AtomicU64,
    heartbeat_insertion_errors: AtomicU64,
    terminal_callback_insertion_failures: AtomicU64,
    terminal_callback_persistence_retries: AtomicU64,
    restart_reconciliation_attempts: AtomicU64,
    existing_containers_reattached: AtomicU64,
    ambiguous_container_states: AtomicU64,
    duplicate_launch_preventions: AtomicU64,
    cleanup_retries: AtomicU64,
    recovery_backlog: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DockerSupervisorMetricsSnapshot {
    pub optional_callbacks_enqueued: u64,
    pub heartbeats_suppressed_capacity: u64,
    pub heartbeat_insertion_errors: u64,
    pub terminal_callback_insertion_failures: u64,
    pub terminal_callback_persistence_retries: u64,
    pub restart_reconciliation_attempts: u64,
    pub existing_containers_reattached: u64,
    pub ambiguous_container_states: u64,
    pub duplicate_launch_preventions: u64,
    pub cleanup_retries: u64,
    pub recovery_backlog: u64,
}

impl DockerSupervisor {
    pub async fn connect(
        config: DockerRuntimeConfig,
        store: SqliteStore,
        soyuz: Arc<dyn SoyuzApi>,
        worker_id: WorkerId,
    ) -> Result<Self, SupervisorError> {
        let socket = config.socket_path().to_string_lossy();
        let docker = Docker::connect_with_socket(socket.as_ref(), 30, API_DEFAULT_VERSION)
            .map_err(|_| SupervisorError::DockerUnavailable)?;
        let docker = docker
            .negotiate_version()
            .await
            .map_err(|_| SupervisorError::DockerUnavailable)?;
        let negotiated_version = docker.client_version();
        if negotiated_version.major_version != 1
            || negotiated_version.minor_version < MIN_DOCKER_API_MINOR
        {
            return Err(SupervisorError::DockerUnavailable);
        }
        docker
            .ping()
            .await
            .map_err(|_| SupervisorError::DockerUnavailable)?;
        let info = docker
            .info()
            .await
            .map_err(|_| SupervisorError::DockerUnavailable)?;
        if !supports_required_resource_controls(&info) {
            return Err(SupervisorError::DockerUnavailable);
        }
        Ok(Self {
            docker,
            config,
            store,
            soyuz,
            worker_id,
            metrics: Arc::default(),
        })
    }

    /// Returns fixed-cardinality counters. No run IDs or task content are
    /// included; callers decide how to export this snapshot.
    pub fn metrics(&self) -> DockerSupervisorMetricsSnapshot {
        DockerSupervisorMetricsSnapshot {
            optional_callbacks_enqueued: self
                .metrics
                .optional_callbacks_enqueued
                .load(Ordering::Relaxed),
            heartbeats_suppressed_capacity: self
                .metrics
                .heartbeats_suppressed_capacity
                .load(Ordering::Relaxed),
            heartbeat_insertion_errors: self
                .metrics
                .heartbeat_insertion_errors
                .load(Ordering::Relaxed),
            terminal_callback_insertion_failures: self
                .metrics
                .terminal_callback_insertion_failures
                .load(Ordering::Relaxed),
            terminal_callback_persistence_retries: self
                .metrics
                .terminal_callback_persistence_retries
                .load(Ordering::Relaxed),
            restart_reconciliation_attempts: self
                .metrics
                .restart_reconciliation_attempts
                .load(Ordering::Relaxed),
            existing_containers_reattached: self
                .metrics
                .existing_containers_reattached
                .load(Ordering::Relaxed),
            ambiguous_container_states: self
                .metrics
                .ambiguous_container_states
                .load(Ordering::Relaxed),
            duplicate_launch_preventions: self
                .metrics
                .duplicate_launch_preventions
                .load(Ordering::Relaxed),
            cleanup_retries: self.metrics.cleanup_retries.load(Ordering::Relaxed),
            recovery_backlog: self.metrics.recovery_backlog.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn record_duplicate_launch_prevented(&self) {
        self.metrics
            .duplicate_launch_preventions
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn set_recovery_backlog(&self, count: usize) {
        self.metrics
            .recovery_backlog
            .store(count as u64, Ordering::Relaxed);
    }

    /// The IPC boundary accepts only a canonical run ID. All descriptor,
    /// worker, fence, slot and current Soyuz checks happen inside this process.
    pub async fn launch(
        &self,
        run_id: RunId,
        scenario: SyntheticScenario,
    ) -> Result<ExecutionOutcome, SupervisorError> {
        let prepared = self.prepare_run(run_id, scenario).await?;
        self.execute(prepared).await
    }

    /// Rebuilds a supervisor-local prepared handoff from the durable run
    /// record. The IPC protocol never accepts authorization objects or request
    /// metadata from its caller.
    pub async fn prepare_run(
        &self,
        run_id: RunId,
        scenario: SyntheticScenario,
    ) -> Result<PreparedExecution, SupervisorError> {
        let authorization = self.refresh_gate(run_id).await?;
        self.prepare(authorization, scenario).await
    }

    /// Resumes only an already persisted launch. It never fabricates a new
    /// handoff and all possible Docker side effects are reconciled by name.
    pub async fn resume_existing(
        &self,
        run_id: RunId,
    ) -> Result<ExecutionOutcome, SupervisorError> {
        self.metrics
            .restart_reconciliation_attempts
            .fetch_add(1, Ordering::Relaxed);
        let launch = self
            .store
            .container_launch(run_id)
            .map_err(|_| SupervisorError::StateUnavailable)?
            .ok_or(SupervisorError::AuthorizationRejected)?;
        if launch.container_id.is_some()
            && !matches!(
                launch.state,
                ContainerLaunchState::CleanupPending
                    | ContainerLaunchState::CleanupCompleted
                    | ContainerLaunchState::ManualReviewRequired
            )
        {
            self.metrics
                .existing_containers_reattached
                .fetch_add(1, Ordering::Relaxed);
        }
        self.execute(prepared_from_launch(&launch)).await
    }

    /// Persists a fail-closed stop request during orderly process shutdown.
    /// The running monitor observes this row and finalizes the exact container.
    pub fn request_stop_for_shutdown(&self, run_id: RunId) -> Result<(), SupervisorError> {
        let launch = self
            .store
            .container_launch(run_id)
            .map_err(|_| SupervisorError::StateUnavailable)?
            .ok_or(SupervisorError::StateUnavailable)?;
        if launch.state == ContainerLaunchState::StopRequested {
            return Ok(());
        }
        if matches!(
            launch.state,
            ContainerLaunchState::LaunchIntentPersisted
                | ContainerLaunchState::CreateRequested
                | ContainerLaunchState::ContainerCreated
                | ContainerLaunchState::StartRequested
                | ContainerLaunchState::ContainerRunning
        ) {
            self.store
                .mark_container_stop_requested(run_id, "execution_uncertain")
                .map_err(|_| SupervisorError::StateUnavailable)?;
        }
        Ok(())
    }

    pub async fn execute_prepared(
        &self,
        prepared: PreparedExecution,
    ) -> Result<ExecutionOutcome, SupervisorError> {
        self.execute(prepared).await
    }

    async fn refresh_gate(
        &self,
        run_id: RunId,
    ) -> Result<ExecutionGateAuthorization, SupervisorError> {
        let run = self
            .store
            .get_run(run_id)
            .map_err(|_| SupervisorError::StateUnavailable)?;
        if run.worker_id != self.worker_id || !run.start_authorized {
            return Err(SupervisorError::AuthorizationRejected);
        }
        let claim_id = run
            .claim_callback_id
            .ok_or(SupervisorError::AuthorizationRejected)?;
        let start_id = run
            .start_callback_id
            .ok_or(SupervisorError::AuthorizationRejected)?;
        let descriptor = self
            .store
            .execution_descriptor(run_id)
            .map_err(|_| SupervisorError::StateUnavailable)?
            .ok_or(SupervisorError::AuthorizationRejected)?;
        if descriptor.run_id != run_id {
            return Err(SupervisorError::AuthorizationRejected);
        }
        let started = self
            .soyuz
            .reconcile_started(run_id, start_id, self.worker_id.clone(), claim_id)
            .await
            .map_err(|_| SupervisorError::SoyuzUnavailable)?;
        self.soyuz
            .verify_execution_gate(&started)
            .await
            .map_err(|_| SupervisorError::AuthorizationRejected)
    }

    async fn inspect_named(
        &self,
        launch: &ContainerLaunchRecord,
    ) -> Result<Option<ContainerInspectResponse>, SupervisorError> {
        match self
            .docker
            .inspect_container(
                &launch.container_name,
                None::<bollard::query_parameters::InspectContainerOptions>,
            )
            .await
        {
            Ok(container) => Ok(Some(container)),
            Err(BollardError::DockerResponseServerError {
                status_code: 404, ..
            }) => Ok(None),
            Err(_) => Err(SupervisorError::DockerUnavailable),
        }
    }

    fn validate_launch_profile(
        &self,
        launch: &ContainerLaunchRecord,
    ) -> Result<(), SupervisorError> {
        if launch.workload_kind != WorkloadKind::Synthetic
            || launch.workload_version != 1
            || launch.profile_version != WORKLOAD_VERSION
            || launch.image_identity.as_deref() != Some(self.config.image())
            || launch.effective_policy_json != self.config.effective_policy_json()
            || launch.runtime_limit_ms != Some(self.config.limits().max_runtime.as_millis() as i64)
            || launch.repository_url.is_some()
            || launch.authorized_git_ref.is_some()
            || launch.resolved_commit.is_some()
            || launch.workspace_identity.is_some()
        {
            return Err(SupervisorError::AmbiguousContainerState);
        }
        Ok(())
    }

    fn validate_container_identity(
        &self,
        launch: &ContainerLaunchRecord,
        inspect: &ContainerInspectResponse,
    ) -> Result<String, SupervisorError> {
        self.validate_launch_profile(launch)?;
        let labels = inspect
            .config
            .as_ref()
            .and_then(|config| config.labels.as_ref())
            .ok_or(SupervisorError::AmbiguousContainerState)?;
        let expected = [
            (MANAGED_LABEL, "true".to_owned()),
            (LABEL_RUN_ID, launch.run_id.to_string()),
            (LABEL_WORKER_ID, launch.worker_id.to_string()),
            (LABEL_CLAIM_ID, launch.claim_callback_id.to_string()),
            (LABEL_START_ID, launch.start_callback_id.to_string()),
            (LABEL_WORKLOAD_VERSION, WORKLOAD_VERSION.to_owned()),
        ];
        if expected
            .iter()
            .any(|(key, value)| labels.get(*key) != Some(value))
        {
            return Err(SupervisorError::AmbiguousContainerState);
        }
        let id = inspect
            .id
            .as_ref()
            .filter(|id| !id.is_empty())
            .ok_or(SupervisorError::AmbiguousContainerState)?;
        if launch
            .container_id
            .as_deref()
            .is_some_and(|persisted| persisted != id)
        {
            return Err(SupervisorError::AmbiguousContainerState);
        }
        let config = inspect
            .config
            .as_ref()
            .ok_or(SupervisorError::AmbiguousContainerState)?;
        let host = inspect
            .host_config
            .as_ref()
            .ok_or(SupervisorError::AmbiguousContainerState)?;
        let limits = self.config.limits();
        let memory = (limits.memory_mib * 1024 * 1024) as i64;
        let nano_cpus = (limits.cpu_millicores * 1_000_000) as i64;
        let profile_ok = config.image.as_deref() == Some(self.config.image())
            && safe_worker_environment(config.env.as_deref(), launch.scenario)
            && config.stop_timeout == Some(limits.cancellation_grace.as_secs() as i64)
            && config.user.as_deref() == Some("65532:65532")
            && host.privileged == Some(false)
            && host.readonly_rootfs == Some(true)
            && host.cap_drop.as_deref() == Some(["ALL".to_owned()].as_slice())
            && host.cap_add.as_ref().is_none_or(Vec::is_empty)
            && host.security_opt.as_deref().is_some_and(|values| {
                values.iter().any(|value| value == "no-new-privileges:true")
                    && !values
                        .iter()
                        .any(|value| value.contains("seccomp=unconfined"))
            })
            && host.network_mode.as_deref() == Some("none")
            && host.pid_mode.as_deref() != Some("host")
            && host.ipc_mode.as_deref() != Some("host")
            && host.uts_mode.as_deref() != Some("host")
            && host.memory == Some(memory)
            && host.memory_swap == Some(memory)
            && host.nano_cpus == Some(nano_cpus)
            && host.pids_limit == Some(limits.pid_limit as i64)
            // Docker omits false-valued fields from inspect responses on some
            // Engine versions; only an explicit true means OOM killing is disabled.
            && host.oom_kill_disable != Some(true)
            && host.auto_remove == Some(false)
            && host.restart_policy.as_ref().is_none_or(|policy| {
                policy
                    .name
                    .as_ref()
                    .is_none_or(|name| name.as_ref().is_empty() || name.as_ref() == "no")
            })
            && host.devices.as_ref().is_none_or(Vec::is_empty)
            && host.device_requests.as_ref().is_none_or(Vec::is_empty)
            && host.device_cgroup_rules.as_ref().is_none_or(Vec::is_empty)
            && host.binds.as_ref().is_none_or(Vec::is_empty)
            && host.mounts.as_ref().is_none_or(Vec::is_empty)
            && host.volumes_from.as_ref().is_none_or(Vec::is_empty)
            && host.extra_hosts.as_ref().is_none_or(Vec::is_empty)
            && host.links.as_ref().is_none_or(Vec::is_empty)
            && host.group_add.as_ref().is_none_or(Vec::is_empty)
            && host.port_bindings.as_ref().is_none_or(HashMap::is_empty)
            && host.publish_all_ports != Some(true)
            && host.log_config.as_ref().is_some_and(|logging| {
                logging.typ.as_deref() == Some("json-file")
                    && logging.config.as_ref().is_some_and(|options| {
                        options.get("max-size").is_some_and(|value| value == "1m")
                            && options.get("max-file").is_some_and(|value| value == "1")
                    })
            })
            && host.tmpfs.as_ref().is_some_and(|filesystems| {
                filesystems.get("/tmp").is_some_and(|options| {
                    options.contains("noexec")
                        && options.contains("nosuid")
                        && options.contains("nodev")
                        && options.contains(&format!(
                            "size={}",
                            limits.temporary_storage_mib * 1024 * 1024
                        ))
                })
            });
        if !profile_ok {
            return Err(SupervisorError::AmbiguousContainerState);
        }
        Ok(id.clone())
    }

    async fn ensure_created(
        &self,
        mut launch: ContainerLaunchRecord,
    ) -> Result<ContainerLaunchRecord, SupervisorError> {
        if launch.state == ContainerLaunchState::LaunchIntentPersisted {
            if self.validate_launch_profile(&launch).is_err() {
                self.mark_manual_review(launch.run_id);
                return Err(SupervisorError::AmbiguousContainerState);
            }
            self.refresh_gate(launch.run_id).await?;
            let owns_create = self
                .store
                .request_container_create(launch.run_id)
                .map_err(|_| SupervisorError::StateUnavailable)?;
            launch = self
                .store
                .container_launch(launch.run_id)
                .map_err(|_| SupervisorError::StateUnavailable)?
                .ok_or(SupervisorError::StateUnavailable)?;
            if owns_create {
                #[cfg(feature = "test-support")]
                process_test_barrier("after_create_requested_commit").await;
                let options = CreateContainerOptionsBuilder::default()
                    .name(&launch.container_name)
                    .build();
                match self
                    .docker
                    .create_container(
                        Some(options),
                        self.config
                            .container_config(&launch)
                            .map_err(|_| SupervisorError::AmbiguousContainerState)?,
                    )
                    .await
                {
                    Ok(response) => {
                        #[cfg(feature = "test-support")]
                        process_test_barrier("after_docker_create_before_identity_commit").await;
                        launch = self
                            .store
                            .record_container_created(launch.run_id, &response.id)
                            .map_err(|_| SupervisorError::StateUnavailable)?;
                        #[cfg(feature = "test-support")]
                        process_test_barrier("after_container_identity_commit").await;
                    }
                    Err(_) => {
                        // An errored response can mean Docker created the
                        // object but the response was lost. Inspect the exact
                        // deterministic name before considering any retry.
                        if let Some(inspect) = self.inspect_named(&launch).await? {
                            let id = self.validate_container_identity(&launch, &inspect)?;
                            launch = self
                                .store
                                .record_container_created(launch.run_id, &id)
                                .map_err(|_| SupervisorError::StateUnavailable)?;
                        } else if launch.updated_at_ms
                            > now_ms().saturating_sub(CREATE_RESPONSE_GRACE.as_millis() as i64)
                        {
                            return Err(SupervisorError::AmbiguousContainerState);
                        } else {
                            self.mark_manual_review(launch.run_id);
                            return Err(SupervisorError::AmbiguousContainerState);
                        }
                    }
                }
            } else {
                return self.reconcile_container_create(launch).await;
            }
        } else if launch.state == ContainerLaunchState::CreateRequested {
            return self.reconcile_container_create(launch).await;
        }

        if let Some(inspect) = self.inspect_named(&launch).await? {
            let id = match self.validate_container_identity(&launch, &inspect) {
                Ok(id) => id,
                Err(error) => {
                    self.mark_manual_review(launch.run_id);
                    return Err(error);
                }
            };
            if launch.container_id.is_none() {
                launch = self
                    .store
                    .record_container_created(launch.run_id, &id)
                    .map_err(|_| SupervisorError::StateUnavailable)?;
            }
            self.verify_effective_profile(&launch, &inspect)?;
            Ok(launch)
        } else {
            self.mark_manual_review(launch.run_id);
            Err(SupervisorError::AmbiguousContainerState)
        }
    }

    async fn reconcile_container_create(
        &self,
        launch: ContainerLaunchRecord,
    ) -> Result<ContainerLaunchRecord, SupervisorError> {
        let Some(inspect) = self.inspect_named(&launch).await? else {
            #[cfg(feature = "test-support")]
            process_test_barrier("after_create_requested_missing_container").await;
            if launch.updated_at_ms
                <= now_ms().saturating_sub(CREATE_RESPONSE_GRACE.as_millis() as i64)
            {
                self.mark_manual_review(launch.run_id);
            }
            return Err(SupervisorError::AmbiguousContainerState);
        };
        let id = match self.validate_container_identity(&launch, &inspect) {
            Ok(id) => id,
            Err(error) => {
                self.mark_manual_review(launch.run_id);
                return Err(error);
            }
        };
        let launch = self
            .store
            .record_container_created(launch.run_id, &id)
            .map_err(|_| SupervisorError::StateUnavailable)?;
        self.verify_effective_profile(&launch, &inspect)?;
        Ok(launch)
    }

    fn verify_effective_profile(
        &self,
        launch: &ContainerLaunchRecord,
        inspect: &ContainerInspectResponse,
    ) -> Result<(), SupervisorError> {
        self.validate_container_identity(launch, inspect)
            .map(|_| ())
            .inspect_err(|_error| {
                self.mark_manual_review(launch.run_id);
            })
    }

    async fn start_or_reattach(
        &self,
        mut launch: ContainerLaunchRecord,
    ) -> Result<ContainerLaunchRecord, SupervisorError> {
        if launch.state == ContainerLaunchState::ContainerCreated {
            launch = self
                .store
                .mark_container_start_requested(launch.run_id)
                .map_err(|_| SupervisorError::StateUnavailable)?;
            #[cfg(feature = "test-support")]
            process_test_barrier("after_start_requested_commit").await;
        }
        if !matches!(
            launch.state,
            ContainerLaunchState::StartRequested | ContainerLaunchState::ContainerRunning
        ) {
            return Err(SupervisorError::AmbiguousContainerState);
        }
        let inspect = self
            .inspect_named(&launch)
            .await?
            .ok_or(SupervisorError::AmbiguousContainerState)?;
        self.verify_effective_profile(&launch, &inspect)?;
        match inspect.state.as_ref().and_then(|state| state.status) {
            Some(ContainerStateStatusEnum::RUNNING) => {
                let started_at_ms = docker_timestamp_ms(
                    inspect
                        .state
                        .as_ref()
                        .and_then(|state| state.started_at.as_deref())
                        .ok_or(SupervisorError::AmbiguousContainerState)?,
                )
                .ok_or(SupervisorError::AmbiguousContainerState)?;
                match launch.state {
                    ContainerLaunchState::StartRequested => {
                        self.recover_container_running(launch.run_id, started_at_ms)
                    }
                    ContainerLaunchState::ContainerRunning => Ok(launch),
                    _ => Err(SupervisorError::AmbiguousContainerState),
                }
            }
            Some(ContainerStateStatusEnum::EXITED | ContainerStateStatusEnum::DEAD) => {
                self.finalize_exited_container(launch.clone(), &inspect, None)
                    .await?;
                self.store
                    .container_launch(launch.run_id)
                    .map_err(|_| SupervisorError::StateUnavailable)?
                    .ok_or(SupervisorError::StateUnavailable)
            }
            Some(ContainerStateStatusEnum::CREATED) => {
                let gate = self.refresh_gate(launch.run_id).await?;
                if launch.state != ContainerLaunchState::StartRequested {
                    return Err(SupervisorError::AmbiguousContainerState);
                }
                let start_result = self
                    .docker
                    .start_container(&launch.container_name, None::<StartContainerOptions>)
                    .await;
                #[cfg(feature = "test-support")]
                if start_result.is_ok() {
                    process_test_barrier("after_docker_start_before_running_commit").await;
                }
                if start_result.is_err() {
                    let inspect = self
                        .inspect_named(&launch)
                        .await?
                        .ok_or(SupervisorError::AmbiguousContainerState)?;
                    match inspect.state.as_ref().and_then(|state| state.status) {
                        Some(ContainerStateStatusEnum::RUNNING) => {}
                        Some(ContainerStateStatusEnum::EXITED | ContainerStateStatusEnum::DEAD) => {
                            self.finalize_exited_container(launch.clone(), &inspect, None)
                                .await?;
                            return self
                                .store
                                .container_launch(launch.run_id)
                                .map_err(|_| SupervisorError::StateUnavailable)?
                                .ok_or(SupervisorError::StateUnavailable);
                        }
                        _ => return Err(SupervisorError::AmbiguousContainerState),
                    }
                }
                let inspect = self
                    .inspect_named(&launch)
                    .await?
                    .ok_or(SupervisorError::AmbiguousContainerState)?;
                let started_at_ms = docker_timestamp_ms(
                    inspect
                        .state
                        .as_ref()
                        .and_then(|state| state.started_at.as_deref())
                        .ok_or(SupervisorError::AmbiguousContainerState)?,
                )
                .ok_or(SupervisorError::AmbiguousContainerState)?;
                match inspect.state.as_ref().and_then(|state| state.status) {
                    Some(ContainerStateStatusEnum::RUNNING) => self
                        .store
                        .mark_container_running(launch.run_id, gate, started_at_ms)
                        .map_err(|_| SupervisorError::StateUnavailable),
                    Some(ContainerStateStatusEnum::EXITED | ContainerStateStatusEnum::DEAD) => {
                        self.finalize_exited_container(launch.clone(), &inspect, None)
                            .await?;
                        self.store
                            .container_launch(launch.run_id)
                            .map_err(|_| SupervisorError::StateUnavailable)?
                            .ok_or(SupervisorError::StateUnavailable)
                    }
                    _ => Err(SupervisorError::AmbiguousContainerState),
                }
            }
            _ => Err(SupervisorError::AmbiguousContainerState),
        }
    }

    async fn monitor(
        &self,
        mut launch: ContainerLaunchRecord,
    ) -> Result<ExecutionOutcome, SupervisorError> {
        let mut soyuz_unavailable_since = None;
        let mut last_heartbeat = tokio::time::Instant::now();
        loop {
            if launch.state == ContainerLaunchState::StopRequested {
                let reason = launch
                    .stop_reason
                    .clone()
                    .ok_or(SupervisorError::StateUnavailable)?;
                return self.stop_and_finalize(launch, &reason).await;
            }
            let inspect = self.inspect_named(&launch).await?.ok_or_else(|| {
                self.mark_manual_review(launch.run_id);
                SupervisorError::AmbiguousContainerState
            })?;
            self.verify_effective_profile(&launch, &inspect)?;
            let status = inspect.state.as_ref().and_then(|state| state.status);
            if matches!(
                status,
                Some(ContainerStateStatusEnum::EXITED | ContainerStateStatusEnum::DEAD)
            ) {
                self.finalize_exited_container(launch.clone(), &inspect, None)
                    .await?;
                return self.load_outcome(launch.run_id);
            }
            if status != Some(ContainerStateStatusEnum::RUNNING) {
                self.mark_manual_review(launch.run_id);
                return Err(SupervisorError::AmbiguousContainerState);
            }
            #[cfg(feature = "test-support")]
            process_test_barrier("while_container_running").await;
            let started_at_ms = launch
                .started_at_ms
                .or_else(|| {
                    inspect
                        .state
                        .as_ref()
                        .and_then(|state| state.started_at.as_deref())
                        .and_then(docker_timestamp_ms)
                })
                .ok_or(SupervisorError::AmbiguousContainerState)?;
            let Some(deadline_at_ms) = launch.effective_deadline_at_ms else {
                self.mark_manual_review(launch.run_id);
                return Err(SupervisorError::AmbiguousContainerState);
            };
            let expected_deadline = launch
                .runtime_limit_ms
                .and_then(|runtime_ms| started_at_ms.checked_add(runtime_ms));
            if expected_deadline != Some(deadline_at_ms) {
                self.mark_manual_review(launch.run_id);
                return Err(SupervisorError::AmbiguousContainerState);
            }
            if now_ms() >= deadline_at_ms {
                return self.stop_and_finalize(launch, "timeout").await;
            }

            match self.soyuz.get_run(launch.run_id).await {
                Ok(canonical)
                    if canonical.worker_id.as_ref() == Some(&launch.worker_id)
                        && canonical.claim_callback_id == Some(launch.claim_callback_id)
                        && (canonical.cancellation_requested()
                            || canonical.status == SoyuzRunStatus::CancelRequested) =>
                {
                    return self.stop_and_finalize(launch, "cancelled").await;
                }
                Ok(canonical)
                    if canonical.worker_id.as_ref() == Some(&launch.worker_id)
                        && canonical.claim_callback_id == Some(launch.claim_callback_id)
                        && canonical.status == SoyuzRunStatus::Running =>
                {
                    soyuz_unavailable_since = None;
                }
                Ok(_) => {
                    self.stop_and_finalize(launch.clone(), "execution_uncertain")
                        .await?;
                    self.mark_manual_review(launch.run_id);
                    return Err(SupervisorError::AuthorizationRejected);
                }
                Err(_) => {
                    let first =
                        soyuz_unavailable_since.get_or_insert_with(tokio::time::Instant::now);
                    if first.elapsed() >= self.config.limits().soyuz_unavailable_grace {
                        return self.stop_and_finalize(launch, "execution_uncertain").await;
                    }
                }
            }

            if last_heartbeat.elapsed() >= Duration::from_secs(30) {
                self.enqueue_heartbeat(&launch);
                last_heartbeat = tokio::time::Instant::now();
            }
            tokio::time::sleep(self.config.limits().monitor_cadence).await;
            launch = self
                .store
                .container_launch(launch.run_id)
                .map_err(|_| SupervisorError::StateUnavailable)?
                .ok_or(SupervisorError::StateUnavailable)?;
        }
    }

    async fn stop_and_finalize(
        &self,
        mut launch: ContainerLaunchRecord,
        reason: &str,
    ) -> Result<ExecutionOutcome, SupervisorError> {
        let reason = launch
            .stop_reason
            .as_deref()
            .filter(|_| launch.state == ContainerLaunchState::StopRequested)
            .unwrap_or(reason)
            .to_owned();
        let Some(inspect) = self.inspect_named(&launch).await? else {
            if let Some(current) = self
                .store
                .container_launch(launch.run_id)
                .map_err(|_| SupervisorError::StateUnavailable)?
                && matches!(
                    current.state,
                    ContainerLaunchState::CleanupPending | ContainerLaunchState::CleanupCompleted
                )
            {
                if current.state == ContainerLaunchState::CleanupPending {
                    self.cleanup(&current).await?;
                }
                return self.load_outcome(launch.run_id);
            }
            if launch.state == ContainerLaunchState::LaunchIntentPersisted
                || (launch.state == ContainerLaunchState::StopRequested
                    && launch.container_id.is_none()
                    && launch.started_at_ms.is_none())
            {
                if launch.state != ContainerLaunchState::StopRequested {
                    launch = self
                        .store
                        .mark_container_stop_requested(launch.run_id, &reason)
                        .map_err(|_| SupervisorError::StateUnavailable)?;
                }
                return self.finalize_unstarted(launch, &reason).await;
            }
            self.mark_manual_review(launch.run_id);
            return Err(SupervisorError::AmbiguousContainerState);
        };
        self.verify_effective_profile(&launch, &inspect)?;
        if matches!(
            inspect.state.as_ref().and_then(|state| state.status),
            Some(ContainerStateStatusEnum::EXITED | ContainerStateStatusEnum::DEAD)
        ) {
            return self.finalize_exited_container(launch, &inspect, None).await;
        }
        if inspect.state.as_ref().and_then(|state| state.status)
            == Some(ContainerStateStatusEnum::CREATED)
        {
            if launch.state == ContainerLaunchState::CreateRequested {
                let id = self.validate_container_identity(&launch, &inspect)?;
                launch = self
                    .store
                    .record_container_created(launch.run_id, &id)
                    .map_err(|_| SupervisorError::StateUnavailable)?;
            }
            if !matches!(
                launch.state,
                ContainerLaunchState::ContainerCreated
                    | ContainerLaunchState::StartRequested
                    | ContainerLaunchState::StopRequested
            ) {
                self.mark_manual_review(launch.run_id);
                return Err(SupervisorError::AmbiguousContainerState);
            }
            if launch.state != ContainerLaunchState::StopRequested {
                launch = self
                    .store
                    .mark_container_stop_requested(launch.run_id, &reason)
                    .map_err(|_| SupervisorError::StateUnavailable)?;
            }
            return self.finalize_unstarted(launch, &reason).await;
        }
        let launch = self
            .store
            .mark_container_stop_requested(launch.run_id, &reason)
            .map_err(|_| SupervisorError::StateUnavailable)?;
        let stop = StopContainerOptionsBuilder::default()
            .t(self.config.limits().cancellation_grace.as_secs() as i32)
            .build();
        let _ = self
            .docker
            .stop_container(&launch.container_name, Some(stop))
            .await;
        let mut inspect = self
            .inspect_named(&launch)
            .await?
            .ok_or(SupervisorError::AmbiguousContainerState)?;
        if inspect.state.as_ref().and_then(|state| state.status)
            == Some(ContainerStateStatusEnum::RUNNING)
        {
            let _ = self
                .docker
                .kill_container(
                    &launch.container_name,
                    None::<bollard::query_parameters::KillContainerOptions>,
                )
                .await;
            tokio::time::sleep(self.config.limits().monitor_cadence).await;
            inspect = self
                .inspect_named(&launch)
                .await?
                .ok_or(SupervisorError::AmbiguousContainerState)?;
        }
        if inspect
            .state
            .as_ref()
            .and_then(|state| state.status)
            .is_none_or(|status| {
                !matches!(
                    status,
                    ContainerStateStatusEnum::EXITED | ContainerStateStatusEnum::DEAD
                )
            })
        {
            self.mark_manual_review(launch.run_id);
            return Err(SupervisorError::AmbiguousContainerState);
        }
        self.finalize_exited_container(launch, &inspect, Some(&reason))
            .await
    }

    async fn finalize_unstarted(
        &self,
        launch: ContainerLaunchRecord,
        reason: &str,
    ) -> Result<ExecutionOutcome, SupervisorError> {
        let (class, summary) = if reason == "cancelled" {
            ("cancelled", "synthetic workload cancelled before start")
        } else {
            (
                "execution_uncertain",
                "synthetic workload was stopped before start authorization",
            )
        };
        let terminal = self
            .store
            .record_container_exit(launch.run_id, None, class, summary, false, now_ms())
            .map_err(|_| SupervisorError::StateUnavailable)?;
        self.commit_terminal(terminal).await
    }

    async fn finalize_exited_container(
        &self,
        mut launch: ContainerLaunchRecord,
        inspect: &ContainerInspectResponse,
        requested_reason: Option<&str>,
    ) -> Result<ExecutionOutcome, SupervisorError> {
        let _ = self.validate_container_identity(&launch, inspect)?;
        let state = inspect
            .state
            .as_ref()
            .ok_or(SupervisorError::AmbiguousContainerState)?;
        let exit_code = state
            .exit_code
            .and_then(|value| i32::try_from(value).ok())
            .ok_or(SupervisorError::AmbiguousContainerState)?;
        let finished_at_ms = state
            .finished_at
            .as_deref()
            .and_then(docker_timestamp_ms)
            .unwrap_or_else(now_ms);
        let started_at_ms = state.started_at.as_deref().and_then(docker_timestamp_ms);
        let stop_reason = requested_reason.or(launch.stop_reason.as_deref());
        let stopped_before_exit = launch
            .stop_requested_at_ms
            .is_some_and(|requested_at| finished_at_ms > requested_at);
        let (class, summary, logs_truncated) = if stopped_before_exit {
            match stop_reason {
                Some("cancelled") if launch.workload_kind == WorkloadKind::Synthetic => {
                    ("cancelled", "synthetic workload cancelled", false)
                }
                Some("cancelled") => ("cancelled", "Pi coding task cancelled", false),
                Some("timeout") if launch.workload_kind == WorkloadKind::Synthetic => {
                    ("timeout", "synthetic workload timed out", false)
                }
                Some("timeout") => ("timeout", "Pi coding task timed out", false),
                _ => (
                    "execution_uncertain",
                    "execution stopped after Soyuz became unavailable",
                    false,
                ),
            }
        } else {
            match launch.workload_kind {
                WorkloadKind::Synthetic => {
                    let logs_truncated = self.read_bounded_logs(&launch).await?.1;
                    let oom = state.oom_killed.unwrap_or(false);
                    let (_, class, summary) = classify_status(launch.scenario, exit_code, oom);
                    (class, summary, logs_truncated)
                }
                WorkloadKind::PiCodingV1 if state.oom_killed.unwrap_or(false) => (
                    "resource_limit_failure",
                    "Pi worker exceeded its resource limit",
                    false,
                ),
                WorkloadKind::PiCodingV1 => {
                    let stream = self.read_pi_event_stream(&launch).await?;
                    let class = classify_pi_execution(stream, Some(exit_code), None);
                    let outcome = class.durable_outcome();
                    (
                        outcome.outcome_class,
                        outcome.diagnostic_summary,
                        outcome.output_truncated,
                    )
                }
            }
        };
        if launch.state != ContainerLaunchState::ContainerExited {
            #[cfg(feature = "test-support")]
            process_test_barrier("after_docker_exit_before_terminal_persist").await;
            launch = self
                .store
                .record_container_exit_with_times(
                    launch.run_id,
                    Some(exit_code),
                    class,
                    summary,
                    logs_truncated,
                    ContainerExitTimes {
                        started_at_ms,
                        completed_at_ms: finished_at_ms,
                    },
                )
                .map_err(|_| SupervisorError::StateUnavailable)?;
        }
        self.commit_terminal(launch).await
    }

    async fn commit_terminal(
        &self,
        launch: ContainerLaunchRecord,
    ) -> Result<ExecutionOutcome, SupervisorError> {
        let mut persisted = launch;
        if persisted.state == ContainerLaunchState::ContainerExited {
            let callback = terminal_callback(&persisted)?;
            let mut terminal_persisted = false;
            for attempt in 0..3 {
                match self
                    .store
                    .transition_terminal_with_callback(persisted.run_id, callback.clone())
                {
                    Ok(()) => {
                        terminal_persisted = true;
                        break;
                    }
                    Err(_) => {
                        // A committed callback and cleanup state survive a lost
                        // IPC response. Repeated finalization observes them here.
                        if self
                            .store
                            .container_launch(persisted.run_id)
                            .ok()
                            .flatten()
                            .is_some_and(|record| {
                                matches!(
                                    record.state,
                                    ContainerLaunchState::CleanupPending
                                        | ContainerLaunchState::CleanupCompleted
                                )
                            })
                        {
                            terminal_persisted = true;
                            break;
                        }
                        if attempt < 2 {
                            self.metrics
                                .terminal_callback_persistence_retries
                                .fetch_add(1, Ordering::Relaxed);
                            tokio::time::sleep(Duration::from_millis(10 << attempt)).await;
                        }
                    }
                }
            }
            if !terminal_persisted {
                self.metrics
                    .terminal_callback_insertion_failures
                    .fetch_add(1, Ordering::Relaxed);
                return Err(SupervisorError::StateUnavailable);
            }
            persisted = self
                .store
                .container_launch(persisted.run_id)
                .map_err(|_| SupervisorError::StateUnavailable)?
                .ok_or(SupervisorError::StateUnavailable)?;
            #[cfg(feature = "test-support")]
            process_test_barrier("after_terminal_persist_before_cleanup").await;
        }
        let outcome = outcome_from_launch(&persisted)?;
        self.cleanup(&persisted).await?;
        Ok(outcome)
    }

    async fn cleanup(&self, launch: &ContainerLaunchRecord) -> Result<(), SupervisorError> {
        let result = self.cleanup_inner(launch).await;
        if result.is_err() {
            self.metrics.cleanup_retries.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    async fn cleanup_inner(&self, launch: &ContainerLaunchRecord) -> Result<(), SupervisorError> {
        if launch.state == ContainerLaunchState::CleanupCompleted {
            return Ok(());
        }
        if launch.state != ContainerLaunchState::CleanupPending {
            return Err(SupervisorError::CleanupPending);
        }
        if let Some(inspect) = self.inspect_named(launch).await? {
            self.validate_container_identity(launch, &inspect)?;
            if !matches!(
                inspect.state.as_ref().and_then(|state| state.status),
                Some(
                    ContainerStateStatusEnum::CREATED
                        | ContainerStateStatusEnum::EXITED
                        | ContainerStateStatusEnum::DEAD
                )
            ) {
                return Err(SupervisorError::CleanupPending);
            }
            let options = RemoveContainerOptionsBuilder::default()
                .force(false)
                .v(false)
                .link(false)
                .build();
            match self
                .docker
                .remove_container(&launch.container_name, Some(options))
                .await
            {
                Ok(())
                | Err(BollardError::DockerResponseServerError {
                    status_code: 404, ..
                }) => {}
                Err(_) => return Err(SupervisorError::CleanupPending),
            }
            #[cfg(feature = "test-support")]
            process_test_barrier("after_docker_remove_before_cleanup_commit").await;
        }
        self.store
            .complete_container_cleanup(launch.run_id)
            .map_err(|_| SupervisorError::StateUnavailable)?;
        Ok(())
    }

    async fn read_bounded_logs(
        &self,
        launch: &ContainerLaunchRecord,
    ) -> Result<(Vec<u8>, bool), SupervisorError> {
        let options = LogsOptionsBuilder::default()
            .follow(false)
            .stdout(true)
            .stderr(true)
            .tail("all")
            .build();
        let mut stream = self.docker.logs(&launch.container_name, Some(options));
        let max_bytes = self.config.limits().max_output_bytes;
        let mut bounded = Vec::with_capacity(max_bytes.min(8192));
        let mut truncated = false;
        while let Some(item) = stream.next().await {
            match item {
                Ok(LogOutput::StdOut { message }) | Ok(LogOutput::StdErr { message }) => {
                    let remaining = max_bytes.saturating_sub(bounded.len());
                    bounded.extend_from_slice(&message[..message.len().min(remaining)]);
                    truncated |= message.len() > remaining;
                }
                Ok(_) => {}
                Err(_) => return Err(SupervisorError::DockerUnavailable),
            }
        }
        Ok((bounded, truncated))
    }

    async fn read_pi_event_stream(
        &self,
        launch: &ContainerLaunchRecord,
    ) -> Result<Result<crate::pi_events::PiEventSummary, PiStreamError>, SupervisorError> {
        let options = LogsOptionsBuilder::default()
            .follow(false)
            .stdout(true)
            .stderr(true)
            .tail("all")
            .build();
        let mut stream = self.docker.logs(&launch.container_name, Some(options));
        let mut parser = PiEventParser::default();
        let mut observed_bytes = 0_usize;
        while let Some(item) = stream.next().await {
            let message = match item {
                Ok(LogOutput::StdOut { message }) => {
                    let Some(total) = observed_bytes
                        .checked_add(message.len())
                        .filter(|bytes| *bytes <= MAX_PI_DOCKER_LOG_BYTES)
                    else {
                        return Ok(Err(PiStreamError::StreamTooLarge));
                    };
                    observed_bytes = total;
                    parser.push(&message)
                }
                Ok(LogOutput::StdErr { message }) => {
                    let Some(total) = observed_bytes
                        .checked_add(message.len())
                        .filter(|bytes| *bytes <= MAX_PI_DOCKER_LOG_BYTES)
                    else {
                        return Ok(Err(PiStreamError::StreamTooLarge));
                    };
                    observed_bytes = total;
                    continue;
                }
                Ok(_) => continue,
                Err(_) => return Err(SupervisorError::DockerUnavailable),
            };
            if let Err(error) = message {
                return Ok(Err(error));
            }
        }
        Ok(parser.finish(false))
    }

    fn enqueue_heartbeat(&self, launch: &ContainerLaunchRecord) {
        let timestamp_ms = now_ms();
        let callback = CallbackOutboxRecord::new(
            EventId::new(),
            launch.run_id,
            CallbackPayload::Heartbeat {
                worker_id: launch.worker_id.clone(),
                claim_callback_id: launch.claim_callback_id,
                timestamp_ms,
            },
        );
        match self.store.enqueue_callback(callback) {
            Ok(fossil_storage::CallbackEnqueueResult::Enqueued)
            | Ok(fossil_storage::CallbackEnqueueResult::AlreadyPresent) => {
                self.metrics
                    .optional_callbacks_enqueued
                    .fetch_add(1, Ordering::Relaxed);
            }
            Ok(fossil_storage::CallbackEnqueueResult::SuppressedCapacity) => {
                self.metrics
                    .heartbeats_suppressed_capacity
                    .fetch_add(1, Ordering::Relaxed);
                tracing::debug!(
                    outcome = "suppressed_capacity",
                    "optional supervisor heartbeat was not persisted"
                );
            }
            Err(_) => {
                self.metrics
                    .heartbeat_insertion_errors
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    outcome = "storage_error",
                    "optional supervisor heartbeat could not be persisted"
                );
            }
        }
    }

    fn mark_manual_review(&self, run_id: RunId) {
        self.record_ambiguous_state(run_id);
        let _ = self.store.mark_container_manual_review(run_id);
    }

    fn mark_manual_review_checked(&self, run_id: RunId) -> Result<(), SupervisorError> {
        self.record_ambiguous_state(run_id);
        self.store
            .mark_container_manual_review(run_id)
            .map(|_| ())
            .map_err(|_| SupervisorError::StateUnavailable)
    }

    fn recover_container_running(
        &self,
        run_id: RunId,
        started_at_ms: i64,
    ) -> Result<ContainerLaunchRecord, SupervisorError> {
        match self.store.recover_container_running(run_id, started_at_ms) {
            Ok(launch) => Ok(launch),
            Err(_) => {
                self.mark_manual_review(run_id);
                Err(SupervisorError::StateUnavailable)
            }
        }
    }

    fn record_ambiguous_state(&self, run_id: RunId) {
        if self
            .store
            .container_launch(run_id)
            .ok()
            .flatten()
            .is_some_and(|launch| launch.state != ContainerLaunchState::ManualReviewRequired)
        {
            self.metrics
                .ambiguous_container_states
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn load_outcome(&self, run_id: RunId) -> Result<ExecutionOutcome, SupervisorError> {
        let launch = self
            .store
            .container_launch(run_id)
            .map_err(|_| SupervisorError::StateUnavailable)?
            .ok_or(SupervisorError::StateUnavailable)?;
        outcome_from_launch(&launch)
    }
}

#[async_trait]
impl ExecutionSupervisor for DockerSupervisor {
    async fn prepare(
        &self,
        authorization: ExecutionGateAuthorization,
        scenario: SyntheticScenario,
    ) -> Result<PreparedExecution, SupervisorError> {
        if authorization.worker_id() != &self.worker_id {
            return Err(SupervisorError::WrongWorker);
        }
        let fresh = self.refresh_gate(authorization.run_id()).await?;
        if !same_gate(&authorization, &fresh) {
            return Err(SupervisorError::AuthorizationRejected);
        }
        let limits = self.config.limits();
        let profile = ContainerLaunchProfile {
            workload_kind: WorkloadKind::Synthetic,
            workload_version: 1,
            image_identity: self.config.image().to_owned(),
            profile_version: WORKLOAD_VERSION.to_owned(),
            effective_policy_json: self.config.effective_policy_json(),
            runtime_limit_ms: limits.max_runtime.as_millis() as i64,
            repository_url: None,
            authorized_git_ref: None,
            resolved_commit: None,
            workspace_identity: None,
        };
        #[cfg(feature = "test-support")]
        process_test_barrier("before_launch_intent_commit").await;
        let launch = self
            .store
            .begin_container_launch(authorization.run_id(), scenario, profile, fresh)
            .map_err(|_| SupervisorError::AuthorizationRejected)?;
        #[cfg(feature = "test-support")]
        process_test_barrier("after_launch_intent_commit").await;
        Ok(prepared_from_launch(&launch))
    }

    async fn execute(
        &self,
        prepared: PreparedExecution,
    ) -> Result<ExecutionOutcome, SupervisorError> {
        let mut launch = self
            .store
            .container_launch(prepared.run_id)
            .map_err(|_| SupervisorError::StateUnavailable)?
            .ok_or(SupervisorError::AuthorizationRejected)?;
        self.validate_launch_profile(&launch)?;
        if prepared.container_name != launch.container_name
            || prepared.worker_id != launch.worker_id.as_str()
            || prepared.claim_callback_id != launch.claim_callback_id.to_string()
            || prepared.start_callback_id != launch.start_callback_id.to_string()
            || prepared.scenario != launch.scenario
        {
            return Err(SupervisorError::IdentityConflict);
        }
        match launch.state {
            ContainerLaunchState::CleanupPending => {
                self.cleanup(&launch).await?;
                return self.load_outcome(launch.run_id);
            }
            ContainerLaunchState::CleanupCompleted => return outcome_from_launch(&launch),
            ContainerLaunchState::StopRequested => {
                let reason = launch
                    .stop_reason
                    .clone()
                    .ok_or(SupervisorError::StateUnavailable)?;
                return self.stop_and_finalize(launch, &reason).await;
            }
            ContainerLaunchState::ContainerExited => {
                let inspect = self
                    .inspect_named(&launch)
                    .await?
                    .ok_or(SupervisorError::AmbiguousContainerState)?;
                self.finalize_exited_container(launch.clone(), &inspect, None)
                    .await?;
                return self.load_outcome(launch.run_id);
            }
            ContainerLaunchState::ManualReviewRequired => {
                return Err(SupervisorError::AmbiguousContainerState);
            }
            _ => {}
        }
        launch = self.ensure_created(launch).await?;
        launch = self.start_or_reattach(launch).await?;
        match launch.state {
            ContainerLaunchState::ContainerRunning => self.monitor(launch).await,
            ContainerLaunchState::CleanupPending | ContainerLaunchState::CleanupCompleted => {
                self.load_outcome(launch.run_id)
            }
            _ => Err(SupervisorError::AmbiguousContainerState),
        }
    }

    async fn cancel(&self, run_id: RunId) -> Result<CancellationOutcome, SupervisorError> {
        let Some(launch) = self
            .store
            .container_launch(run_id)
            .map_err(|_| SupervisorError::StateUnavailable)?
        else {
            return Ok(CancellationOutcome::NotFound);
        };
        if matches!(
            launch.state,
            ContainerLaunchState::ContainerExited
                | ContainerLaunchState::CleanupPending
                | ContainerLaunchState::CleanupCompleted
        ) {
            return Ok(CancellationOutcome::AlreadyTerminal);
        }
        let canonical = self
            .soyuz
            .get_run(run_id)
            .await
            .map_err(|_| SupervisorError::SoyuzUnavailable)?;
        if canonical.worker_id.as_ref() != Some(&launch.worker_id)
            || canonical.claim_callback_id != Some(launch.claim_callback_id)
        {
            self.store
                .mark_container_stop_requested(run_id, "execution_uncertain")
                .map_err(|_| SupervisorError::StateUnavailable)?;
            return Err(SupervisorError::AuthorizationRejected);
        }
        if !canonical.cancellation_requested()
            && canonical.status != SoyuzRunStatus::CancelRequested
        {
            return Err(SupervisorError::AuthorizationRejected);
        }
        self.store
            .mark_container_stop_requested(run_id, "cancelled")
            .map_err(|_| SupervisorError::StateUnavailable)?;
        Ok(CancellationOutcome::Requested)
    }

    async fn reconcile(&self, run_id: RunId) -> Result<ReconciliationOutcome, SupervisorError> {
        let Some(mut launch) = self
            .store
            .container_launch(run_id)
            .map_err(|_| SupervisorError::StateUnavailable)?
        else {
            return Ok(ReconciliationOutcome::NeverPrepared);
        };
        match launch.state {
            ContainerLaunchState::ManualReviewRequired => {
                return Ok(ReconciliationOutcome::Ambiguous);
            }
            ContainerLaunchState::CleanupPending => {
                self.cleanup(&launch).await?;
                return Ok(ReconciliationOutcome::Terminal);
            }
            ContainerLaunchState::CleanupCompleted => return Ok(ReconciliationOutcome::Terminal),
            ContainerLaunchState::LaunchIntentPersisted => {
                return Ok(ReconciliationOutcome::Prepared);
            }
            ContainerLaunchState::StopRequested => return Ok(ReconciliationOutcome::Prepared),
            _ => {}
        }
        let Some(inspect) = self.inspect_named(&launch).await? else {
            if matches!(
                launch.state,
                ContainerLaunchState::CreateRequested
                    | ContainerLaunchState::ContainerCreated
                    | ContainerLaunchState::StartRequested
                    | ContainerLaunchState::ContainerRunning
                    | ContainerLaunchState::ContainerExited
            ) {
                if launch.updated_at_ms
                    <= now_ms().saturating_sub(CREATE_RESPONSE_GRACE.as_millis() as i64)
                {
                    self.mark_manual_review_checked(run_id)?;
                    return Ok(ReconciliationOutcome::Ambiguous);
                }
                return Ok(ReconciliationOutcome::Prepared);
            }
            return Ok(ReconciliationOutcome::Prepared);
        };
        let id = match self.validate_container_identity(&launch, &inspect) {
            Ok(id) => id,
            Err(_) => {
                self.mark_manual_review_checked(run_id)?;
                return Ok(ReconciliationOutcome::Ambiguous);
            }
        };
        if launch.container_id.is_none() {
            launch = self
                .store
                .record_container_created(run_id, &id)
                .map_err(|_| SupervisorError::StateUnavailable)?;
        }
        self.verify_effective_profile(&launch, &inspect)?;
        match inspect.state.as_ref().and_then(|state| state.status) {
            Some(ContainerStateStatusEnum::RUNNING) => {
                let started_at_ms = inspect
                    .state
                    .as_ref()
                    .and_then(|state| state.started_at.as_deref())
                    .and_then(docker_timestamp_ms)
                    .ok_or(SupervisorError::AmbiguousContainerState)?;
                if launch.state == ContainerLaunchState::StartRequested {
                    self.recover_container_running(run_id, started_at_ms)?;
                } else if launch.state != ContainerLaunchState::ContainerRunning {
                    return Ok(ReconciliationOutcome::Ambiguous);
                }
                Ok(ReconciliationOutcome::Running)
            }
            Some(ContainerStateStatusEnum::EXITED | ContainerStateStatusEnum::DEAD) => {
                self.finalize_exited_container(launch, &inspect, None)
                    .await?;
                Ok(ReconciliationOutcome::Terminal)
            }
            Some(ContainerStateStatusEnum::CREATED) => Ok(ReconciliationOutcome::Prepared),
            _ => Ok(ReconciliationOutcome::Ambiguous),
        }
    }
}

fn prepared_from_launch(launch: &ContainerLaunchRecord) -> PreparedExecution {
    PreparedExecution {
        run_id: launch.run_id,
        worker_id: launch.worker_id.to_string(),
        claim_callback_id: launch.claim_callback_id.to_string(),
        start_callback_id: launch.start_callback_id.to_string(),
        container_name: launch.container_name.clone(),
        scenario: launch.scenario,
    }
}

fn same_gate(expected: &ExecutionGateAuthorization, actual: &ExecutionGateAuthorization) -> bool {
    expected.run_id() == actual.run_id()
        && expected.worker_id() == actual.worker_id()
        && expected.claim_callback_id() == actual.claim_callback_id()
        && expected.start_callback_id() == actual.start_callback_id()
}

fn terminal_callback(
    launch: &ContainerLaunchRecord,
) -> Result<CallbackOutboxRecord, SupervisorError> {
    let event_id = launch
        .terminal_callback_id
        .ok_or(SupervisorError::StateUnavailable)?;
    let completed_at_ms = launch.completed_at_ms.unwrap_or_else(now_ms);
    let exit_code = launch.exit_code;
    let summary = launch
        .diagnostic_summary
        .as_deref()
        .unwrap_or(match launch.workload_kind {
            WorkloadKind::Synthetic => "synthetic execution finalized",
            WorkloadKind::PiCodingV1 => "Pi execution finalized",
        });
    let payload = match launch.outcome_class.as_deref() {
        Some("success" | "pi_completed") => CallbackPayload::Completed {
            worker_id: launch.worker_id.clone(),
            claim_callback_id: launch.claim_callback_id,
            completed_at_ms,
            exit_code: Some(0),
            summary: Some(summary.to_owned()),
            published_url: None,
            changed_file_count: None,
            outcome_summary: None,
        },
        Some("cancelled") => CallbackPayload::Cancelled {
            worker_id: launch.worker_id.clone(),
            claim_callback_id: launch.claim_callback_id,
            completed_at_ms,
            exit_code,
            reason: Some(summary.to_owned()),
            outcome_summary: None,
        },
        Some(failure_class) => CallbackPayload::Failed {
            worker_id: launch.worker_id.clone(),
            claim_callback_id: launch.claim_callback_id,
            completed_at_ms,
            exit_code,
            failure_class: failure_class.to_owned(),
            failure_message: summary.to_owned(),
            outcome_summary: None,
        },
        None => return Err(SupervisorError::StateUnavailable),
    };
    Ok(CallbackOutboxRecord::new(event_id, launch.run_id, payload))
}

fn outcome_from_launch(
    launch: &ContainerLaunchRecord,
) -> Result<ExecutionOutcome, SupervisorError> {
    let reason = match launch.outcome_class.as_deref() {
        Some("success" | "pi_completed") => TerminalReason::Completed,
        Some("nonzero_exit") => TerminalReason::NonzeroExit,
        Some("timeout") => TerminalReason::TimedOut,
        Some("cancelled") => TerminalReason::Cancelled,
        Some("unexpected_termination") => TerminalReason::UnexpectedTermination,
        Some("resource_limit_failure") => TerminalReason::ResourceLimitFailure,
        Some(
            "provider_error"
            | "pi_process_failure"
            | "invalid_event_stream"
            | "output_truncated"
            | "execution_uncertain",
        ) => TerminalReason::ExecutionUncertain,
        Some("docker_failure") => TerminalReason::DockerFailure,
        _ => return Err(SupervisorError::StateUnavailable),
    };
    Ok(ExecutionOutcome {
        run_id: launch.run_id,
        exit_code: launch.exit_code,
        reason,
        diagnostic_summary: launch
            .diagnostic_summary
            .clone()
            .unwrap_or_else(|| "synthetic execution finalized".to_owned()),
        output_truncated: launch.output_truncated,
        workload_id: match launch.workload_kind {
            WorkloadKind::Synthetic => WORKLOAD_VERSION.to_owned(),
            WorkloadKind::PiCodingV1 => "pi-coding-v1".to_owned(),
        },
        started_at: launch.started_at_ms.and_then(format_timestamp_ms),
        completed_at: launch.completed_at_ms.and_then(format_timestamp_ms),
        elapsed_ms: launch
            .started_at_ms
            .zip(launch.completed_at_ms)
            .map(|(started, completed)| completed.saturating_sub(started) as u64),
    })
}

fn safe_worker_environment(env: Option<&[String]>, scenario: SyntheticScenario) -> bool {
    let Some(env) = env else {
        return false;
    };
    env.len() == 2
        && env
            .iter()
            .any(|value| value == &format!("FOSSIL_SCENARIO={}", scenario.as_str()))
        && env.iter().any(|value| value == "PATH=/")
}

fn docker_timestamp_ms(value: &str) -> Option<i64> {
    OffsetDateTime::parse(value, &Rfc3339)
        .ok()
        .and_then(|timestamp| unix_nanos_to_millis(timestamp.unix_timestamp_nanos()))
}

fn unix_nanos_to_millis(value: i128) -> Option<i64> {
    i64::try_from(value / 1_000_000).ok()
}

fn format_timestamp_ms(value: i64) -> Option<String> {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(value) * 1_000_000)
        .ok()
        .and_then(|timestamp| timestamp.format(&Rfc3339).ok())
}

fn now_ms() -> i64 {
    unix_nanos_to_millis(OffsetDateTime::now_utc().unix_timestamp_nanos())
        .expect("current Unix millisecond timestamp fits in i64")
}

/// Test-only deterministic process barrier for crash-boundary integration
/// tests. Production builds contain no environment-controlled pause points.
#[cfg(feature = "test-support")]
async fn process_test_barrier(checkpoint: &str) {
    if std::env::var("FOSSIL_SUPERVISOR_TEST_BARRIER").as_deref() != Ok(checkpoint) {
        return;
    }
    let Some(marker) = std::env::var_os("FOSSIL_SUPERVISOR_TEST_BARRIER_MARKER") else {
        return;
    };
    if std::fs::write(marker, checkpoint).is_ok() {
        std::future::pending::<()>().await;
    }
}

fn supports_required_resource_controls(info: &SystemInfo) -> bool {
    info.os_type.as_deref() == Some("linux")
        && info.memory_limit == Some(true)
        && info.cpu_cfs_period == Some(true)
        && info.cpu_cfs_quota == Some(true)
        && info.pids_limit == Some(true)
        && info.security_options.as_ref().is_some_and(|options| {
            options
                .iter()
                .any(|option| option == "seccomp" || option.starts_with("name=seccomp"))
                && !options.iter().any(|option| {
                    option.contains("profile=unconfined") || option.contains("seccomp=unconfined")
                })
        })
}

#[cfg(test)]
mod tests {
    use super::{
        safe_worker_environment, supports_required_resource_controls, unix_nanos_to_millis,
    };
    use bollard::models::SystemInfo;
    use fossil_core::execution::SyntheticScenario;

    fn docker_info(os_type: &str, memory: bool, cpu: bool, pids: bool) -> SystemInfo {
        serde_json::from_value(serde_json::json!({
            "OSType": os_type,
            "MemoryLimit": memory,
            "CpuCfsPeriod": cpu,
            "CpuCfsQuota": cpu,
            "PidsLimit": pids,
            "SecurityOptions": ["name=seccomp,profile=builtin"]
        }))
        .unwrap()
    }

    #[test]
    fn docker_connection_requires_linux_and_enforced_resource_features() {
        assert!(supports_required_resource_controls(&docker_info(
            "linux", true, true, true
        )));
        assert!(!supports_required_resource_controls(&docker_info(
            "linux", false, true, true
        )));
        assert!(!supports_required_resource_controls(&docker_info(
            "linux", true, false, true
        )));
        assert!(!supports_required_resource_controls(&docker_info(
            "linux", true, true, false
        )));
        assert!(!supports_required_resource_controls(&docker_info(
            "windows", true, true, true
        )));
    }

    #[test]
    fn docker_connection_rejects_an_unconfined_default_seccomp_profile() {
        let info: SystemInfo = serde_json::from_value(serde_json::json!({
            "OSType": "linux",
            "MemoryLimit": true,
            "CpuCfsPeriod": true,
            "CpuCfsQuota": true,
            "PidsLimit": true,
            "SecurityOptions": ["name=seccomp,profile=unconfined"]
        }))
        .unwrap();

        assert!(!supports_required_resource_controls(&info));
    }

    #[test]
    fn unix_nanoseconds_convert_to_milliseconds_before_range_check() {
        assert_eq!(
            unix_nanos_to_millis(1_760_000_000_123_456_789),
            Some(1_760_000_000_123)
        );
        assert_eq!(
            unix_nanos_to_millis((i128::from(i64::MAX) + 1) * 1_000_000),
            None
        );
    }

    #[test]
    fn worker_environment_is_limited_to_scenario_and_minimal_path() {
        let allowed = vec!["PATH=/".to_owned(), "FOSSIL_SCENARIO=success".to_owned()];
        assert!(safe_worker_environment(
            Some(&allowed),
            SyntheticScenario::Success
        ));
        let default_path = vec![
            "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin".to_owned(),
            "FOSSIL_SCENARIO=success".to_owned(),
        ];
        assert!(!safe_worker_environment(
            Some(&default_path),
            SyntheticScenario::Success
        ));
        let with_extra = vec![
            "PATH=/".to_owned(),
            "FOSSIL_SCENARIO=success".to_owned(),
            "LD_PRELOAD=/tmp/evil.so".to_owned(),
        ];
        assert!(!safe_worker_environment(
            Some(&with_extra),
            SyntheticScenario::Success
        ));
    }
}
