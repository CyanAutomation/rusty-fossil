use fossil_core::contract::{ContractVersion, OperationalEventType};
use fossil_core::ids::{ClaimCallbackId, EventId, RunId, WorkerId};
use fossil_core::lifecycle::{CallbackDeliveryState, LifecycleError, LocalLifecycleState};
use fossil_core::queue::{PublishMode, QueueEnvelope, SoyuzRunRequest, TaskMode};
use fossil_soyuz::{
    CallbackDeliveryAuthorization, ClaimAuthorization, ExecutionGateAuthorization,
    StartAuthorizationSource, StartedAuthorization,
    valid_outcome_summary as valid_soyuz_outcome_summary,
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use url::Url;

const SCHEMA_VERSION: u32 = 7;
const MAXIMUM_RETENTION_MS: i64 = 90 * 24 * 60 * 60 * 1_000;
const MAX_CALLBACK_PAYLOAD_BYTES: usize = 12 * 1024;
const MAX_PENDING_CALLBACKS: u32 = 1_000;
const MAX_EXECUTION_DESCRIPTOR_BYTES: usize = 96 * 1024;
const TRUSTED_PI_WORKER_IMAGE: &str = "ghcr.io/cyanautomation/rusty-fossil-pi-worker";
// Single Phase 2C-A pilot pin designated by the user and observed from GitHub
// metadata on 2026-11-26. Repository preparation must still resolve and verify
// this exact revision before checkout; this is not an egress or checkout guard.
const PI_PILOT_REPOSITORY_URL: &str = "https://github.com/CyanAutomation/somatic";
const PI_PILOT_GIT_REF: &str = "main";
const PI_PILOT_RESOLVED_COMMIT: &str = "0d02873b71256892b4ec8026a3b0e2a5ab6aaa91";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExecutionDescriptor {
    pub contract_version: ContractVersion,
    pub run_id: RunId,
    pub created_at: String,
    pub correlation_id: String,
    pub request_id: String,
    pub request: SoyuzRunRequest,
}

impl From<&QueueEnvelope> for ExecutionDescriptor {
    fn from(envelope: &QueueEnvelope) -> Self {
        Self {
            contract_version: envelope.contract_version,
            run_id: envelope.run_id,
            created_at: envelope.created_at.clone(),
            correlation_id: envelope.correlation_id.clone(),
            request_id: envelope.request_id.clone(),
            request: envelope.request.clone(),
        }
    }
}

impl ExecutionDescriptor {
    fn validate(&self) -> bool {
        QueueEnvelope {
            contract_version: self.contract_version,
            run_id: self.run_id,
            created_at: self.created_at.clone(),
            correlation_id: self.correlation_id.clone(),
            request_id: self.request_id.clone(),
            request: self.request.clone(),
        }
        .validate()
        .is_ok()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryAction {
    None,
    ReconcileWithSoyuz,
    AwaitingSoyuz,
    ClaimResultUncertain,
    StartResultUncertain,
    Recovered,
    OwnedElsewhere,
    TerminalReconciled,
    AmbiguousExecution,
    RetryCallback,
    ManualReview,
}

/// Durable Docker lifecycle for one canonical run and one persisted start
/// authorization. The run record remains the canonical local workflow state;
/// this value records Docker side effects that must be reconciled after a
/// process or daemon interruption.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContainerLaunchState {
    LaunchIntentPersisted,
    CreateRequested,
    ContainerCreated,
    StartRequested,
    ContainerRunning,
    StopRequested,
    ContainerExited,
    CleanupPending,
    CleanupCompleted,
    ManualReviewRequired,
}

impl ContainerLaunchState {
    fn as_str(self) -> &'static str {
        match self {
            Self::LaunchIntentPersisted => "launch_intent_persisted",
            Self::CreateRequested => "create_requested",
            Self::ContainerCreated => "container_created",
            Self::StartRequested => "start_requested",
            Self::ContainerRunning => "container_running",
            Self::StopRequested => "stop_requested",
            Self::ContainerExited => "container_exited",
            Self::CleanupPending => "cleanup_pending",
            Self::CleanupCompleted => "cleanup_completed",
            Self::ManualReviewRequired => "manual_review_required",
        }
    }

    fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "launch_intent_persisted" => Ok(Self::LaunchIntentPersisted),
            "create_requested" => Ok(Self::CreateRequested),
            "container_created" => Ok(Self::ContainerCreated),
            "start_requested" => Ok(Self::StartRequested),
            "container_running" => Ok(Self::ContainerRunning),
            "stop_requested" => Ok(Self::StopRequested),
            "container_exited" => Ok(Self::ContainerExited),
            "cleanup_pending" => Ok(Self::CleanupPending),
            "cleanup_completed" => Ok(Self::CleanupCompleted),
            "manual_review_required" => Ok(Self::ManualReviewRequired),
            _ => Err(StoreError::InvalidStoredValue("container launch state")),
        }
    }

    fn can_advance_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::LaunchIntentPersisted, Self::CreateRequested)
                | (Self::LaunchIntentPersisted, Self::StopRequested)
                | (Self::LaunchIntentPersisted, Self::ManualReviewRequired)
                | (Self::CreateRequested, Self::ContainerCreated)
                | (Self::CreateRequested, Self::ManualReviewRequired)
                | (Self::ContainerCreated, Self::StartRequested)
                | (Self::ContainerCreated, Self::StopRequested)
                | (Self::ContainerCreated, Self::ManualReviewRequired)
                | (Self::StartRequested, Self::ContainerRunning)
                | (Self::StartRequested, Self::StopRequested)
                | (Self::StartRequested, Self::ContainerExited)
                | (Self::StartRequested, Self::ManualReviewRequired)
                | (Self::ContainerRunning, Self::StopRequested)
                | (Self::ContainerRunning, Self::ContainerExited)
                | (Self::ContainerRunning, Self::ManualReviewRequired)
                | (Self::StopRequested, Self::ContainerExited)
                | (Self::StopRequested, Self::ManualReviewRequired)
                | (Self::ContainerExited, Self::CleanupPending)
                | (Self::ContainerExited, Self::ManualReviewRequired)
                | (Self::CleanupPending, Self::CleanupCompleted)
                | (Self::CleanupPending, Self::ManualReviewRequired)
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerLaunchRecord {
    pub run_id: RunId,
    pub worker_id: WorkerId,
    pub claim_callback_id: ClaimCallbackId,
    pub start_callback_id: EventId,
    pub workload_kind: WorkloadKind,
    pub workload_version: u32,
    pub scenario: fossil_core::execution::SyntheticScenario,
    pub image_identity: Option<String>,
    pub profile_version: String,
    pub effective_policy_json: String,
    /// Exact repository URL and ref from the authorized descriptor. These are
    /// populated only for Pi coding launches.
    pub repository_url: Option<String>,
    pub authorized_git_ref: Option<String>,
    pub resolved_commit: Option<String>,
    /// Opaque per-run workspace identity, never a host path.
    pub workspace_identity: Option<String>,
    pub runtime_limit_ms: Option<i64>,
    pub effective_deadline_at_ms: Option<i64>,
    pub container_name: String,
    pub container_id: Option<String>,
    pub state: ContainerLaunchState,
    pub exit_code: Option<i32>,
    pub outcome_class: Option<String>,
    pub diagnostic_summary: Option<String>,
    pub output_truncated: bool,
    pub terminal_callback_id: Option<EventId>,
    pub stop_reason: Option<String>,
    pub stop_requested_at_ms: Option<i64>,
    pub started_at_ms: Option<i64>,
    pub completed_at_ms: Option<i64>,
    pub updated_at_ms: i64,
}

/// Workload types that the supervisor currently knows how to launch. New
/// workload kinds must be added explicitly; unknown persisted kinds fail
/// closed during recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkloadKind {
    Synthetic,
    PiCodingV1,
}

impl WorkloadKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Synthetic => "synthetic",
            Self::PiCodingV1 => "pi_coding_v1",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerLaunchProfile {
    pub workload_kind: WorkloadKind,
    pub workload_version: u32,
    pub image_identity: String,
    pub profile_version: String,
    pub effective_policy_json: String,
    pub runtime_limit_ms: i64,
    pub repository_url: Option<String>,
    pub authorized_git_ref: Option<String>,
    pub resolved_commit: Option<String>,
    pub workspace_identity: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContainerExitTimes {
    pub started_at_ms: Option<i64>,
    pub completed_at_ms: i64,
}

impl RecoveryAction {
    fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::ReconcileWithSoyuz => "reconcile_with_soyuz",
            Self::AwaitingSoyuz => "awaiting_soyuz",
            Self::ClaimResultUncertain => "claim_result_uncertain",
            Self::StartResultUncertain => "start_result_uncertain",
            Self::Recovered => "recovered",
            Self::OwnedElsewhere => "owned_elsewhere",
            Self::TerminalReconciled => "terminal_reconciled",
            Self::AmbiguousExecution => "ambiguous_execution",
            Self::RetryCallback => "retry_callback",
            Self::ManualReview => "manual_review",
        }
    }

    fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "none" => Ok(Self::None),
            "reconcile_with_soyuz" => Ok(Self::ReconcileWithSoyuz),
            "awaiting_soyuz" => Ok(Self::AwaitingSoyuz),
            "claim_result_uncertain" => Ok(Self::ClaimResultUncertain),
            "start_result_uncertain" => Ok(Self::StartResultUncertain),
            "recovered" => Ok(Self::Recovered),
            "owned_elsewhere" => Ok(Self::OwnedElsewhere),
            "terminal_reconciled" => Ok(Self::TerminalReconciled),
            "ambiguous_execution" => Ok(Self::AmbiguousExecution),
            "retry_callback" => Ok(Self::RetryCallback),
            "manual_review" => Ok(Self::ManualReview),
            _ => Err(StoreError::InvalidStoredValue("recovery action")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CallbackKind {
    Event,
    Heartbeat,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CallbackPayload {
    Event {
        event_type: OperationalEventType,
        worker_id: WorkerId,
        claim_callback_id: ClaimCallbackId,
        #[serde(skip_serializing_if = "Option::is_none")]
        stage: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        step: Option<String>,
        timestamp_ms: i64,
        #[serde(default)]
        payload: serde_json::Value,
    },
    Heartbeat {
        worker_id: WorkerId,
        claim_callback_id: ClaimCallbackId,
        timestamp_ms: i64,
    },
    Completed {
        worker_id: WorkerId,
        claim_callback_id: ClaimCallbackId,
        completed_at_ms: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        exit_code: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        summary: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        published_url: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        changed_file_count: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        outcome_summary: Option<serde_json::Value>,
    },
    Failed {
        worker_id: WorkerId,
        claim_callback_id: ClaimCallbackId,
        completed_at_ms: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        exit_code: Option<i32>,
        failure_class: String,
        failure_message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        outcome_summary: Option<serde_json::Value>,
    },
    Cancelled {
        worker_id: WorkerId,
        claim_callback_id: ClaimCallbackId,
        completed_at_ms: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        exit_code: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        outcome_summary: Option<serde_json::Value>,
    },
}

impl CallbackPayload {
    fn kind(&self) -> CallbackKind {
        match self {
            Self::Event { .. } => CallbackKind::Event,
            Self::Heartbeat { .. } => CallbackKind::Heartbeat,
            Self::Completed { .. } => CallbackKind::Completed,
            Self::Failed { .. } => CallbackKind::Failed,
            Self::Cancelled { .. } => CallbackKind::Cancelled,
        }
    }

    fn is_optional(&self) -> bool {
        matches!(self, Self::Event { .. } | Self::Heartbeat { .. })
    }

    fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed { .. } | Self::Failed { .. } | Self::Cancelled { .. }
        )
    }

    fn worker_id(&self) -> &WorkerId {
        match self {
            Self::Event { worker_id, .. }
            | Self::Heartbeat { worker_id, .. }
            | Self::Completed { worker_id, .. }
            | Self::Failed { worker_id, .. }
            | Self::Cancelled { worker_id, .. } => worker_id,
        }
    }

    fn claim_callback_id(&self) -> ClaimCallbackId {
        match self {
            Self::Event {
                claim_callback_id, ..
            }
            | Self::Heartbeat {
                claim_callback_id, ..
            }
            | Self::Completed {
                claim_callback_id, ..
            }
            | Self::Failed {
                claim_callback_id, ..
            }
            | Self::Cancelled {
                claim_callback_id, ..
            } => *claim_callback_id,
        }
    }

    fn validate(&self) -> Result<(), StoreError> {
        match self {
            Self::Event {
                event_type,
                stage,
                step,
                timestamp_ms,
                payload,
                ..
            } => {
                if *timestamp_ms < 0
                    || stage
                        .as_deref()
                        .is_some_and(|value| !bounded_text(value, 1, 120))
                    || step
                        .as_deref()
                        .is_some_and(|value| !bounded_text(value, 1, 160))
                    || (*event_type == OperationalEventType::StageChanged && stage.is_none())
                    || (*event_type == OperationalEventType::StepChanged && step.is_none())
                    || json_too_large(payload, MAX_CALLBACK_PAYLOAD_BYTES)
                {
                    return Err(StoreError::InvalidCallbackPayload);
                }
            }
            Self::Heartbeat { timestamp_ms, .. } => {
                if *timestamp_ms < 0 {
                    return Err(StoreError::InvalidCallbackPayload);
                }
            }
            Self::Completed {
                completed_at_ms,
                exit_code,
                summary,
                published_url,
                changed_file_count,
                outcome_summary,
                ..
            } => {
                if *completed_at_ms < 0
                    || exit_code.is_some_and(|value| value != 0)
                    || summary.as_deref().is_some_and(|value| value.len() > 2_000)
                    || published_url.as_deref().is_some_and(|value| {
                        value.len() > 2_048
                            || Url::parse(value)
                                .map_or(true, |url| !matches!(url.scheme(), "https" | "http"))
                    })
                    || changed_file_count.is_some_and(|value| value > 100_000)
                    || !valid_outcome_summary(outcome_summary.as_ref())
                {
                    return Err(StoreError::InvalidCallbackPayload);
                }
            }
            Self::Failed {
                completed_at_ms,
                failure_class,
                failure_message,
                outcome_summary,
                ..
            } => {
                if *completed_at_ms < 0
                    || !valid_failure_class(failure_class)
                    || !bounded_text(failure_message, 1, 4_000)
                    || !valid_outcome_summary(outcome_summary.as_ref())
                {
                    return Err(StoreError::InvalidCallbackPayload);
                }
            }
            Self::Cancelled {
                completed_at_ms,
                reason,
                outcome_summary,
                ..
            } => {
                if *completed_at_ms < 0
                    || reason.as_deref().is_some_and(|value| value.len() > 2_000)
                    || !valid_outcome_summary(outcome_summary.as_ref())
                {
                    return Err(StoreError::InvalidCallbackPayload);
                }
            }
        }
        Ok(())
    }
}

fn json_too_large(value: &serde_json::Value, max_bytes: usize) -> bool {
    serde_json::to_vec(value).map_or(true, |bytes| bytes.len() > max_bytes)
}

fn bounded_text(value: &str, min_bytes: usize, max_bytes: usize) -> bool {
    (min_bytes..=max_bytes).contains(&value.len()) && !value.chars().any(char::is_control)
}

fn valid_failure_class(value: &str) -> bool {
    bounded_text(value, 1, 120)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

fn valid_outcome_summary(value: Option<&serde_json::Value>) -> bool {
    value.is_none_or(valid_soyuz_outcome_summary)
}

fn callback_matches_container_result(
    callback: &CallbackOutboxRecord,
    launch: &ContainerLaunchRecord,
) -> bool {
    if callback.run_id != launch.run_id
        || callback.payload.worker_id() != &launch.worker_id
        || callback.payload.claim_callback_id() != launch.claim_callback_id
    {
        return false;
    }
    match (&callback.payload, launch.outcome_class.as_deref()) {
        (CallbackPayload::Completed { exit_code, .. }, Some("success" | "pi_completed")) => {
            *exit_code == Some(0) && launch.exit_code == Some(0)
        }
        (CallbackPayload::Cancelled { exit_code, .. }, Some("cancelled")) => {
            *exit_code == launch.exit_code
        }
        (
            CallbackPayload::Failed { exit_code, .. },
            Some(
                "nonzero_exit"
                | "provider_error"
                | "pi_process_failure"
                | "invalid_event_stream"
                | "output_truncated"
                | "timeout"
                | "docker_failure"
                | "resource_limit_failure"
                | "unexpected_termination"
                | "execution_uncertain",
            ),
        ) => *exit_code == launch.exit_code,
        _ => false,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallbackOutboxRecord {
    pub event_id: EventId,
    pub run_id: RunId,
    pub payload: CallbackPayload,
    pub delivery_state: CallbackDeliveryState,
    pub attempts: u32,
    pub next_eligible_at_ms: i64,
    pub failure_class: Option<String>,
}

/// Result of an outbox enqueue. Optional operational events can be suppressed
/// at the storage boundary while terminal outcomes retain the reserved slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallbackEnqueueResult {
    Enqueued,
    AlreadyPresent,
    SuppressedCapacity,
}

impl CallbackOutboxRecord {
    pub fn new(event_id: EventId, run_id: RunId, payload: CallbackPayload) -> Self {
        Self {
            event_id,
            run_id,
            payload,
            delivery_state: CallbackDeliveryState::Pending,
            attempts: 0,
            next_eligible_at_ms: 0,
            failure_class: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRecord {
    pub run_id: RunId,
    pub worker_id: WorkerId,
    pub lifecycle_state: LocalLifecycleState,
    pub claim_callback_id: Option<ClaimCallbackId>,
    pub start_callback_id: Option<EventId>,
    pub start_authorization_source: Option<StartAuthorizationSource>,
    pub start_authorized: bool,
    pub callback_delivery: CallbackDeliveryState,
    pub recovery_attempts: u32,
    pub recovery_action: RecoveryAction,
    pub created_at_ms: i64,
    pub expires_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CallbackOutboxStatus {
    pub pending: u64,
    pub blocked: u64,
    pub oldest_pending_age_ms: Option<u64>,
    pub intake_backpressured: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoveryDiagnostics {
    pub unresolved_runs: u64,
    pub attempts: u64,
    pub recovered_runs: u64,
    pub awaiting_soyuz_runs: u64,
    pub owned_elsewhere: u64,
    pub manual_review_runs: u64,
    pub ambiguous_execution_runs: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetentionSweepSummary {
    pub expired_runs_removed: u64,
    pub expired_run_content_removed: u64,
    pub expired_runs_fenced_for_cleanup: u64,
}

struct RecoveryDiagnosticRow {
    unresolved: Option<i64>,
    attempts: Option<i64>,
    recovered: Option<i64>,
    awaiting_soyuz: Option<i64>,
    owned_elsewhere: Option<i64>,
    manual_review: Option<i64>,
    ambiguous: Option<i64>,
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("SQLite operation failed: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("store path operation failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("SQLite state directory permissions are not private")]
    InsecureStateDirectory,
    #[error("SQLite state database path must not be a symbolic link")]
    SymlinkStateDatabase,
    #[error("stored record encoding failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("database schema version {0} is not supported")]
    UnsupportedSchemaVersion(u32),
    #[error("run record was not found")]
    RunNotFound,
    #[error("run identity already exists for a different worker")]
    WorkerIdentityConflict,
    #[error("claim callback fencing identity cannot be replaced")]
    ClaimFenceConflict,
    #[error("claim authorization can only be persisted with a successful Soyuz claim proof")]
    ClaimAuthorizationProofRequired,
    #[error("Soyuz claim authorization does not match the persisted run, worker, or claim")]
    ClaimAuthorizationMismatch,
    #[error(
        "start authorization can only be persisted with a successful Soyuz authorization proof"
    )]
    AuthorizationProofRequired,
    #[error("Soyuz start authorization does not match the persisted run, worker, or claim")]
    StartAuthorizationMismatch,
    #[error("start callback ID cannot be replaced after its intent is persisted")]
    StartCallbackConflict,
    #[error("execution descriptor is invalid, missing, or corrupt")]
    InvalidExecutionDescriptor,
    #[error("execution descriptor already exists with different content")]
    ExecutionDescriptorConflict,
    #[error("execution slot is occupied or another run has unresolved recovery state")]
    ExecutionSlotOccupied,
    #[error("execution slot can only be reserved for a durably authorized run")]
    ExecutionSlotNotAuthorized,
    #[error("execution can only cross the local gate with a fresh Soyuz canonical-state proof")]
    ExecutionGateProofRequired,
    #[error("execution slot can only be released after terminal completion")]
    ExecutionSlotReleaseDenied,
    #[error("Docker launch intent requires the matching fresh execution-gate proof")]
    ContainerLaunchAuthorizationMismatch,
    #[error("Docker launch identity conflicts with an existing attempt")]
    ContainerLaunchIdentityConflict,
    #[error("Docker launch state does not permit this operation")]
    ContainerLaunchTransitionDenied,
    #[error("Docker container identity conflicts with the persisted attempt")]
    ContainerIdentityConflict,
    #[error("Docker execution result or terminal callback does not match the persisted result")]
    ContainerLaunchResultMismatch,
    #[error("Docker execution result is invalid or too large")]
    InvalidContainerLaunchResult,
    #[error("execution descriptor can only be removed after terminal callbacks are delivered")]
    ExecutionDescriptorRetentionDenied,
    #[error("callback event ID is already bound to different callback data")]
    CallbackIdConflict,
    #[error("callback worker or claim fence does not match the durable run record")]
    CallbackAuthorizationMismatch,
    #[error("callback outbox reached its configured capacity")]
    CallbackOutboxCapacityExceeded,
    #[error("intake is paused to reserve mandatory callback capacity")]
    OutboxBackpressure,
    #[error("callback is not pending or is not yet eligible for delivery")]
    CallbackNotEligible,
    #[error("callback payload exceeds 12 KiB or contains invalid data")]
    InvalidCallbackPayload,
    #[error("lifecycle transition was rejected: {0}")]
    Lifecycle(#[from] LifecycleError),
    #[error("SQLite store lock was poisoned")]
    LockPoisoned,
    #[error("stored value for {0} is invalid")]
    InvalidStoredValue(&'static str),
}

#[derive(Clone)]
pub struct SqliteStore {
    connection: Arc<Mutex<Connection>>,
}

struct StoredRunRow {
    run_id: String,
    worker_id: String,
    claim_callback_id: Option<String>,
    start_callback_id: Option<String>,
    start_authorization_source: Option<String>,
    start_authorized: bool,
    callback_delivery: String,
    recovery_attempts: u32,
    recovery_action: String,
    created_at_ms: i64,
    expires_at_ms: i64,
    updated_at_ms: i64,
}

impl SqliteStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            let parent_existed = parent.exists();
            std::fs::create_dir_all(parent)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if !parent_existed {
                    std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
                }
                let mode = std::fs::metadata(parent)?.permissions().mode();
                if mode & 0o077 != 0 {
                    return Err(StoreError::InsecureStateDirectory);
                }
            }
        }
        if std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
            return Err(StoreError::SymlinkStateDatabase);
        }
        let mut connection = Connection::open(path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        }
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "secure_delete", "ON")?;
        Self::migrate(&mut connection)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    fn migrate(connection: &mut Connection) -> Result<(), StoreError> {
        let version: u32 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if version > SCHEMA_VERSION {
            return Err(StoreError::UnsupportedSchemaVersion(version));
        }
        if version == SCHEMA_VERSION {
            return Ok(());
        }

        if version == 1 {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            transaction.execute_batch(
                "CREATE TABLE execution_handoffs (
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

                INSERT INTO schema_migrations(version, applied_at_ms) VALUES (2, 0);
                PRAGMA user_version = 2;",
            )?;
            transaction.commit()?;
            return Self::migrate(connection);
        }

        if version == 2 {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            transaction.execute_batch(
                "ALTER TABLE callback_outbox RENAME TO callback_outbox_v2;

                CREATE TABLE callback_outbox (
                    event_id TEXT PRIMARY KEY NOT NULL,
                    run_id TEXT NOT NULL REFERENCES run_records(run_id) ON DELETE CASCADE,
                    callback_kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL CHECK (length(payload_json) <= 12288),
                    delivery_state TEXT NOT NULL CHECK (delivery_state IN ('pending', 'delivered', 'blocked')),
                    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
                    next_eligible_at_ms INTEGER NOT NULL DEFAULT 0,
                    failure_class TEXT,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL
                );

                INSERT INTO callback_outbox (
                    event_id, run_id, callback_kind, payload_json, delivery_state,
                    attempts, next_eligible_at_ms, failure_class, created_at_ms, updated_at_ms
                )
                SELECT event_id, run_id, callback_kind, payload_json, delivery_state,
                       attempts, 0, NULL, created_at_ms, updated_at_ms
                FROM callback_outbox_v2;

                DROP TABLE callback_outbox_v2;
                CREATE INDEX callback_outbox_pending_idx
                    ON callback_outbox(delivery_state, next_eligible_at_ms, created_at_ms);
                INSERT INTO schema_migrations(version, applied_at_ms) VALUES (3, 0);
                PRAGMA user_version = 3;",
            )?;
            transaction.commit()?;
            return Self::migrate(connection);
        }

        if version == 3 {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            transaction.execute_batch(
                "CREATE TABLE container_launches (
                    run_id TEXT PRIMARY KEY NOT NULL REFERENCES run_records(run_id) ON DELETE CASCADE,
                    worker_id TEXT NOT NULL,
                    claim_callback_id TEXT NOT NULL,
                    start_callback_id TEXT NOT NULL UNIQUE,
                    scenario TEXT NOT NULL CHECK (scenario IN (
                        'success', 'controlled_failure', 'timeout', 'cancellation',
                        'bounded_output', 'unexpected_termination'
                    )),
                    container_name TEXT NOT NULL UNIQUE,
                    container_id TEXT UNIQUE,
                    state TEXT NOT NULL CHECK (state IN (
                        'launch_intent_persisted', 'create_requested', 'container_created',
                        'start_requested', 'container_running', 'stop_requested',
                        'container_exited', 'cleanup_pending', 'cleanup_completed',
                        'manual_review_required'
                    )),
                    exit_code INTEGER,
                    outcome_class TEXT,
                    diagnostic_summary TEXT,
                    output_truncated INTEGER NOT NULL DEFAULT 0 CHECK (output_truncated IN (0, 1)),
                    terminal_callback_id TEXT UNIQUE,
                    stop_reason TEXT CHECK (stop_reason IN ('cancelled', 'timeout', 'execution_uncertain') OR stop_reason IS NULL),
                    stop_requested_at_ms INTEGER,
                    started_at_ms INTEGER,
                    completed_at_ms INTEGER,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL
                );
                CREATE INDEX container_launches_state_idx ON container_launches(state);
                INSERT INTO schema_migrations(version, applied_at_ms) VALUES (4, 0);
                PRAGMA user_version = 4;",
            )?;
            transaction.commit()?;
            return Self::migrate(connection);
        }

        if version == 4 {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            transaction.execute_batch(
                "ALTER TABLE container_launches
                    ADD COLUMN workload_kind TEXT NOT NULL DEFAULT 'synthetic';
                ALTER TABLE container_launches
                    ADD COLUMN workload_version INTEGER NOT NULL DEFAULT 1;
                ALTER TABLE container_launches ADD COLUMN image_identity TEXT;
                ALTER TABLE container_launches
                    ADD COLUMN profile_version TEXT NOT NULL DEFAULT 'synthetic-v1';
                ALTER TABLE container_launches
                    ADD COLUMN effective_policy_json TEXT NOT NULL DEFAULT '{}'
                        CHECK (length(effective_policy_json) <= 4096);
                ALTER TABLE container_launches ADD COLUMN runtime_limit_ms INTEGER;
                ALTER TABLE container_launches ADD COLUMN effective_deadline_at_ms INTEGER;

                INSERT INTO schema_migrations(version, applied_at_ms) VALUES (5, 0);
                PRAGMA user_version = 5;",
            )?;
            transaction.commit()?;
            return Self::migrate(connection);
        }

        if version == 5 {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            transaction.execute_batch(
                "ALTER TABLE run_records ADD COLUMN expires_at_ms INTEGER NOT NULL DEFAULT 0;
                UPDATE run_records SET expires_at_ms = COALESCE(
                    (SELECT CAST(strftime('%s', json_extract(h.descriptor_json, '$.createdAt')) AS INTEGER) * 1000
                        + CAST(substr(json_extract(h.descriptor_json, '$.createdAt'), 21, 3) AS INTEGER)
                        + 7776000000
                     FROM execution_handoffs h WHERE h.run_id = run_records.run_id),
                    created_at_ms + 7776000000
                );
                CREATE INDEX run_records_expiration_idx ON run_records(expires_at_ms, run_id);
                INSERT INTO schema_migrations(version, applied_at_ms) VALUES (6, 0);
                PRAGMA user_version = 6;",
            )?;
            transaction.commit()?;
            return Self::migrate(connection);
        }

        if version == 6 {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            transaction.execute_batch(
                "ALTER TABLE container_launches ADD COLUMN repository_url TEXT;
                ALTER TABLE container_launches ADD COLUMN authorized_git_ref TEXT;
                ALTER TABLE container_launches ADD COLUMN resolved_commit TEXT;
                ALTER TABLE container_launches ADD COLUMN workspace_identity TEXT;
                CREATE UNIQUE INDEX container_launches_workspace_identity_idx
                    ON container_launches(workspace_identity)
                    WHERE workspace_identity IS NOT NULL;

                INSERT INTO schema_migrations(version, applied_at_ms) VALUES (7, 0);
                PRAGMA user_version = 7;",
            )?;
            transaction.commit()?;
            return Ok(());
        }

        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute_batch(
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
                start_authorized INTEGER NOT NULL DEFAULT 0 CHECK (start_authorized IN (0, 1)),
                callback_delivery TEXT NOT NULL DEFAULT 'not_required',
                recovery_attempts INTEGER NOT NULL DEFAULT 0 CHECK (recovery_attempts >= 0),
                recovery_action TEXT NOT NULL DEFAULT 'none',
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL
            );

            CREATE TABLE callback_outbox (
                event_id TEXT PRIMARY KEY NOT NULL,
                run_id TEXT NOT NULL REFERENCES run_records(run_id) ON DELETE CASCADE,
                callback_kind TEXT NOT NULL,
                payload_json TEXT NOT NULL CHECK (length(payload_json) <= 12288),
                delivery_state TEXT NOT NULL CHECK (delivery_state IN ('pending', 'delivered')),
                attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
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

            INSERT INTO schema_migrations(version, applied_at_ms) VALUES (1, 0);
            INSERT INTO schema_migrations(version, applied_at_ms) VALUES (2, 0);
            PRAGMA user_version = 2;",
        )?;
        transaction.commit()?;
        Self::migrate(connection)
    }

    pub fn observe_run(&self, run_id: RunId, worker_id: WorkerId) -> Result<RunRecord, StoreError> {
        self.observe_run_at(run_id, worker_id, now_ms())
    }

    pub fn observe_run_at(
        &self,
        run_id: RunId,
        worker_id: WorkerId,
        canonical_created_at_ms: i64,
    ) -> Result<RunRecord, StoreError> {
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing_worker: Option<String> = transaction
            .query_row(
                "SELECT worker_id FROM run_records WHERE run_id = ?1",
                [run_id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(existing_worker) = existing_worker {
            if existing_worker != worker_id.as_str() {
                return Err(StoreError::WorkerIdentityConflict);
            }
            let record = load_run(&transaction, run_id)?;
            transaction.commit()?;
            return Ok(record);
        }

        let now = now_ms();
        let expires_at_ms = canonical_created_at_ms
            .checked_add(MAXIMUM_RETENTION_MS)
            .ok_or(StoreError::InvalidStoredValue("retention deadline"))?;
        transaction.execute(
            "INSERT INTO run_records (
                run_id, worker_id, lifecycle_state, created_at_ms, expires_at_ms, updated_at_ms
            ) VALUES (?1, ?2, 'queue_observed', ?3, ?4, ?5)",
            params![
                run_id.to_string(),
                worker_id.as_str(),
                canonical_created_at_ms,
                expires_at_ms,
                now
            ],
        )?;
        let record = load_run(&transaction, run_id)?;
        transaction.commit()?;
        Ok(record)
    }

    pub fn get_run(&self, run_id: RunId) -> Result<RunRecord, StoreError> {
        let connection = self.lock()?;
        load_run(&connection, run_id)
    }

    pub fn transition(
        &self,
        run_id: RunId,
        next: LocalLifecycleState,
    ) -> Result<RunRecord, StoreError> {
        if next == LocalLifecycleState::ClaimAuthorized {
            return Err(StoreError::ClaimAuthorizationProofRequired);
        }
        if next == LocalLifecycleState::StartAuthorizationPersisted {
            return Err(StoreError::AuthorizationProofRequired);
        }
        if next == LocalLifecycleState::ExecutionActive {
            return Err(StoreError::ExecutionGateProofRequired);
        }
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = load_run(&transaction, run_id)?;
        current.lifecycle_state.transition(next)?;
        let start_authorized = current.start_authorized;
        let updated = transaction.execute(
            "UPDATE run_records
             SET lifecycle_state = ?2, start_authorized = ?3, updated_at_ms = ?4
             WHERE run_id = ?1 AND lifecycle_state = ?5",
            params![
                run_id.to_string(),
                lifecycle_state_str(next),
                start_authorized,
                now_ms(),
                lifecycle_state_str(current.lifecycle_state)
            ],
        )?;
        if updated != 1 {
            return Err(StoreError::InvalidStoredValue("lifecycle state"));
        }
        let record = load_run(&transaction, run_id)?;
        transaction.commit()?;
        Ok(record)
    }

    pub fn record_claim_authorization(
        &self,
        run_id: RunId,
        authorization: ClaimAuthorization,
    ) -> Result<RunRecord, StoreError> {
        if authorization.run_id() != run_id {
            return Err(StoreError::ClaimAuthorizationMismatch);
        }
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = load_run(&transaction, run_id)?;
        if current.worker_id != *authorization.worker_id()
            || current.claim_callback_id != Some(authorization.callback_id())
        {
            return Err(StoreError::ClaimAuthorizationMismatch);
        }
        let has_descriptor: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM execution_handoffs WHERE run_id = ?1)",
            [run_id.to_string()],
            |row| row.get(0),
        )?;
        if !has_descriptor {
            return Err(StoreError::InvalidExecutionDescriptor);
        }
        if current.lifecycle_state == LocalLifecycleState::ClaimAuthorized {
            transaction.commit()?;
            return Ok(current);
        }
        if current.lifecycle_state != LocalLifecycleState::ClaimIntentPersisted {
            return Err(StoreError::ClaimAuthorizationMismatch);
        }
        let updated = transaction.execute(
            "UPDATE run_records SET lifecycle_state = 'claim_authorized', updated_at_ms = ?2
             WHERE run_id = ?1 AND worker_id = ?3 AND claim_callback_id = ?4
               AND lifecycle_state = 'claim_intent_persisted'",
            params![
                run_id.to_string(),
                now_ms(),
                authorization.worker_id().as_str(),
                authorization.callback_id().to_string()
            ],
        )?;
        if updated != 1 {
            return Err(StoreError::ClaimAuthorizationMismatch);
        }
        let record = load_run(&transaction, run_id)?;
        transaction.commit()?;
        Ok(record)
    }

    pub fn record_start_authorization(
        &self,
        run_id: RunId,
        authorization: StartedAuthorization,
    ) -> Result<RunRecord, StoreError> {
        if authorization.run_id() != run_id {
            return Err(StoreError::StartAuthorizationMismatch);
        }
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = load_run(&transaction, run_id)?;
        if current.worker_id != *authorization.worker_id()
            || current.claim_callback_id != Some(authorization.claim_callback_id())
            || current.start_callback_id != Some(authorization.callback_id())
        {
            return Err(StoreError::StartAuthorizationMismatch);
        }
        let has_descriptor: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM execution_handoffs WHERE run_id = ?1)",
            [run_id.to_string()],
            |row| row.get(0),
        )?;
        if !has_descriptor {
            return Err(StoreError::InvalidExecutionDescriptor);
        }
        if current.lifecycle_state == LocalLifecycleState::StartAuthorizationPersisted
            && current.start_callback_id == Some(authorization.callback_id())
            && current.start_authorized
        {
            transaction.commit()?;
            return Ok(current);
        }
        if !matches!(
            current.lifecycle_state,
            LocalLifecycleState::ClaimAuthorized | LocalLifecycleState::StartIntentPersisted
        ) {
            return Err(StoreError::StartAuthorizationMismatch);
        }
        current
            .lifecycle_state
            .transition(LocalLifecycleState::StartAuthorizationPersisted)?;
        let updated = transaction.execute(
            "UPDATE run_records
             SET lifecycle_state = 'start_authorization_persisted',
                 start_authorization_source = ?3,
                 start_authorized = 1, updated_at_ms = ?4
             WHERE run_id = ?1 AND worker_id = ?5 AND claim_callback_id = ?6
               AND start_callback_id = ?2
               AND lifecycle_state IN ('claim_authorized', 'start_intent_persisted')",
            params![
                run_id.to_string(),
                authorization.callback_id().to_string(),
                start_authorization_source_str(authorization.source()),
                now_ms(),
                authorization.worker_id().as_str(),
                authorization.claim_callback_id().to_string()
            ],
        )?;
        if updated != 1 {
            return Err(StoreError::StartAuthorizationMismatch);
        }
        let record = load_run(&transaction, run_id)?;
        transaction.commit()?;
        Ok(record)
    }

    pub fn mark_execution_active(
        &self,
        run_id: RunId,
        authorization: ExecutionGateAuthorization,
    ) -> Result<RunRecord, StoreError> {
        if authorization.run_id() != run_id {
            return Err(StoreError::ExecutionGateProofRequired);
        }
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = load_run(&transaction, run_id)?;
        if current.lifecycle_state != LocalLifecycleState::StartAuthorizationPersisted
            || !current.start_authorized
            || current.start_callback_id != Some(authorization.start_callback_id())
            || current.worker_id != *authorization.worker_id()
            || current.claim_callback_id != Some(authorization.claim_callback_id())
        {
            return Err(StoreError::ExecutionGateProofRequired);
        }
        let slot_run_id: Option<String> = transaction
            .query_row(
                "SELECT run_id FROM execution_slot WHERE slot_id = 1",
                [],
                |row| row.get(0),
            )
            .optional()?;
        let expected_slot_run_id = run_id.to_string();
        if slot_run_id.as_deref() != Some(expected_slot_run_id.as_str()) {
            return Err(StoreError::ExecutionSlotNotAuthorized);
        }
        let descriptor_payload: Option<String> = transaction
            .query_row(
                "SELECT descriptor_json FROM execution_handoffs WHERE run_id = ?1",
                [run_id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        let Some(descriptor_payload) = descriptor_payload else {
            return Err(StoreError::InvalidExecutionDescriptor);
        };
        if descriptor_payload.len() > MAX_EXECUTION_DESCRIPTOR_BYTES {
            return Err(StoreError::InvalidExecutionDescriptor);
        }
        let descriptor: ExecutionDescriptor = serde_json::from_str(&descriptor_payload)
            .map_err(|_| StoreError::InvalidExecutionDescriptor)?;
        if descriptor.run_id != run_id || !descriptor.validate() {
            return Err(StoreError::InvalidExecutionDescriptor);
        }

        let updated = transaction.execute(
            "UPDATE run_records SET lifecycle_state = 'execution_active', updated_at_ms = ?2
             WHERE run_id = ?1 AND worker_id = ?3 AND claim_callback_id = ?4
               AND start_callback_id = ?5 AND start_authorized = 1
               AND lifecycle_state = 'start_authorization_persisted'",
            params![
                run_id.to_string(),
                now_ms(),
                authorization.worker_id().as_str(),
                authorization.claim_callback_id().to_string(),
                authorization.start_callback_id().to_string()
            ],
        )?;
        if updated != 1 {
            return Err(StoreError::ExecutionGateProofRequired);
        }
        let record = load_run(&transaction, run_id)?;
        transaction.commit()?;
        Ok(record)
    }

    pub fn begin_start_intent(
        &self,
        run_id: RunId,
        callback_id: EventId,
    ) -> Result<RunRecord, StoreError> {
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = load_run(&transaction, run_id)?;
        match current.start_callback_id {
            Some(existing) if existing == callback_id => {
                transaction.commit()?;
                return Ok(current);
            }
            Some(_) => return Err(StoreError::StartCallbackConflict),
            None => {}
        }
        if !matches!(
            current.lifecycle_state,
            LocalLifecycleState::ClaimAuthorized | LocalLifecycleState::StartIntentPersisted
        ) {
            return Err(StoreError::StartAuthorizationMismatch);
        }
        let next_state = if current.lifecycle_state == LocalLifecycleState::ClaimAuthorized {
            LocalLifecycleState::StartIntentPersisted
        } else {
            current.lifecycle_state
        };
        let updated = transaction.execute(
            "UPDATE run_records SET start_callback_id = ?2, lifecycle_state = ?4, updated_at_ms = ?3
             WHERE run_id = ?1 AND start_callback_id IS NULL
               AND lifecycle_state IN ('claim_authorized', 'start_intent_persisted')",
            params![
                run_id.to_string(),
                callback_id.to_string(),
                now_ms(),
                lifecycle_state_str(next_state)
            ],
        )?;
        if updated != 1 {
            return Err(StoreError::StartCallbackConflict);
        }
        let record = load_run(&transaction, run_id)?;
        transaction.commit()?;
        Ok(record)
    }

    pub fn begin_claim_intent(
        &self,
        run_id: RunId,
        callback_id: ClaimCallbackId,
    ) -> Result<RunRecord, StoreError> {
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = load_run(&transaction, run_id)?;
        match current.claim_callback_id {
            Some(existing) if existing == callback_id => {
                let record = current;
                transaction.commit()?;
                return Ok(record);
            }
            Some(_) => return Err(StoreError::ClaimFenceConflict),
            None => {}
        }
        current
            .lifecycle_state
            .transition(LocalLifecycleState::ClaimIntentPersisted)?;
        let updated = transaction.execute(
            "UPDATE run_records
             SET lifecycle_state = 'claim_intent_persisted', claim_callback_id = ?2, updated_at_ms = ?3
             WHERE run_id = ?1 AND claim_callback_id IS NULL AND lifecycle_state = 'canonical_run_checked'",
            params![run_id.to_string(), callback_id.to_string(), now_ms()],
        );
        match updated {
            Ok(1) => {}
            Ok(_) => return Err(StoreError::ClaimFenceConflict),
            Err(rusqlite::Error::SqliteFailure(failure, _))
                if failure.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                return Err(StoreError::ClaimFenceConflict);
            }
            Err(error) => return Err(StoreError::Sqlite(error)),
        }
        let record = load_run(&transaction, run_id)?;
        transaction.commit()?;
        Ok(record)
    }

    pub fn record_recovery_action(
        &self,
        run_id: RunId,
        action: RecoveryAction,
    ) -> Result<(), StoreError> {
        let connection = self.lock()?;
        let updated = connection.execute(
            "UPDATE run_records
             SET recovery_attempts = recovery_attempts + 1, recovery_action = ?2, updated_at_ms = ?3
             WHERE run_id = ?1",
            params![run_id.to_string(), action.as_str(), now_ms()],
        )?;
        if updated != 1 {
            return Err(StoreError::RunNotFound);
        }
        Ok(())
    }

    /// Persists a reconciliation outcome without treating the outcome itself
    /// as another recovery attempt.
    pub fn set_recovery_action(
        &self,
        run_id: RunId,
        action: RecoveryAction,
    ) -> Result<(), StoreError> {
        let connection = self.lock()?;
        let updated = connection.execute(
            "UPDATE run_records SET recovery_action = ?2, updated_at_ms = ?3 WHERE run_id = ?1",
            params![run_id.to_string(), action.as_str(), now_ms()],
        )?;
        if updated != 1 {
            return Err(StoreError::RunNotFound);
        }
        Ok(())
    }

    /// Persists only the validated Soyuz execution descriptor, not the Cloudflare
    /// message ID, lease, or response metadata. The task prompt is retained so a
    /// future worker can recover the exact authorized request after restart.
    pub fn persist_execution_descriptor(
        &self,
        descriptor: ExecutionDescriptor,
    ) -> Result<(), StoreError> {
        if !descriptor.validate() {
            return Err(StoreError::InvalidExecutionDescriptor);
        }
        let descriptor_json = serde_json::to_string(&descriptor)?;
        if descriptor_json.len() > MAX_EXECUTION_DESCRIPTOR_BYTES {
            return Err(StoreError::InvalidExecutionDescriptor);
        }

        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        load_run(&transaction, descriptor.run_id)?;
        let existing: Option<String> = transaction
            .query_row(
                "SELECT descriptor_json FROM execution_handoffs WHERE run_id = ?1",
                [descriptor.run_id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(existing) = existing {
            if existing != descriptor_json {
                return Err(StoreError::ExecutionDescriptorConflict);
            }
            transaction.commit()?;
            return Ok(());
        }
        let now = now_ms();
        transaction.execute(
            "INSERT INTO execution_handoffs(run_id, descriptor_json, created_at_ms, updated_at_ms)
             VALUES (?1, ?2, ?3, ?3)",
            params![descriptor.run_id.to_string(), descriptor_json, now],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn execution_descriptor(
        &self,
        run_id: RunId,
    ) -> Result<Option<ExecutionDescriptor>, StoreError> {
        let connection = self.lock()?;
        let payload: Option<String> = connection
            .query_row(
                "SELECT descriptor_json FROM execution_handoffs WHERE run_id = ?1",
                [run_id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        payload
            .map(|payload| {
                if payload.len() > MAX_EXECUTION_DESCRIPTOR_BYTES {
                    return Err(StoreError::InvalidExecutionDescriptor);
                }
                let descriptor: ExecutionDescriptor = serde_json::from_str(&payload)
                    .map_err(|_| StoreError::InvalidExecutionDescriptor)?;
                if descriptor.run_id != run_id || !descriptor.validate() {
                    return Err(StoreError::InvalidExecutionDescriptor);
                }
                Ok(descriptor)
            })
            .transpose()
    }

    /// Claims the sole host slot before any Soyuz claim/start call so parallel
    /// coordinators cannot advance different runs through authorization.
    pub fn reserve_intake_slot(&self, run_id: RunId) -> Result<bool, StoreError> {
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let record = load_run(&transaction, run_id)?;
        if record.lifecycle_state == LocalLifecycleState::ExecutionTerminal {
            return Err(StoreError::ExecutionSlotNotAuthorized);
        }
        let has_descriptor: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM execution_handoffs WHERE run_id = ?1)",
            [run_id.to_string()],
            |row| row.get(0),
        )?;
        if !has_descriptor {
            return Err(StoreError::InvalidExecutionDescriptor);
        }
        let current_slot: Option<String> = transaction
            .query_row(
                "SELECT run_id FROM execution_slot WHERE slot_id = 1",
                [],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(current_run_id) = current_slot {
            if current_run_id == run_id.to_string() {
                transaction.commit()?;
                return Ok(false);
            }
            return Err(StoreError::ExecutionSlotOccupied);
        }
        let outstanding_callbacks: u32 = transaction.query_row(
            "SELECT COUNT(*) FROM callback_outbox WHERE delivery_state IN ('pending', 'blocked')",
            [],
            |row| row.get(0),
        )?;
        if outstanding_callbacks >= MAX_PENDING_CALLBACKS.saturating_sub(1) {
            return Err(StoreError::OutboxBackpressure);
        }
        let unresolved_run: Option<String> = transaction
            .query_row(
                "SELECT run_id FROM run_records
                 WHERE run_id != ?1 AND lifecycle_state != 'execution_terminal'
                 ORDER BY created_at_ms LIMIT 1",
                [run_id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        if unresolved_run.is_some() {
            return Err(StoreError::ExecutionSlotOccupied);
        }
        transaction.execute(
            "INSERT INTO execution_slot(slot_id, run_id, reserved_at_ms) VALUES (1, ?1, ?2)",
            params![run_id.to_string(), now_ms()],
        )?;
        transaction.commit()?;
        Ok(true)
    }

    /// Confirms the handoff only after start authorization and a fresh
    /// canonical gate. A pre-reserved intake slot is idempotently retained.
    pub fn reserve_execution_slot(&self, run_id: RunId) -> Result<bool, StoreError> {
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let record = load_run(&transaction, run_id)?;
        if !record.start_authorized
            || record.lifecycle_state != LocalLifecycleState::StartAuthorizationPersisted
        {
            return Err(StoreError::ExecutionSlotNotAuthorized);
        }
        let has_descriptor: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM execution_handoffs WHERE run_id = ?1)",
            [run_id.to_string()],
            |row| row.get(0),
        )?;
        if !has_descriptor {
            return Err(StoreError::InvalidExecutionDescriptor);
        }
        let current_slot: Option<String> = transaction
            .query_row(
                "SELECT run_id FROM execution_slot WHERE slot_id = 1",
                [],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(current_run_id) = current_slot {
            if current_run_id == run_id.to_string() {
                transaction.commit()?;
                return Ok(false);
            }
            return Err(StoreError::ExecutionSlotOccupied);
        }
        let unresolved_run: Option<String> = transaction
            .query_row(
                "SELECT run_id FROM run_records
                 WHERE run_id != ?1 AND lifecycle_state != 'execution_terminal'
                 ORDER BY created_at_ms LIMIT 1",
                [run_id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        if unresolved_run.is_some() {
            return Err(StoreError::ExecutionSlotOccupied);
        }
        transaction.execute(
            "INSERT INTO execution_slot(slot_id, run_id, reserved_at_ms) VALUES (1, ?1, ?2)",
            params![run_id.to_string(), now_ms()],
        )?;
        transaction.commit()?;
        Ok(true)
    }

    /// Persists the deterministic container identity before any Docker API
    /// side effect. The supplied proof is the result of a fresh Soyuz gate
    /// check; IPC callers must send only the canonical run ID and have the
    /// supervisor rebuild this proof from the durable record plus Soyuz.
    pub fn begin_container_launch(
        &self,
        run_id: RunId,
        scenario: fossil_core::execution::SyntheticScenario,
        profile: ContainerLaunchProfile,
        authorization: ExecutionGateAuthorization,
    ) -> Result<ContainerLaunchRecord, StoreError> {
        let kind_is_valid = match profile.workload_kind {
            WorkloadKind::Synthetic => {
                profile.profile_version == "synthetic-v1"
                    && profile.repository_url.is_none()
                    && profile.authorized_git_ref.is_none()
                    && profile.resolved_commit.is_none()
                    && profile.workspace_identity.is_none()
            }
            WorkloadKind::PiCodingV1 => {
                // The v3 launch table requires a synthetic scenario value.
                // Success is only a neutral compatibility sentinel here; the
                // Docker supervisor rejects Pi records before interpreting it.
                profile.profile_version == "pi-coding-v1"
                    && scenario == fossil_core::execution::SyntheticScenario::Success
                    && is_pinned_pi_worker_image(&profile.image_identity)
                    && profile.repository_url.as_deref().is_some_and(|url| {
                        valid_repository_url(url) && url == PI_PILOT_REPOSITORY_URL
                    })
                    && profile
                        .authorized_git_ref
                        .as_deref()
                        .is_some_and(|git_ref| {
                            valid_authorized_git_ref(git_ref) && git_ref == PI_PILOT_GIT_REF
                        })
                    && profile.resolved_commit.as_deref().is_some_and(|commit| {
                        valid_resolved_commit(commit) && commit == PI_PILOT_RESOLVED_COMMIT
                    })
                    && profile
                        .workspace_identity
                        .as_deref()
                        .is_some_and(valid_workspace_identity)
            }
        };
        if !kind_is_valid
            || profile.workload_version != 1
            || profile.image_identity.is_empty()
            || profile.image_identity.len() > 512
            || profile
                .image_identity
                .bytes()
                .any(|byte| byte.is_ascii_control())
            || profile.effective_policy_json.len() > 4096
            || !matches!(
                serde_json::from_str::<serde_json::Value>(&profile.effective_policy_json),
                Ok(value) if value.is_object()
            )
            || !(1..=10_800_000).contains(&profile.runtime_limit_ms)
        {
            return Err(StoreError::InvalidContainerLaunchResult);
        }
        if authorization.run_id() != run_id {
            return Err(StoreError::ContainerLaunchAuthorizationMismatch);
        }
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let run = load_run(&transaction, run_id)?;
        if run.worker_id != *authorization.worker_id()
            || run.claim_callback_id != Some(authorization.claim_callback_id())
            || run.start_callback_id != Some(authorization.start_callback_id())
            || !run.start_authorized
        {
            return Err(StoreError::ContainerLaunchAuthorizationMismatch);
        }

        let slot_owner: Option<String> = transaction
            .query_row(
                "SELECT run_id FROM execution_slot WHERE slot_id = 1",
                [],
                |row| row.get(0),
            )
            .optional()?;
        if slot_owner.as_deref() != Some(run_id.to_string().as_str()) {
            return Err(StoreError::ExecutionSlotNotAuthorized);
        }

        if let Some(existing) = load_container_launch_optional(&transaction, run_id)? {
            if existing.worker_id != run.worker_id
                || existing.claim_callback_id != authorization.claim_callback_id()
                || existing.start_callback_id != authorization.start_callback_id()
                || existing.scenario != scenario
                || existing.workload_kind != profile.workload_kind
                || existing.workload_version != profile.workload_version
                || existing.image_identity.as_deref() != Some(&profile.image_identity)
                || existing.profile_version != profile.profile_version
                || existing.effective_policy_json != profile.effective_policy_json
                || existing.runtime_limit_ms != Some(profile.runtime_limit_ms)
                || existing.repository_url != profile.repository_url
                || existing.authorized_git_ref != profile.authorized_git_ref
                || existing.resolved_commit != profile.resolved_commit
                || existing.workspace_identity != profile.workspace_identity
            {
                return Err(StoreError::ContainerLaunchIdentityConflict);
            }
            transaction.commit()?;
            return Ok(existing);
        }

        if run.lifecycle_state != LocalLifecycleState::StartAuthorizationPersisted {
            return Err(StoreError::ContainerLaunchAuthorizationMismatch);
        }
        let descriptor_payload: Option<String> = transaction
            .query_row(
                "SELECT descriptor_json FROM execution_handoffs WHERE run_id = ?1",
                [run_id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        let Some(descriptor_payload) = descriptor_payload else {
            return Err(StoreError::InvalidExecutionDescriptor);
        };
        if descriptor_payload.len() > MAX_EXECUTION_DESCRIPTOR_BYTES {
            return Err(StoreError::InvalidExecutionDescriptor);
        }
        let descriptor: ExecutionDescriptor = serde_json::from_str(&descriptor_payload)
            .map_err(|_| StoreError::InvalidExecutionDescriptor)?;
        if descriptor.run_id != run_id || !descriptor.validate() {
            return Err(StoreError::InvalidExecutionDescriptor);
        }
        if profile.workload_kind == WorkloadKind::PiCodingV1 {
            let request = &descriptor.request;
            let canonical_timeout_ms = i64::from(request.timeout_seconds.unwrap_or(7_200)) * 1_000;
            if request.task_mode != TaskMode::Patch
                || request.publish_mode != PublishMode::None
                || profile.repository_url.as_deref() != Some(request.repo_url.as_str())
                || profile.authorized_git_ref.as_deref() != Some(request.git_ref.as_str())
                || profile.runtime_limit_ms > canonical_timeout_ms
                || profile.runtime_limit_ms > 10_800_000
            {
                return Err(StoreError::InvalidContainerLaunchResult);
            }
        }

        let other_launch: Option<String> = transaction
            .query_row(
                "SELECT run_id FROM container_launches
                 WHERE run_id != ?1 AND state != 'cleanup_completed' LIMIT 1",
                [run_id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        if other_launch.is_some() {
            return Err(StoreError::ExecutionSlotOccupied);
        }

        let container_name = stable_container_name(
            run_id,
            authorization.start_callback_id(),
            profile.workload_kind,
        );
        let now = now_ms();
        transaction.execute(
            "INSERT INTO container_launches (
                run_id, worker_id, claim_callback_id, start_callback_id, scenario,
                container_name, state, created_at_ms, updated_at_ms, workload_kind,
                workload_version, image_identity, profile_version, effective_policy_json,
                runtime_limit_ms, repository_url, authorized_git_ref, resolved_commit,
                workspace_identity
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'launch_intent_persisted', ?7, ?7,
                       ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
            params![
                run_id.to_string(),
                run.worker_id.as_str(),
                authorization.claim_callback_id().to_string(),
                authorization.start_callback_id().to_string(),
                scenario.as_str(),
                container_name,
                now,
                profile.workload_kind.as_str(),
                i64::from(profile.workload_version),
                profile.image_identity,
                profile.profile_version,
                profile.effective_policy_json,
                profile.runtime_limit_ms,
                profile.repository_url,
                profile.authorized_git_ref,
                profile.resolved_commit,
                profile.workspace_identity,
            ],
        )?;
        let launch = load_container_launch(&transaction, run_id)?;
        transaction.commit()?;
        Ok(launch)
    }

    pub fn container_launch(
        &self,
        run_id: RunId,
    ) -> Result<Option<ContainerLaunchRecord>, StoreError> {
        let connection = self.lock()?;
        load_container_launch_optional(&connection, run_id)
    }

    pub fn container_launches_requiring_reconciliation(
        &self,
    ) -> Result<Vec<ContainerLaunchRecord>, StoreError> {
        let connection = self.lock()?;
        let mut statement = connection.prepare(
            "SELECT run_id FROM container_launches WHERE state != 'cleanup_completed'
             ORDER BY created_at_ms, run_id",
        )?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        let mut launches = Vec::new();
        for row in rows {
            let run_id =
                RunId::parse(&row?).map_err(|_| StoreError::InvalidStoredValue("run ID"))?;
            launches.push(load_container_launch(&connection, run_id)?);
        }
        Ok(launches)
    }

    pub fn mark_container_create_requested(
        &self,
        run_id: RunId,
    ) -> Result<ContainerLaunchRecord, StoreError> {
        self.advance_container_launch(run_id, ContainerLaunchState::CreateRequested)
    }

    /// Exactly one cooperating supervisor process receives permission to send
    /// the initial create request. Once persisted, all other callers reconcile
    /// by deterministic name and never repeat an ambiguous create side effect.
    pub fn request_container_create(&self, run_id: RunId) -> Result<bool, StoreError> {
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = load_container_launch(&transaction, run_id)?;
        if current.state != ContainerLaunchState::LaunchIntentPersisted {
            transaction.commit()?;
            return Ok(false);
        }
        transaction.execute(
            "UPDATE container_launches SET state = 'create_requested', updated_at_ms = ?2
             WHERE run_id = ?1 AND state = 'launch_intent_persisted'",
            params![run_id.to_string(), now_ms()],
        )?;
        transaction.commit()?;
        Ok(true)
    }

    pub fn record_container_created(
        &self,
        run_id: RunId,
        container_id: &str,
    ) -> Result<ContainerLaunchRecord, StoreError> {
        if container_id.is_empty()
            || container_id.len() > 128
            || !container_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        {
            return Err(StoreError::ContainerIdentityConflict);
        }
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = load_container_launch(&transaction, run_id)?;
        if let Some(existing_id) = &current.container_id {
            if existing_id != container_id {
                return Err(StoreError::ContainerIdentityConflict);
            }
            transaction.commit()?;
            return Ok(current);
        }
        if current.state != ContainerLaunchState::CreateRequested {
            return Err(StoreError::ContainerLaunchTransitionDenied);
        }
        transaction.execute(
            "UPDATE container_launches SET container_id = ?2, state = 'container_created',
                updated_at_ms = ?3 WHERE run_id = ?1 AND state = 'create_requested'",
            params![run_id.to_string(), container_id, now_ms()],
        )?;
        let updated = load_container_launch(&transaction, run_id)?;
        transaction.commit()?;
        Ok(updated)
    }

    pub fn mark_container_start_requested(
        &self,
        run_id: RunId,
    ) -> Result<ContainerLaunchRecord, StoreError> {
        self.advance_container_launch(run_id, ContainerLaunchState::StartRequested)
    }

    /// Atomically records that Docker reports the exact persisted container
    /// running and advances the canonical local run into active execution.
    pub fn mark_container_running(
        &self,
        run_id: RunId,
        authorization: ExecutionGateAuthorization,
        started_at_ms: i64,
    ) -> Result<ContainerLaunchRecord, StoreError> {
        if started_at_ms < 0 {
            return Err(StoreError::InvalidContainerLaunchResult);
        }
        if authorization.run_id() != run_id {
            return Err(StoreError::ExecutionGateProofRequired);
        }
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let run = load_run(&transaction, run_id)?;
        let mut launch = load_container_launch(&transaction, run_id)?;
        if run.worker_id != *authorization.worker_id()
            || run.claim_callback_id != Some(authorization.claim_callback_id())
            || run.start_callback_id != Some(authorization.start_callback_id())
            || !run.start_authorized
            || launch.worker_id != run.worker_id
            || launch.claim_callback_id != authorization.claim_callback_id()
            || launch.start_callback_id != authorization.start_callback_id()
            || launch.container_id.is_none()
        {
            return Err(StoreError::ExecutionGateProofRequired);
        }
        if launch.state == ContainerLaunchState::ContainerRunning
            && run.lifecycle_state == LocalLifecycleState::ExecutionActive
        {
            transaction.commit()?;
            return Ok(launch);
        }
        if launch.state != ContainerLaunchState::StartRequested
            || run.lifecycle_state != LocalLifecycleState::StartAuthorizationPersisted
        {
            return Err(StoreError::ContainerLaunchTransitionDenied);
        }
        let slot_owner: Option<String> = transaction
            .query_row(
                "SELECT run_id FROM execution_slot WHERE slot_id = 1",
                [],
                |row| row.get(0),
            )
            .optional()?;
        if slot_owner.as_deref() != Some(run_id.to_string().as_str()) {
            return Err(StoreError::ExecutionSlotNotAuthorized);
        }
        let descriptor_payload: Option<String> = transaction
            .query_row(
                "SELECT descriptor_json FROM execution_handoffs WHERE run_id = ?1",
                [run_id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        let Some(descriptor_payload) = descriptor_payload else {
            return Err(StoreError::InvalidExecutionDescriptor);
        };
        if descriptor_payload.len() > MAX_EXECUTION_DESCRIPTOR_BYTES {
            return Err(StoreError::InvalidExecutionDescriptor);
        }
        let descriptor: ExecutionDescriptor = serde_json::from_str(&descriptor_payload)
            .map_err(|_| StoreError::InvalidExecutionDescriptor)?;
        if descriptor.run_id != run_id || !descriptor.validate() {
            return Err(StoreError::InvalidExecutionDescriptor);
        }
        run.lifecycle_state
            .transition(LocalLifecycleState::ExecutionActive)?;
        let effective_deadline_at_ms = calculate_effective_deadline(&launch, started_at_ms)?;
        transaction.execute(
            "UPDATE run_records SET lifecycle_state = 'execution_active', updated_at_ms = ?2
             WHERE run_id = ?1 AND lifecycle_state = 'start_authorization_persisted'",
            params![run_id.to_string(), now_ms()],
        )?;
        transaction.execute(
            "UPDATE container_launches SET state = 'container_running', started_at_ms = ?2,
                effective_deadline_at_ms = ?3, updated_at_ms = ?2
             WHERE run_id = ?1 AND state = 'start_requested'",
            params![run_id.to_string(), started_at_ms, effective_deadline_at_ms],
        )?;
        launch = load_container_launch(&transaction, run_id)?;
        transaction.commit()?;
        Ok(launch)
    }

    /// Recovers an already-started container after a process restart. The
    /// caller must first inspect Docker and verify the persisted container
    /// name, ID, labels, and isolation profile. This accepts only the durable
    /// start proof and fenced slot already recorded for this exact launch.
    pub fn recover_container_running(
        &self,
        run_id: RunId,
        started_at_ms: i64,
    ) -> Result<ContainerLaunchRecord, StoreError> {
        if started_at_ms < 0 {
            return Err(StoreError::InvalidContainerLaunchResult);
        }
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let run = load_run(&transaction, run_id)?;
        let mut launch = load_container_launch(&transaction, run_id)?;
        if !run.start_authorized
            || launch.worker_id != run.worker_id
            || launch.container_id.is_none()
        {
            return Err(StoreError::ExecutionGateProofRequired);
        }
        if launch.state == ContainerLaunchState::ContainerRunning
            && run.lifecycle_state == LocalLifecycleState::ExecutionActive
        {
            transaction.commit()?;
            return Ok(launch);
        }
        if launch.state != ContainerLaunchState::StartRequested
            || run.lifecycle_state != LocalLifecycleState::StartAuthorizationPersisted
        {
            return Err(StoreError::ContainerLaunchTransitionDenied);
        }
        let slot_owner: Option<String> = transaction
            .query_row(
                "SELECT run_id FROM execution_slot WHERE slot_id = 1",
                [],
                |row| row.get(0),
            )
            .optional()?;
        if slot_owner.as_deref() != Some(run_id.to_string().as_str()) {
            return Err(StoreError::ExecutionSlotNotAuthorized);
        }
        let descriptor_payload: Option<String> = transaction
            .query_row(
                "SELECT descriptor_json FROM execution_handoffs WHERE run_id = ?1",
                [run_id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        let Some(descriptor_payload) = descriptor_payload else {
            return Err(StoreError::InvalidExecutionDescriptor);
        };
        if descriptor_payload.len() > MAX_EXECUTION_DESCRIPTOR_BYTES {
            return Err(StoreError::InvalidExecutionDescriptor);
        }
        let descriptor: ExecutionDescriptor = serde_json::from_str(&descriptor_payload)
            .map_err(|_| StoreError::InvalidExecutionDescriptor)?;
        if descriptor.run_id != run_id || !descriptor.validate() {
            return Err(StoreError::InvalidExecutionDescriptor);
        }
        run.lifecycle_state
            .transition(LocalLifecycleState::ExecutionActive)?;
        let effective_deadline_at_ms = calculate_effective_deadline(&launch, started_at_ms)?;
        transaction.execute(
            "UPDATE run_records SET lifecycle_state = 'execution_active', updated_at_ms = ?2
             WHERE run_id = ?1 AND lifecycle_state = 'start_authorization_persisted'",
            params![run_id.to_string(), now_ms()],
        )?;
        transaction.execute(
            "UPDATE container_launches SET state = 'container_running', started_at_ms = ?2,
                effective_deadline_at_ms = ?3, updated_at_ms = ?2
             WHERE run_id = ?1 AND state = 'start_requested'",
            params![run_id.to_string(), started_at_ms, effective_deadline_at_ms],
        )?;
        launch = load_container_launch(&transaction, run_id)?;
        transaction.commit()?;
        Ok(launch)
    }

    pub fn mark_container_stop_requested(
        &self,
        run_id: RunId,
        reason: &str,
    ) -> Result<ContainerLaunchRecord, StoreError> {
        if !matches!(reason, "cancelled" | "timeout" | "execution_uncertain") {
            return Err(StoreError::InvalidContainerLaunchResult);
        }
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = load_container_launch(&transaction, run_id)?;
        if current.state == ContainerLaunchState::StopRequested {
            if current.stop_reason.as_deref() != Some(reason) {
                return Err(StoreError::ContainerLaunchResultMismatch);
            }
            transaction.commit()?;
            return Ok(current);
        }
        if !current
            .state
            .can_advance_to(ContainerLaunchState::StopRequested)
        {
            return Err(StoreError::ContainerLaunchTransitionDenied);
        }
        let now = now_ms();
        transaction.execute(
            "UPDATE container_launches SET state = 'stop_requested', stop_reason = ?2,
                stop_requested_at_ms = ?3, updated_at_ms = ?3
             WHERE run_id = ?1 AND state = ?4",
            params![run_id.to_string(), reason, now, current.state.as_str()],
        )?;
        let updated = load_container_launch(&transaction, run_id)?;
        transaction.commit()?;
        Ok(updated)
    }

    pub fn mark_container_manual_review(
        &self,
        run_id: RunId,
    ) -> Result<ContainerLaunchRecord, StoreError> {
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = load_container_launch(&transaction, run_id)?;
        if current.state == ContainerLaunchState::CleanupCompleted
            || current.state == ContainerLaunchState::ManualReviewRequired
        {
            transaction.commit()?;
            return Ok(current);
        }
        transaction.execute(
            "UPDATE container_launches SET state = 'manual_review_required', updated_at_ms = ?2
             WHERE run_id = ?1",
            params![run_id.to_string(), now_ms()],
        )?;
        let updated = load_container_launch(&transaction, run_id)?;
        transaction.commit()?;
        Ok(updated)
    }

    pub fn record_container_exit(
        &self,
        run_id: RunId,
        exit_code: Option<i32>,
        outcome_class: &str,
        diagnostic_summary: &str,
        output_truncated: bool,
        completed_at_ms: i64,
    ) -> Result<ContainerLaunchRecord, StoreError> {
        self.record_container_exit_with_times(
            run_id,
            exit_code,
            outcome_class,
            diagnostic_summary,
            output_truncated,
            ContainerExitTimes {
                started_at_ms: None,
                completed_at_ms,
            },
        )
    }

    pub fn record_container_exit_with_times(
        &self,
        run_id: RunId,
        exit_code: Option<i32>,
        outcome_class: &str,
        diagnostic_summary: &str,
        output_truncated: bool,
        times: ContainerExitTimes,
    ) -> Result<ContainerLaunchRecord, StoreError> {
        let ContainerExitTimes {
            started_at_ms,
            completed_at_ms,
        } = times;
        if diagnostic_summary.len() > 512
            || diagnostic_summary.chars().any(char::is_control)
            || started_at_ms.is_some_and(|started| started < 0 || started > completed_at_ms)
            || completed_at_ms < 0
        {
            return Err(StoreError::InvalidContainerLaunchResult);
        }
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = load_container_launch(&transaction, run_id)?;
        let valid_class = match current.workload_kind {
            WorkloadKind::Synthetic => matches!(
                outcome_class,
                "success"
                    | "nonzero_exit"
                    | "timeout"
                    | "cancelled"
                    | "docker_failure"
                    | "resource_limit_failure"
                    | "unexpected_termination"
                    | "execution_uncertain"
            ),
            WorkloadKind::PiCodingV1 => matches!(
                outcome_class,
                "pi_completed"
                    | "provider_error"
                    | "pi_process_failure"
                    | "invalid_event_stream"
                    | "output_truncated"
                    | "timeout"
                    | "cancelled"
                    | "docker_failure"
                    | "resource_limit_failure"
                    | "unexpected_termination"
                    | "execution_uncertain"
            ),
        };
        let valid_truncation_state = match current.workload_kind {
            WorkloadKind::Synthetic => true,
            WorkloadKind::PiCodingV1 => match outcome_class {
                "output_truncated" => output_truncated,
                _ => !output_truncated,
            },
        };
        if !valid_class
            || !valid_truncation_state
            || (matches!(outcome_class, "success" | "pi_completed") && exit_code != Some(0))
            || (outcome_class == "nonzero_exit" && exit_code.is_none_or(|value| value == 0))
        {
            return Err(StoreError::InvalidContainerLaunchResult);
        }
        if current.state == ContainerLaunchState::ContainerExited {
            if current.exit_code != exit_code
                || current.outcome_class.as_deref() != Some(outcome_class)
                || current.diagnostic_summary.as_deref() != Some(diagnostic_summary)
                || current.output_truncated != output_truncated
                || current.completed_at_ms != Some(completed_at_ms)
                || started_at_ms.is_some_and(|started| current.started_at_ms != Some(started))
            {
                return Err(StoreError::ContainerLaunchResultMismatch);
            }
            transaction.commit()?;
            return Ok(current);
        }
        if !matches!(
            current.state,
            ContainerLaunchState::LaunchIntentPersisted
                | ContainerLaunchState::ContainerCreated
                | ContainerLaunchState::StartRequested
                | ContainerLaunchState::ContainerRunning
                | ContainerLaunchState::StopRequested
        ) {
            return Err(StoreError::ContainerLaunchTransitionDenied);
        }
        let callback_id = current.terminal_callback_id.unwrap_or_else(EventId::new);
        transaction.execute(
            "UPDATE container_launches SET state = 'container_exited', exit_code = ?2,
                outcome_class = ?3, diagnostic_summary = ?4, output_truncated = ?5,
                terminal_callback_id = ?6, started_at_ms = COALESCE(started_at_ms, ?7),
                completed_at_ms = ?8, updated_at_ms = ?8
             WHERE run_id = ?1",
            params![
                run_id.to_string(),
                exit_code,
                outcome_class,
                diagnostic_summary,
                output_truncated,
                callback_id.to_string(),
                started_at_ms,
                completed_at_ms
            ],
        )?;
        let updated = load_container_launch(&transaction, run_id)?;
        transaction.commit()?;
        Ok(updated)
    }

    pub fn complete_container_cleanup(
        &self,
        run_id: RunId,
    ) -> Result<ContainerLaunchRecord, StoreError> {
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = load_container_launch(&transaction, run_id)?;
        if current.state == ContainerLaunchState::CleanupCompleted {
            transaction.commit()?;
            return Ok(current);
        }
        if current.state != ContainerLaunchState::CleanupPending
            || load_run(&transaction, run_id)?.lifecycle_state
                != LocalLifecycleState::ExecutionTerminal
        {
            return Err(StoreError::ContainerLaunchTransitionDenied);
        }
        transaction.execute(
            "UPDATE container_launches SET state = 'cleanup_completed', updated_at_ms = ?2
             WHERE run_id = ?1 AND state = 'cleanup_pending'",
            params![run_id.to_string(), now_ms()],
        )?;
        transaction.execute(
            "DELETE FROM execution_slot WHERE slot_id = 1 AND run_id = ?1",
            [run_id.to_string()],
        )?;
        let updated = load_container_launch(&transaction, run_id)?;
        transaction.commit()?;
        Ok(updated)
    }

    pub fn execution_slot_owner(&self) -> Result<Option<RunId>, StoreError> {
        let connection = self.lock()?;
        connection
            .query_row(
                "SELECT run_id FROM execution_slot WHERE slot_id = 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|run_id| {
                RunId::parse(&run_id).map_err(|_| StoreError::InvalidStoredValue("run ID"))
            })
            .transpose()
    }

    fn advance_container_launch(
        &self,
        run_id: RunId,
        next: ContainerLaunchState,
    ) -> Result<ContainerLaunchRecord, StoreError> {
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = load_container_launch(&transaction, run_id)?;
        if current.state == next
            || matches!(
                (next, current.state),
                (
                    ContainerLaunchState::CreateRequested,
                    ContainerLaunchState::ContainerCreated
                ) | (
                    ContainerLaunchState::CreateRequested,
                    ContainerLaunchState::StartRequested
                ) | (
                    ContainerLaunchState::CreateRequested,
                    ContainerLaunchState::ContainerRunning
                ) | (
                    ContainerLaunchState::StartRequested,
                    ContainerLaunchState::ContainerRunning
                ) | (
                    ContainerLaunchState::StartRequested,
                    ContainerLaunchState::StopRequested
                ) | (
                    ContainerLaunchState::StartRequested,
                    ContainerLaunchState::ContainerExited
                ) | (
                    ContainerLaunchState::StopRequested,
                    ContainerLaunchState::ContainerExited
                ) | (
                    ContainerLaunchState::StopRequested,
                    ContainerLaunchState::CleanupPending
                ) | (
                    ContainerLaunchState::StopRequested,
                    ContainerLaunchState::CleanupCompleted
                )
            )
        {
            transaction.commit()?;
            return Ok(current);
        }
        if !current.state.can_advance_to(next) {
            return Err(StoreError::ContainerLaunchTransitionDenied);
        }
        transaction.execute(
            "UPDATE container_launches SET state = ?2, updated_at_ms = ?3
             WHERE run_id = ?1 AND state = ?4",
            params![
                run_id.to_string(),
                next.as_str(),
                now_ms(),
                current.state.as_str()
            ],
        )?;
        let updated = load_container_launch(&transaction, run_id)?;
        transaction.commit()?;
        Ok(updated)
    }

    pub fn release_execution_slot(&self, run_id: RunId) -> Result<(), StoreError> {
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let record = load_run(&transaction, run_id)?;
        if record.lifecycle_state != LocalLifecycleState::ExecutionTerminal {
            return Err(StoreError::ExecutionSlotReleaseDenied);
        }
        if load_container_launch_optional(&transaction, run_id)?
            .is_some_and(|launch| launch.state != ContainerLaunchState::CleanupCompleted)
        {
            return Err(StoreError::ExecutionSlotReleaseDenied);
        }
        transaction.execute(
            "DELETE FROM execution_slot WHERE slot_id = 1 AND run_id = ?1",
            [run_id.to_string()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Closes a local intake record after a fresh canonical read proves that a
    /// different worker owns the run. Active or cancellation-pending local
    /// execution is never discarded by this path.
    pub fn supersede_run_owned_elsewhere(&self, run_id: RunId) -> Result<(), StoreError> {
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let record = load_run(&transaction, run_id)?;
        if matches!(
            record.lifecycle_state,
            LocalLifecycleState::ExecutionActive | LocalLifecycleState::CancellationRequested
        ) {
            return Err(StoreError::ExecutionSlotReleaseDenied);
        }
        if record.lifecycle_state != LocalLifecycleState::ExecutionTerminal {
            record
                .lifecycle_state
                .transition(LocalLifecycleState::ExecutionTerminal)?;
        }
        let now = now_ms();
        transaction.execute(
            "UPDATE run_records
             SET lifecycle_state = 'execution_terminal', recovery_action = 'owned_elsewhere',
                 recovery_attempts = recovery_attempts + 1, updated_at_ms = ?2
             WHERE run_id = ?1",
            params![run_id.to_string(), now],
        )?;
        transaction.execute(
            "DELETE FROM execution_slot WHERE slot_id = 1 AND run_id = ?1",
            [run_id.to_string()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Deletes retained prompt-bearing input only after terminal work and all
    /// callback delivery have completed. Operators should retain the small
    /// lifecycle record for audit and duplicate-delivery reconciliation.
    pub fn remove_terminal_execution_descriptor(&self, run_id: RunId) -> Result<bool, StoreError> {
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let record = load_run(&transaction, run_id)?;
        if record.lifecycle_state != LocalLifecycleState::ExecutionTerminal
            || matches!(
                record.callback_delivery,
                CallbackDeliveryState::Pending | CallbackDeliveryState::Blocked
            )
        {
            return Err(StoreError::ExecutionDescriptorRetentionDenied);
        }
        let deleted = transaction.execute(
            "DELETE FROM execution_handoffs WHERE run_id = ?1",
            [run_id.to_string()],
        )?;
        transaction.commit()?;
        Ok(deleted == 1)
    }

    /// Expire local prompt-bearing descriptors and callback payloads at the
    /// canonical deadline. Keep only the run/container fence while Docker
    /// cleanup is unresolved; remove the complete row set once terminal and safe.
    pub fn purge_expired_records(&self, at_ms: i64) -> Result<RetentionSweepSummary, StoreError> {
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut statement = transaction.prepare(
            "SELECT run_id, lifecycle_state FROM run_records WHERE expires_at_ms <= ?1 ORDER BY expires_at_ms, run_id",
        )?;
        let candidates = statement
            .query_map([at_ms], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);

        let mut summary = RetentionSweepSummary::default();
        for (run_id, lifecycle_state) in candidates {
            let updated_at_ms = now_ms();
            let content_removed = transaction.execute(
                "DELETE FROM execution_handoffs WHERE run_id = ?1",
                [&run_id],
            )?;
            let callbacks_removed =
                transaction.execute("DELETE FROM callback_outbox WHERE run_id = ?1", [&run_id])?;
            if content_removed + callbacks_removed > 0 {
                summary.expired_run_content_removed += 1;
            }
            if callbacks_removed > 0 {
                transaction.execute(
                    "UPDATE run_records SET callback_delivery = 'blocked', updated_at_ms = ?2 WHERE run_id = ?1",
                    params![run_id, updated_at_ms],
                )?;
            }
            let container_state: Option<String> = transaction
                .query_row(
                    "SELECT state FROM container_launches WHERE run_id = ?1",
                    [&run_id],
                    |row| row.get(0),
                )
                .optional()?;
            let container_is_safe = container_state
                .as_deref()
                .is_none_or(|state| state == "cleanup_completed");
            if lifecycle_state == "execution_terminal" && container_is_safe {
                summary.expired_runs_removed += transaction.execute(
                    "DELETE FROM run_records WHERE run_id = ?1 AND expires_at_ms <= ?2",
                    params![run_id, at_ms],
                )? as u64;
            } else {
                transaction.execute(
                    "UPDATE run_records SET recovery_action = 'manual_review', updated_at_ms = ?2 WHERE run_id = ?1",
                    params![run_id, updated_at_ms],
                )?;
                summary.expired_runs_fenced_for_cleanup += 1;
            }
        }
        transaction.commit()?;
        connection.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
        Ok(summary)
    }

    pub fn recovery_candidates(&self) -> Result<Vec<RunRecord>, StoreError> {
        let connection = self.lock()?;
        let mut statement = connection.prepare(
            "SELECT run_id FROM run_records
             WHERE lifecycle_state != 'execution_terminal' OR callback_delivery IN ('pending', 'blocked')
             ORDER BY created_at_ms, run_id",
        )?;
        let ids = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        ids.iter()
            .map(|id| {
                let run_id =
                    RunId::parse(id).map_err(|_| StoreError::InvalidStoredValue("run ID"))?;
                load_run(&connection, run_id)
            })
            .collect()
    }

    pub fn enqueue_callback(
        &self,
        callback: CallbackOutboxRecord,
    ) -> Result<CallbackEnqueueResult, StoreError> {
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = now_ms();
        let result = insert_callback(&transaction, &callback, now)?;
        transaction.commit()?;
        Ok(result)
    }

    /// Commits a terminal local transition and its Soyuz callback intent in
    /// one SQLite transaction so a crash cannot leave one without the other.
    pub fn transition_terminal_with_callback(
        &self,
        run_id: RunId,
        callback: CallbackOutboxRecord,
    ) -> Result<(), StoreError> {
        if callback.run_id != run_id
            || !matches!(
                callback.payload,
                CallbackPayload::Completed { .. }
                    | CallbackPayload::Failed { .. }
                    | CallbackPayload::Cancelled { .. }
            )
        {
            return Err(StoreError::InvalidCallbackPayload);
        }
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let record = load_run(&transaction, run_id)?;
        record
            .lifecycle_state
            .transition(LocalLifecycleState::ExecutionTerminal)?;
        let docker_launch = load_container_launch_optional(&transaction, run_id)?;
        if let Some(launch) = &docker_launch
            && (launch.state != ContainerLaunchState::ContainerExited
                || launch.terminal_callback_id != Some(callback.event_id)
                || !callback_matches_container_result(&callback, launch))
        {
            return Err(StoreError::ContainerLaunchResultMismatch);
        }
        let now = now_ms();
        insert_callback(&transaction, &callback, now)?;
        transaction.execute(
            "UPDATE run_records SET lifecycle_state = 'execution_terminal', updated_at_ms = ?2 WHERE run_id = ?1",
            params![run_id.to_string(), now],
        )?;
        if docker_launch.is_some() {
            transaction.execute(
                "UPDATE container_launches SET state = 'cleanup_pending', updated_at_ms = ?2
                 WHERE run_id = ?1 AND state = 'container_exited'",
                params![run_id.to_string(), now],
            )?;
        } else {
            transaction.execute(
                "DELETE FROM execution_slot WHERE slot_id = 1 AND run_id = ?1",
                [run_id.to_string()],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn pending_callbacks(&self) -> Result<Vec<CallbackOutboxRecord>, StoreError> {
        let connection = self.lock()?;
        let mut statement = connection.prepare(
            "SELECT event_id, run_id, payload_json, attempts, next_eligible_at_ms, failure_class
             FROM callback_outbox WHERE delivery_state = 'pending'
             ORDER BY next_eligible_at_ms, created_at_ms, event_id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, u32>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, Option<String>>(5)?,
            ))
        })?;
        rows.map(|row| {
            let (event_id, run_id, payload, attempts, next_eligible_at_ms, failure_class) = row?;
            Ok(CallbackOutboxRecord {
                event_id: EventId::parse(&event_id)
                    .map_err(|_| StoreError::InvalidStoredValue("event ID"))?,
                run_id: RunId::parse(&run_id)
                    .map_err(|_| StoreError::InvalidStoredValue("run ID"))?,
                payload: serde_json::from_str(&payload)?,
                delivery_state: CallbackDeliveryState::Pending,
                attempts,
                next_eligible_at_ms,
                failure_class,
            })
        })
        .collect()
    }

    /// Persist the retry deadline before contacting Soyuz. If the controller
    /// crashes after Soyuz accepts the request, replay still uses the same ID
    /// and waits out this deadline before another attempt.
    pub fn begin_callback_attempt(
        &self,
        event_id: EventId,
        now_ms: i64,
        retry_at_ms: i64,
    ) -> Result<u32, StoreError> {
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = transaction.execute(
            "UPDATE callback_outbox
             SET attempts = attempts + 1, next_eligible_at_ms = ?3,
                 failure_class = NULL, updated_at_ms = ?2
             WHERE event_id = ?1 AND delivery_state = 'pending' AND next_eligible_at_ms <= ?2",
            params![event_id.to_string(), now_ms, retry_at_ms.max(now_ms)],
        )?;
        if changed != 1 {
            return Err(StoreError::CallbackNotEligible);
        }
        let attempts = transaction.query_row(
            "SELECT attempts FROM callback_outbox WHERE event_id = ?1",
            [event_id.to_string()],
            |row| row.get(0),
        )?;
        transaction.commit()?;
        Ok(attempts)
    }

    pub fn defer_callback(
        &self,
        event_id: EventId,
        failure_class: &str,
        next_eligible_at_ms: i64,
    ) -> Result<(), StoreError> {
        if !valid_failure_class(failure_class) || next_eligible_at_ms < 0 {
            return Err(StoreError::InvalidCallbackPayload);
        }
        let connection = self.lock()?;
        let changed = connection.execute(
            "UPDATE callback_outbox
             SET failure_class = ?2, next_eligible_at_ms = ?3, updated_at_ms = ?4
             WHERE event_id = ?1 AND delivery_state = 'pending'",
            params![
                event_id.to_string(),
                failure_class,
                next_eligible_at_ms,
                now_ms()
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::CallbackNotEligible);
        }
        Ok(())
    }

    pub fn block_callback(&self, event_id: EventId, failure_class: &str) -> Result<(), StoreError> {
        if !valid_failure_class(failure_class) {
            return Err(StoreError::InvalidCallbackPayload);
        }
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let run_id: Option<String> = transaction
            .query_row(
                "SELECT run_id FROM callback_outbox
                 WHERE event_id = ?1 AND delivery_state = 'pending'",
                [event_id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        let run_id = run_id.ok_or(StoreError::CallbackNotEligible)?;
        let now = now_ms();
        transaction.execute(
            "UPDATE callback_outbox
             SET delivery_state = 'blocked', failure_class = ?2, updated_at_ms = ?3
             WHERE event_id = ?1",
            params![event_id.to_string(), failure_class, now],
        )?;
        update_run_callback_state(&transaction, &run_id, now)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn callback_outbox_counts(&self) -> Result<(u64, u64), StoreError> {
        let connection = self.lock()?;
        let (pending, blocked): (Option<i64>, Option<i64>) = connection.query_row(
            "SELECT
                SUM(CASE WHEN delivery_state = 'pending' THEN 1 ELSE 0 END),
                SUM(CASE WHEN delivery_state = 'blocked' THEN 1 ELSE 0 END)
             FROM callback_outbox",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        Ok((pending.unwrap_or(0) as u64, blocked.unwrap_or(0) as u64))
    }

    pub fn callback_outbox_status(&self) -> Result<CallbackOutboxStatus, StoreError> {
        let connection = self.lock()?;
        let (pending, blocked, oldest_pending): (Option<i64>, Option<i64>, Option<i64>) =
            connection.query_row(
                "SELECT
                    SUM(CASE WHEN delivery_state = 'pending' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN delivery_state = 'blocked' THEN 1 ELSE 0 END),
                    MIN(CASE WHEN delivery_state = 'pending' THEN created_at_ms END)
                 FROM callback_outbox",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?;
        let pending = pending.unwrap_or(0).max(0) as u64;
        let blocked = blocked.unwrap_or(0).max(0) as u64;
        let oldest_pending_age_ms =
            oldest_pending.map(|created| now_ms().saturating_sub(created).max(0) as u64);
        Ok(CallbackOutboxStatus {
            pending,
            blocked,
            oldest_pending_age_ms,
            intake_backpressured: pending.saturating_add(blocked)
                >= u64::from(MAX_PENDING_CALLBACKS.saturating_sub(1)),
        })
    }

    pub fn mark_callback_delivered(
        &self,
        authorization: CallbackDeliveryAuthorization,
    ) -> Result<(), StoreError> {
        let event_id = authorization.event_id();
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let entry: Option<(String, String, String)> = transaction
            .query_row(
                "SELECT run_id, callback_kind, delivery_state FROM callback_outbox WHERE event_id = ?1",
                [event_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let (run_id, kind, delivery_state) = entry.ok_or(StoreError::CallbackNotEligible)?;
        let operation_matches = match authorization.operation() {
            "event" => matches!(kind.as_str(), "event" | "heartbeat"),
            "completed" | "failed" | "cancelled" => kind == authorization.operation(),
            _ => false,
        };
        if run_id != authorization.run_id().to_string() || !operation_matches {
            return Err(StoreError::CallbackAuthorizationMismatch);
        }
        if delivery_state == "delivered" {
            transaction.commit()?;
            return Ok(());
        }
        if delivery_state != "pending" {
            return Err(StoreError::CallbackNotEligible);
        }
        let now = now_ms();
        transaction.execute(
            "UPDATE callback_outbox
             SET delivery_state = 'delivered', failure_class = NULL, updated_at_ms = ?2
             WHERE event_id = ?1",
            params![event_id.to_string(), now],
        )?;
        update_run_callback_state(&transaction, &run_id, now)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn run_count(&self) -> Result<u64, StoreError> {
        let connection = self.lock()?;
        let count: i64 =
            connection.query_row("SELECT COUNT(*) FROM run_records", [], |row| row.get(0))?;
        Ok(count as u64)
    }

    /// Callback replay remains independent of intake. Intake also reserves one
    /// outbox entry for the active run's mandatory terminal callback.
    pub fn execution_capacity_available(&self) -> Result<bool, StoreError> {
        self.intake_capacity_available()
    }

    pub fn intake_capacity_available(&self) -> Result<bool, StoreError> {
        let connection = self.lock()?;
        let blocked: bool = connection.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM run_records WHERE lifecycle_state != 'execution_terminal'
             ) OR EXISTS(
                 SELECT 1 FROM execution_slot
             )",
            [],
            |row| row.get(0),
        )?;
        let outstanding_callbacks: u32 = connection.query_row(
            "SELECT COUNT(*) FROM callback_outbox WHERE delivery_state IN ('pending', 'blocked')",
            [],
            |row| row.get(0),
        )?;
        Ok(!blocked && outstanding_callbacks < MAX_PENDING_CALLBACKS.saturating_sub(1))
    }

    pub fn recovery_backlog_available(&self) -> Result<bool, StoreError> {
        let connection = self.lock()?;
        let blocked: bool = connection.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM run_records WHERE lifecycle_state != 'execution_terminal'
             ) OR EXISTS(SELECT 1 FROM execution_slot)",
            [],
            |row| row.get(0),
        )?;
        Ok(!blocked)
    }

    pub fn recovery_diagnostics(&self) -> Result<RecoveryDiagnostics, StoreError> {
        let connection = self.lock()?;
        let row = connection.query_row(
            "SELECT
                SUM(CASE WHEN lifecycle_state != 'execution_terminal' THEN 1 ELSE 0 END),
                SUM(recovery_attempts),
                SUM(CASE WHEN recovery_action = 'recovered' THEN 1 ELSE 0 END),
                SUM(CASE WHEN recovery_action = 'awaiting_soyuz' THEN 1 ELSE 0 END),
                SUM(CASE WHEN recovery_action = 'owned_elsewhere' THEN 1 ELSE 0 END),
                SUM(CASE WHEN recovery_action = 'manual_review' THEN 1 ELSE 0 END),
                SUM(CASE WHEN recovery_action = 'ambiguous_execution' THEN 1 ELSE 0 END)
             FROM run_records",
            [],
            |row| {
                Ok(RecoveryDiagnosticRow {
                    unresolved: row.get(0)?,
                    attempts: row.get(1)?,
                    recovered: row.get(2)?,
                    awaiting_soyuz: row.get(3)?,
                    owned_elsewhere: row.get(4)?,
                    manual_review: row.get(5)?,
                    ambiguous: row.get(6)?,
                })
            },
        )?;
        Ok(RecoveryDiagnostics {
            unresolved_runs: row.unresolved.unwrap_or(0).max(0) as u64,
            attempts: row.attempts.unwrap_or(0).max(0) as u64,
            recovered_runs: row.recovered.unwrap_or(0).max(0) as u64,
            awaiting_soyuz_runs: row.awaiting_soyuz.unwrap_or(0).max(0) as u64,
            owned_elsewhere: row.owned_elsewhere.unwrap_or(0).max(0) as u64,
            manual_review_runs: row.manual_review.unwrap_or(0).max(0) as u64,
            ambiguous_execution_runs: row.ambiguous.unwrap_or(0).max(0) as u64,
        })
    }

    fn lock(&self) -> Result<MutexGuard<'_, Connection>, StoreError> {
        self.connection.lock().map_err(|_| StoreError::LockPoisoned)
    }
}

fn load_run(connection: &Connection, run_id: RunId) -> Result<RunRecord, StoreError> {
    let row: Option<StoredRunRow> = connection
        .query_row(
            "SELECT run_id, worker_id, claim_callback_id, start_callback_id,
                    start_authorization_source, start_authorized, callback_delivery,
                    recovery_attempts, recovery_action, created_at_ms, expires_at_ms, updated_at_ms
             FROM run_records WHERE run_id = ?1",
            [run_id.to_string()],
            |row| {
                Ok(StoredRunRow {
                    run_id: row.get(0)?,
                    worker_id: row.get(1)?,
                    claim_callback_id: row.get(2)?,
                    start_callback_id: row.get(3)?,
                    start_authorization_source: row.get(4)?,
                    start_authorized: row.get(5)?,
                    callback_delivery: row.get(6)?,
                    recovery_attempts: row.get(7)?,
                    recovery_action: row.get(8)?,
                    created_at_ms: row.get(9)?,
                    expires_at_ms: row.get(10)?,
                    updated_at_ms: row.get(11)?,
                })
            },
        )
        .optional()?;
    let StoredRunRow {
        run_id: stored_run_id,
        worker_id,
        claim_callback_id,
        start_callback_id,
        start_authorization_source,
        start_authorized,
        callback_delivery,
        recovery_attempts,
        recovery_action,
        created_at_ms,
        expires_at_ms,
        updated_at_ms,
    } = row.ok_or(StoreError::RunNotFound)?;

    let start_callback_id = start_callback_id
        .map(|value| EventId::parse(&value))
        .transpose()
        .map_err(|_| StoreError::InvalidStoredValue("start callback ID"))?;
    let start_authorization_source = start_authorization_source
        .as_deref()
        .map(parse_start_authorization_source)
        .transpose()?;
    if (start_authorized && (start_callback_id.is_none() || start_authorization_source.is_none()))
        || (!start_authorized && start_authorization_source.is_some())
    {
        return Err(StoreError::InvalidStoredValue("start authorization"));
    }

    Ok(RunRecord {
        run_id: RunId::parse(&stored_run_id)
            .map_err(|_| StoreError::InvalidStoredValue("run ID"))?,
        worker_id: WorkerId::parse(worker_id)
            .map_err(|_| StoreError::InvalidStoredValue("worker ID"))?,
        lifecycle_state: parse_lifecycle_state(connection, run_id)?,
        claim_callback_id: claim_callback_id
            .map(|value| ClaimCallbackId::parse(&value))
            .transpose()
            .map_err(|_| StoreError::InvalidStoredValue("claim callback ID"))?,
        start_callback_id,
        start_authorization_source,
        start_authorized,
        callback_delivery: parse_callback_delivery(&callback_delivery)?,
        recovery_attempts,
        recovery_action: RecoveryAction::parse(&recovery_action)?,
        created_at_ms,
        expires_at_ms,
        updated_at_ms,
    })
}

fn parse_lifecycle_state(
    connection: &Connection,
    run_id: RunId,
) -> Result<LocalLifecycleState, StoreError> {
    let value: String = connection.query_row(
        "SELECT lifecycle_state FROM run_records WHERE run_id = ?1",
        [run_id.to_string()],
        |row| row.get(0),
    )?;
    match value.as_str() {
        "queue_observed" => Ok(LocalLifecycleState::QueueObserved),
        "message_validated" => Ok(LocalLifecycleState::MessageValidated),
        "canonical_run_checked" => Ok(LocalLifecycleState::CanonicalRunChecked),
        "claim_intent_persisted" => Ok(LocalLifecycleState::ClaimIntentPersisted),
        "claim_authorized" => Ok(LocalLifecycleState::ClaimAuthorized),
        "start_intent_persisted" => Ok(LocalLifecycleState::StartIntentPersisted),
        "start_authorization_persisted" => Ok(LocalLifecycleState::StartAuthorizationPersisted),
        "execution_active" => Ok(LocalLifecycleState::ExecutionActive),
        "cancellation_requested" => Ok(LocalLifecycleState::CancellationRequested),
        "execution_terminal" => Ok(LocalLifecycleState::ExecutionTerminal),
        _ => Err(StoreError::InvalidStoredValue("lifecycle state")),
    }
}

fn calculate_effective_deadline(
    launch: &ContainerLaunchRecord,
    started_at_ms: i64,
) -> Result<i64, StoreError> {
    let runtime_limit_ms = launch
        .runtime_limit_ms
        .filter(|limit| *limit > 0)
        .ok_or(StoreError::InvalidContainerLaunchResult)?;
    started_at_ms
        .checked_add(runtime_limit_ms)
        .ok_or(StoreError::InvalidContainerLaunchResult)
}

fn load_container_launch(
    connection: &Connection,
    run_id: RunId,
) -> Result<ContainerLaunchRecord, StoreError> {
    load_container_launch_optional(connection, run_id)?
        .ok_or(StoreError::InvalidStoredValue("container launch record"))
}

fn load_container_launch_optional(
    connection: &Connection,
    run_id: RunId,
) -> Result<Option<ContainerLaunchRecord>, StoreError> {
    let row = connection
        .query_row(
            "SELECT worker_id, claim_callback_id, start_callback_id, scenario,
                    container_name, container_id, state, exit_code, outcome_class,
                    diagnostic_summary, output_truncated, terminal_callback_id,
                    stop_reason, stop_requested_at_ms, started_at_ms, completed_at_ms,
                    updated_at_ms, workload_kind, workload_version, image_identity,
                    profile_version, runtime_limit_ms, effective_deadline_at_ms,
                    effective_policy_json, repository_url, authorized_git_ref,
                    resolved_commit, workspace_identity
             FROM container_launches WHERE run_id = ?1",
            [run_id.to_string()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, Option<i32>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, bool>(10)?,
                    row.get::<_, Option<String>>(11)?,
                    row.get::<_, Option<String>>(12)?,
                    row.get::<_, Option<i64>>(13)?,
                    row.get::<_, Option<i64>>(14)?,
                    row.get::<_, Option<i64>>(15)?,
                    row.get::<_, i64>(16)?,
                    row.get::<_, String>(17)?,
                    row.get::<_, i64>(18)?,
                    row.get::<_, Option<String>>(19)?,
                    row.get::<_, String>(20)?,
                    row.get::<_, Option<i64>>(21)?,
                    row.get::<_, Option<i64>>(22)?,
                    row.get::<_, String>(23)?,
                    row.get::<_, Option<String>>(24)?,
                    row.get::<_, Option<String>>(25)?,
                    row.get::<_, Option<String>>(26)?,
                    row.get::<_, Option<String>>(27)?,
                ))
            },
        )
        .optional()?;
    row.map(
        |(
            worker_id,
            claim_callback_id,
            start_callback_id,
            scenario,
            container_name,
            container_id,
            state,
            exit_code,
            outcome_class,
            diagnostic_summary,
            output_truncated,
            terminal_callback_id,
            stop_reason,
            stop_requested_at_ms,
            started_at_ms,
            completed_at_ms,
            updated_at_ms,
            workload_kind,
            workload_version,
            image_identity,
            profile_version,
            runtime_limit_ms,
            effective_deadline_at_ms,
            effective_policy_json,
            repository_url,
            authorized_git_ref,
            resolved_commit,
            workspace_identity,
        )| {
            let workload_version = u32::try_from(workload_version)
                .map_err(|_| StoreError::InvalidStoredValue("workload version"))?;
            Ok(ContainerLaunchRecord {
                run_id,
                worker_id: WorkerId::parse(&worker_id)
                    .map_err(|_| StoreError::InvalidStoredValue("container worker ID"))?,
                claim_callback_id: ClaimCallbackId::parse(&claim_callback_id)
                    .map_err(|_| StoreError::InvalidStoredValue("container claim ID"))?,
                start_callback_id: EventId::parse(&start_callback_id)
                    .map_err(|_| StoreError::InvalidStoredValue("container start ID"))?,
                workload_kind: parse_workload_kind(&workload_kind)?,
                workload_version,
                scenario: parse_synthetic_scenario(&scenario)?,
                image_identity,
                profile_version,
                effective_policy_json,
                repository_url,
                authorized_git_ref,
                resolved_commit,
                workspace_identity,
                runtime_limit_ms,
                effective_deadline_at_ms,
                container_name,
                container_id,
                state: ContainerLaunchState::parse(&state)?,
                exit_code,
                outcome_class,
                diagnostic_summary,
                output_truncated,
                terminal_callback_id: terminal_callback_id
                    .map(|id| {
                        EventId::parse(&id)
                            .map_err(|_| StoreError::InvalidStoredValue("terminal callback ID"))
                    })
                    .transpose()?,
                stop_reason,
                stop_requested_at_ms,
                started_at_ms,
                completed_at_ms,
                updated_at_ms,
            })
        },
    )
    .transpose()
}

fn parse_workload_kind(value: &str) -> Result<WorkloadKind, StoreError> {
    match value {
        "synthetic" => Ok(WorkloadKind::Synthetic),
        "pi_coding_v1" => Ok(WorkloadKind::PiCodingV1),
        _ => Err(StoreError::InvalidStoredValue("container workload kind")),
    }
}

fn valid_repository_url(value: &str) -> bool {
    // This canonical-format check is not a repository allowlist or an egress
    // control. The not-yet-enabled repository preparer must enforce both.
    if value.len() > 2_048 || value.bytes().any(|byte| byte.is_ascii_control()) {
        return false;
    }
    let Ok(url) = Url::parse(value) else {
        return false;
    };
    let Some(host) = url.host_str() else {
        return false;
    };
    url.scheme() == "https"
        && url.username().is_empty()
        && url.password().is_none()
        && url.query().is_none()
        && url.fragment().is_none()
        && url.port_or_known_default() == Some(443)
        && host.parse::<std::net::IpAddr>().is_err()
        && !host.eq_ignore_ascii_case("localhost")
        && !host.to_ascii_lowercase().ends_with(".localhost")
        && !host.to_ascii_lowercase().ends_with(".local")
        && url.as_str() == value
}

fn valid_authorized_git_ref(value: &str) -> bool {
    if value.is_empty()
        || value.len() > 512
        || value.starts_with('-')
        || value == "@"
        || value.contains("..")
        || value.contains("@{")
        || value.ends_with('/')
        || value.ends_with('.')
        || value.contains("//")
    {
        return false;
    }
    value.split('/').all(|component| {
        !component.is_empty()
            && !component.starts_with('.')
            && !component.to_ascii_lowercase().ends_with(".lock")
            && component
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    })
}

fn valid_resolved_commit(value: &str) -> bool {
    matches!(value.len(), 40 | 64)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn valid_workspace_identity(value: &str) -> bool {
    let Some(uuid) = value.strip_prefix("wsp_") else {
        return false;
    };
    uuid.len() == 36
        && uuid
            .parse::<uuid::Uuid>()
            .is_ok_and(|parsed| parsed.to_string() == uuid)
}

fn is_sha256_digest(digest: Option<&str>) -> bool {
    digest.is_some_and(|digest| {
        digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

fn is_pinned_pi_worker_image(image: &str) -> bool {
    let Some((repository, digest)) = image.split_once("@sha256:") else {
        return false;
    };
    repository == TRUSTED_PI_WORKER_IMAGE && is_sha256_digest(Some(digest))
}

fn parse_synthetic_scenario(
    value: &str,
) -> Result<fossil_core::execution::SyntheticScenario, StoreError> {
    match value {
        "success" => Ok(fossil_core::execution::SyntheticScenario::Success),
        "controlled_failure" => Ok(fossil_core::execution::SyntheticScenario::ControlledFailure),
        "timeout" => Ok(fossil_core::execution::SyntheticScenario::Timeout),
        "cancellation" => Ok(fossil_core::execution::SyntheticScenario::Cancellation),
        "bounded_output" => Ok(fossil_core::execution::SyntheticScenario::BoundedOutput),
        "unexpected_termination" => {
            Ok(fossil_core::execution::SyntheticScenario::UnexpectedTermination)
        }
        _ => Err(StoreError::InvalidStoredValue("synthetic scenario")),
    }
}

fn stable_container_name(
    run_id: RunId,
    start_callback_id: EventId,
    workload_kind: WorkloadKind,
) -> String {
    let workload_prefix = match workload_kind {
        WorkloadKind::Synthetic => "synth",
        WorkloadKind::PiCodingV1 => "pi",
    };
    format!("rusty-fossil-{workload_prefix}-{run_id}-{start_callback_id}")
}

fn parse_callback_delivery(value: &str) -> Result<CallbackDeliveryState, StoreError> {
    match value {
        "not_required" => Ok(CallbackDeliveryState::NotRequired),
        "pending" => Ok(CallbackDeliveryState::Pending),
        "delivered" => Ok(CallbackDeliveryState::Delivered),
        "blocked" => Ok(CallbackDeliveryState::Blocked),
        _ => Err(StoreError::InvalidStoredValue("callback delivery state")),
    }
}

fn update_run_callback_state(
    connection: &Connection,
    run_id: &str,
    timestamp_ms: i64,
) -> Result<(), StoreError> {
    let has_pending: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM callback_outbox WHERE run_id = ?1 AND delivery_state = 'pending')",
        [run_id],
        |row| row.get(0),
    )?;
    let has_blocked: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM callback_outbox WHERE run_id = ?1 AND delivery_state = 'blocked')",
        [run_id],
        |row| row.get(0),
    )?;
    let state = if has_pending {
        "pending"
    } else if has_blocked {
        "blocked"
    } else {
        "delivered"
    };
    connection.execute(
        "UPDATE run_records SET callback_delivery = ?2, updated_at_ms = ?3 WHERE run_id = ?1",
        params![run_id, state, timestamp_ms],
    )?;
    Ok(())
}

fn insert_callback(
    transaction: &rusqlite::Transaction<'_>,
    callback: &CallbackOutboxRecord,
    now: i64,
) -> Result<CallbackEnqueueResult, StoreError> {
    callback.payload.validate()?;
    let payload_json = serde_json::to_string(&callback.payload)?;
    if payload_json.len() > MAX_CALLBACK_PAYLOAD_BYTES {
        return Err(StoreError::InvalidCallbackPayload);
    }

    let (worker_id, claim_id): (String, Option<String>) = transaction
        .query_row(
            "SELECT worker_id, claim_callback_id FROM run_records WHERE run_id = ?1",
            [callback.run_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?
        .ok_or(StoreError::RunNotFound)?;
    let expected_claim_id = callback.payload.claim_callback_id().to_string();
    if worker_id != callback.payload.worker_id().as_str()
        || claim_id.as_deref() != Some(expected_claim_id.as_str())
    {
        return Err(StoreError::CallbackAuthorizationMismatch);
    }

    let existing: Option<(String, String, String)> = transaction
        .query_row(
            "SELECT run_id, callback_kind, payload_json FROM callback_outbox WHERE event_id = ?1",
            [callback.event_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let kind = callback_kind_str(callback.payload.kind());
    if let Some((existing_run, existing_kind, existing_payload)) = existing {
        if existing_run != callback.run_id.to_string()
            || existing_kind != kind
            || existing_payload != payload_json
        {
            return Err(StoreError::CallbackIdConflict);
        }
        return Ok(CallbackEnqueueResult::AlreadyPresent);
    }

    let pending_count: u32 = transaction.query_row(
        "SELECT COUNT(*) FROM callback_outbox WHERE delivery_state IN ('pending', 'blocked')",
        [],
        |row| row.get(0),
    )?;
    if callback.payload.is_optional() && pending_count >= MAX_PENDING_CALLBACKS.saturating_sub(1) {
        return Ok(CallbackEnqueueResult::SuppressedCapacity);
    }

    if callback.payload.is_terminal() && pending_count >= MAX_PENDING_CALLBACKS.saturating_sub(1) {
        let active_run: Option<String> = transaction
            .query_row(
                "SELECT run_id FROM execution_slot WHERE slot_id = 1",
                [],
                |row| row.get(0),
            )
            .optional()?;
        if active_run.as_deref() != Some(callback.run_id.to_string().as_str()) {
            return Err(StoreError::OutboxBackpressure);
        }
    }

    if pending_count >= MAX_PENDING_CALLBACKS {
        return Err(StoreError::CallbackOutboxCapacityExceeded);
    }

    transaction.execute(
        "INSERT INTO callback_outbox (
            event_id, run_id, callback_kind, payload_json, delivery_state,
            attempts, next_eligible_at_ms, failure_class, created_at_ms, updated_at_ms
        ) VALUES (?1, ?2, ?3, ?4, 'pending', 0, 0, NULL, ?5, ?5)",
        params![
            callback.event_id.to_string(),
            callback.run_id.to_string(),
            kind,
            payload_json,
            now
        ],
    )?;
    transaction.execute(
        "UPDATE run_records SET callback_delivery = 'pending', updated_at_ms = ?2 WHERE run_id = ?1",
        params![callback.run_id.to_string(), now],
    )?;
    Ok(CallbackEnqueueResult::Enqueued)
}

/// Capped exponential retry with stable per-event jitter so hosts do not
/// synchronize retries and a restarted controller retains the same schedule.
pub fn callback_retry_deadline_ms(now: i64, attempt: u32, event_id: EventId) -> i64 {
    const MAX_DELAY_MS: u64 = 15 * 60 * 1_000;
    let exponent = attempt.saturating_sub(1).min(10);
    let base = (1_000_u64.saturating_mul(1_u64 << exponent)).min(MAX_DELAY_MS);
    let jitter_bound = (base / 2).max(1);
    let hash = event_id
        .as_str()
        .bytes()
        .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
        });
    let delay = base.saturating_add(hash % jitter_bound).min(MAX_DELAY_MS);
    now.saturating_add(delay as i64)
}

fn lifecycle_state_str(value: LocalLifecycleState) -> &'static str {
    match value {
        LocalLifecycleState::QueueObserved => "queue_observed",
        LocalLifecycleState::MessageValidated => "message_validated",
        LocalLifecycleState::CanonicalRunChecked => "canonical_run_checked",
        LocalLifecycleState::ClaimIntentPersisted => "claim_intent_persisted",
        LocalLifecycleState::ClaimAuthorized => "claim_authorized",
        LocalLifecycleState::StartIntentPersisted => "start_intent_persisted",
        LocalLifecycleState::StartAuthorizationPersisted => "start_authorization_persisted",
        LocalLifecycleState::ExecutionActive => "execution_active",
        LocalLifecycleState::CancellationRequested => "cancellation_requested",
        LocalLifecycleState::ExecutionTerminal => "execution_terminal",
    }
}

fn callback_kind_str(value: CallbackKind) -> &'static str {
    match value {
        CallbackKind::Event => "event",
        CallbackKind::Heartbeat => "heartbeat",
        CallbackKind::Completed => "completed",
        CallbackKind::Failed => "failed",
        CallbackKind::Cancelled => "cancelled",
    }
}

fn start_authorization_source_str(value: StartAuthorizationSource) -> &'static str {
    match value {
        StartAuthorizationSource::StartedCallbackAccepted => "started_callback_accepted",
        StartAuthorizationSource::CanonicalStateReconciled => "canonical_state_reconciled",
    }
}

fn parse_start_authorization_source(value: &str) -> Result<StartAuthorizationSource, StoreError> {
    match value {
        "started_callback_accepted" => Ok(StartAuthorizationSource::StartedCallbackAccepted),
        "canonical_state_reconciled" => Ok(StartAuthorizationSource::CanonicalStateReconciled),
        _ => Err(StoreError::InvalidStoredValue("start authorization source")),
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
