use fossil_core::contract::{ContractVersion, OperationalEventType};
use fossil_core::ids::{ClaimCallbackId, EventId, RunId, WorkerId};
use fossil_core::lifecycle::{CallbackDeliveryState, LifecycleError, LocalLifecycleState};
use fossil_core::queue::{QueueEnvelope, SoyuzRunRequest};
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

const SCHEMA_VERSION: u32 = 3;
const MAX_CALLBACK_PAYLOAD_BYTES: usize = 12 * 1024;
const MAX_PENDING_CALLBACKS: u32 = 1_000;
const MAX_EXECUTION_DESCRIPTOR_BYTES: usize = 96 * 1024;

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
    RetryCallback,
    ManualReview,
}

impl RecoveryAction {
    fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::ReconcileWithSoyuz => "reconcile_with_soyuz",
            Self::RetryCallback => "retry_callback",
            Self::ManualReview => "manual_review",
        }
    }

    fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "none" => Ok(Self::None),
            "reconcile_with_soyuz" => Ok(Self::ReconcileWithSoyuz),
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
    pub updated_at_ms: i64,
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
    #[error("execution descriptor can only be removed after terminal callbacks are delivered")]
    ExecutionDescriptorRetentionDenied,
    #[error("callback event ID is already bound to different callback data")]
    CallbackIdConflict,
    #[error("callback worker or claim fence does not match the durable run record")]
    CallbackAuthorizationMismatch,
    #[error("callback outbox reached its configured capacity")]
    CallbackOutboxCapacityExceeded,
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
        transaction.execute(
            "INSERT INTO run_records (
                run_id, worker_id, lifecycle_state, created_at_ms, updated_at_ms
            ) VALUES (?1, ?2, 'queue_observed', ?3, ?3)",
            params![run_id.to_string(), worker_id.as_str(), now],
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
               AND lifecycle_state = 'claim_authorized'",
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
        if current.lifecycle_state != LocalLifecycleState::ClaimAuthorized {
            return Err(StoreError::StartAuthorizationMismatch);
        }
        let updated = transaction.execute(
            "UPDATE run_records SET start_callback_id = ?2, updated_at_ms = ?3
             WHERE run_id = ?1 AND start_callback_id IS NULL
               AND lifecycle_state = 'claim_authorized'",
            params![run_id.to_string(), callback_id.to_string(), now_ms()],
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

    pub fn release_execution_slot(&self, run_id: RunId) -> Result<(), StoreError> {
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let record = load_run(&transaction, run_id)?;
        if record.lifecycle_state != LocalLifecycleState::ExecutionTerminal {
            return Err(StoreError::ExecutionSlotReleaseDenied);
        }
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

    pub fn enqueue_callback(&self, callback: CallbackOutboxRecord) -> Result<(), StoreError> {
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = now_ms();
        insert_callback(&transaction, &callback, now)?;
        transaction.commit()?;
        Ok(())
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
        let now = now_ms();
        insert_callback(&transaction, &callback, now)?;
        transaction.execute(
            "UPDATE run_records SET lifecycle_state = 'execution_terminal', updated_at_ms = ?2 WHERE run_id = ?1",
            params![run_id.to_string(), now],
        )?;
        transaction.execute(
            "DELETE FROM execution_slot WHERE slot_id = 1 AND run_id = ?1",
            [run_id.to_string()],
        )?;
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

    /// Callback replay is independent from execution capacity. Only unresolved
    /// run recovery state or a reserved single slot prevents another pull.
    pub fn execution_capacity_available(&self) -> Result<bool, StoreError> {
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
        Ok(!blocked)
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
                    recovery_attempts, recovery_action, created_at_ms, updated_at_ms
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
                    updated_at_ms: row.get(10)?,
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
        "start_authorization_persisted" => Ok(LocalLifecycleState::StartAuthorizationPersisted),
        "execution_active" => Ok(LocalLifecycleState::ExecutionActive),
        "cancellation_requested" => Ok(LocalLifecycleState::CancellationRequested),
        "execution_terminal" => Ok(LocalLifecycleState::ExecutionTerminal),
        _ => Err(StoreError::InvalidStoredValue("lifecycle state")),
    }
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
) -> Result<(), StoreError> {
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
        return Ok(());
    }

    let pending_count: u32 = transaction.query_row(
        "SELECT COUNT(*) FROM callback_outbox WHERE delivery_state IN ('pending', 'blocked')",
        [],
        |row| row.get(0),
    )?;
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
    Ok(())
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
