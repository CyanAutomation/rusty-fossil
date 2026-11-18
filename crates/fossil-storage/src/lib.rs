use fossil_core::contract::{OperationalEventType, SoyuzRunStatus};
use fossil_core::ids::{ClaimCallbackId, EventId, RunId, WorkerId};
use fossil_core::lifecycle::{CallbackDeliveryState, LifecycleError, LocalLifecycleState};
use fossil_soyuz::{StartAuthorizationSource, StartedAuthorization};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;

const SCHEMA_VERSION: u32 = 1;
const MAX_CALLBACK_PAYLOAD_BYTES: usize = 12 * 1024;

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
    Terminal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CallbackPayload {
    Event {
        event_type: OperationalEventType,
        #[serde(skip_serializing_if = "Option::is_none")]
        stage: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        step: Option<String>,
        timestamp_ms: i64,
    },
    Heartbeat {
        timestamp_ms: i64,
    },
    Terminal {
        status: SoyuzRunStatus,
        completed_at_ms: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        exit_code: Option<i32>,
    },
}

impl CallbackPayload {
    fn kind(&self) -> CallbackKind {
        match self {
            Self::Event { .. } => CallbackKind::Event,
            Self::Heartbeat { .. } => CallbackKind::Heartbeat,
            Self::Terminal { .. } => CallbackKind::Terminal,
        }
    }

    fn validate(&self) -> Result<(), StoreError> {
        let valid_label = |value: &str| {
            !value.is_empty() && value.len() <= 128 && !value.chars().any(char::is_control)
        };
        match self {
            Self::Event {
                stage,
                step,
                timestamp_ms,
                ..
            } => {
                if *timestamp_ms < 0
                    || stage.as_deref().is_some_and(|value| !valid_label(value))
                    || step.as_deref().is_some_and(|value| !valid_label(value))
                {
                    return Err(StoreError::InvalidCallbackPayload);
                }
            }
            Self::Heartbeat { timestamp_ms } if *timestamp_ms < 0 => {
                return Err(StoreError::InvalidCallbackPayload);
            }
            Self::Terminal {
                status,
                completed_at_ms,
                ..
            } if *completed_at_ms < 0
                || !matches!(
                    status,
                    SoyuzRunStatus::Completed | SoyuzRunStatus::Failed | SoyuzRunStatus::Cancelled
                ) =>
            {
                return Err(StoreError::InvalidCallbackPayload);
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallbackOutboxRecord {
    pub event_id: EventId,
    pub run_id: RunId,
    pub payload: CallbackPayload,
    pub delivery_state: CallbackDeliveryState,
    pub attempts: u32,
}

impl CallbackOutboxRecord {
    pub fn new(event_id: EventId, run_id: RunId, payload: CallbackPayload) -> Self {
        Self {
            event_id,
            run_id,
            payload,
            delivery_state: CallbackDeliveryState::Pending,
            attempts: 0,
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
    #[error(
        "start authorization can only be persisted with a successful Soyuz authorization proof"
    )]
    AuthorizationProofRequired,
    #[error("Soyuz start authorization does not match the persisted run, worker, or claim")]
    StartAuthorizationMismatch,
    #[error("callback event ID is already bound to different callback data")]
    CallbackIdConflict,
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
            std::fs::create_dir_all(parent)?;
        }
        let mut connection = Connection::open(path)?;
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
            INSERT INTO schema_migrations(version, applied_at_ms) VALUES (1, 0);
            PRAGMA user_version = 1;",
        )?;
        transaction.commit()?;
        Ok(())
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
        if next == LocalLifecycleState::StartAuthorizationPersisted {
            return Err(StoreError::AuthorizationProofRequired);
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
        {
            return Err(StoreError::StartAuthorizationMismatch);
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
                 start_callback_id = ?2, start_authorization_source = ?3,
                 start_authorized = 1, updated_at_ms = ?4
             WHERE run_id = ?1 AND worker_id = ?5 AND claim_callback_id = ?6
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

    pub fn recovery_candidates(&self) -> Result<Vec<RunRecord>, StoreError> {
        let connection = self.lock()?;
        let mut statement = connection.prepare(
            "SELECT run_id FROM run_records
             WHERE lifecycle_state != 'execution_terminal' OR callback_delivery = 'pending'
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
        callback.payload.validate()?;
        let payload_json = serde_json::to_string(&callback.payload)?;
        if payload_json.len() > MAX_CALLBACK_PAYLOAD_BYTES {
            return Err(StoreError::InvalidCallbackPayload);
        }

        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing: Option<(String, String)> = transaction
            .query_row(
                "SELECT callback_kind, payload_json FROM callback_outbox WHERE event_id = ?1",
                [callback.event_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let kind = callback_kind_str(callback.payload.kind());
        if let Some((existing_kind, existing_payload)) = existing {
            if existing_kind != kind || existing_payload != payload_json {
                return Err(StoreError::CallbackIdConflict);
            }
            transaction.commit()?;
            return Ok(());
        }

        let now = now_ms();
        transaction.execute(
            "INSERT INTO callback_outbox (
                event_id, run_id, callback_kind, payload_json, delivery_state,
                attempts, created_at_ms, updated_at_ms
            ) VALUES (?1, ?2, ?3, ?4, 'pending', 0, ?5, ?5)",
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
        transaction.commit()?;
        Ok(())
    }

    pub fn pending_callbacks(&self) -> Result<Vec<CallbackOutboxRecord>, StoreError> {
        let connection = self.lock()?;
        let mut statement = connection.prepare(
            "SELECT event_id, run_id, payload_json, attempts
             FROM callback_outbox WHERE delivery_state = 'pending'
             ORDER BY created_at_ms, event_id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, u32>(3)?,
            ))
        })?;
        rows.map(|row| {
            let (event_id, run_id, payload, attempts) = row?;
            Ok(CallbackOutboxRecord {
                event_id: EventId::parse(&event_id)
                    .map_err(|_| StoreError::InvalidStoredValue("event ID"))?,
                run_id: RunId::parse(&run_id)
                    .map_err(|_| StoreError::InvalidStoredValue("run ID"))?,
                payload: serde_json::from_str(&payload)?,
                delivery_state: CallbackDeliveryState::Pending,
                attempts,
            })
        })
        .collect()
    }

    pub fn mark_callback_delivered(&self, event_id: EventId) -> Result<(), StoreError> {
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let run_id: Option<String> = transaction
            .query_row(
                "SELECT run_id FROM callback_outbox WHERE event_id = ?1",
                [event_id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        let run_id = run_id.ok_or(StoreError::RunNotFound)?;
        let now = now_ms();
        transaction.execute(
            "UPDATE callback_outbox
             SET delivery_state = 'delivered', attempts = attempts + 1, updated_at_ms = ?2
             WHERE event_id = ?1",
            params![event_id.to_string(), now],
        )?;
        transaction.execute(
            "UPDATE run_records
             SET callback_delivery = CASE
                 WHEN EXISTS (
                     SELECT 1 FROM callback_outbox
                     WHERE run_id = ?1 AND delivery_state = 'pending'
                 ) THEN 'pending' ELSE 'delivered' END,
                 updated_at_ms = ?2
             WHERE run_id = ?1",
            params![run_id, now],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn run_count(&self) -> Result<u64, StoreError> {
        let connection = self.lock()?;
        let count: i64 =
            connection.query_row("SELECT COUNT(*) FROM run_records", [], |row| row.get(0))?;
        Ok(count as u64)
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
    if start_authorized != start_callback_id.is_some()
        || start_authorized != start_authorization_source.is_some()
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
        _ => Err(StoreError::InvalidStoredValue("callback delivery state")),
    }
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
        CallbackKind::Terminal => "terminal",
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
