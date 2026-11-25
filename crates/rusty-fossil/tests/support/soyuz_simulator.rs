//! Test-only stateful implementation of the Soyuz worker HTTP contract.
//!
//! This intentionally models only the worker routes consumed by Rusty-Fossil.
//! It is based on Soyuz's `docs/CONTRACT.md`, `src/api/workers.ts`,
//! `src/db/runs.ts`, and `tests/api/worker-api.test.ts`; it is not a copy of
//! Soyuz's D1/Queue service and does not claim to replace tests against Soyuz.

use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use fossil_core::contract::SoyuzRunStatus;
use fossil_core::ids::RunId;
use serde_json::{Map, Value, json};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use url::Url;
use uuid::Uuid;

const DEFAULT_LEASE_SECONDS: u64 = 120;
const MAX_EVENT_BYTES: usize = 12 * 1024;

#[derive(Clone)]
pub struct SoyuzSimulator {
    inner: Arc<Mutex<SimulatorState>>,
    origin: Url,
    token: Arc<str>,
    _server: Arc<ServerTask>,
}

struct ServerTask(JoinHandle<()>);

impl Drop for ServerTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Debug, Clone)]
pub struct SoyuzSimulatorSnapshot {
    pub status: SoyuzRunStatus,
    pub worker_id: Option<String>,
    pub claim_callback_id: Option<String>,
    pub claim_expires_at: Option<String>,
    pub cancel_requested_at: Option<String>,
    pub stage: Option<String>,
    pub operational_health: String,
    callbacks: Vec<String>,
    requests: HashMap<String, usize>,
}

impl SoyuzSimulatorSnapshot {
    pub fn callback_count(&self, operation: &str) -> usize {
        self.callbacks
            .iter()
            .filter(|seen| seen.as_str() == operation)
            .count()
    }

    pub fn request_count(&self, operation: &str) -> usize {
        self.requests.get(operation).copied().unwrap_or_default()
    }
}

impl SoyuzSimulator {
    pub async fn start(token: impl Into<String>) -> Result<Self, std::io::Error> {
        let token: Arc<str> = Arc::from(token.into());
        let inner = Arc::new(Mutex::new(SimulatorState::new()));
        let app = router(Arc::clone(&inner), Arc::clone(&token));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self {
            inner,
            origin: Url::parse(&format!("http://{address}")).expect("bound address is a URL"),
            token,
            _server: Arc::new(ServerTask(server)),
        })
    }

    pub fn uri(&self) -> String {
        self.origin.as_str().trim_end_matches('/').to_owned()
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    pub async fn seed_queued_run(&self, run_id: RunId, display_id: impl Into<String>) {
        let mut state = self.inner.lock().await;
        let now = state.now_ms;
        state.runs.insert(
            run_id.to_string(),
            SimRun::queued(run_id.to_string(), display_id.into(), now),
        );
    }

    /// Simulates Soyuz's queued/claimed cancellation and running cancellation
    /// intent transitions through its run-control API.
    pub async fn request_cancel(&self, run_id: RunId) -> Result<(), String> {
        let mut state = self.inner.lock().await;
        let now = state.now_ms;
        let run = state
            .runs
            .get_mut(&run_id.to_string())
            .ok_or_else(|| "run not found".to_owned())?;
        match run.status {
            SoyuzRunStatus::Queued | SoyuzRunStatus::Claimed => {
                run.status = SoyuzRunStatus::Cancelled;
                run.cancel_requested_at = Some(format_time(now));
            }
            SoyuzRunStatus::Running => {
                run.status = SoyuzRunStatus::CancelRequested;
                run.cancel_requested_at = Some(format_time(now));
            }
            SoyuzRunStatus::CancelRequested => {}
            SoyuzRunStatus::Admitting
            | SoyuzRunStatus::Completed
            | SoyuzRunStatus::Failed
            | SoyuzRunStatus::Cancelled
            | SoyuzRunStatus::AdmissionFailed => {}
        }
        run.updated_at = format_time(now);
        Ok(())
    }

    /// Makes the next successful operation commit its state change and then
    /// return a service error, reproducing an ambiguous lost response.
    pub async fn fail_after_commit_once(&self, run_id: RunId, operation: &str) {
        let mut state = self.inner.lock().await;
        state
            .fail_after_commit
            .insert((run_id.to_string(), operation.to_owned()));
    }

    /// Runs cancellation after Soyuz commits `/started` but before returning
    /// its response, making the coordinator's final canonical gate observable.
    pub async fn cancel_after_started_once(&self, run_id: RunId) {
        self.inner
            .lock()
            .await
            .cancel_after_started
            .insert(run_id.to_string());
    }

    /// Advances the simulator clock without sleeping so lease-boundary tests
    /// remain deterministic.
    pub async fn advance_time(&self, duration: Duration) {
        let mut state = self.inner.lock().await;
        let millis = i64::try_from(duration.as_millis()).unwrap_or(i64::MAX);
        state.now_ms = state.now_ms.saturating_add(millis);
    }

    /// Models Soyuz's scheduled expired-claim reconciliation and subsequent
    /// requeue. It does not emulate Cloudflare Queue publication or leases.
    pub async fn reconcile_expired_claim(&self, run_id: RunId) -> Result<bool, String> {
        let mut state = self.inner.lock().await;
        let now_ms = state.now_ms;
        let run = state
            .runs
            .get_mut(&run_id.to_string())
            .ok_or_else(|| "run not found".to_owned())?;
        if run.status != SoyuzRunStatus::Claimed || !claim_expired(run, now_ms) {
            return Ok(false);
        }
        run.status = SoyuzRunStatus::Queued;
        run.worker_id = None;
        run.claim_expires_at = None;
        run.updated_at = format_time(now_ms);
        Ok(true)
    }

    pub async fn snapshot(&self, run_id: RunId) -> Result<SoyuzSimulatorSnapshot, String> {
        let state = self.inner.lock().await;
        let run = state
            .runs
            .get(&run_id.to_string())
            .ok_or_else(|| "run not found".to_owned())?;
        Ok(SoyuzSimulatorSnapshot {
            status: run.status,
            worker_id: run.worker_id.clone(),
            claim_callback_id: run.claim_callback_id.clone(),
            claim_expires_at: run.claim_expires_at.clone(),
            cancel_requested_at: run.cancel_requested_at.clone(),
            stage: run.stage.clone(),
            operational_health: run.operational_health.clone(),
            callbacks: run
                .callbacks
                .values()
                .map(|event| event.operation.clone())
                .collect(),
            requests: run.request_counts.clone(),
        })
    }
}

#[derive(Clone)]
struct SimulatorState {
    now_ms: i64,
    runs: HashMap<String, SimRun>,
    fail_after_commit: HashSet<(String, String)>,
    cancel_after_started: HashSet<String>,
}

impl SimulatorState {
    fn new() -> Self {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(i64::MAX as u128) as i64;
        Self {
            now_ms,
            runs: HashMap::new(),
            fail_after_commit: HashSet::new(),
            cancel_after_started: HashSet::new(),
        }
    }
}

#[derive(Clone)]
struct SimRun {
    run_id: String,
    display_id: String,
    status: SoyuzRunStatus,
    worker_id: Option<String>,
    claim_callback_id: Option<String>,
    claim_expires_at: Option<String>,
    cancel_requested_at: Option<String>,
    stage: Option<String>,
    updated_at: String,
    last_heartbeat_at: Option<String>,
    operational_health: String,
    callbacks: HashMap<String, RecordedCallback>,
    request_counts: HashMap<String, usize>,
}

impl SimRun {
    fn queued(run_id: String, display_id: String, now_ms: i64) -> Self {
        Self {
            run_id,
            display_id,
            status: SoyuzRunStatus::Queued,
            worker_id: None,
            claim_callback_id: None,
            claim_expires_at: None,
            cancel_requested_at: None,
            stage: None,
            updated_at: format_time(now_ms),
            last_heartbeat_at: None,
            operational_health: "unknown".to_owned(),
            callbacks: HashMap::new(),
            request_counts: HashMap::new(),
        }
    }

    fn canonical_json(&self) -> Value {
        json!({
            "runId": self.run_id,
            "displayId": self.display_id,
            "contractVersion": "1",
            "status": self.status.as_str(),
            "cancelRequestedAt": self.cancel_requested_at,
            "claimExpiresAt": self.claim_expires_at,
            "workerId": self.worker_id,
            "claimCallbackId": self.claim_callback_id,
            "stage": self.stage,
            "updatedAt": self.updated_at,
            "lastHeartbeatAt": self.last_heartbeat_at,
            "operationalHealth": self.operational_health,
            "origin": { "type": "kaseki", "id": "soyuz-simulator" },
            "retryOf": null,
        })
    }
}

#[derive(Clone)]
struct RecordedCallback {
    operation: String,
    kind: String,
    worker_id: String,
    stage: Option<String>,
    payload: Value,
}

impl RecordedCallback {
    fn matches(&self, other: &Self) -> bool {
        self.kind == other.kind
            && self.worker_id == other.worker_id
            && self.stage == other.stage
            && self.payload == other.payload
    }
}

fn router(inner: Arc<Mutex<SimulatorState>>, token: Arc<str>) -> Router {
    Router::new()
        .route("/v1/worker/runs/{run_id}", get(read_run))
        .route("/v1/worker/runs/{run_id}/claim", post(claim))
        .route("/v1/worker/runs/{run_id}/started", post(started))
        .route("/v1/worker/runs/{run_id}/events", post(event))
        .route("/v1/worker/runs/{run_id}/completed", post(completed))
        .route("/v1/worker/runs/{run_id}/failed", post(failed))
        .route("/v1/worker/runs/{run_id}/cancelled", post(cancelled))
        .layer(DefaultBodyLimit::max(96 * 1024))
        .with_state((inner, token))
}

type ServerState = (Arc<Mutex<SimulatorState>>, Arc<str>);
type SimResponse = (StatusCode, Json<Value>);

async fn read_run(
    State((state, token)): State<ServerState>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> SimResponse {
    if !authorized(&headers, &token) {
        return error(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "Worker token rejected",
        );
    }
    let mut state = state.lock().await;
    let Some(run) = state.runs.get_mut(&run_id) else {
        return error(StatusCode::NOT_FOUND, "RUN_NOT_FOUND", "Run was not found");
    };
    *run.request_counts.entry("read".to_owned()).or_default() += 1;
    success(run.canonical_json())
}

async fn claim(
    State((state, token)): State<ServerState>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> SimResponse {
    post_operation(state, token, run_id, "claim", headers, body).await
}

async fn started(
    State((state, token)): State<ServerState>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> SimResponse {
    post_operation(state, token, run_id, "started", headers, body).await
}

async fn event(
    State((state, token)): State<ServerState>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> SimResponse {
    post_operation(state, token, run_id, "events", headers, body).await
}

async fn completed(
    State((state, token)): State<ServerState>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> SimResponse {
    post_operation(state, token, run_id, "completed", headers, body).await
}

async fn failed(
    State((state, token)): State<ServerState>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> SimResponse {
    post_operation(state, token, run_id, "failed", headers, body).await
}

async fn cancelled(
    State((state, token)): State<ServerState>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> SimResponse {
    post_operation(state, token, run_id, "cancelled", headers, body).await
}

async fn post_operation(
    shared: Arc<Mutex<SimulatorState>>,
    token: Arc<str>,
    run_id: String,
    operation: &'static str,
    headers: HeaderMap,
    body: Value,
) -> SimResponse {
    if !authorized(&headers, &token) {
        return error(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "Worker token rejected",
        );
    }
    let mut state = shared.lock().await;
    let now_ms = state.now_ms;
    let data = {
        let Some(run) = state.runs.get_mut(&run_id) else {
            return error(StatusCode::NOT_FOUND, "RUN_NOT_FOUND", "Run was not found");
        };
        *run.request_counts.entry(operation.to_owned()).or_default() += 1;
        let result = match operation {
            "claim" => apply_claim(run, &body, now_ms),
            "started" => apply_started(run, &body, now_ms),
            "events" => apply_event(run, &body, now_ms),
            "completed" => apply_terminal(run, &body, now_ms, "completed"),
            "failed" => apply_terminal(run, &body, now_ms, "failed"),
            "cancelled" => apply_terminal(run, &body, now_ms, "cancelled"),
            _ => unreachable!("routes name supported operations"),
        };
        let data = match result {
            Ok(data) => data,
            Err(failure) => return failure.response(),
        };
        run.updated_at = format_time(now_ms);
        data
    };
    if operation == "started" && state.cancel_after_started.remove(&run_id) {
        let run = state.runs.get_mut(&run_id).expect("run was checked above");
        run.status = SoyuzRunStatus::CancelRequested;
        run.cancel_requested_at = Some(format_time(now_ms));
    }
    if state
        .fail_after_commit
        .remove(&(run_id.clone(), operation.to_owned()))
    {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "CALLBACK_UNAVAILABLE",
            "Soyuz committed the operation but its response was lost.",
        );
    }
    if operation == "events" {
        success_with_status(StatusCode::ACCEPTED, data)
    } else {
        success(data)
    }
}

fn apply_claim(run: &mut SimRun, body: &Value, now_ms: i64) -> Result<Value, ApiFailure> {
    strict_body(
        body,
        &["contractVersion", "callbackId", "workerId", "leaseSeconds"],
        &["contractVersion", "callbackId", "workerId"],
    )?;
    require_version(body)?;
    let callback_id = required_uuid(body, "callbackId")?;
    let worker_id = required_string(body, "workerId")?;
    let lease_seconds = match body.get("leaseSeconds") {
        None => DEFAULT_LEASE_SECONDS,
        Some(Value::Number(value)) => value
            .as_u64()
            .ok_or_else(|| invalid_callback("Claim lease must be an integer"))?,
        Some(_) => return Err(invalid_callback("Claim lease must be an integer")),
    };
    if !(30..=300).contains(&lease_seconds) {
        return Err(ApiFailure::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "INVALID_WORKER_CALLBACK",
            "Claim lease is outside the contract range",
        ));
    }
    let fingerprint = RecordedCallback {
        operation: "claim".to_owned(),
        kind: "run.claimed".to_owned(),
        worker_id: worker_id.to_owned(),
        stage: None,
        payload: json!({ "leaseSeconds": lease_seconds }),
    };
    if let Some(previous) = run.callbacks.get(callback_id) {
        if !previous.matches(&fingerprint) {
            return Err(callback_id_reused());
        }
        if run.claim_callback_id.as_deref() != Some(callback_id) {
            return Err(stale_claim());
        }
        return Ok(run.canonical_json());
    }
    if matches!(
        run.status,
        SoyuzRunStatus::Claimed | SoyuzRunStatus::Running
    ) {
        if run.worker_id.as_deref() == Some(worker_id) {
            return Err(stale_claim());
        }
        return Err(ApiFailure::new(
            StatusCode::CONFLICT,
            "RUN_CLAIMED",
            "Another worker already owns this run",
        ));
    }
    if run.status != SoyuzRunStatus::Queued {
        return Err(ApiFailure::new(
            StatusCode::CONFLICT,
            "INVALID_STATE_TRANSITION",
            "Run is not queued",
        ));
    }
    let expiry = now_ms.saturating_add((lease_seconds as i64).saturating_mul(1_000));
    run.status = SoyuzRunStatus::Claimed;
    run.worker_id = Some(worker_id.to_owned());
    run.claim_callback_id = Some(callback_id.to_owned());
    run.claim_expires_at = Some(format_time(expiry));
    run.callbacks.insert(callback_id.to_owned(), fingerprint);
    Ok(run.canonical_json())
}

fn apply_started(run: &mut SimRun, body: &Value, now_ms: i64) -> Result<Value, ApiFailure> {
    strict_body(
        body,
        &[
            "contractVersion",
            "callbackId",
            "workerId",
            "claimCallbackId",
            "startedAt",
            "stage",
        ],
        &[
            "contractVersion",
            "callbackId",
            "workerId",
            "claimCallbackId",
        ],
    )?;
    require_version(body)?;
    let callback_id = required_uuid(body, "callbackId")?;
    let worker_id = required_string(body, "workerId")?;
    let claim_id = required_uuid(body, "claimCallbackId")?;
    validate_optional_timestamp(body, "startedAt")?;
    validate_label(body, "stage", 120)?;
    ensure_owner_and_fence(run, worker_id, Some(claim_id))?;
    let stage = optional_string(body, "stage").map(str::to_owned);
    let fingerprint = RecordedCallback {
        operation: "started".to_owned(),
        kind: "run.started".to_owned(),
        worker_id: worker_id.to_owned(),
        stage: stage.clone(),
        payload: json!({ "status": "running" }),
    };
    if let Some(previous) = run.callbacks.get(callback_id) {
        if !previous.matches(&fingerprint) {
            return Err(callback_id_reused());
        }
        return Ok(run.canonical_json());
    }
    if run.status == SoyuzRunStatus::Running {
        return Ok(run.canonical_json());
    }
    if run.status == SoyuzRunStatus::Cancelled {
        return Err(ApiFailure::new(
            StatusCode::CONFLICT,
            "RUN_CANCELLED",
            "Run was cancelled before it started",
        ));
    }
    if run.status != SoyuzRunStatus::Claimed {
        return Err(ApiFailure::new(
            StatusCode::CONFLICT,
            "INVALID_STATE_TRANSITION",
            "Run cannot start from its current state",
        ));
    }
    if claim_expired(run, now_ms) {
        return Err(ApiFailure::new(
            StatusCode::CONFLICT,
            "CLAIM_EXPIRED",
            "Worker claim expired before start",
        ));
    }
    run.status = SoyuzRunStatus::Running;
    run.claim_expires_at = None;
    run.stage = stage;
    run.operational_health = "healthy".to_owned();
    run.updated_at = format_time(now_ms);
    run.callbacks.insert(callback_id.to_owned(), fingerprint);
    Ok(run.canonical_json())
}

fn apply_event(run: &mut SimRun, body: &Value, now_ms: i64) -> Result<Value, ApiFailure> {
    strict_body(
        body,
        &[
            "contractVersion",
            "eventId",
            "workerId",
            "claimCallbackId",
            "type",
            "stage",
            "step",
            "timestamp",
            "payload",
        ],
        &["contractVersion", "eventId", "workerId", "type"],
    )?;
    require_version(body)?;
    let event_id = required_uuid(body, "eventId")?;
    let worker_id = required_string(body, "workerId")?;
    let event_type = required_string(body, "type")?;
    let stage = optional_string(body, "stage").map(str::to_owned);
    validate_optional_timestamp(body, "timestamp")?;
    validate_label(body, "stage", 120)?;
    validate_label(body, "step", 160)?;
    if ![
        "stage.changed",
        "step.changed",
        "progress.updated",
        "worker.heartbeat",
        "validation.completed",
        "publish.completed",
        "artifacts.uploaded",
    ]
    .contains(&event_type)
    {
        return Err(invalid_callback("Unknown worker event type"));
    }
    let payload = body.get("payload").cloned().unwrap_or_else(|| json!({}));
    if serde_json::to_vec(&payload).unwrap_or_default().len() > MAX_EVENT_BYTES {
        return Err(invalid_callback("Worker event payload exceeds 12 KB"));
    }
    if event_type == "stage.changed" && stage.is_none() {
        return Err(invalid_callback("stage is required for stage.changed"));
    }
    if event_type == "step.changed" && optional_string(body, "step").is_none() {
        return Err(invalid_callback("step is required for step.changed"));
    }
    let claim_id = optional_string(body, "claimCallbackId");
    ensure_owner_and_fence(run, worker_id, claim_id)?;
    let fingerprint = RecordedCallback {
        operation: "events".to_owned(),
        kind: event_type.to_owned(),
        worker_id: worker_id.to_owned(),
        stage: stage.clone(),
        payload: payload.clone(),
    };
    if let Some(previous) = run.callbacks.get(event_id) {
        if !previous.matches(&fingerprint) {
            return Err(callback_id_reused());
        }
        return Ok(json!({ "eventId": event_id, "type": event_type }));
    }
    if !matches!(
        run.status,
        SoyuzRunStatus::Running | SoyuzRunStatus::CancelRequested
    ) {
        return Err(ApiFailure::new(
            StatusCode::CONFLICT,
            "RUN_NOT_ACTIVE",
            "Run no longer accepts operational events",
        ));
    }
    if event_type == "stage.changed" {
        run.stage = stage;
    }
    if event_type == "worker.heartbeat" {
        let unhealthy = payload.get("executionActive").and_then(Value::as_bool) == Some(false)
            || payload.get("hostHealthy").and_then(Value::as_bool) == Some(false);
        run.operational_health = if unhealthy {
            "suspect_stalled"
        } else {
            "healthy"
        }
        .to_owned();
        run.last_heartbeat_at = Some(format_time(now_ms));
    }
    run.callbacks.insert(event_id.to_owned(), fingerprint);
    Ok(json!({ "eventId": event_id, "type": event_type }))
}

fn apply_terminal(
    run: &mut SimRun,
    body: &Value,
    now_ms: i64,
    terminal: &str,
) -> Result<Value, ApiFailure> {
    let (allowed, required): (&[&str], &[&str]) = match terminal {
        "completed" => (
            &[
                "contractVersion",
                "callbackId",
                "workerId",
                "claimCallbackId",
                "completedAt",
                "exitCode",
                "summary",
                "publishedUrl",
                "changedFileCount",
                "outcomeSummary",
            ],
            &["contractVersion", "callbackId", "workerId"],
        ),
        "failed" => (
            &[
                "contractVersion",
                "callbackId",
                "workerId",
                "claimCallbackId",
                "completedAt",
                "exitCode",
                "failureClass",
                "failureMessage",
                "summary",
                "publishedUrl",
                "changedFileCount",
                "outcomeSummary",
            ],
            &[
                "contractVersion",
                "callbackId",
                "workerId",
                "failureClass",
                "failureMessage",
            ],
        ),
        "cancelled" => (
            &[
                "contractVersion",
                "callbackId",
                "workerId",
                "claimCallbackId",
                "completedAt",
                "exitCode",
                "reason",
                "summary",
                "publishedUrl",
                "changedFileCount",
                "outcomeSummary",
            ],
            &["contractVersion", "callbackId", "workerId"],
        ),
        _ => unreachable!(),
    };
    strict_body(body, allowed, required)?;
    require_version(body)?;
    let callback_id = required_uuid(body, "callbackId")?;
    let worker_id = required_string(body, "workerId")?;
    let claim_id = optional_string(body, "claimCallbackId");
    ensure_owner_and_fence(run, worker_id, claim_id)?;
    if !matches!(
        run.status,
        SoyuzRunStatus::Claimed
            | SoyuzRunStatus::Running
            | SoyuzRunStatus::CancelRequested
            | SoyuzRunStatus::Completed
            | SoyuzRunStatus::Failed
            | SoyuzRunStatus::Cancelled
    ) {
        return Err(ApiFailure::new(
            StatusCode::CONFLICT,
            "INVALID_STATE_TRANSITION",
            "Worker does not own an active run",
        ));
    }
    if run.status == SoyuzRunStatus::Claimed && claim_expired(run, now_ms) {
        return Err(ApiFailure::new(
            StatusCode::CONFLICT,
            "CLAIM_EXPIRED",
            "Worker claim expired before terminal callback",
        ));
    }
    let payload = match terminal {
        "completed" => terminal_result(body),
        "failed" => {
            let mut payload = Map::new();
            payload.insert("failureClass".to_owned(), body["failureClass"].clone());
            payload.insert("failureMessage".to_owned(), body["failureMessage"].clone());
            if let Some(exit_code) = body.get("exitCode") {
                payload.insert("exitCode".to_owned(), exit_code.clone());
            }
            payload.extend(terminal_result(body));
            payload
        }
        "cancelled" => {
            let mut payload = Map::new();
            if let Some(reason) = body.get("reason") {
                if !reason.as_str().is_some_and(|value| value.len() <= 2_000) {
                    return Err(invalid_callback(
                        "Cancellation reason exceeds 2,000 characters",
                    ));
                }
                payload.insert("reason".to_owned(), reason.clone());
            }
            if let Some(exit_code) = body.get("exitCode") {
                payload.insert("exitCode".to_owned(), exit_code.clone());
            }
            payload.extend(terminal_result(body));
            payload
        }
        _ => unreachable!(),
    };
    let fingerprint = RecordedCallback {
        operation: terminal.to_owned(),
        kind: format!("run.{terminal}"),
        worker_id: worker_id.to_owned(),
        stage: None,
        payload: Value::Object(payload),
    };
    if let Some(previous) = run.callbacks.get(callback_id) {
        if !previous.matches(&fingerprint) {
            return Err(callback_id_reused());
        }
        return Ok(run.canonical_json());
    }
    let expected_status = match terminal {
        "completed" => SoyuzRunStatus::Completed,
        "failed" => SoyuzRunStatus::Failed,
        "cancelled" => SoyuzRunStatus::Cancelled,
        _ => unreachable!(),
    };
    if matches!(
        run.status,
        SoyuzRunStatus::Completed | SoyuzRunStatus::Failed | SoyuzRunStatus::Cancelled
    ) {
        if run.status == expected_status {
            return Ok(run.canonical_json());
        }
        return Err(ApiFailure::new(
            StatusCode::CONFLICT,
            "INVALID_STATE_TRANSITION",
            "Run already has a different terminal outcome",
        ));
    }
    if terminal == "completed"
        && !matches!(
            run.status,
            SoyuzRunStatus::Running | SoyuzRunStatus::CancelRequested
        )
    {
        return Err(ApiFailure::new(
            StatusCode::CONFLICT,
            "INVALID_STATE_TRANSITION",
            "Completion requires a running run",
        ));
    }
    run.status = expected_status;
    run.claim_expires_at = None;
    run.callbacks.insert(callback_id.to_owned(), fingerprint);
    Ok(run.canonical_json())
}

fn terminal_result(body: &Value) -> Map<String, Value> {
    let mut result = Map::new();
    for key in [
        "summary",
        "publishedUrl",
        "changedFileCount",
        "outcomeSummary",
    ] {
        if let Some(value) = body.get(key) {
            result.insert(key.to_owned(), value.clone());
        }
    }
    result
}

fn ensure_owner_and_fence(
    run: &SimRun,
    worker_id: &str,
    claim_id: Option<&str>,
) -> Result<(), ApiFailure> {
    if run.worker_id.as_deref() != Some(worker_id) {
        return Err(ApiFailure::new(
            StatusCode::CONFLICT,
            "WORKER_MISMATCH",
            "Worker does not own this run",
        ));
    }
    if claim_id.is_some_and(|claim_id| run.claim_callback_id.as_deref() != Some(claim_id)) {
        return Err(stale_claim());
    }
    Ok(())
}

fn claim_expired(run: &SimRun, now_ms: i64) -> bool {
    run.claim_expires_at
        .as_deref()
        .and_then(parse_time)
        .is_some_and(|expires_at| expires_at <= now_ms)
}

fn strict_body(body: &Value, allowed: &[&str], required: &[&str]) -> Result<(), ApiFailure> {
    let Some(object) = body.as_object() else {
        return Err(invalid_callback("Worker callback must be a JSON object"));
    };
    if object.keys().any(|key| !allowed.contains(&key.as_str()))
        || required.iter().any(|key| !object.contains_key(*key))
    {
        return Err(invalid_callback(
            "Worker callback fields do not match contract v1",
        ));
    }
    Ok(())
}

fn require_version(body: &Value) -> Result<(), ApiFailure> {
    if body.get("contractVersion").and_then(Value::as_str) == Some("1") {
        Ok(())
    } else {
        Err(invalid_callback(
            "Unsupported worker callback contract version",
        ))
    }
}

fn required_string<'a>(body: &'a Value, field: &str) -> Result<&'a str, ApiFailure> {
    body.get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid_callback("Worker callback is missing a required string"))
}

fn required_uuid<'a>(body: &'a Value, field: &str) -> Result<&'a str, ApiFailure> {
    let value = required_string(body, field)?;
    Uuid::parse_str(value)
        .map(|_| value)
        .map_err(|_| invalid_callback("Worker callback identifier must be a UUID"))
}

fn optional_string<'a>(body: &'a Value, field: &str) -> Option<&'a str> {
    body.get(field).and_then(Value::as_str)
}

fn validate_optional_timestamp(body: &Value, field: &str) -> Result<(), ApiFailure> {
    if let Some(value) = body.get(field) {
        let Some(value) = value.as_str() else {
            return Err(invalid_callback(
                "Worker callback timestamp must be a string",
            ));
        };
        OffsetDateTime::parse(value, &Rfc3339)
            .map_err(|_| invalid_callback("Worker callback timestamp must be RFC 3339"))?;
    }
    Ok(())
}

fn validate_label(body: &Value, field: &str, max_bytes: usize) -> Result<(), ApiFailure> {
    if let Some(value) = body.get(field) {
        let Some(value) = value.as_str() else {
            return Err(invalid_callback("Worker callback label must be a string"));
        };
        if value.is_empty() || value.len() > max_bytes || value.chars().any(char::is_control) {
            return Err(invalid_callback(
                "Worker callback label is outside the contract bounds",
            ));
        }
    }
    Ok(())
}

fn authorized(headers: &HeaderMap, token: &str) -> bool {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.strip_prefix("Bearer ") == Some(token))
}

fn success(data: Value) -> SimResponse {
    success_with_status(StatusCode::OK, data)
}

fn success_with_status(status: StatusCode, data: Value) -> SimResponse {
    (
        status,
        Json(json!({ "data": data, "requestId": Uuid::new_v4().to_string() })),
    )
}

fn error(status: StatusCode, code: &str, message: &str) -> SimResponse {
    (
        status,
        Json(json!({
            "error": {
                "code": code,
                "message": message,
                "requestId": Uuid::new_v4().to_string(),
            }
        })),
    )
}

#[derive(Debug)]
struct ApiFailure {
    status: StatusCode,
    code: &'static str,
    message: &'static str,
}

impl ApiFailure {
    fn new(status: StatusCode, code: &'static str, message: &'static str) -> Self {
        Self {
            status,
            code,
            message,
        }
    }

    fn response(self) -> SimResponse {
        error(self.status, self.code, self.message)
    }
}

fn invalid_callback(message: &'static str) -> ApiFailure {
    ApiFailure::new(
        StatusCode::UNPROCESSABLE_ENTITY,
        "INVALID_WORKER_CALLBACK",
        message,
    )
}

fn stale_claim() -> ApiFailure {
    ApiFailure::new(
        StatusCode::CONFLICT,
        "STALE_CLAIM",
        "Worker claim was superseded by a newer execution attempt",
    )
}

fn callback_id_reused() -> ApiFailure {
    ApiFailure::new(
        StatusCode::CONFLICT,
        "CALLBACK_ID_REUSED",
        "callbackId was already used for a different callback",
    )
}

fn format_time(unix_ms: i64) -> String {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(unix_ms) * 1_000_000)
        .expect("simulator time is representable")
        .format(&Rfc3339)
        .expect("timestamp formatting is infallible")
}

fn parse_time(timestamp: &str) -> Option<i64> {
    OffsetDateTime::parse(timestamp, &Rfc3339)
        .ok()
        .and_then(|value| i64::try_from(value.unix_timestamp_nanos() / 1_000_000).ok())
}

#[cfg(test)]
mod tests {
    use super::{SimRun, claim_expired, format_time};

    #[test]
    fn claim_without_expiry_is_not_expired() {
        let mut run = SimRun::queued("run-1".to_owned(), "display-1".to_owned(), 0);

        assert!(!claim_expired(&run, 0));

        run.claim_expires_at = Some(format_time(1_000));
        assert!(claim_expired(&run, 1_000));
        assert!(!claim_expired(&run, 999));
    }
}
