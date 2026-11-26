use crate::config::{AppConfig, ReadinessReport};
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;
use axum::{Json, Router};
use fossil_storage::SqliteStore;
use serde::Serialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const OPENAPI_DOCUMENT: &str = include_str!("../openapi.json");
const DOCS_HTML: &str = include_str!("../static/docs.html");
const DOCS_SCRIPT: &str = include_str!("../static/docs.js");
const DOCS_CONTENT_SECURITY_POLICY: &str = "default-src 'none'; style-src 'unsafe-inline'; script-src 'self'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'";

#[derive(Clone)]
pub struct ApiState {
    config: Arc<AppConfig>,
    store: SqliteStore,
    metrics: Arc<ApiMetrics>,
}

#[derive(Default)]
struct ApiMetrics {
    health_requests: AtomicU64,
    readiness_requests: AtomicU64,
    metrics_requests: AtomicU64,
}

impl ApiState {
    pub fn new(config: AppConfig, store: SqliteStore) -> Self {
        Self {
            config: Arc::new(config),
            store,
            metrics: Arc::new(ApiMetrics::default()),
        }
    }

    fn readiness(&self) -> ReadinessReport {
        let state_store_ready = self.store.run_count().is_ok();
        let recovery_backlog_healthy = self.store.recovery_backlog_available().unwrap_or(false);
        let mut report = self.config.readiness_report_with_state(
            state_store_ready,
            recovery_backlog_healthy,
            None,
        );
        if let Ok(outbox) = self.store.callback_outbox_status() {
            report.outbox_backpressured = outbox.intake_backpressured;
            report.callbacks_pending = outbox.pending;
            report.callbacks_blocked = outbox.blocked;
            report.oldest_pending_callback_age_ms = outbox.oldest_pending_age_ms;
            if report.outbox_backpressured {
                report.execution_handoff_ready = false;
            }
        }
        report
    }
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ReadyResponse {
    ready: bool,
    execution_ready: bool,
    execution_handoff_ready: bool,
    state_store_ready: bool,
    soyuz_configured: bool,
    queue_configured: bool,
    queue_transport_healthy: Option<bool>,
    recovery_backlog_healthy: bool,
    outbox_backpressured: bool,
    callbacks_pending: u64,
    callbacks_blocked: u64,
    oldest_pending_callback_age_ms: Option<u64>,
    docker_supervisor_available: Option<bool>,
    synthetic_execution_available: bool,
    production_coding_execution_ready: bool,
    senku_configured: bool,
    senku_advisory_healthy: Option<bool>,
    missing_dependencies: Vec<String>,
    limitations: Vec<String>,
}

impl From<ReadinessReport> for ReadyResponse {
    fn from(report: ReadinessReport) -> Self {
        Self {
            ready: report.ready,
            execution_ready: report.execution_ready,
            execution_handoff_ready: report.execution_handoff_ready,
            state_store_ready: report.state_store_ready,
            soyuz_configured: report.soyuz_configured,
            queue_configured: report.queue_configured,
            queue_transport_healthy: report.queue_transport_healthy,
            recovery_backlog_healthy: report.recovery_backlog_healthy,
            outbox_backpressured: report.outbox_backpressured,
            callbacks_pending: report.callbacks_pending,
            callbacks_blocked: report.callbacks_blocked,
            oldest_pending_callback_age_ms: report.oldest_pending_callback_age_ms,
            docker_supervisor_available: report.docker_supervisor_available,
            synthetic_execution_available: report.synthetic_execution_available,
            production_coding_execution_ready: report.production_coding_execution_ready,
            senku_configured: report.senku_configured,
            senku_advisory_healthy: report.senku_advisory_healthy,
            missing_dependencies: report.missing_dependencies,
            limitations: report.limitations,
        }
    }
}

pub fn router(state: ApiState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(readiness))
        .route("/metrics", get(metrics))
        .route("/docs", get(docs_redirect))
        .route("/docs/", get(docs))
        .route("/docs/app.js", get(docs_script))
        .route("/openapi.json", get(openapi_document))
        .with_state(state)
}

async fn docs_redirect() -> Redirect {
    Redirect::permanent("./docs/")
}

async fn docs() -> Response {
    (
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            (
                header::CONTENT_SECURITY_POLICY,
                DOCS_CONTENT_SECURITY_POLICY,
            ),
        ],
        DOCS_HTML,
    )
        .into_response()
}

async fn docs_script() -> Response {
    (
        [
            (header::CONTENT_TYPE, "text/javascript; charset=utf-8"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            (
                header::CONTENT_SECURITY_POLICY,
                DOCS_CONTENT_SECURITY_POLICY,
            ),
        ],
        DOCS_SCRIPT,
    )
        .into_response()
}

async fn openapi_document() -> Response {
    (
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        OPENAPI_DOCUMENT,
    )
        .into_response()
}

async fn health(State(state): State<ApiState>) -> Json<HealthResponse> {
    state
        .metrics
        .health_requests
        .fetch_add(1, Ordering::Relaxed);
    Json(HealthResponse { status: "ok" })
}

async fn readiness(State(state): State<ApiState>) -> Response {
    state
        .metrics
        .readiness_requests
        .fetch_add(1, Ordering::Relaxed);
    let report = state.readiness();
    let status = if report.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(ReadyResponse::from(report))).into_response()
}

async fn metrics(State(state): State<ApiState>) -> Response {
    state
        .metrics
        .metrics_requests
        .fetch_add(1, Ordering::Relaxed);
    let readiness = state.readiness();
    let ready = u8::from(readiness.execution_ready);
    let docker_supervisor_probe_known = u8::from(readiness.docker_supervisor_available.is_some());
    let docker_supervisor_available = u8::from(readiness.docker_supervisor_available == Some(true));
    let outbox = state.store.callback_outbox_status().unwrap_or_default();
    let recovery = state.store.recovery_diagnostics().unwrap_or_default();
    let execution_slot_available =
        u8::from(state.store.intake_capacity_available().unwrap_or(false));
    let outbox_backpressured = u8::from(outbox.intake_backpressured);
    let body = format!(
        "# HELP fossil_controller_up Whether the Rusty-Fossil process is live.\n\
         # TYPE fossil_controller_up gauge\n\
         fossil_controller_up 1\n\
         # HELP fossil_execution_ready Whether the controller can safely accept execution work.\n\
         # TYPE fossil_execution_ready gauge\n\
         fossil_execution_ready {ready}\n\
         # HELP fossil_docker_supervisor_probe_known Whether the controller has probed the supervisor IPC endpoint.\n\
         # TYPE fossil_docker_supervisor_probe_known gauge\n\
         fossil_docker_supervisor_probe_known {docker_supervisor_probe_known}\n\
         # HELP fossil_docker_supervisor_available Whether the controller has a reachable Docker supervisor.\n\
         # TYPE fossil_docker_supervisor_available gauge\n\
         fossil_docker_supervisor_available {docker_supervisor_available}\n\
         # HELP fossil_synthetic_execution_available Whether synthetic worker execution is wired in this process.\n\
         # TYPE fossil_synthetic_execution_available gauge\n\
         fossil_synthetic_execution_available {}\n\
         # HELP fossil_production_coding_execution_ready Whether real coding execution is ready.\n\
         # TYPE fossil_production_coding_execution_ready gauge\n\
         fossil_production_coding_execution_ready {}\n\
         # HELP fossil_http_requests_total Requests served by diagnostic endpoints.\n\
         # TYPE fossil_http_requests_total counter\n\
         fossil_http_requests_total{{route=\"health\"}} {}\n\
         fossil_http_requests_total{{route=\"ready\"}} {}\n\
         fossil_http_requests_total{{route=\"metrics\"}} {}\n\
         # HELP fossil_callbacks_pending Number of callbacks awaiting delivery.\n\
         # TYPE fossil_callbacks_pending gauge\n\
         fossil_callbacks_pending {}\n\
         # HELP fossil_callbacks_blocked Number of callbacks requiring operator review.\n\
         # TYPE fossil_callbacks_blocked gauge\n\
         fossil_callbacks_blocked {}\n\
         # HELP fossil_execution_slot_available Whether local recovery state permits Queue intake.\n\
         # TYPE fossil_execution_slot_available gauge\n\
         fossil_execution_slot_available {}\n\
         # HELP fossil_callbacks_oldest_pending_age_ms Age of the oldest pending callback.\n\
         # TYPE fossil_callbacks_oldest_pending_age_ms gauge\n\
         fossil_callbacks_oldest_pending_age_ms {}\n\
         # HELP fossil_intake_outbox_backpressured Whether intake is paused to protect callback capacity.\n\
         # TYPE fossil_intake_outbox_backpressured gauge\n\
         fossil_intake_outbox_backpressured {}\n\
         # HELP fossil_queue_intake_blocked_outbox Whether new intake is paused by callback capacity.\n\
         # TYPE fossil_queue_intake_blocked_outbox gauge\n\
         fossil_queue_intake_blocked_outbox {}\n\
         # HELP fossil_queue_intake_blocked_recovery Whether new intake is paused by unresolved recovery state.\n\
         # TYPE fossil_queue_intake_blocked_recovery gauge\n\
         fossil_queue_intake_blocked_recovery {}\n\
         # HELP fossil_recovery_unresolved_runs Number of nonterminal runs awaiting reconciliation.\n\
         # TYPE fossil_recovery_unresolved_runs gauge\n\
         fossil_recovery_unresolved_runs {}\n\
         # HELP fossil_recovery_attempts_total Durable recovery attempts recorded by SQLite.\n\
         # TYPE fossil_recovery_attempts_total counter\n\
         fossil_recovery_attempts_total {}\n\
         # HELP fossil_recovery_successful_runs Runs safely reconciled to a durable start authorization.\n\
         # TYPE fossil_recovery_successful_runs gauge\n\
         fossil_recovery_successful_runs {}\n\
         # HELP fossil_recovery_awaiting_soyuz_runs Runs awaiting another canonical Soyuz read.\n\
         # TYPE fossil_recovery_awaiting_soyuz_runs gauge\n\
         fossil_recovery_awaiting_soyuz_runs {}\n\
         # HELP fossil_recovery_owned_elsewhere_runs Local runs superseded after canonical ownership proof.\n\
         # TYPE fossil_recovery_owned_elsewhere_runs gauge\n\
         fossil_recovery_owned_elsewhere_runs {}\n\
         # HELP fossil_recovery_manual_review_runs Runs requiring operator review.\n\
         # TYPE fossil_recovery_manual_review_runs gauge\n\
         fossil_recovery_manual_review_runs {}\n\
         # HELP fossil_recovery_ambiguous_execution_runs Runs with potentially active execution.\n\
         # TYPE fossil_recovery_ambiguous_execution_runs gauge\n\
         fossil_recovery_ambiguous_execution_runs {}\n",
        u8::from(readiness.synthetic_execution_available),
        u8::from(readiness.production_coding_execution_ready),
        state.metrics.health_requests.load(Ordering::Relaxed),
        state.metrics.readiness_requests.load(Ordering::Relaxed),
        state.metrics.metrics_requests.load(Ordering::Relaxed),
        outbox.pending,
        outbox.blocked,
        execution_slot_available,
        outbox.oldest_pending_age_ms.unwrap_or(0),
        outbox_backpressured,
        outbox_backpressured,
        u8::from(recovery.unresolved_runs > 0),
        recovery.unresolved_runs,
        recovery.attempts,
        recovery.recovered_runs,
        recovery.awaiting_soyuz_runs,
        recovery.owned_elsewhere,
        recovery.manual_review_runs,
        recovery.ambiguous_execution_runs,
    );
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
        .into_response()
}
