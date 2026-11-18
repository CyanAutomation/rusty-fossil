use crate::config::{AppConfig, ReadinessReport};
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use fossil_storage::SqliteStore;
use serde::Serialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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
        self.config.readiness_report(self.store.run_count().is_ok())
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
    state_store_ready: bool,
    soyuz_configured: bool,
    queue_configured: bool,
    missing_dependencies: Vec<String>,
    limitations: Vec<String>,
}

impl From<ReadinessReport> for ReadyResponse {
    fn from(report: ReadinessReport) -> Self {
        Self {
            ready: report.ready,
            execution_ready: report.execution_ready,
            state_store_ready: report.state_store_ready,
            soyuz_configured: report.soyuz_configured,
            queue_configured: report.queue_configured,
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
        .with_state(state)
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
    let ready = u8::from(state.readiness().execution_ready);
    let body = format!(
        "# HELP fossil_controller_up Whether the Rusty-Fossil process is live.\n\
         # TYPE fossil_controller_up gauge\n\
         fossil_controller_up 1\n\
         # HELP fossil_execution_ready Whether the controller can safely accept execution work.\n\
         # TYPE fossil_execution_ready gauge\n\
         fossil_execution_ready {ready}\n\
         # HELP fossil_http_requests_total Requests served by diagnostic endpoints.\n\
         # TYPE fossil_http_requests_total counter\n\
         fossil_http_requests_total{{route=\"health\"}} {}\n\
         fossil_http_requests_total{{route=\"ready\"}} {}\n\
         fossil_http_requests_total{{route=\"metrics\"}} {}\n",
        state.metrics.health_requests.load(Ordering::Relaxed),
        state.metrics.readiness_requests.load(Ordering::Relaxed),
        state.metrics.metrics_requests.load(Ordering::Relaxed),
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
