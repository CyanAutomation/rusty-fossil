use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use fossil_storage::SqliteStore;
use http_body_util::BodyExt;
use rusty_fossil::api::{ApiState, router};
use rusty_fossil::config::{AppConfig, ConfigError};
use secrecy::ExposeSecret;
use std::path::PathBuf;
use tempfile::tempdir;
use tower::ServiceExt;

fn config(pairs: &[(&str, &str)]) -> Result<AppConfig, ConfigError> {
    AppConfig::from_pairs(
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string())),
    )
}

#[test]
// Soyuz requirement: AUTH-CONFIG-01.
fn missing_soyuz_configuration_prevents_execution_readiness() {
    let config = config(&[]).unwrap();
    let report = config.readiness_report(true);
    assert!(!report.ready);
    assert!(
        report
            .missing_dependencies
            .contains(&"soyuz_api_origin".to_owned())
    );
    assert!(
        report
            .limitations
            .contains(&"execution_runtime_not_implemented".to_owned())
    );
}

#[test]
fn phase_one_limitations_block_execution_readiness_even_with_complete_configuration() {
    let config = config(&[
        ("SOYUZ_API_ORIGIN", "https://soyuz.example"),
        ("SOYUZ_WORKER_API_TOKEN", "worker-token"),
        ("SOYUZ_WORKER_ID", "rusty-fossil-host"),
        ("CLOUDFLARE_ACCOUNT_ID", "account-123"),
        ("CLOUDFLARE_QUEUE_ID", "queue-123"),
        ("CLOUDFLARE_QUEUE_TOKEN", "queue-token"),
    ])
    .unwrap();

    let report = config.readiness_report(true);
    assert!(report.missing_dependencies.is_empty());
    assert!(report.soyuz_configured);
    assert!(report.queue_configured);
    assert!(!report.ready);
    assert!(!report.execution_ready);
    assert!(
        report
            .limitations
            .contains(&"execution_runtime_not_implemented".to_owned())
    );
}

#[test]
fn invalid_worker_ids_and_unsupported_or_insecure_urls_are_rejected() {
    assert!(matches!(
        config(&[("SOYUZ_WORKER_ID", "bad worker id")]),
        Err(ConfigError::InvalidValue("SOYUZ_WORKER_ID"))
    ));
    assert!(matches!(
        config(&[("SOYUZ_API_ORIGIN", "ftp://soyuz.example")]),
        Err(ConfigError::InvalidValue("SOYUZ_API_ORIGIN"))
    ));
    assert!(matches!(
        config(&[
            ("RUSTY_FOSSIL_ENV", "production"),
            ("SOYUZ_API_ORIGIN", "http://soyuz.example")
        ]),
        Err(ConfigError::InvalidValue("SOYUZ_API_ORIGIN"))
    ));
}

#[test]
fn development_only_loopback_http_is_accepted() {
    let configured = config(&[
        ("RUSTY_FOSSIL_ENV", "development"),
        ("SOYUZ_API_ORIGIN", "http://127.0.0.1:8787"),
    ])
    .unwrap();
    assert!(configured.soyuz_api_origin.is_some());
}

#[test]
fn diagnostics_never_render_secret_values() {
    let config = config(&[
        ("SOYUZ_WORKER_API_TOKEN", "worker-super-secret"),
        ("CLOUDFLARE_QUEUE_TOKEN", "queue-super-secret"),
    ])
    .unwrap();
    let output = format!("{config:?}");
    assert!(!output.contains("worker-super-secret"));
    assert!(!output.contains("queue-super-secret"));
    assert!(output.contains("[REDACTED]"));
}

#[test]
fn mounted_secret_file_takes_precedence_and_blank_files_remain_missing() {
    let directory = tempdir().unwrap();
    let file = directory.path().join("soyuz-worker-token");
    std::fs::write(&file, "mounted-worker-secret\n").unwrap();
    let configured = config(&[
        ("SOYUZ_WORKER_API_TOKEN_FILE", file.to_str().unwrap()),
        ("SOYUZ_WORKER_API_TOKEN", "environment-worker-secret"),
    ])
    .unwrap();
    assert_eq!(
        configured
            .worker_api_token
            .as_ref()
            .unwrap()
            .expose_secret(),
        "mounted-worker-secret"
    );

    let empty = directory.path().join("empty-token");
    std::fs::write(&empty, "\n").unwrap();
    let configured = config(&[("SOYUZ_WORKER_API_TOKEN_FILE", empty.to_str().unwrap())]).unwrap();
    assert!(configured.worker_api_token.is_none());
}

#[tokio::test]
// Soyuz requirement: HEALTH-DB-01.
async fn health_ready_and_metrics_are_distinct_and_task_routes_do_not_exist() {
    let directory = tempdir().unwrap();
    let store = SqliteStore::open(directory.path().join("state.sqlite3")).unwrap();
    let app = router(ApiState::new(config(&[]).unwrap(), store));

    let health = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);

    let ready = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = ready.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["executionReady"], false);

    for path in ["/runs", "/execute", "/v1/runs"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(path)
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
    }

    let metrics = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(metrics.status(), StatusCode::OK);
    let body = metrics.into_body().collect().await.unwrap().to_bytes();
    let text = std::str::from_utf8(&body).unwrap();
    assert!(text.contains("fossil_controller_up 1"));
    assert!(text.contains("fossil_execution_ready 0"));
}

#[test]
fn default_bind_is_loopback_and_default_state_is_local() {
    let config = config(&[]).unwrap();
    assert_eq!(config.bind.to_string(), "127.0.0.1:8080");
    assert_eq!(config.state_dir, PathBuf::from("./state"));
}
