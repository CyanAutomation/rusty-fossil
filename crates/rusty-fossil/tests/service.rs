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

fn json_matches_openapi_type(value: &serde_json::Value, type_name: &str) -> bool {
    match type_name {
        "array" => value.is_array(),
        "boolean" => value.is_boolean(),
        "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
        "null" => value.is_null(),
        "number" => value.as_f64().is_some(),
        "object" => value.is_object(),
        "string" => value.is_string(),
        _ => false,
    }
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
            .contains(&"synthetic_supervisor_not_wired".to_owned())
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
    assert!(!report.execution_handoff_ready);
    assert!(!report.execution_ready);
    assert_eq!(report.queue_transport_healthy, None);
    assert!(
        report
            .limitations
            .contains(&"synthetic_supervisor_not_wired".to_owned())
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
    let store = SqliteStore::open(directory.path().join("private-state/state.sqlite3")).unwrap();
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
    assert_eq!(json["executionHandoffReady"], false);
    assert_eq!(json["dockerSupervisorAvailable"], serde_json::Value::Null);
    assert_eq!(json["syntheticExecutionAvailable"], false);
    assert_eq!(json["productionCodingExecutionReady"], false);
    assert_eq!(json["queueTransportHealthy"], serde_json::Value::Null);
    assert_eq!(json["recoveryBacklogHealthy"], true);

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
    assert!(text.contains("fossil_docker_supervisor_probe_known 0"));
    assert!(text.contains("fossil_docker_supervisor_available 0"));
    assert!(text.contains("fossil_synthetic_execution_available 0"));
    assert!(text.contains("fossil_production_coding_execution_ready 0"));
}

#[tokio::test]
async fn openapi_document_covers_the_api_get_routes() {
    let directory = tempdir().unwrap();
    let store = SqliteStore::open(directory.path().join("private-state/state.sqlite3")).unwrap();
    let app = router(ApiState::new(config(&[]).unwrap(), store));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/openapi.json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[axum::http::header::CONTENT_TYPE],
        "application/json"
    );
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let spec: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(spec["openapi"], "3.1.1");

    let paths = spec["paths"].as_object().expect("OpenAPI paths object");
    for required_path in [
        "/health",
        "/ready",
        "/metrics",
        "/docs",
        "/docs/",
        "/docs/app.js",
        "/openapi.json",
    ] {
        assert!(paths.contains_key(required_path), "missing {required_path}");
    }
    for unsupported_path in ["/runs", "/execute", "/v1/runs"] {
        assert!(
            !paths.contains_key(unsupported_path),
            "unexpected task endpoint {unsupported_path}"
        );
    }

    for (path, path_item) in paths {
        let documented_get = path_item
            .get("get")
            .expect("every public path currently supports GET");
        assert_eq!(
            path_item.as_object().unwrap().len(),
            1,
            "GET {path} is the only documented method"
        );
        let responses = documented_get["responses"]
            .as_object()
            .expect("operation response map");
        let response = app
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status().as_u16().to_string();
        assert!(
            responses.contains_key(&status),
            "GET {path} returned undocumented status {}",
            response.status()
        );
        if let Some(content_type) = response.headers().get(axum::http::header::CONTENT_TYPE) {
            let content_type = content_type.to_str().unwrap();
            assert!(
                responses[&status]["content"]
                    .as_object()
                    .is_some_and(|content| content.contains_key(content_type)),
                "GET {path} returned undocumented content type {content_type}"
            );
        }
    }

    assert!(
        spec["paths"]["/ready"]["get"]["responses"]
            .get("503")
            .is_some()
    );
    assert_eq!(
        spec["paths"]["/docs"]["get"]["responses"]["308"]["headers"]["Location"]["schema"]["const"],
        "./docs/"
    );
    assert_eq!(
        spec["paths"]["/metrics"]["get"]["responses"]["200"]["content"]
            .as_object()
            .unwrap()
            .keys()
            .next()
            .unwrap(),
        "text/plain; version=0.0.4; charset=utf-8"
    );

    let ready = app
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
    let readiness: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let actual_fields = readiness
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let schema_fields = spec["components"]["schemas"]["ReadinessResponse"]["properties"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let required_fields = spec["components"]["schemas"]["ReadinessResponse"]["required"]
        .as_array()
        .unwrap()
        .iter()
        .map(|field| field.as_str().unwrap().to_owned())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(actual_fields, schema_fields);
    assert_eq!(actual_fields, required_fields);
    let schema_properties = spec["components"]["schemas"]["ReadinessResponse"]["properties"]
        .as_object()
        .unwrap();
    for (field, field_schema) in schema_properties {
        let expected_types = match &field_schema["type"] {
            serde_json::Value::String(type_name) => vec![type_name.as_str()],
            serde_json::Value::Array(type_names) => type_names
                .iter()
                .map(|type_name| type_name.as_str().unwrap())
                .collect(),
            _ => panic!("missing type for readiness field {field}"),
        };
        assert!(
            expected_types
                .iter()
                .any(|type_name| json_matches_openapi_type(&readiness[field], type_name)),
            "readiness field {field} does not match its OpenAPI type"
        );
    }
}

#[tokio::test]
async fn docs_page_is_local_and_serves_its_typescript_compiled_asset() {
    let directory = tempdir().unwrap();
    let store = SqliteStore::open(directory.path().join("private-state/state.sqlite3")).unwrap();
    let app = router(ApiState::new(config(&[]).unwrap(), store));

    let redirect = app
        .clone()
        .oneshot(Request::builder().uri("/docs").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(redirect.status(), StatusCode::PERMANENT_REDIRECT);
    assert_eq!(redirect.headers()[axum::http::header::LOCATION], "./docs/");

    let page = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/docs/")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(page.status(), StatusCode::OK);
    assert!(
        page.headers()[axum::http::header::CONTENT_TYPE]
            .to_str()
            .unwrap()
            .starts_with("text/html; charset=utf-8")
    );
    assert!(
        page.headers()[axum::http::header::CONTENT_SECURITY_POLICY]
            .to_str()
            .unwrap()
            .contains("script-src 'self'")
    );
    assert_eq!(
        page.headers()[axum::http::header::X_CONTENT_TYPE_OPTIONS],
        "nosniff"
    );
    let page = page.into_body().collect().await.unwrap().to_bytes();
    let page = std::str::from_utf8(&page).unwrap();
    assert!(page.contains("Rusty-Fossil API"));
    assert!(page.contains("src=\"app.js\""));
    assert!(page.contains("/openapi.json"));
    assert!(
        !page.contains("https://"),
        "docs page loads no remote assets"
    );

    let script = app
        .oneshot(
            Request::builder()
                .uri("/docs/app.js")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(script.status(), StatusCode::OK);
    assert!(
        script.headers()[axum::http::header::CONTENT_TYPE]
            .to_str()
            .unwrap()
            .starts_with("text/javascript; charset=utf-8")
    );
    assert!(
        !script
            .headers()
            .contains_key(axum::http::header::CONTENT_SECURITY_POLICY),
        "CSP belongs on the HTML document, not the JavaScript response"
    );
    assert_eq!(
        script.headers()[axum::http::header::X_CONTENT_TYPE_OPTIONS],
        "nosniff"
    );
    let script = script.into_body().collect().await.unwrap().to_bytes();
    let script = std::str::from_utf8(&script).unwrap();
    assert!(script.contains("fetch("));
    assert!(script.contains("createElement"));
    assert!(script.contains("Endpoint groups"));
    assert!(script.contains("Headers"));
    assert!(
        !script.contains("https://"),
        "docs script loads no remote code"
    );
}

#[test]
fn default_bind_is_loopback_and_default_state_is_local() {
    let config = config(&[]).unwrap();
    assert_eq!(config.bind.to_string(), "127.0.0.1:8080");
    assert_eq!(config.state_dir, PathBuf::from("./state"));
}
