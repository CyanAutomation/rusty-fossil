use fossil_core::ids::{ClaimCallbackId, EventId, RunId, WorkerId};
use fossil_gateway::{
    BrokerError, GatewayEndpoint, GatewayRequestLedger, GatewayTokenClaims, GatewayTokenSigner,
    validate_completion_request,
};
use secrecy::SecretString;
use std::os::unix::fs::PermissionsExt;
use std::time::{SystemTime, UNIX_EPOCH};

fn claims(issued_at_ms: i64, expires_at_ms: i64) -> GatewayTokenClaims {
    GatewayTokenClaims {
        version: 1,
        run_id: RunId::parse("018f6f7b-3a1c-7cc1-8df2-2f16a7a66d01").unwrap(),
        worker_id: WorkerId::parse("pilot-worker").unwrap(),
        claim_callback_id: ClaimCallbackId::parse("cc1c1b68-5f60-4d7e-8f50-5deabc37000a").unwrap(),
        start_callback_id: EventId::parse("cc1c1b68-5f60-4d7e-8f50-5deabc37000b").unwrap(),
        issued_at_ms,
        expires_at_ms,
    }
}

#[test]
fn run_token_is_fence_scoped_and_expires() {
    let signer = GatewayTokenSigner::new(SecretString::from("s".repeat(48))).unwrap();
    let claims = claims(1_000, 61_000);
    let token = signer.issue(&claims).unwrap();

    assert_eq!(signer.verify(&token, 2_000).unwrap(), claims);
    assert_eq!(
        signer.verify(&token, 61_000),
        Err(BrokerError::InvalidToken)
    );

    let mut changed = token.into_bytes();
    let last = changed.len() - 1;
    changed[last] = if changed[last] == b'A' { b'B' } else { b'A' };
    let changed = String::from_utf8(changed).unwrap();
    assert_eq!(
        signer.verify(&changed, 2_000),
        Err(BrokerError::InvalidToken)
    );
}

#[test]
fn token_signing_rejects_weak_keys_and_excessive_lifetimes() {
    assert!(matches!(
        GatewayTokenSigner::new(SecretString::from("short")),
        Err(BrokerError::InvalidConfiguration)
    ));
    let signer = GatewayTokenSigner::new(SecretString::from("s".repeat(48))).unwrap();
    assert_eq!(
        signer.issue(&claims(1_000, 11_000_000)),
        Err(BrokerError::InvalidToken)
    );
}

#[test]
fn endpoint_accepts_only_the_cloudflare_universal_compatibility_route() {
    let endpoint = GatewayEndpoint::parse(
        "https://gateway.ai.cloudflare.com/v1/0123456789abcdef0123456789abcdef/pilot/compat/chat/completions",
    )
    .unwrap();
    assert_eq!(
        endpoint.as_str(),
        "https://gateway.ai.cloudflare.com/v1/0123456789abcdef0123456789abcdef/pilot/compat/chat/completions"
    );

    for untrusted in [
        "http://gateway.ai.cloudflare.com/v1/0123456789abcdef0123456789abcdef/pilot/compat/chat/completions",
        "https://example.com/v1/0123456789abcdef0123456789abcdef/pilot/compat/chat/completions",
        "https://gateway.ai.cloudflare.com/v1/0123456789abcdef0123456789abcdef/pilot/compat/chat/completions?url=https://example.com",
        "https://gateway.ai.cloudflare.com/v1/0123456789abcdef0123456789abcdef/pilot/openai/chat/completions",
        "https://user:pass@gateway.ai.cloudflare.com/v1/0123456789abcdef0123456789abcdef/pilot/compat/chat/completions",
    ] {
        assert!(
            GatewayEndpoint::parse(untrusted).is_err(),
            "accepted {untrusted}"
        );
    }
}

#[test]
fn request_token_fields_do_not_become_gateway_token_budgets() {
    let good = br#"{"model":"workers-ai/@cf/meta/test-model","messages":[{"role":"user","content":"hello"}],"stream":true}"#;
    let parsed = validate_completion_request(good, "workers-ai/@cf/meta/test-model").unwrap();
    assert_eq!(parsed["model"], "workers-ai/@cf/meta/test-model");
    assert!(parsed.get("max_tokens").is_none());
    assert!(parsed.get("max_completion_tokens").is_none());

    let large_requested_output = br#"{"model":"workers-ai/@cf/meta/test-model","messages":[{"role":"user","content":"hello"}],"max_completion_tokens":1000000,"n":4}"#;
    let parsed =
        validate_completion_request(large_requested_output, "workers-ai/@cf/meta/test-model")
            .unwrap();
    assert_eq!(parsed["n"], 4);
    assert!(parsed.get("max_completion_tokens").is_none());
    assert!(parsed.get("max_tokens").is_none());

    let other_model = br#"{"model":"openai/gpt-x","messages":[{"role":"user","content":"hello"}]}"#;
    assert_eq!(
        validate_completion_request(other_model, "workers-ai/@cf/meta/test-model"),
        Err(BrokerError::InvalidRequest)
    );
    assert_eq!(
        validate_completion_request(&vec![b'x'; 512 * 1024 + 1], "model"),
        Err(BrokerError::InvalidRequest)
    );

    let image_content = br#"{"model":"workers-ai/@cf/meta/test-model","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://example.test/image.png"}}]}]}"#;
    assert_eq!(
        validate_completion_request(image_content, "workers-ai/@cf/meta/test-model"),
        Err(BrokerError::InvalidRequest)
    );
    let unaccounted_modality = br#"{"model":"workers-ai/@cf/meta/test-model","messages":[{"role":"user","content":"hello"}],"modalities":["image"]}"#;
    assert_eq!(
        validate_completion_request(unaccounted_modality, "workers-ai/@cf/meta/test-model"),
        Err(BrokerError::InvalidRequest)
    );
}

#[test]
fn request_ledger_tracks_cost_without_limiting_inference_count() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let database = directory.path().join("gateway.sqlite3");
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let run = claims(now, now + 60_000);

    {
        let ledger = GatewayRequestLedger::open(&database).unwrap();
        for index in 0..1_000 {
            let request_id = EventId::new();
            let log_id = format!("log-{index}");
            ledger.begin_request(&run, request_id).unwrap();
            ledger
                .record_gateway_log(&run, request_id, &log_id)
                .unwrap();
            ledger.record_gateway_cost(&log_id, 0.00025).unwrap();
        }
        let request_without_cost = EventId::new();
        ledger.begin_request(&run, request_without_cost).unwrap();
        let report = ledger.cost_report(&run).unwrap();
        assert_eq!(report.request_count, 1_001);
        assert_eq!(report.priced_request_count, 1_000);
        assert_eq!(report.pending_request_count, 1);
        assert!((report.estimated_cost_usd - 0.25).abs() < 0.000_001);
    }

    let ledger = GatewayRequestLedger::open(&database).unwrap();
    let report = ledger.cost_report(&run).unwrap();
    assert_eq!(report.request_count, 1_001);
    assert_eq!(report.priced_request_count, 1_000);
    assert_eq!(report.pending_request_count, 1);

    let mut changed_fence = run.clone();
    changed_fence.worker_id = WorkerId::parse("other-worker").unwrap();
    assert_eq!(
        ledger.cost_report(&changed_fence),
        Err(BrokerError::InvalidToken)
    );
}

#[test]
fn opening_the_ledger_discards_obsolete_budget_state() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let database = directory.path().join("legacy.sqlite3");
    let connection = rusqlite::Connection::open(&database).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE gateway_run_budget (
                run_id TEXT PRIMARY KEY,
                worker_id TEXT NOT NULL,
                claim_callback_id TEXT NOT NULL,
                start_callback_id TEXT NOT NULL,
                expires_at_ms INTEGER NOT NULL,
                request_count INTEGER NOT NULL,
                request_bytes INTEGER NOT NULL,
                rate_window_started_at_ms INTEGER NOT NULL,
                rate_window_request_count INTEGER NOT NULL,
                reserved_cost_microusd INTEGER NOT NULL DEFAULT 0,
                maximum_cost_microusd INTEGER NOT NULL DEFAULT 0,
                cost_policy_identity TEXT NOT NULL DEFAULT ''
            );",
        )
        .unwrap();
    drop(connection);
    std::fs::set_permissions(&database, std::fs::Permissions::from_mode(0o600)).unwrap();

    let ledger = GatewayRequestLedger::open(&database).unwrap();
    let table_count: i64 = rusqlite::Connection::open(&database)
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'gateway_run_budget'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(table_count, 0);

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    ledger
        .begin_request(&claims(now, now + 60_000), EventId::new())
        .unwrap();
}

#[test]
fn gateway_ledger_requires_private_state_and_rejects_symlink_database_paths() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let database = directory.path().join("gateway.sqlite3");
    drop(GatewayRequestLedger::open(&database).unwrap());
    let database_mode = std::fs::metadata(&database).unwrap().permissions().mode();
    assert_eq!(database_mode & 0o077, 0);

    let alias = directory.path().join("alias.sqlite3");
    std::os::unix::fs::symlink(&database, &alias).unwrap();
    assert!(GatewayRequestLedger::open(alias).is_err());

    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(GatewayRequestLedger::open(directory.path().join("public.sqlite3")).is_err());
}
