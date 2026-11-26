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
fn model_requests_are_bounded_and_cannot_select_another_model() {
    let good = br#"{"model":"workers-ai/@cf/meta/test-model","messages":[{"role":"user","content":"hello"}],"max_tokens":64,"stream":true}"#;
    let parsed = validate_completion_request(good, "workers-ai/@cf/meta/test-model", 128).unwrap();
    assert_eq!(parsed["model"], "workers-ai/@cf/meta/test-model");

    let other_model = br#"{"model":"openai/gpt-x","messages":[{"role":"user","content":"hello"}]}"#;
    assert_eq!(
        validate_completion_request(other_model, "workers-ai/@cf/meta/test-model", 128),
        Err(BrokerError::InvalidRequest)
    );
    let too_many = br#"{"model":"workers-ai/@cf/meta/test-model","messages":[{"role":"user","content":"hello"}],"max_tokens":129}"#;
    assert_eq!(
        validate_completion_request(too_many, "workers-ai/@cf/meta/test-model", 128),
        Err(BrokerError::InvalidRequest)
    );
    assert_eq!(
        validate_completion_request(&vec![b'x'; 512 * 1024 + 1], "model", 128),
        Err(BrokerError::InvalidRequest)
    );
}

#[test]
fn request_budget_is_atomic_and_persists_only_counts() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("gateway.sqlite3");
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let request_claims = claims(now, now + 60_000);
    {
        let ledger = GatewayRequestLedger::open(&database).unwrap();
        ledger.reserve(&request_claims, 100, 2, 250).unwrap();
        ledger.reserve(&request_claims, 100, 2, 250).unwrap();
        assert_eq!(
            ledger.reserve(&request_claims, 1, 2, 250),
            Err(BrokerError::RateLimited)
        );
    }
    let ledger = GatewayRequestLedger::open(&database).unwrap();
    assert_eq!(
        ledger.reserve(&request_claims, 1, 2, 250),
        Err(BrokerError::RateLimited)
    );
    assert_eq!(
        ledger.reserve(&claims(1_000, 62_000), 1, 3, 250),
        Err(BrokerError::InvalidToken)
    );
}

#[test]
fn gateway_enforces_a_per_run_request_rate_window() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let ledger = GatewayRequestLedger::open(directory.path().join("rate.sqlite3")).unwrap();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let request_claims = claims(now, now + 60_000);
    for _ in 0..10 {
        ledger.reserve(&request_claims, 1, 64, 1_000).unwrap();
    }
    assert_eq!(
        ledger.reserve(&request_claims, 1, 64, 1_000),
        Err(BrokerError::RateLimited)
    );
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
