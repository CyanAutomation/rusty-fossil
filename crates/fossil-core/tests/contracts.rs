use fossil_core::contract::{ContractVersion, SoyuzRunStatus};
use fossil_core::ids::{ClaimCallbackId, DisplayId, QueueLeaseId, RunId, WorkerId};
use fossil_core::lifecycle::{LifecycleError, LocalLifecycleState};
use fossil_core::queue::{
    CloudflareAckRequest, CloudflarePullRequest, CloudflarePullResponse, QueueEnvelope,
    QueueEnvelopeError,
};
use serde_json::json;

const RUN_ID: &str = "f8a0c4c5-d628-7b93-a7cf-4b4629a28853";

#[test]
// Soyuz requirement: QUEUE-MESSAGE-01.
fn valid_contract_v1_queue_envelope_deserializes_without_confusing_identifiers() {
    let message = QueueEnvelope::parse(include_bytes!("fixtures/queue-v1-valid.json")).unwrap();
    let worker_id = WorkerId::parse("fossil-host-01").unwrap();
    let callback_id = ClaimCallbackId::parse("d744c5a2-1d8f-4a2e-bc4d-6813d6fc0133").unwrap();
    let display_id = DisplayId::parse("soyuz-213").unwrap();
    let lease_id = QueueLeaseId::parse("opaque-lease-token").unwrap();

    assert_eq!(message.contract_version, ContractVersion::V1);
    assert_eq!(message.run_id, RunId::parse(RUN_ID).unwrap());
    assert_eq!(worker_id.as_str(), "fossil-host-01");
    assert_eq!(callback_id.as_str(), "d744c5a2-1d8f-4a2e-bc4d-6813d6fc0133");
    assert_eq!(display_id.as_str(), "soyuz-213");
    assert_eq!(lease_id.as_str(), "opaque-lease-token");
    assert_eq!(message.request.repo_url.scheme(), "https");
}

#[test]
fn worker_id_policy_is_1_to_64_ascii_alphanumeric_hyphen_or_underscore() {
    let maximum = "w".repeat(64);
    assert_eq!(WorkerId::parse(maximum.clone()).unwrap().as_str(), maximum);
    assert!(WorkerId::parse("w".repeat(65)).is_err());
    for invalid in ["host.name", "host:name", "host name", "høst"] {
        assert!(WorkerId::parse(invalid).is_err(), "accepted {invalid:?}");
    }
    let worker = WorkerId::parse("fossil_host-01").unwrap();
    let serialized = serde_json::to_string(&worker).unwrap();
    assert_eq!(
        serde_json::from_str::<WorkerId>(&serialized).unwrap(),
        worker
    );
}

#[test]
// Soyuz requirement: QUEUE-MESSAGE-01.
fn unknown_contract_versions_are_rejected() {
    assert!(matches!(
        QueueEnvelope::parse(include_bytes!("fixtures/queue-v2-unsupported.json")),
        Err(QueueEnvelopeError::UnsupportedContractVersion(version)) if version == "2"
    ));
}

#[test]
// Soyuz requirement: RUN-REPO-HTTPS-01.
fn malformed_or_non_https_queue_messages_are_not_validated_work() {
    assert!(QueueEnvelope::parse(b"{not-json").is_err());

    assert!(QueueEnvelope::parse(include_bytes!("fixtures/queue-v1-invalid-url.json")).is_err());
}

#[test]
// Soyuz requirements: CONTRACT-VERSION-01, QUEUE-MESSAGE-01, RUN-VALIDATE-01.
fn minimal_request_uses_soyuz_normalized_defaults() {
    let value = serde_json::json!({
        "contractVersion": "1",
        "runId": RUN_ID,
        "createdAt": "2026-10-02T12:00:00.000Z",
        "correlationId": "15870068-6723-4b70-9e9f-53b8271e032d",
        "requestId": "7867a1e3-ed9d-4f23-81b0-0b47c6831cdd",
        "request": {
            "repoUrl": "https://github.com/example/service",
            "taskPrompt": "Add a health endpoint."
        }
    });

    let envelope = QueueEnvelope::parse(&serde_json::to_vec(&value).unwrap()).unwrap();
    let request = serde_json::to_value(envelope.request).unwrap();
    assert_eq!(request["ref"], "main");
    assert_eq!(request["taskMode"], "patch");
    assert_eq!(request["publishMode"], "pr");
    assert!(request.get("projectName").is_none());
    assert!(request.get("allowlist").is_none());
}

#[test]
fn absent_optional_allowlist_fields_remain_absent_when_persisted() {
    let fixture = include_str!("fixtures/queue-v1-valid.json").replace(
        "\"taskMode\":\"patch\"",
        "\"allowlist\":{},\"taskMode\":\"patch\"",
    );
    let message = QueueEnvelope::parse(fixture.as_bytes()).unwrap();
    let serialized = serde_json::to_value(&message.request).unwrap();
    assert_eq!(serialized["allowlist"], serde_json::json!({}));
}

#[test]
// Soyuz requirements: RUN-VALIDATE-01 and QUEUE-MESSAGE-01.
fn fully_populated_request_preserves_supported_execution_options() {
    let envelope = QueueEnvelope::parse(include_bytes!("fixtures/queue-v1-full.json")).unwrap();
    let request = serde_json::to_value(envelope.request).unwrap();
    assert_eq!(request["projectName"], "example-service");
    assert_eq!(request["changedFilesAllowlist"][0], "src/**");
    assert_eq!(request["allowlist"]["include"][0], "README.md");
    assert_eq!(request["maxDiffBytes"], 200_000);
    assert_eq!(request["validation"]["autoLintCleanup"]["enabled"], true);
    assert_eq!(request["goalSetting"]["model"], "gpt-test");
    assert_eq!(request["scouting"]["timeoutSeconds"], 60);
    assert_eq!(request["goalCheck"]["maxRetries"], 2);
    assert_eq!(request["runEvaluation"]["enabled"], true);
    assert_eq!(request["startupCheckMode"], "baseline-validation");
    assert_eq!(request["timeoutSeconds"], 3600);
    assert_eq!(
        request["tracing"]["correlationId"],
        "15870068-6723-4b70-9e9f-53b8271e032d"
    );
    assert_eq!(
        request["idempotencyKey"],
        "d744c5a2-1d8f-4a2e-bc4d-6813d6fc0133"
    );
}

#[test]
// Soyuz requirements: RUN-VALIDATE-01 and QUEUE-MESSAGE-01.
fn unsupported_and_unknown_request_options_fail_closed() {
    let fixture = include_str!("fixtures/queue-v1-valid.json");
    let with_webhook = fixture.replace(
        "\"request\":{",
        "\"request\":{\"webhookConfig\":{\"secret\":\"must-not-be-kept\"},",
    );
    let with_unknown_security_option =
        fixture.replace("\"request\":{", "\"request\":{\"skipAuthorization\":true,");
    assert!(QueueEnvelope::parse(with_webhook.as_bytes()).is_err());
    assert!(QueueEnvelope::parse(with_unknown_security_option.as_bytes()).is_err());
}

#[test]
// Soyuz requirement: RUN-VALIDATE-01.
fn invalid_modes_nested_settings_and_missing_fields_are_rejected() {
    let fixture = include_str!("fixtures/queue-v1-valid.json");
    for invalid in [
        fixture.replace("\"taskMode\":\"patch\"", "\"taskMode\":\"unrestricted\""),
        fixture.replace(
            "\"publishMode\":\"pr\"",
            "\"publishMode\":\"upload_everything\"",
        ),
        fixture.replace(
            "\"taskMode\":\"patch\"",
            "\"validationCommands\":[\"\"],\"taskMode\":\"patch\"",
        ),
        fixture.replace(
            "\"taskMode\":\"patch\"",
            "\"goalCheck\":{\"maxRetries\":6},\"taskMode\":\"patch\"",
        ),
        fixture.replace(
            ",\"taskPrompt\":\"Add a health endpoint and update the API documentation.\"",
            "",
        ),
        fixture.replace("\"repoUrl\":\"https://github.com/example/service\",", ""),
        fixture.replace("https://github.com/example/service", "file:///tmp/repo"),
    ] {
        assert!(QueueEnvelope::parse(invalid.as_bytes()).is_err());
    }
}

#[test]
// Soyuz requirement: RUN-VALIDATE-01.
fn nested_string_and_collection_limits_are_enforced() {
    let fixture = include_str!("fixtures/queue-v1-valid.json");
    let request_prefix = &fixture[..fixture.find("\"request\":{").unwrap() + "\"request\":{".len()];
    let commands = vec!["\"x\""; 51].join(",");
    let request_suffix = &fixture[fixture.find("\"repoUrl\"").unwrap()..];
    let too_many_commands =
        format!("{request_prefix}\"validationCommands\":[{commands}],{request_suffix}");
    let too_long_model = fixture.replace(
        "\"taskMode\":\"patch\"",
        &format!(
            "\"goalSetting\":{{\"model\":\"{}\"}},\"taskMode\":\"patch\"",
            "m".repeat(201)
        ),
    );
    let invalid_allowlist = include_str!("fixtures/queue-v1-full.json").replace(
        "\"changedFilesAllowlist\":[\"src/**\"]",
        "\"changedFilesAllowlist\":[\"\"]",
    );
    assert!(QueueEnvelope::parse(too_many_commands.as_bytes()).is_err());
    assert!(QueueEnvelope::parse(too_long_model.as_bytes()).is_err());
    assert!(QueueEnvelope::parse(invalid_allowlist.as_bytes()).is_err());
}

#[test]
fn external_statuses_match_contract_v1_and_unknown_values_fail_closed() {
    assert_eq!(
        serde_json::from_str::<SoyuzRunStatus>("\"cancel_requested\"").unwrap(),
        SoyuzRunStatus::CancelRequested
    );
    assert!(serde_json::from_str::<SoyuzRunStatus>("\"waiting\"").is_err());
}

#[test]
// Soyuz requirements: WORKER-START-01 and WORKER-CLAIM-FENCE-01.
fn lifecycle_rejects_execution_without_the_authorization_steps() {
    assert_eq!(
        LocalLifecycleState::CanonicalRunChecked.transition(LocalLifecycleState::ExecutionActive),
        Err(LifecycleError::InvalidTransition {
            from: LocalLifecycleState::CanonicalRunChecked,
            to: LocalLifecycleState::ExecutionActive,
        })
    );
}

#[test]
// Soyuz requirements: WORKER-START-01 and CANCEL-RUNNING-01.
fn lifecycle_accepts_only_the_ordered_authorized_path() {
    let states = [
        LocalLifecycleState::QueueObserved,
        LocalLifecycleState::MessageValidated,
        LocalLifecycleState::CanonicalRunChecked,
        LocalLifecycleState::ClaimIntentPersisted,
    ];
    for pair in states.windows(2) {
        assert!(
            pair[0].transition(pair[1]).is_ok(),
            "{:?} to {:?}",
            pair[0],
            pair[1]
        );
    }
    assert!(matches!(
        LocalLifecycleState::ClaimIntentPersisted.transition(LocalLifecycleState::ClaimAuthorized),
        Err(LifecycleError::InvalidTransition { .. })
    ));
}

#[test]
fn cancellation_and_callback_delivery_are_separate_from_external_status() {
    assert!(
        LocalLifecycleState::ExecutionActive
            .transition(LocalLifecycleState::CancellationRequested)
            .is_ok()
    );
    assert_eq!(SoyuzRunStatus::CancelRequested.as_str(), "cancel_requested");
}

#[test]
// Cloudflare Queues HTTP pull/ack behavior; lease identity stays separate from QUEUE-MESSAGE-01 run identity.
fn cloudflare_ack_models_keep_lease_ids_in_their_own_field() {
    let request = CloudflareAckRequest::acknowledge([QueueLeaseId::parse("lease-1").unwrap()]);
    let value = serde_json::to_value(request).unwrap();
    assert_eq!(value, json!({"acks":[{"lease_id":"lease-1"}],"retries":[]}));
}

#[test]
// Cloudflare Queues HTTP pull body encoding and lease behavior.
fn cloudflare_pull_fixture_decodes_json_body_and_keeps_lease_identity_separate() {
    let response: CloudflarePullResponse =
        serde_json::from_slice(include_bytes!("fixtures/cloudflare-pull-v1.json")).unwrap();
    assert!(response.success);
    let message = response
        .result
        .unwrap()
        .messages
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(message.id.as_str(), "msg-0001");
    assert_eq!(message.lease_id.as_str(), "opaque-lease-token");
    assert_eq!(
        message.decode_envelope().unwrap().run_id.to_string(),
        RUN_ID
    );
    assert!(CloudflarePullRequest::new(2, 10_000).is_some());
    assert!(CloudflarePullRequest::new(101, 10_000).is_none());
    assert!(CloudflarePullRequest::new(2, 43_200_001).is_none());
}
