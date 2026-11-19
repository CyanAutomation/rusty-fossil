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
        LocalLifecycleState::ClaimAuthorized,
        LocalLifecycleState::StartAuthorizationPersisted,
        LocalLifecycleState::ExecutionActive,
        LocalLifecycleState::ExecutionTerminal,
    ];
    for pair in states.windows(2) {
        assert!(
            pair[0].transition(pair[1]).is_ok(),
            "{:?} to {:?}",
            pair[0],
            pair[1]
        );
    }
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
