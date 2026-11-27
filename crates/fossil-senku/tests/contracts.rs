use fossil_senku::{
    ContextRoutingInput, ContextRoutingMode, DecisionService, DegradationPolicy,
    GoalCheckCriterion, GoalCheckInput, ProfileEvaluation, ProfileId, RunEvaluationInput,
    SenkuClient, SenkuClientConfig, TaskAdmissionInput, ValidationRecoveryInput,
    degradation_policy,
};
use secrecy::SecretString;
use serde_json::{Value, json};
use std::time::Duration;
use url::Url;
use wiremock::matchers::{body_json, header, header_exists, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[path = "../../../tests/support/http_faults.rs"]
mod http_faults;

fn task_admission() -> ProfileEvaluation {
    ProfileEvaluation::task_admission(TaskAdmissionInput {
        repo_url: "https://example.test/acme/project".to_owned(),
        ref_name: Some("main".to_owned()),
        task_prompt: Some("Add a small health endpoint.".to_owned()),
        ..TaskAdmissionInput::default()
    })
    .unwrap()
}

fn choice(choice: &str, labels: &[&str]) -> Value {
    let probability = 1.0 / labels.len() as f64;
    json!({
        "type": "choice",
        "choice": choice,
        "probabilities": labels.iter().map(|label| ((*label).to_owned(), json!(probability))).collect::<serde_json::Map<_, _>>(),
        "confidence": null
    })
}

fn valid_task_response() -> Value {
    let answers = json!({
        "contains_credentials": {"type":"noul","noul":0.05,"probabilities":{"false":0.95,"true":0.05},"confidence":null},
        "changes_permissions": {"type":"noul","noul":0.02,"probabilities":{"false":0.98,"true":0.02},"confidence":null},
        "crosses_security_boundary": {"type":"noul","noul":0.01,"probabilities":{"false":0.99,"true":0.01},"confidence":null},
        "risk_level": choice("low", &["low", "review", "high"]),
        "task_type": choice("feature", &["feature", "bug_fix", "refactor", "documentation", "investigation", "test_only", "infrastructure"]),
        "validation_focus": choice("integration_tests", &["unit_tests", "integration_tests", "type_and_lint", "docs_checks", "repo_defined_checks"])
    });
    json!({
        "schemaVersion":"1", "requestId":"dec-test-001", "status":"completed",
        "profileId":"task_admission_v1", "profileVersion":"1", "provider":"jev",
        "model":"test/model", "answers":answers, "result":answers,
        "usage":{"input_tokens":10,"output_tokens":8,"total_tokens":18},
        "durationMs":12, "attemptCount":1
    })
}

fn response(profile: ProfileId, answers: Value) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "schemaVersion":"1", "requestId":"dec-test-001", "status":"completed",
        "profileId":profile.as_str(), "profileVersion":"1", "provider":"jev",
        "model":"test/model", "answers":answers, "result":answers,
        "durationMs":12, "attemptCount":1
    }))
    .unwrap()
}

fn noul_answer(value: f64) -> Value {
    json!({
        "type":"noul", "noul":value,
        "probabilities":{"false":1.0-value,"true":value}, "confidence":null
    })
}

fn score_answer() -> Value {
    json!({
        "type":"score", "score":3, "legend":{
            "0":"Largely unrealized",
            "1":"Major requirements unmet",
            "2":"Partially complete",
            "3":"Nearly complete",
            "4":"All requirements verified"
        },
        "probabilities":{"0":0.1,"1":0.1,"2":0.1,"3":0.6,"4":0.1}, "confidence":null
    })
}

#[test]
fn supports_the_five_published_profile_ids() {
    let ids = [
        ProfileId::TaskAdmission,
        ProfileId::KasekiContextRouting,
        ProfileId::ValidationRecovery,
        ProfileId::KasekiGoalCheck,
        ProfileId::KasekiRunEvaluation,
    ];
    assert_eq!(
        ids.map(|id| id.as_str()),
        [
            "task_admission_v1",
            "kaseki_context_routing_v1",
            "validation_recovery_v1",
            "kaseki_goal_check_v1",
            "kaseki_run_evaluation_v1",
        ]
    );
    assert_eq!(
        degradation_policy(ProfileId::TaskAdmission),
        DegradationPolicy::ContinueDeterministicAdmission
    );
    assert_eq!(
        degradation_policy(ProfileId::ValidationRecovery),
        DegradationPolicy::PreserveFailedValidationWithoutRetry
    );
}

#[test]
fn pinned_contract_artifact_contains_only_generic_contract_keys() {
    let artifact: Value =
        serde_json::from_str(include_str!("../contracts/senku-profiles.v1.json")).unwrap();
    let contracts = artifact["contracts"].as_array().unwrap();
    let artifact_keys = contracts
        .iter()
        .map(|contract| contract["key"].as_str().unwrap())
        .collect::<std::collections::BTreeSet<_>>();
    let rust_keys = ProfileId::ALL
        .map(ProfileId::contract_key)
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(artifact_keys, rust_keys);

    for contract in contracts {
        for private_field in [
            "id",
            "owner",
            "allowedClients",
            "evaluationEndpoint",
            "limits",
            "operations",
        ] {
            assert!(
                contract.get(private_field).is_none(),
                "unexpected field: {private_field}"
            );
        }
        assert!(contract.get("outputSchema").is_some());
    }
}

#[test]
fn task_admission_uses_the_versioned_senku_envelope() {
    let request: Value = serde_json::from_slice(&task_admission().request_body().unwrap()).unwrap();
    assert_eq!(request["schemaVersion"], "1");
    assert_eq!(
        request["input"]["repoUrl"],
        "https://example.test/acme/project"
    );
    assert_eq!(request["input"]["ref"], "main");
    assert!(request.get("questions").is_none());
}

#[test]
fn rejects_wrong_answer_ids_and_inconsistent_result_mirrors() {
    let evaluation = task_admission();
    let mut response = valid_task_response();
    response["answers"]["risk_level"] = choice("low", &["low", "review"]);
    assert!(
        evaluation
            .validate_response(&serde_json::to_vec(&response).unwrap())
            .is_err()
    );

    let mut response = valid_task_response();
    response["result"]["risk_level"] = json!(null);
    assert!(
        evaluation
            .validate_response(&serde_json::to_vec(&response).unwrap())
            .is_err()
    );
}

#[test]
fn rejects_unrecognized_response_envelope_fields() {
    let evaluation = task_admission();
    let mut response = valid_task_response();
    response.as_object_mut().unwrap().remove("usage");
    response["unexpectedEnvelopeField"] = json!("synthetic extra metadata");

    assert!(
        evaluation
            .validate_response(&serde_json::to_vec(&response).unwrap())
            .is_err()
    );
}

#[test]
fn validates_dynamic_profile_answer_ids_and_all_remaining_profile_contracts() {
    let context = ProfileEvaluation::context_routing(ContextRoutingInput {
        mode: ContextRoutingMode::PhaseTransfer,
        evidence: json!({
            "phase_from":"scouting", "phase_to":"coding", "goal_summary":"Implement feature",
            "candidates":[{"id":"file_1","kind":"inspected_file","path":"src/lib.rs","text":"The project entry point"}]
        }),
    })
    .unwrap();
    let context_answers = json!({"file_1":choice("preserve", &["preserve","condense","discard"]) });
    assert!(
        context
            .validate_response(&response(ProfileId::KasekiContextRouting, context_answers))
            .is_ok()
    );
    let wrong_context_answers =
        json!({"file_2":choice("preserve", &["preserve","condense","discard"]) });
    assert!(
        context
            .validate_response(&response(
                ProfileId::KasekiContextRouting,
                wrong_context_answers
            ))
            .is_err()
    );

    let validation = ProfileEvaluation::validation_recovery(ValidationRecoveryInput {
        command: "cargo test".to_owned(),
        exit_code: 1,
        output: "assertion failed".to_owned(),
    })
    .unwrap();
    let validation_answers = json!({
        "validation_failure_cause":choice("test_failure", &["test_failure","type_or_lint_failure","build_failure","dependency_or_environment","timeout_or_flaky","implementation_logic","unknown"]),
        "validation_recovery_action":choice("inspect_diagnostics", &["fix_implementation","repair_environment","retry_validation_once","inspect_diagnostics"])
    });
    assert!(
        validation
            .validate_response(&response(ProfileId::ValidationRecovery, validation_answers))
            .is_ok()
    );

    let goal = ProfileEvaluation::goal_check(GoalCheckInput {
        state: json!({"validation":"passed"}),
        criteria: vec![GoalCheckCriterion {
            id: "criterion_1".to_owned(),
            criterion: "The test passes".to_owned(),
            applies_when: Some("The test exists".to_owned()),
            source_requirement: None,
            verification_sources: None,
        }],
    })
    .unwrap();
    let goal_answers = json!({
        "criterion_1":noul_answer(0.9),
        "criterion_1_applicability":choice("applicable", &["applicable","not_applicable","unknown"])
    });
    assert!(
        goal.validate_response(&response(ProfileId::KasekiGoalCheck, goal_answers))
            .is_ok()
    );

    let run = ProfileEvaluation::run_evaluation(RunEvaluationInput {
        state: json!({"summary":"run completed"}),
        include_failure_diagnosis: true,
    })
    .unwrap();
    let run_answers = json!({
        "overall_assessment":choice("good", &["excellent","good","mixed","poor"]),
        "reviewer_confidence":choice("medium", &["high","medium","low"]),
        "task_completion_score":score_answer(),
        "validation_failure_cause":choice("unknown", &["test_failure","type_or_lint_failure","build_failure","dependency_or_environment","timeout_or_flaky","implementation_logic","unknown"]),
        "validation_recovery_action":choice("inspect_diagnostics", &["fix_implementation","repair_environment","retry_validation_once","inspect_diagnostics"])
    });
    assert!(
        run.validate_response(&response(ProfileId::KasekiRunEvaluation, run_answers))
            .is_ok()
    );
}

#[test]
fn accepts_additive_envelope_fields_and_multiple_provider_attempts() {
    let evaluation = task_admission();
    let mut response = valid_task_response();
    response["attemptCount"] = json!(2);
    response["serverTiming"] = json!({ "providerAttempts": 2 });

    let validated = evaluation
        .validate_response(&serde_json::to_vec(&response).unwrap())
        .unwrap();
    assert_eq!(validated.attempt_count, 2);
}

#[test]
fn request_evidence_is_redacted_before_it_is_sent() {
    let evaluation = ProfileEvaluation::task_admission(TaskAdmissionInput {
        repo_url: "https://example.test/acme/project".to_owned(),
        task_prompt: Some(
            "Use Authorization: Bearer bearer-secret-value and ghp_secret_token_value".to_owned(),
        ),
        ..TaskAdmissionInput::default()
    })
    .unwrap();
    let request: Value = serde_json::from_slice(&evaluation.request_body().unwrap()).unwrap();
    let prompt = request["input"]["taskPrompt"].as_str().unwrap();
    assert!(!prompt.contains("bearer-secret-value"));
    assert!(!prompt.contains("ghp_secret_token_value"));
    assert!(prompt.contains("[REDACTED]"));
}

#[test]
fn senku_origin_allows_only_https_or_loopback_http_and_config_redacts_tokens() {
    assert!(
        SenkuClientConfig::new(
            Url::parse("http://127.0.0.1:8787").unwrap(),
            SecretString::from("synthetic-senku-token-000000000000".to_owned()),
            Duration::from_secs(2),
        )
        .is_ok()
    );
    assert!(
        SenkuClientConfig::new(
            Url::parse("http://example.test").unwrap(),
            SecretString::from("synthetic-senku-token-000000000000".to_owned()),
            Duration::from_secs(2),
        )
        .is_err()
    );
    let config = SenkuClientConfig::new(
        Url::parse("https://senku.example.test").unwrap(),
        SecretString::from("synthetic-senku-token-000000000000".to_owned()),
        Duration::from_secs(2),
    )
    .unwrap();
    assert!(!format!("{config:?}").contains("synthetic-senku-token"));
}

#[tokio::test]
async fn sends_one_authenticated_request_and_validates_the_profile_response() {
    let server = MockServer::start().await;
    let response = valid_task_response();
    Mock::given(method("POST"))
        .and(path("/v1/profiles/task_admission_v1/evaluate"))
        .and(header(
            "authorization",
            "Bearer synthetic-senku-token-000000000000",
        ))
        .and(header_exists("x-correlation-id"))
        .and(header_exists("x-senku-deadline-ms"))
        .and(body_json(json!({
            "schemaVersion":"1",
            "input": {
                "repoUrl":"https://example.test/acme/project",
                "ref":"main",
                "taskPrompt":"Add a small health endpoint."
            }
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(response))
        .expect(1)
        .mount(&server)
        .await;

    let config = SenkuClientConfig::new(
        Url::parse(&server.uri()).unwrap(),
        SecretString::from("synthetic-senku-token-000000000000".to_owned()),
        Duration::from_secs(2),
    )
    .unwrap();
    let client = SenkuClient::new(config).unwrap();
    let decision = client.evaluate(task_admission()).await.unwrap();
    assert_eq!(decision.profile_id, ProfileId::TaskAdmission);
    assert_eq!(decision.request_id, "dec-test-001");
    assert_eq!(decision.attempt_count, 1);
    assert!(!decision.correlation_id.is_empty());
    assert!(decision.answers.contains_key("risk_level"));
}

#[tokio::test]
async fn upstream_faults_are_bounded_classified_and_never_retried() {
    use fossil_senku::DecisionError;
    use http_faults::{HttpFaultScenario, mount_http_fault};

    let scenarios = [
        (
            HttpFaultScenario::Status(429),
            DecisionError::RateLimited,
            true,
        ),
        (
            HttpFaultScenario::Status(503),
            DecisionError::Unavailable,
            true,
        ),
        (
            HttpFaultScenario::MalformedJson,
            DecisionError::InvalidResponse,
            false,
        ),
        (
            HttpFaultScenario::Oversized {
                bytes: 64 * 1024 + 1,
            },
            DecisionError::InvalidResponse,
            false,
        ),
        (
            HttpFaultScenario::Delayed {
                duration: Duration::from_millis(250),
            },
            DecisionError::Timeout,
            true,
        ),
    ];

    for (scenario, expected, retryable) in scenarios {
        let server = MockServer::start().await;
        mount_http_fault(
            &server,
            "POST",
            "/v1/profiles/task_admission_v1/evaluate",
            scenario,
        )
        .await;
        let timeout = if expected == DecisionError::Timeout {
            Duration::from_millis(100)
        } else {
            Duration::from_secs(1)
        };
        let client = SenkuClient::new(
            SenkuClientConfig::new(
                Url::parse(&server.uri()).unwrap(),
                SecretString::from("synthetic-senku-token-000000000000".to_owned()),
                timeout,
            )
            .unwrap(),
        )
        .unwrap();

        let error = client.evaluate(task_admission()).await.unwrap_err();
        assert_eq!(error, expected);
        assert_eq!(error.is_retryable(), retryable);
        assert!(!error.to_string().contains("PRIVATE_UPSTREAM_DIAGNOSTIC"));
        let metric = client.metrics().profiles[0];
        assert_eq!(metric.attempts, 1);
        assert_eq!(metric.successes, 0);
        if retryable {
            assert_eq!(metric.transport_failures, 1);
        } else {
            assert_eq!(metric.invalid_responses, 1);
        }
    }
}
