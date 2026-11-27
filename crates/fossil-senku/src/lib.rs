//! Contract-checked, advisory-only client for Senku's Kaseki profiles.

use async_trait::async_trait;
use reqwest::{Client, Method, Response, StatusCode};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use url::Url;
use uuid::Uuid;

const MAX_REQUEST_BYTES: usize = 128 * 1024;
const MAX_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_CONTEXT_EVIDENCE_BYTES: usize = 32 * 1024;
const MAX_GOAL_EVIDENCE_BYTES: usize = 48 * 1024;
const MAX_RUN_EVIDENCE_BYTES: usize = 96 * 1024;
const MAX_VALIDATION_EVIDENCE_BYTES: usize = 8 * 1024;
const MAX_JSON_DEPTH: usize = 8;
const MAX_LIST_ITEMS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProfileId {
    TaskAdmission,
    KasekiContextRouting,
    ValidationRecovery,
    KasekiGoalCheck,
    KasekiRunEvaluation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DegradationPolicy {
    ContinueDeterministicAdmission,
    PreserveRequiredContext,
    PreserveFailedValidationWithoutRetry,
    TreatConfidenceAsUnavailable,
    ContinueWithoutRunScore,
}

impl ProfileId {
    pub const ALL: [Self; 5] = [
        Self::TaskAdmission,
        Self::KasekiContextRouting,
        Self::ValidationRecovery,
        Self::KasekiGoalCheck,
        Self::KasekiRunEvaluation,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TaskAdmission => "task_admission_v1",
            Self::KasekiContextRouting => "kaseki_context_routing_v1",
            Self::ValidationRecovery => "validation_recovery_v1",
            Self::KasekiGoalCheck => "kaseki_goal_check_v1",
            Self::KasekiRunEvaluation => "kaseki_run_evaluation_v1",
        }
    }

    pub const fn contract_key(self) -> &'static str {
        match self {
            Self::TaskAdmission => "contract_01",
            Self::KasekiContextRouting => "contract_02",
            Self::ValidationRecovery => "contract_03",
            Self::KasekiGoalCheck => "contract_04",
            Self::KasekiRunEvaluation => "contract_05",
        }
    }
}

pub const fn degradation_policy(profile: ProfileId) -> DegradationPolicy {
    match profile {
        ProfileId::TaskAdmission => DegradationPolicy::ContinueDeterministicAdmission,
        ProfileId::KasekiContextRouting => DegradationPolicy::PreserveRequiredContext,
        ProfileId::ValidationRecovery => DegradationPolicy::PreserveFailedValidationWithoutRetry,
        ProfileId::KasekiGoalCheck => DegradationPolicy::TreatConfidenceAsUnavailable,
        ProfileId::KasekiRunEvaluation => DegradationPolicy::ContinueWithoutRunScore,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskMode {
    Patch,
    Inspect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublishMode {
    Auto,
    None,
    Branch,
    Pr,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentStageSettings {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u32>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskAdmissionInput {
    pub repo_url: String,
    #[serde(rename = "ref", skip_serializing_if = "Option::is_none")]
    pub ref_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_prompt: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validation_commands: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub changed_files_allowlist: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_mode: Option<TaskMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publish_mode: Option<PublishMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub startup_check: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scouting: Option<AgentStageSettings>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub goal_setting: Option<AgentStageSettings>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextRoutingMode {
    ToolOutput,
    SemanticContext,
    PhaseTransfer,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ContextRoutingInput {
    pub mode: ContextRoutingMode,
    pub evidence: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ValidationRecoveryInput {
    pub command: String,
    pub exit_code: u16,
    pub output: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GoalCheckCriterion {
    pub id: String,
    pub criterion: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub applies_when: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_requirement: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verification_sources: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GoalCheckInput {
    pub state: Value,
    pub criteria: Vec<GoalCheckCriterion>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunEvaluationInput {
    pub state: Value,
    pub include_failure_diagnosis: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProfileEvaluation {
    TaskAdmission(TaskAdmissionInput),
    KasekiContextRouting(ContextRoutingInput),
    ValidationRecovery(ValidationRecoveryInput),
    KasekiGoalCheck(GoalCheckInput),
    KasekiRunEvaluation(RunEvaluationInput),
}

impl ProfileEvaluation {
    pub fn task_admission(input: TaskAdmissionInput) -> Result<Self, DecisionError> {
        let evaluation = Self::TaskAdmission(input);
        evaluation.validate_input()?;
        Ok(evaluation)
    }

    pub fn context_routing(input: ContextRoutingInput) -> Result<Self, DecisionError> {
        let evaluation = Self::KasekiContextRouting(input);
        evaluation.validate_input()?;
        Ok(evaluation)
    }

    pub fn validation_recovery(input: ValidationRecoveryInput) -> Result<Self, DecisionError> {
        let evaluation = Self::ValidationRecovery(input);
        evaluation.validate_input()?;
        Ok(evaluation)
    }

    pub fn goal_check(input: GoalCheckInput) -> Result<Self, DecisionError> {
        let evaluation = Self::KasekiGoalCheck(input);
        evaluation.validate_input()?;
        Ok(evaluation)
    }

    pub fn run_evaluation(input: RunEvaluationInput) -> Result<Self, DecisionError> {
        let evaluation = Self::KasekiRunEvaluation(input);
        evaluation.validate_input()?;
        Ok(evaluation)
    }

    pub const fn profile_id(&self) -> ProfileId {
        match self {
            Self::TaskAdmission(_) => ProfileId::TaskAdmission,
            Self::KasekiContextRouting(_) => ProfileId::KasekiContextRouting,
            Self::ValidationRecovery(_) => ProfileId::ValidationRecovery,
            Self::KasekiGoalCheck(_) => ProfileId::KasekiGoalCheck,
            Self::KasekiRunEvaluation(_) => ProfileId::KasekiRunEvaluation,
        }
    }

    /// Returns the exact Senku `{ schemaVersion, input }` request envelope.
    /// Evidence is redacted before it is serialized for transport.
    pub fn request_body(&self) -> Result<Vec<u8>, DecisionError> {
        self.validate_input()?;
        let input = self.sanitized_input()?;
        let bytes = serde_json::to_vec(&json!({ "schemaVersion": "1", "input": input }))
            .map_err(|_| DecisionError::InvalidRequest)?;
        if bytes.len() > MAX_REQUEST_BYTES {
            return Err(DecisionError::InvalidRequest);
        }
        Ok(bytes)
    }

    pub fn validate_response(&self, body: &[u8]) -> Result<ValidatedDecision, DecisionError> {
        validate_response(self, body, "")
    }

    fn validate_input(&self) -> Result<(), DecisionError> {
        match self {
            Self::TaskAdmission(input) => validate_task_admission(input),
            Self::KasekiContextRouting(input) => validate_context_routing(input),
            Self::ValidationRecovery(input) => validate_validation_recovery(input),
            Self::KasekiGoalCheck(input) => validate_goal_check(input),
            Self::KasekiRunEvaluation(input) => validate_run_evaluation(input),
        }
    }

    fn sanitized_input(&self) -> Result<Value, DecisionError> {
        let mut value = match self {
            Self::TaskAdmission(input) => serde_json::to_value(input),
            Self::KasekiContextRouting(input) => serde_json::to_value(input),
            Self::ValidationRecovery(input) => serde_json::to_value(input),
            Self::KasekiGoalCheck(input) => serde_json::to_value(input),
            Self::KasekiRunEvaluation(input) => serde_json::to_value(input),
        }
        .map_err(|_| DecisionError::InvalidRequest)?;
        if let Self::ValidationRecovery(_) = self
            && let Some(output) = value
                .get("output")
                .and_then(Value::as_str)
                .map(|output| tail_chars(&redact_text(output), 5_000))
        {
            value["output"] = Value::String(output);
        }
        redact_value(&mut value, 0)?;
        let bytes = serde_json::to_vec(&value).map_err(|_| DecisionError::InvalidRequest)?;
        let maximum_input_bytes = match self {
            Self::TaskAdmission(_) => Some(32 * 1024),
            Self::ValidationRecovery(_) => Some(MAX_VALIDATION_EVIDENCE_BYTES),
            _ => None,
        };
        if maximum_input_bytes.is_some_and(|maximum| bytes.len() > maximum) {
            return Err(DecisionError::InvalidRequest);
        }
        Ok(value)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum DecisionAnswer {
    Noul {
        noul: f64,
        probabilities: BTreeMap<String, f64>,
    },
    Choice {
        choice: String,
        probabilities: BTreeMap<String, f64>,
        confidence: Option<f64>,
    },
    Score {
        score: f64,
        legend: BTreeMap<String, String>,
        probabilities: BTreeMap<String, f64>,
        confidence: Option<f64>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
    pub cost_micros: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ValidatedDecision {
    pub profile_id: ProfileId,
    pub profile_version: String,
    pub request_id: String,
    pub provider: String,
    pub model: String,
    pub answers: BTreeMap<String, DecisionAnswer>,
    pub usage: Option<ProviderUsage>,
    pub duration_ms: u64,
    pub attempt_count: u64,
    pub correlation_id: String,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DecisionError {
    #[error("Senku client configuration is invalid")]
    InvalidConfiguration,
    #[error("Senku request is invalid or exceeds a profile limit")]
    InvalidRequest,
    #[error("Senku request timed out")]
    Timeout,
    #[error("Senku network request failed")]
    Network,
    #[error("Senku authentication was rejected")]
    Unauthorized,
    #[error("Senku credential is not authorized for this profile")]
    Forbidden,
    #[error("Senku rate limited the request")]
    RateLimited,
    #[error("Senku service is unavailable")]
    Unavailable,
    #[error("Senku returned an unexpected HTTP status")]
    HttpStatus,
    #[error("Senku response is malformed, oversized, or incompatible")]
    InvalidResponse,
}

impl DecisionError {
    pub const fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Timeout | Self::Network | Self::RateLimited | Self::Unavailable
        )
    }
}

#[async_trait]
pub trait DecisionService: Send + Sync {
    async fn evaluate(
        &self,
        request: ProfileEvaluation,
    ) -> Result<ValidatedDecision, DecisionError>;
}

#[derive(Clone)]
pub struct SenkuClientConfig {
    origin: Url,
    token: SecretString,
    request_timeout: Duration,
}

impl SenkuClientConfig {
    pub fn new(
        origin: Url,
        token: SecretString,
        request_timeout: Duration,
    ) -> Result<Self, DecisionError> {
        let secure_scheme = origin.scheme() == "https"
            || (origin.scheme() == "http" && is_loopback_origin(&origin));
        if !secure_scheme
            || origin.host_str().is_none()
            || !origin.username().is_empty()
            || origin.password().is_some()
            || !matches!(origin.path(), "" | "/")
            || origin.query().is_some()
            || origin.fragment().is_some()
            || token.expose_secret().len() < 32
            || token.expose_secret().len() > 1_024
            || !token
                .expose_secret()
                .bytes()
                .all(|byte| (0x21..=0x7e).contains(&byte))
            || !(Duration::from_millis(100)..=Duration::from_secs(300)).contains(&request_timeout)
        {
            return Err(DecisionError::InvalidConfiguration);
        }
        Ok(Self {
            origin,
            token,
            request_timeout,
        })
    }
}

impl fmt::Debug for SenkuClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SenkuClientConfig")
            .field("origin", &self.origin)
            .field("token", &"[REDACTED]")
            .field("request_timeout", &self.request_timeout)
            .finish()
    }
}

#[derive(Default)]
struct ClientMetrics {
    attempts: [AtomicU64; 5],
    successes: [AtomicU64; 5],
    invalid_responses: [AtomicU64; 5],
    transport_failures: [AtomicU64; 5],
    degraded: [AtomicU64; 5],
    duration_ms_total: [AtomicU64; 5],
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProfileMetricSnapshot {
    pub attempts: u64,
    pub successes: u64,
    pub invalid_responses: u64,
    pub transport_failures: u64,
    pub degraded: u64,
    pub duration_ms_total: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SenkuMetricsSnapshot {
    pub profiles: [ProfileMetricSnapshot; 5],
}

#[derive(Clone)]
pub struct SenkuClient {
    client: Client,
    origin: Url,
    token: SecretString,
    provider_deadline_ms: u64,
    metrics: Arc<ClientMetrics>,
}

impl SenkuClient {
    pub fn new(config: SenkuClientConfig) -> Result<Self, DecisionError> {
        let provider_deadline_ms = provider_deadline_budget_ms(config.request_timeout);
        let client = Client::builder()
            .timeout(config.request_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| DecisionError::InvalidConfiguration)?;
        Ok(Self {
            client,
            origin: config.origin,
            token: config.token,
            provider_deadline_ms,
            metrics: Arc::new(ClientMetrics::default()),
        })
    }

    pub fn metrics(&self) -> SenkuMetricsSnapshot {
        SenkuMetricsSnapshot {
            profiles: std::array::from_fn(|index| ProfileMetricSnapshot {
                attempts: self.metrics.attempts[index].load(Ordering::Relaxed),
                successes: self.metrics.successes[index].load(Ordering::Relaxed),
                invalid_responses: self.metrics.invalid_responses[index].load(Ordering::Relaxed),
                transport_failures: self.metrics.transport_failures[index].load(Ordering::Relaxed),
                degraded: self.metrics.degraded[index].load(Ordering::Relaxed),
                duration_ms_total: self.metrics.duration_ms_total[index].load(Ordering::Relaxed),
            }),
        }
    }

    pub fn record_degraded(&self, profile: ProfileId) {
        self.metrics.degraded[profile_index(profile)].fetch_add(1, Ordering::Relaxed);
    }
}

#[async_trait]
impl DecisionService for SenkuClient {
    async fn evaluate(
        &self,
        request: ProfileEvaluation,
    ) -> Result<ValidatedDecision, DecisionError> {
        let profile = request.profile_id();
        let index = profile_index(profile);
        let body = request.request_body()?;
        self.metrics.attempts[index].fetch_add(1, Ordering::Relaxed);
        let started = std::time::Instant::now();
        let correlation_id = Uuid::new_v4().to_string();
        let deadline_at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .saturating_add(self.provider_deadline_ms as u128)
            .min(u64::MAX as u128) as u64;
        let url = format!(
            "{}/v1/profiles/{}/evaluate",
            self.origin.as_str().trim_end_matches('/'),
            profile.as_str()
        );
        let response = self
            .client
            .request(Method::POST, url)
            .bearer_auth(self.token.expose_secret())
            .header(reqwest::header::ACCEPT, "application/json")
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header("x-correlation-id", &correlation_id)
            .header("x-senku-deadline-ms", deadline_at_ms.to_string())
            .body(body)
            .send()
            .await
            .map_err(classify_transport);
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                self.metrics.transport_failures[index].fetch_add(1, Ordering::Relaxed);
                return Err(error);
            }
        };
        if !response.status().is_success() {
            self.metrics.transport_failures[index].fetch_add(1, Ordering::Relaxed);
            return Err(classify_status(response.status()));
        }
        let body = match read_limited_body(response).await {
            Ok(body) => body,
            Err(error) => {
                self.metrics.invalid_responses[index].fetch_add(1, Ordering::Relaxed);
                return Err(error);
            }
        };
        match validate_response(&request, &body, &correlation_id) {
            Ok(decision) => {
                let duration_ms = started.elapsed().as_millis().min(u64::MAX as u128) as u64;
                self.metrics.successes[index].fetch_add(1, Ordering::Relaxed);
                self.metrics.duration_ms_total[index].fetch_add(duration_ms, Ordering::Relaxed);
                Ok(decision)
            }
            Err(error) => {
                self.metrics.invalid_responses[index].fetch_add(1, Ordering::Relaxed);
                Err(error)
            }
        }
    }
}

fn profile_index(profile: ProfileId) -> usize {
    match profile {
        ProfileId::TaskAdmission => 0,
        ProfileId::KasekiContextRouting => 1,
        ProfileId::ValidationRecovery => 2,
        ProfileId::KasekiGoalCheck => 3,
        ProfileId::KasekiRunEvaluation => 4,
    }
}

fn is_loopback_origin(url: &Url) -> bool {
    match url.host_str() {
        Some("localhost") => true,
        Some(host) => host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback()),
        None => false,
    }
}

fn classify_transport(error: reqwest::Error) -> DecisionError {
    if error.is_timeout() {
        DecisionError::Timeout
    } else {
        DecisionError::Network
    }
}

fn classify_status(status: StatusCode) -> DecisionError {
    match status {
        StatusCode::UNAUTHORIZED => DecisionError::Unauthorized,
        StatusCode::FORBIDDEN => DecisionError::Forbidden,
        StatusCode::TOO_MANY_REQUESTS => DecisionError::RateLimited,
        StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT => DecisionError::Unavailable,
        _ if status.is_server_error() => DecisionError::Unavailable,
        _ => DecisionError::HttpStatus,
    }
}

async fn read_limited_body(mut response: Response) -> Result<Vec<u8>, DecisionError> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
    {
        return Err(DecisionError::InvalidResponse);
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| DecisionError::Network)? {
        if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err(DecisionError::InvalidResponse);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn validate_task_admission(input: &TaskAdmissionInput) -> Result<(), DecisionError> {
    bounded_text(&input.repo_url, 1, 2_048)?;
    let url = Url::parse(&input.repo_url).map_err(|_| DecisionError::InvalidRequest)?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(DecisionError::InvalidRequest);
    }
    if let Some(value) = &input.ref_name {
        bounded_text(value, 1, 256)?;
    }
    if let Some(value) = &input.task_prompt {
        bounded_text(value, 10, 16 * 1024)?;
    }
    validate_string_list(input.validation_commands.as_deref(), 32, 512)?;
    validate_string_list(input.changed_files_allowlist.as_deref(), 64, 512)?;
    for settings in [&input.scouting, &input.goal_setting].into_iter().flatten() {
        if settings
            .model
            .as_deref()
            .is_some_and(|model| bounded_text(model, 1, 256).is_err())
            || settings
                .timeout_seconds
                .is_some_and(|seconds| !(60..=10_800).contains(&seconds))
        {
            return Err(DecisionError::InvalidRequest);
        }
    }
    Ok(())
}

fn validate_context_routing(input: &ContextRoutingInput) -> Result<(), DecisionError> {
    if !input.evidence.is_object() {
        return Err(DecisionError::InvalidRequest);
    }
    validate_json_shape(&input.evidence, 0)?;
    let evidence = input.evidence.as_object().expect("checked object");
    match input.mode {
        ContextRoutingMode::ToolOutput | ContextRoutingMode::SemanticContext => {
            let mut allowed = vec![
                "phase",
                "toolName",
                "resultKind",
                "isError",
                "characters",
                "bytes",
                "approximateTokens",
                "lineCount",
                "consecutiveDuplicateLineRatio",
                "hasReadRange",
                "toolOutputTargetTokens",
                "exitStatus",
                "operation",
                "filePath",
                "overSoftToolOutputTarget",
                "changedState",
                "containsFailureSignal",
            ];
            if input.mode == ContextRoutingMode::SemanticContext {
                allowed.push("goalSummary");
            }
            if evidence.keys().any(|key| !allowed.contains(&key.as_str())) {
                return Err(DecisionError::InvalidRequest);
            }
            for key in ["phase", "toolName", "resultKind"] {
                required_text(evidence, key, 1, 256)?;
            }
            if ![
                "source_code",
                "search_output",
                "command_output",
                "file_content",
                "other",
            ]
            .contains(&evidence["resultKind"].as_str().unwrap_or_default())
            {
                return Err(DecisionError::InvalidRequest);
            }
            if !evidence.get("isError").is_some_and(Value::is_boolean)
                || !evidence.get("hasReadRange").is_some_and(Value::is_boolean)
                || !evidence
                    .get("consecutiveDuplicateLineRatio")
                    .and_then(Value::as_f64)
                    .is_some_and(|value| (0.0..=1.0).contains(&value))
            {
                return Err(DecisionError::InvalidRequest);
            }
            for key in ["characters", "bytes", "approximateTokens", "lineCount"] {
                if !evidence
                    .get(key)
                    .and_then(Value::as_f64)
                    .is_some_and(|value| value.is_finite() && value >= 0.0)
                {
                    return Err(DecisionError::InvalidRequest);
                }
            }
            for key in ["toolOutputTargetTokens", "exitStatus"] {
                if let Some(value) = evidence.get(key)
                    && !value.as_u64().is_some()
                {
                    return Err(DecisionError::InvalidRequest);
                }
            }
            for key in [
                "overSoftToolOutputTarget",
                "changedState",
                "containsFailureSignal",
            ] {
                if let Some(value) = evidence.get(key)
                    && !value.is_boolean()
                {
                    return Err(DecisionError::InvalidRequest);
                }
            }
            for key in ["operation", "filePath"] {
                if let Some(value) = evidence.get(key)
                    && !value
                        .as_str()
                        .is_some_and(|text| !text.trim().is_empty() && text.len() <= 256)
                {
                    return Err(DecisionError::InvalidRequest);
                }
            }
            if let Some(value) = evidence.get("goalSummary")
                && !value.as_str().is_some_and(|text| text.len() <= 1_200)
            {
                return Err(DecisionError::InvalidRequest);
            }
            for key in ["command", "output", "sourceCode", "rawOutput", "toolOutput"] {
                if evidence.contains_key(key) {
                    return Err(DecisionError::InvalidRequest);
                }
            }
        }
        ContextRoutingMode::PhaseTransfer => {
            if evidence.keys().any(|key| {
                !["phase_from", "phase_to", "goal_summary", "candidates"].contains(&key.as_str())
            }) {
                return Err(DecisionError::InvalidRequest);
            }
            required_text(evidence, "phase_from", 1, 120)?;
            if required_text(evidence, "phase_to", 1, 120)? != "coding" {
                return Err(DecisionError::InvalidRequest);
            }
            required_text(evidence, "goal_summary", 0, 1_200)?;
            let candidates = evidence
                .get("candidates")
                .and_then(Value::as_array)
                .filter(|items| (1..=32).contains(&items.len()))
                .ok_or(DecisionError::InvalidRequest)?;
            let mut ids = BTreeSet::new();
            for candidate in candidates {
                let candidate = candidate.as_object().ok_or(DecisionError::InvalidRequest)?;
                if candidate
                    .keys()
                    .any(|key| !["id", "kind", "path", "text"].contains(&key.as_str()))
                {
                    return Err(DecisionError::InvalidRequest);
                }
                let id = required_text(candidate, "id", 1, 64)?;
                if !is_context_candidate_id(id) || !ids.insert(id.to_owned()) {
                    return Err(DecisionError::InvalidRequest);
                }
                let kind = required_text(candidate, "kind", 1, 64)?;
                if !["inspected_file", "observation", "plan", "test_impact"].contains(&kind) {
                    return Err(DecisionError::InvalidRequest);
                }
                required_text(candidate, "text", 1, 700)?;
                if let Some(path) = candidate.get("path")
                    && !path
                        .as_str()
                        .is_some_and(|value| (1..=256).contains(&value.len()))
                {
                    return Err(DecisionError::InvalidRequest);
                }
            }
        }
    }
    let bytes = serde_json::to_vec(&input.evidence).map_err(|_| DecisionError::InvalidRequest)?;
    if bytes.len() > MAX_CONTEXT_EVIDENCE_BYTES {
        return Err(DecisionError::InvalidRequest);
    }
    Ok(())
}

fn validate_validation_recovery(input: &ValidationRecoveryInput) -> Result<(), DecisionError> {
    bounded_text(&input.command, 1, 1_000)?;
    if input.exit_code > 255 || input.output.len() > 6_000 {
        return Err(DecisionError::InvalidRequest);
    }
    Ok(())
}

fn validate_goal_check(input: &GoalCheckInput) -> Result<(), DecisionError> {
    if !input.state.is_object() || !(1..=32).contains(&input.criteria.len()) {
        return Err(DecisionError::InvalidRequest);
    }
    validate_json_shape(&input.state, 0)?;
    let bytes = serde_json::to_vec(&input.state).map_err(|_| DecisionError::InvalidRequest)?;
    if bytes.len() > MAX_GOAL_EVIDENCE_BYTES {
        return Err(DecisionError::InvalidRequest);
    }
    let mut total_questions = 0usize;
    for (index, criterion) in input.criteria.iter().enumerate() {
        let expected_id = format!("criterion_{}", index + 1);
        if criterion.id != expected_id {
            return Err(DecisionError::InvalidRequest);
        }
        bounded_text(&criterion.criterion, 1, 4_096)?;
        if let Some(value) = &criterion.applies_when {
            bounded_text(value, 1, 2_048)?;
            total_questions += 1;
        }
        if let Some(value) = &criterion.source_requirement {
            bounded_text(value, 1, 2_048)?;
        }
        validate_string_list(criterion.verification_sources.as_deref(), 16, 512)?;
        total_questions += 1;
    }
    if total_questions > 64 {
        return Err(DecisionError::InvalidRequest);
    }
    Ok(())
}

fn validate_run_evaluation(input: &RunEvaluationInput) -> Result<(), DecisionError> {
    if !input.state.is_object() {
        return Err(DecisionError::InvalidRequest);
    }
    validate_json_shape(&input.state, 0)?;
    let bytes = serde_json::to_vec(&input.state).map_err(|_| DecisionError::InvalidRequest)?;
    if bytes.len() > MAX_RUN_EVIDENCE_BYTES {
        return Err(DecisionError::InvalidRequest);
    }
    Ok(())
}

fn validate_string_list(
    values: Option<&[String]>,
    max_items: usize,
    max_bytes: usize,
) -> Result<(), DecisionError> {
    if let Some(values) = values {
        if values.len() > max_items {
            return Err(DecisionError::InvalidRequest);
        }
        for value in values {
            bounded_text(value, 1, max_bytes)?;
        }
    }
    Ok(())
}

fn bounded_text(value: &str, minimum: usize, maximum: usize) -> Result<(), DecisionError> {
    if value.len() < minimum || value.len() > maximum || value.trim().is_empty() {
        return Err(DecisionError::InvalidRequest);
    }
    Ok(())
}

fn required_text<'a>(
    object: &'a Map<String, Value>,
    key: &str,
    minimum: usize,
    maximum: usize,
) -> Result<&'a str, DecisionError> {
    let value = object
        .get(key)
        .and_then(Value::as_str)
        .ok_or(DecisionError::InvalidRequest)?;
    if minimum == 0 && value.is_empty() && value.len() <= maximum {
        return Ok(value);
    }
    bounded_text(value, minimum, maximum)?;
    Ok(value)
}

fn is_context_candidate_id(value: &str) -> bool {
    ["file_", "observation_", "plan_", "test_"]
        .iter()
        .any(|prefix| {
            value.strip_prefix(prefix).is_some_and(|suffix| {
                !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit())
            })
        })
}

fn validate_json_shape(value: &Value, depth: usize) -> Result<(), DecisionError> {
    if depth > MAX_JSON_DEPTH {
        return Err(DecisionError::InvalidRequest);
    }
    match value {
        Value::String(value) if value.len() > 16 * 1024 => Err(DecisionError::InvalidRequest),
        Value::Array(values) if values.len() > MAX_LIST_ITEMS => Err(DecisionError::InvalidRequest),
        Value::Array(values) => {
            for value in values {
                validate_json_shape(value, depth + 1)?;
            }
            Ok(())
        }
        Value::Object(values) => {
            if values.len() > 128 {
                return Err(DecisionError::InvalidRequest);
            }
            for value in values.values() {
                validate_json_shape(value, depth + 1)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn redact_value(value: &mut Value, depth: usize) -> Result<(), DecisionError> {
    if depth > MAX_JSON_DEPTH {
        return Err(DecisionError::InvalidRequest);
    }
    match value {
        Value::String(text) => *text = redact_text(text),
        Value::Array(values) => {
            if values.len() > MAX_LIST_ITEMS {
                return Err(DecisionError::InvalidRequest);
            }
            for value in values {
                redact_value(value, depth + 1)?;
            }
        }
        Value::Object(values) => {
            if values.len() > 128 {
                return Err(DecisionError::InvalidRequest);
            }
            for (key, value) in values.iter_mut() {
                if is_secret_key(key) {
                    *value = Value::String("[REDACTED]".to_owned());
                } else {
                    redact_value(value, depth + 1)?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn is_secret_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase().replace(['-', '_'], "");
    [
        "password",
        "passwd",
        "token",
        "secret",
        "apikey",
        "privatekey",
        "credential",
    ]
    .iter()
    .any(|pattern| key.contains(pattern))
}

fn redact_text(value: &str) -> String {
    let mut result = value.to_owned();
    let markers = [
        "ghp_",
        "gho_",
        "ghu_",
        "ghs_",
        "ghr_",
        "github_pat_",
        "sk-proj-",
        "sk-ant-",
        "hf_",
        "xoxb-",
        "xoxp-",
        "AKIA",
        "sk-",
    ];
    for marker in markers {
        let mut cursor = 0;
        while let Some(offset) = result[cursor..].find(marker) {
            let start = cursor + offset;
            let end = result[start..]
                .find(|character: char| {
                    character.is_whitespace()
                        || matches!(character, '"' | '\'' | ')' | ']' | '}' | ',' | ';')
                })
                .map(|offset| start + offset)
                .unwrap_or(result.len());
            result.replace_range(start..end, "[REDACTED]");
            cursor = start + "[REDACTED]".len();
            if cursor >= result.len() {
                break;
            }
        }
    }
    let words = result.split_whitespace().collect::<Vec<_>>();
    let mut redacted_words = Vec::with_capacity(words.len());
    let mut index = 0;
    while index < words.len() {
        let word = words[index];
        if word.eq_ignore_ascii_case("bearer") && words.get(index + 1).is_some() {
            redacted_words.push("Bearer".to_owned());
            redacted_words.push("[REDACTED]".to_owned());
            index += 2;
            continue;
        }
        if looks_like_jwt(word) {
            redacted_words.push("[REDACTED]".to_owned());
        } else if let Some((key, _)) = word.split_once(['=', ':'])
            && is_secret_key(key)
        {
            redacted_words.push(format!("{key}=[REDACTED]"));
        } else {
            redacted_words.push(redact_url_password(word));
        }
        index += 1;
    }
    let mut result = redacted_words.join(" ");
    let mut cursor = 0;
    while let Some(offset) = result[cursor..].find("-----BEGIN") {
        let start = cursor + offset;
        let header_end_offset = result[start + 5..].find("-----");
        if header_end_offset.is_none() && result[start..].contains("PRIVATE KEY") {
            result.replace_range(start.., "[REDACTED PRIVATE KEY]");
            break;
        }
        let header_end = header_end_offset
            .map(|end| start + 5 + end + 5)
            .unwrap_or(result.len());
        if !result[start..header_end].contains("PRIVATE KEY-----") {
            cursor = header_end.max(start + 1);
            if cursor >= result.len() {
                break;
            }
            continue;
        }
        let end = result[header_end..]
            .find("-----END")
            .and_then(|offset| {
                result[header_end + offset + 8..]
                    .find("-----")
                    .map(|tail| header_end + offset + 8 + tail + 5)
            })
            .unwrap_or(result.len());
        result.replace_range(start..end, "[REDACTED PRIVATE KEY]");
        cursor = start + "[REDACTED PRIVATE KEY]".len();
        if cursor >= result.len() {
            break;
        }
    }
    result
}

fn looks_like_jwt(value: &str) -> bool {
    let value = value.trim_matches(|character: char| {
        !character.is_ascii_alphanumeric()
            && character != '.'
            && character != '_'
            && character != '-'
    });
    let mut parts = value.split('.');
    let (Some(header), Some(payload), Some(signature), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    value.len() >= 32
        && [header, payload, signature].iter().all(|part| {
            !part.is_empty()
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        })
}

fn redact_url_password(value: &str) -> String {
    let leading = value.len()
        - value
            .trim_start_matches(|character: char| !character.is_ascii_alphanumeric())
            .len();
    let candidate = &value[leading..];
    let trailing = candidate
        .trim_end_matches(|character: char| {
            !character.is_ascii_alphanumeric()
                && !matches!(character, '/' | ':' | '@' | '?' | '#' | '.' | '-' | '_')
        })
        .len();
    let (url_text, suffix) = candidate.split_at(trailing);
    if let Ok(mut url) = Url::parse(url_text)
        && url.password().is_some()
        && url.set_password(Some("[REDACTED]")).is_ok()
    {
        return format!("{}{}{}", &value[..leading], url, suffix);
    }
    value.to_owned()
}

fn tail_chars(value: &str, maximum: usize) -> String {
    let chars = value.chars().count();
    if chars <= maximum {
        return value.to_owned();
    }
    value.chars().skip(chars - maximum).collect()
}

#[derive(Clone)]
enum ExpectedAnswer {
    Noul,
    Choice(Vec<String>),
    Score {
        maximum: f64,
        legend: BTreeMap<String, String>,
    },
}

fn profile_contract_artifact() -> &'static Value {
    static ARTIFACT: OnceLock<Value> = OnceLock::new();
    ARTIFACT.get_or_init(|| {
        serde_json::from_str(include_str!("../contracts/senku-profiles.v1.json"))
            .expect("checked-in Senku profile contract artifact must be valid JSON")
    })
}

fn contract_profile(profile_id: ProfileId) -> Result<&'static Value, DecisionError> {
    profile_contract_artifact()
        .get("contracts")
        .and_then(Value::as_array)
        .and_then(|contracts| {
            contracts.iter().find(|contract| {
                contract.get("key").and_then(Value::as_str) == Some(profile_id.contract_key())
            })
        })
        .ok_or(DecisionError::InvalidResponse)
}

fn answer_from_schema(schema: &Value) -> Result<ExpectedAnswer, DecisionError> {
    let properties = schema
        .get("properties")
        .and_then(Value::as_object)
        .ok_or(DecisionError::InvalidResponse)?;
    match properties
        .get("type")
        .and_then(|value| value.get("const"))
        .and_then(Value::as_str)
    {
        Some("noul") => Ok(ExpectedAnswer::Noul),
        Some("choice") => {
            let labels = properties
                .get("choice")
                .and_then(|value| value.get("enum"))
                .and_then(Value::as_array)
                .ok_or(DecisionError::InvalidResponse)?
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .map(str::to_owned)
                        .ok_or(DecisionError::InvalidResponse)
                })
                .collect::<Result<Vec<_>, _>>()?;
            if labels.len() < 2 {
                return Err(DecisionError::InvalidResponse);
            }
            Ok(ExpectedAnswer::Choice(labels))
        }
        Some("score") => {
            let score = properties
                .get("score")
                .ok_or(DecisionError::InvalidResponse)?;
            let maximum = score
                .get("maximum")
                .and_then(Value::as_f64)
                .ok_or(DecisionError::InvalidResponse)?;
            let legend_properties = properties
                .get("legend")
                .and_then(|value| value.get("properties"))
                .and_then(Value::as_object)
                .ok_or(DecisionError::InvalidResponse)?;
            let legend = legend_properties
                .iter()
                .map(|(key, value)| {
                    let text = value
                        .get("const")
                        .and_then(Value::as_str)
                        .ok_or(DecisionError::InvalidResponse)?;
                    Ok((key.clone(), text.to_owned()))
                })
                .collect::<Result<BTreeMap<_, _>, DecisionError>>()?;
            if !maximum.is_finite() || maximum < 1.0 || legend.len() != maximum as usize + 1 {
                return Err(DecisionError::InvalidResponse);
            }
            Ok(ExpectedAnswer::Score { maximum, legend })
        }
        _ => Err(DecisionError::InvalidResponse),
    }
}

fn contracts_from_schema(
    schema: &Value,
) -> Result<BTreeMap<String, ExpectedAnswer>, DecisionError> {
    schema
        .get("properties")
        .and_then(Value::as_object)
        .ok_or(DecisionError::InvalidResponse)?
        .iter()
        .map(|(id, schema)| Ok((id.clone(), answer_from_schema(schema)?)))
        .collect()
}

fn variant_schema<'a>(profile: &'a Value, variant: &str) -> Result<&'a Value, DecisionError> {
    profile
        .get("outputVariants")
        .and_then(|variants| variants.get(variant))
        .ok_or(DecisionError::InvalidResponse)
}

fn expected_answers(
    evaluation: &ProfileEvaluation,
) -> Result<BTreeMap<String, ExpectedAnswer>, DecisionError> {
    let profile = contract_profile(evaluation.profile_id())?;
    match evaluation {
        ProfileEvaluation::TaskAdmission(_) | ProfileEvaluation::ValidationRecovery(_) => {
            contracts_from_schema(
                profile
                    .get("outputSchema")
                    .ok_or(DecisionError::InvalidResponse)?,
            )
        }
        ProfileEvaluation::KasekiContextRouting(input) => match input.mode {
            ContextRoutingMode::ToolOutput => {
                contracts_from_schema(variant_schema(profile, "tool_output")?)
            }
            ContextRoutingMode::SemanticContext => {
                contracts_from_schema(variant_schema(profile, "semantic_context")?)
            }
            ContextRoutingMode::PhaseTransfer => {
                let template = contracts_from_schema(variant_schema(profile, "phase_transfer")?)?;
                let candidates = input
                    .evidence
                    .get("candidates")
                    .and_then(Value::as_array)
                    .ok_or(DecisionError::InvalidResponse)?;
                candidates
                    .iter()
                    .map(|candidate| {
                        let id = candidate
                            .get("id")
                            .and_then(Value::as_str)
                            .ok_or(DecisionError::InvalidResponse)?;
                        let family = id
                            .rsplit_once('_')
                            .map(|(family, _)| family)
                            .ok_or(DecisionError::InvalidResponse)?;
                        let template_id = format!("{family}_0");
                        let contract = template
                            .get(&template_id)
                            .ok_or(DecisionError::InvalidResponse)?
                            .clone();
                        Ok((id.to_owned(), contract))
                    })
                    .collect()
            }
        },
        ProfileEvaluation::KasekiGoalCheck(input) => {
            let template = contracts_from_schema(variant_schema(profile, "with_applicability")?)?;
            let mut expected = BTreeMap::new();
            for criterion in &input.criteria {
                expected.insert(
                    criterion.id.clone(),
                    template
                        .get("criterion_1")
                        .ok_or(DecisionError::InvalidResponse)?
                        .clone(),
                );
                if criterion.applies_when.is_some() {
                    expected.insert(
                        format!("{}_applicability", criterion.id),
                        template
                            .get("criterion_1_applicability")
                            .ok_or(DecisionError::InvalidResponse)?
                            .clone(),
                    );
                }
            }
            Ok(expected)
        }
        ProfileEvaluation::KasekiRunEvaluation(input) => {
            let mut expected =
                contracts_from_schema(variant_schema(profile, "with_failure_diagnosis")?)?;
            if !input.include_failure_diagnosis {
                expected.remove("validation_failure_cause");
                expected.remove("validation_recovery_action");
            }
            Ok(expected)
        }
    }
}

fn validate_response(
    evaluation: &ProfileEvaluation,
    body: &[u8],
    correlation_id: &str,
) -> Result<ValidatedDecision, DecisionError> {
    if body.len() > MAX_RESPONSE_BYTES {
        return Err(DecisionError::InvalidResponse);
    }
    let response: Value =
        serde_json::from_slice(body).map_err(|_| DecisionError::InvalidResponse)?;
    let object = response.as_object().ok_or(DecisionError::InvalidResponse)?;
    const RESPONSE_ENVELOPE_FIELDS: [&str; 12] = [
        "schemaVersion",
        "requestId",
        "status",
        "profileId",
        "profileVersion",
        "provider",
        "model",
        "answers",
        "result",
        "durationMs",
        "attemptCount",
        "usage",
    ];
    if !matches!(object.len(), 11 | 12)
        || object
            .keys()
            .any(|key| !RESPONSE_ENVELOPE_FIELDS.contains(&key.as_str()))
    {
        return Err(DecisionError::InvalidResponse);
    }
    let attempt_count = object
        .get("attemptCount")
        .and_then(Value::as_u64)
        .filter(|value| *value >= 1)
        .ok_or(DecisionError::InvalidResponse)?;
    if object.get("schemaVersion").and_then(Value::as_str) != Some("1")
        || object.get("status").and_then(Value::as_str) != Some("completed")
        || object.get("profileId").and_then(Value::as_str) != Some(evaluation.profile_id().as_str())
        || object.get("profileVersion").and_then(Value::as_str) != Some("1")
        || object.get("provider").and_then(Value::as_str) != Some("jev")
    {
        return Err(DecisionError::InvalidResponse);
    }
    let request_id = response_text(object, "requestId", 1, 200)?;
    let provider = response_text(object, "provider", 1, 64)?;
    let model = response_text(object, "model", 1, 256)?;
    let duration_ms = object
        .get("durationMs")
        .and_then(Value::as_u64)
        .filter(|value| *value <= 3_600_000)
        .ok_or(DecisionError::InvalidResponse)?;
    let answers = object
        .get("answers")
        .and_then(Value::as_object)
        .ok_or(DecisionError::InvalidResponse)?;
    let result = object
        .get("result")
        .and_then(Value::as_object)
        .ok_or(DecisionError::InvalidResponse)?;
    if answers != result {
        return Err(DecisionError::InvalidResponse);
    }
    let expected = expected_answers(evaluation)?;
    if answers.len() != expected.len() || answers.keys().any(|id| !expected.contains_key(id)) {
        return Err(DecisionError::InvalidResponse);
    }
    let mut parsed_answers = BTreeMap::new();
    for (id, kind) in expected {
        let value = answers.get(&id).ok_or(DecisionError::InvalidResponse)?;
        parsed_answers.insert(id, parse_answer(value, kind)?);
    }
    let usage = object.get("usage").map(parse_usage).transpose()?;
    Ok(ValidatedDecision {
        profile_id: evaluation.profile_id(),
        profile_version: "1".to_owned(),
        request_id: request_id.to_owned(),
        provider: provider.to_owned(),
        model: model.to_owned(),
        answers: parsed_answers,
        usage,
        duration_ms,
        attempt_count,
        correlation_id: correlation_id.to_owned(),
    })
}

fn provider_deadline_budget_ms(request_timeout: Duration) -> u64 {
    const MAX_PROVIDER_BUDGET_MS: u64 = 30_000;
    let timeout_ms = request_timeout.as_millis().min(u64::MAX as u128) as u64;
    let margin_ms = 1_000.min((timeout_ms / 10).max(1));
    MAX_PROVIDER_BUDGET_MS.min(timeout_ms.saturating_sub(margin_ms).max(1))
}

fn response_text<'a>(
    object: &'a Map<String, Value>,
    key: &str,
    min: usize,
    max: usize,
) -> Result<&'a str, DecisionError> {
    object
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| (min..=max).contains(&value.len()) && !value.trim().is_empty())
        .ok_or(DecisionError::InvalidResponse)
}

fn parse_usage(value: &Value) -> Result<ProviderUsage, DecisionError> {
    let object = value.as_object().ok_or(DecisionError::InvalidResponse)?;
    let allowed = ["input_tokens", "output_tokens", "total_tokens", "cost"];
    if object.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(DecisionError::InvalidResponse);
    }
    let tokens = |key: &str| -> Result<Option<u64>, DecisionError> {
        object
            .get(key)
            .map(|value| value.as_u64().ok_or(DecisionError::InvalidResponse))
            .transpose()
    };
    let cost_micros = object
        .get("cost")
        .map(|value| {
            let number = value.as_f64().ok_or(DecisionError::InvalidResponse)?;
            if !number.is_finite() || number < 0.0 || number > u64::MAX as f64 / 1_000_000.0 {
                return Err(DecisionError::InvalidResponse);
            }
            Ok((number * 1_000_000.0).round() as u64)
        })
        .transpose()?;
    Ok(ProviderUsage {
        input_tokens: tokens("input_tokens")?,
        output_tokens: tokens("output_tokens")?,
        total_tokens: tokens("total_tokens")?,
        cost_micros,
    })
}

fn parse_answer(value: &Value, expected: ExpectedAnswer) -> Result<DecisionAnswer, DecisionError> {
    let object = value.as_object().ok_or(DecisionError::InvalidResponse)?;
    let kind = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or(DecisionError::InvalidResponse)?;
    match expected {
        ExpectedAnswer::Noul => {
            if kind != "noul"
                || !has_exact_keys(object, &["type", "noul", "probabilities", "confidence"])
            {
                return Err(DecisionError::InvalidResponse);
            }
            let noul = probability(object.get("noul"))?;
            if !object.get("confidence").is_some_and(Value::is_null) {
                return Err(DecisionError::InvalidResponse);
            }
            let labels = vec!["false".to_owned(), "true".to_owned()];
            let probabilities = parse_probabilities(object.get("probabilities"), &labels)?;
            if (probabilities["true"] - noul).abs() > 0.000_001 {
                return Err(DecisionError::InvalidResponse);
            }
            Ok(DecisionAnswer::Noul {
                noul,
                probabilities,
            })
        }
        ExpectedAnswer::Choice(labels) => parse_choice(object, kind, &labels),
        ExpectedAnswer::Score {
            maximum,
            legend: expected_legend,
        } => {
            if kind != "score"
                || !has_exact_keys(
                    object,
                    &["type", "score", "legend", "probabilities", "confidence"],
                )
            {
                return Err(DecisionError::InvalidResponse);
            }
            let score = object
                .get("score")
                .and_then(Value::as_f64)
                .filter(|score| score.is_finite() && (0.0..=maximum).contains(score))
                .ok_or(DecisionError::InvalidResponse)?;
            let mut labels = expected_legend.keys().cloned().collect::<Vec<_>>();
            labels.sort_by_key(|label| label.parse::<usize>().unwrap_or(usize::MAX));
            let probabilities = parse_probabilities(object.get("probabilities"), &labels)?;
            let legend_object = object
                .get("legend")
                .and_then(Value::as_object)
                .ok_or(DecisionError::InvalidResponse)?;
            if legend_object.len() != labels.len()
                || labels
                    .iter()
                    .any(|label| !legend_object.contains_key(label))
            {
                return Err(DecisionError::InvalidResponse);
            }
            let mut legend = BTreeMap::new();
            for label in labels {
                let text = legend_object
                    .get(&label)
                    .and_then(Value::as_str)
                    .filter(|text| !text.trim().is_empty() && text.len() <= 512)
                    .ok_or(DecisionError::InvalidResponse)?;
                if expected_legend.get(&label).map(String::as_str) != Some(text) {
                    return Err(DecisionError::InvalidResponse);
                }
                legend.insert(label, text.to_owned());
            }
            let confidence = parse_confidence(object.get("confidence"))?;
            Ok(DecisionAnswer::Score {
                score,
                legend,
                probabilities,
                confidence,
            })
        }
    }
}

fn parse_choice(
    object: &Map<String, Value>,
    kind: &str,
    labels: &[String],
) -> Result<DecisionAnswer, DecisionError> {
    if kind != "choice"
        || !has_exact_keys(object, &["type", "choice", "probabilities", "confidence"])
    {
        return Err(DecisionError::InvalidResponse);
    }
    let choice = object
        .get("choice")
        .and_then(Value::as_str)
        .filter(|choice| labels.iter().any(|label| label == choice))
        .ok_or(DecisionError::InvalidResponse)?;
    let probabilities = parse_probabilities(object.get("probabilities"), labels)?;
    let confidence = parse_confidence(object.get("confidence"))?;
    Ok(DecisionAnswer::Choice {
        choice: choice.to_owned(),
        probabilities,
        confidence,
    })
}

fn parse_confidence(value: Option<&Value>) -> Result<Option<f64>, DecisionError> {
    match value {
        Some(Value::Null) => Ok(None),
        Some(value) => Ok(Some(probability(Some(value))?)),
        None => Err(DecisionError::InvalidResponse),
    }
}

fn parse_probabilities(
    value: Option<&Value>,
    labels: &[String],
) -> Result<BTreeMap<String, f64>, DecisionError> {
    let object = value
        .and_then(Value::as_object)
        .ok_or(DecisionError::InvalidResponse)?;
    if object.len() != labels.len() || labels.iter().any(|label| !object.contains_key(label)) {
        return Err(DecisionError::InvalidResponse);
    }
    let mut result = BTreeMap::new();
    let mut sum = 0.0;
    for label in labels {
        let value = probability(object.get(label))?;
        result.insert(label.clone(), value);
        sum += value;
    }
    if (sum - 1.0).abs() > 0.03 {
        return Err(DecisionError::InvalidResponse);
    }
    Ok(result)
}

fn probability(value: Option<&Value>) -> Result<f64, DecisionError> {
    value
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
        .ok_or(DecisionError::InvalidResponse)
}

fn has_exact_keys(object: &Map<String, Value>, expected: &[&str]) -> bool {
    object.len() == expected.len() && expected.iter().all(|key| object.contains_key(*key))
}

#[cfg(test)]
mod redaction_regression_tests {
    use super::redact_text;

    #[test]
    fn redacts_all_private_key_blocks_and_adjacent_credentials() {
        let first = "-----BEGIN RSA PRIVATE KEY-----\nalpha-secret\n-----END RSA PRIVATE KEY-----";
        let second =
            "-----BEGIN OPENSSH PRIVATE KEY-----\nbeta-secret\n-----END OPENSSH PRIVATE KEY-----";
        let input = format!("{first} ghp_sensitive-token {second} password=also-sensitive");
        let sanitized = redact_text(&input);
        assert!(!sanitized.contains("alpha-secret"));
        assert!(!sanitized.contains("beta-secret"));
        assert!(!sanitized.contains("ghp_sensitive-token"));
        assert!(!sanitized.contains("also-sensitive"));
        assert!(sanitized.contains("[REDACTED PRIVATE KEY]"));
        assert_eq!(sanitized.matches("[REDACTED PRIVATE KEY]").count(), 2);
    }

    #[test]
    fn unterminated_private_key_redacts_the_remainder() {
        let sanitized = redact_text("safe prefix\n-----BEGIN PRIVATE KEY-----\nsecret-tail");
        assert!(sanitized.starts_with("safe prefix"));
        assert!(!sanitized.contains("secret-tail"));

        let malformed = redact_text("safe -----BEGIN PRIVATE KEY\nsecret-tail");
        assert!(!malformed.contains("secret-tail"));
    }
}
