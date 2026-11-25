use crate::contract::ContractVersion;
use crate::ids::{QueueLeaseId, QueueMessageId, RunId};
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use url::Url;

const MAX_QUEUE_BODY_BYTES: usize = 96 * 1024;
pub const MAXIMUM_RETENTION_DAYS: i64 = 90;
const MILLIS_PER_DAY: i64 = 24 * 60 * 60 * 1_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueueEnvelope {
    pub contract_version: ContractVersion,
    pub run_id: RunId,
    pub created_at: String,
    pub correlation_id: String,
    pub request_id: String,
    pub request: SoyuzRunRequest,
}

impl QueueEnvelope {
    pub fn parse(bytes: &[u8]) -> Result<Self, QueueEnvelopeError> {
        if bytes.len() > MAX_QUEUE_BODY_BYTES {
            return Err(QueueEnvelopeError::TooLarge);
        }
        let value: Value = serde_json::from_slice(bytes)?;
        match value.get("contractVersion").and_then(Value::as_str) {
            Some("1") => {}
            Some(version) => {
                return Err(QueueEnvelopeError::UnsupportedContractVersion(
                    version.to_owned(),
                ));
            }
            None => return Err(QueueEnvelopeError::InvalidContractVersion),
        }
        // Parse original bytes so serde can reject duplicate fields as well as unknown fields.
        let envelope: Self = serde_json::from_slice(bytes)?;
        envelope.validate()?;
        Ok(envelope)
    }

    pub fn validate(&self) -> Result<(), QueueEnvelopeError> {
        OffsetDateTime::parse(&self.created_at, &Rfc3339)
            .map_err(|_| QueueEnvelopeError::InvalidTimestamp)?;
        if !looks_like_uuid(&self.correlation_id) || !looks_like_uuid(&self.request_id) {
            return Err(QueueEnvelopeError::InvalidTraceId);
        }
        self.request.validate()
    }

    /// Canonical run expiry is anchored to Soyuz's queue `createdAt`, never to
    /// queue delivery, host recovery, or callback retry time.
    pub fn expires_at_unix_ms(&self) -> Result<i64, QueueEnvelopeError> {
        self.created_at_unix_ms()?
            .checked_add(MAXIMUM_RETENTION_DAYS * MILLIS_PER_DAY)
            .ok_or(QueueEnvelopeError::InvalidTimestamp)
    }

    pub fn created_at_unix_ms(&self) -> Result<i64, QueueEnvelopeError> {
        let created_at = OffsetDateTime::parse(&self.created_at, &Rfc3339)
            .map_err(|_| QueueEnvelopeError::InvalidTimestamp)?;
        i64::try_from(created_at.unix_timestamp_nanos() / 1_000_000)
            .map_err(|_| QueueEnvelopeError::InvalidTimestamp)
    }

    pub fn is_expired_at(&self, now_unix_ms: i64) -> bool {
        self.expires_at_unix_ms()
            .is_ok_and(|expires_at| now_unix_ms >= expires_at)
    }
}

/// Contract-v1 execution input normalized by Soyuz. Unknown fields are rejected to
/// prevent new security or execution controls from being silently discarded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SoyuzRunRequest {
    #[serde(alias = "repo_url")]
    pub repo_url: Url,
    #[serde(rename = "ref", alias = "git_ref", default = "default_ref")]
    pub git_ref: String,
    #[serde(
        default,
        alias = "project_name",
        skip_serializing_if = "Option::is_none"
    )]
    pub project_name: Option<String>,
    #[serde(
        default,
        alias = "task_prompt",
        skip_serializing_if = "Option::is_none"
    )]
    pub task_prompt: Option<String>,
    #[serde(
        default,
        alias = "changed_files_allowlist",
        skip_serializing_if = "Option::is_none"
    )]
    pub changed_files_allowlist: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowlist: Option<FileAllowlist>,
    #[serde(
        default,
        alias = "max_diff_bytes",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_diff_bytes: Option<u64>,
    #[serde(
        default,
        alias = "validation_commands",
        skip_serializing_if = "Option::is_none"
    )]
    pub validation_commands: Option<Vec<String>>,
    #[serde(
        default,
        alias = "auto_lint_cleanup",
        skip_serializing_if = "Option::is_none"
    )]
    pub auto_lint_cleanup: Option<AutoLintCleanup>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation: Option<ValidationSettings>,
    #[serde(
        default,
        alias = "goal_setting",
        skip_serializing_if = "Option::is_none"
    )]
    pub goal_setting: Option<AdvisoryStageSettings>,
    #[serde(
        default,
        alias = "scouting_config",
        skip_serializing_if = "Option::is_none"
    )]
    pub scouting: Option<AdvisoryStageSettings>,
    #[serde(default, alias = "goal_check", skip_serializing_if = "Option::is_none")]
    pub goal_check: Option<GoalCheckSettings>,
    #[serde(
        default,
        alias = "run_evaluation",
        skip_serializing_if = "Option::is_none"
    )]
    pub run_evaluation: Option<AdvisoryStageSettings>,
    #[serde(default = "default_task_mode", alias = "task_mode")]
    pub task_mode: TaskMode,
    #[serde(default = "default_publish_mode", alias = "publish_mode")]
    pub publish_mode: PublishMode,
    #[serde(
        default,
        alias = "startup_check",
        skip_serializing_if = "Option::is_none"
    )]
    pub startup_check: Option<bool>,
    #[serde(
        default,
        alias = "startup_check_mode",
        skip_serializing_if = "Option::is_none"
    )]
    pub startup_check_mode: Option<StartupCheckMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracing: Option<TracingMetadata>,
    #[serde(
        default,
        alias = "idempotency_key",
        skip_serializing_if = "Option::is_none"
    )]
    pub idempotency_key: Option<uuid::Uuid>,
    #[serde(
        default,
        alias = "timeout_seconds",
        skip_serializing_if = "Option::is_none"
    )]
    pub timeout_seconds: Option<u32>,
}

impl SoyuzRunRequest {
    fn validate(&self) -> Result<(), QueueEnvelopeError> {
        if self.repo_url.scheme() != "https"
            || self.repo_url.host_str().is_none()
            || !self.repo_url.username().is_empty()
            || self.repo_url.password().is_some()
            || utf16_len(self.repo_url.as_str()) > 2_048
        {
            return Err(QueueEnvelopeError::InvalidRepositoryUrl);
        }
        if !bounded_string(&self.git_ref, 1, 512) {
            return Err(QueueEnvelopeError::InvalidGitRef);
        }
        if self
            .project_name
            .as_deref()
            .is_some_and(|value| !bounded_string(value, 1, 200))
            || self
                .task_prompt
                .as_deref()
                .is_some_and(|value| !bounded_string(value, 10, 32_000))
        {
            return Err(QueueEnvelopeError::InvalidRequestOptions);
        }
        if self.task_prompt.is_none() && self.startup_check != Some(true) {
            return Err(QueueEnvelopeError::MissingTaskPrompt);
        }
        if self
            .changed_files_allowlist
            .as_deref()
            .is_some_and(|values| !valid_string_list(values, 500, 512))
            || self.allowlist.as_ref().is_some_and(|allowlist| {
                allowlist
                    .include
                    .as_deref()
                    .is_some_and(|values| !valid_string_list(values, 500, 512))
            })
            || self
                .max_diff_bytes
                .is_some_and(|value| !(1..=100_000_000).contains(&value))
            || self
                .validation_commands
                .as_deref()
                .is_some_and(|values| !valid_string_list(values, 50, 2_048))
            || self
                .auto_lint_cleanup
                .as_ref()
                .is_some_and(|value| !value.is_valid())
            || self
                .validation
                .as_ref()
                .is_some_and(|value| !value.is_valid())
            || self
                .goal_setting
                .as_ref()
                .is_some_and(|value| !value.is_valid())
            || self
                .scouting
                .as_ref()
                .is_some_and(|value| !value.is_valid())
            || self
                .goal_check
                .as_ref()
                .is_some_and(|value| !value.is_valid())
            || self
                .run_evaluation
                .as_ref()
                .is_some_and(|value| !value.is_valid())
            || self
                .timeout_seconds
                .is_some_and(|value| !(60..=10_800).contains(&value))
        {
            return Err(QueueEnvelopeError::InvalidRequestOptions);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileAllowlist {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AutoLintCleanup {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commands: Option<Vec<String>>,
}

impl AutoLintCleanup {
    fn is_valid(&self) -> bool {
        self.commands
            .as_deref()
            .is_none_or(|values| valid_string_list(values, 50, 2_048))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ValidationSettings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commands: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_lint_cleanup: Option<AutoLintCleanup>,
}

impl ValidationSettings {
    fn is_valid(&self) -> bool {
        self.commands
            .as_deref()
            .is_none_or(|values| valid_string_list(values, 50, 2_048))
            && self
                .auto_lint_cleanup
                .as_ref()
                .is_none_or(AutoLintCleanup::is_valid)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AdvisoryStageSettings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u32>,
}

impl AdvisoryStageSettings {
    fn is_valid(&self) -> bool {
        self.model
            .as_deref()
            .is_none_or(|value| bounded_string(value, 1, 200))
            && self
                .timeout_seconds
                .is_none_or(|value| (60..=10_800).contains(&value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GoalCheckSettings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u32>,
}

impl GoalCheckSettings {
    fn is_valid(&self) -> bool {
        self.max_retries.is_none_or(|value| value <= 5)
            && self
                .model
                .as_deref()
                .is_none_or(|value| bounded_string(value, 1, 200))
            && self
                .timeout_seconds
                .is_none_or(|value| (60..=10_800).contains(&value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskMode {
    Patch,
    Inspect,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublishMode {
    Auto,
    None,
    Branch,
    Pr,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StartupCheckMode {
    Boot,
    BaselineValidation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TracingMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<uuid::Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<uuid::Uuid>,
}

fn default_ref() -> String {
    "main".to_owned()
}

fn default_task_mode() -> TaskMode {
    TaskMode::Patch
}

fn default_publish_mode() -> PublishMode {
    PublishMode::Pr
}

fn utf16_len(value: &str) -> usize {
    value.encode_utf16().count()
}

fn bounded_string(value: &str, minimum: usize, maximum: usize) -> bool {
    (minimum..=maximum).contains(&utf16_len(value))
}

fn valid_string_list(values: &[String], maximum_items: usize, maximum_item_length: usize) -> bool {
    values.len() <= maximum_items
        && values
            .iter()
            .all(|value| bounded_string(value, 1, maximum_item_length))
}

fn looks_like_uuid(value: &str) -> bool {
    uuid::Uuid::parse_str(value).is_ok_and(|uuid| uuid.to_string() == value)
}

#[derive(Debug, Error)]
pub enum QueueEnvelopeError {
    #[error("queue message exceeds 96 KiB")]
    TooLarge,
    #[error("invalid queue message JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unsupported Soyuz contract version {0}")]
    UnsupportedContractVersion(String),
    #[error("queue message contractVersion must be a string")]
    InvalidContractVersion,
    #[error("queue message timestamp is not RFC 3339")]
    InvalidTimestamp,
    #[error("queue message correlationId or requestId is not a UUID")]
    InvalidTraceId,
    #[error("repository URL must be an absolute HTTPS URL without credentials")]
    InvalidRepositoryUrl,
    #[error("repository ref is missing or invalid")]
    InvalidGitRef,
    #[error("Soyuz execution request contains invalid or unsupported options")]
    InvalidRequestOptions,
    #[error("taskPrompt is required unless startupCheck is true")]
    MissingTaskPrompt,
    #[error("Cloudflare Queue message body is not valid base64 JSON")]
    InvalidCloudflareBody,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloudflarePullRequest {
    pub batch_size: u8,
    pub visibility_timeout_ms: u32,
}

impl CloudflarePullRequest {
    pub fn new(batch_size: u8, visibility_timeout_ms: u32) -> Option<Self> {
        if !(1..=100).contains(&batch_size)
            || !(1_000..=43_200_000).contains(&visibility_timeout_ms)
        {
            return None;
        }
        Some(Self {
            batch_size,
            visibility_timeout_ms,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloudflareQueueMessage {
    pub id: QueueMessageId,
    pub attempts: u32,
    pub body: String,
    pub lease_id: QueueLeaseId,
    #[serde(default)]
    pub timestamp_ms: Option<u64>,
    #[serde(default)]
    pub metadata: Option<Value>,
}

impl CloudflareQueueMessage {
    /// Cloudflare HTTP pull returns JSON-content messages as base64-encoded bodies.
    pub fn decode_envelope(&self) -> Result<QueueEnvelope, QueueEnvelopeError> {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(self.body.as_bytes())
            .map_err(|_| QueueEnvelopeError::InvalidCloudflareBody)?;
        QueueEnvelope::parse(&decoded)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CloudflarePullResult {
    #[serde(default)]
    pub message_backlog_count: Option<u64>,
    #[serde(default)]
    pub messages: Vec<CloudflareQueueMessage>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CloudflarePullResponse {
    pub success: bool,
    #[serde(default)]
    pub result: Option<CloudflarePullResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloudflareAckItem {
    pub lease_id: QueueLeaseId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloudflareRetryItem {
    pub lease_id: QueueLeaseId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delay_seconds: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloudflareAckRequest {
    pub acks: Vec<CloudflareAckItem>,
    pub retries: Vec<CloudflareRetryItem>,
}

impl CloudflareAckRequest {
    pub fn acknowledge(leases: impl IntoIterator<Item = QueueLeaseId>) -> Self {
        Self {
            acks: leases
                .into_iter()
                .map(|lease_id| CloudflareAckItem { lease_id })
                .collect(),
            retries: Vec::new(),
        }
    }
}
