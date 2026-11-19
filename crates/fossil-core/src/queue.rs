use crate::contract::ContractVersion;
use crate::ids::{QueueLeaseId, QueueMessageId, RunId};
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use url::Url;

const MAX_QUEUE_BODY_BYTES: usize = 96 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueEnvelope {
    #[serde(rename = "contractVersion")]
    pub contract_version: ContractVersion,
    #[serde(rename = "runId")]
    pub run_id: RunId,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    #[serde(rename = "correlationId")]
    pub correlation_id: String,
    #[serde(rename = "requestId")]
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
        let envelope: Self = serde_json::from_value(value)?;
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoyuzRunRequest {
    #[serde(rename = "repoUrl")]
    pub repo_url: Url,
    #[serde(rename = "ref")]
    pub git_ref: String,
    #[serde(rename = "taskPrompt", default)]
    pub task_prompt: Option<String>,
    #[serde(rename = "startupCheck", default)]
    pub startup_check: Option<bool>,
    #[serde(rename = "taskMode", default)]
    pub task_mode: Option<String>,
    #[serde(rename = "publishMode", default)]
    pub publish_mode: Option<String>,
}

impl SoyuzRunRequest {
    fn validate(&self) -> Result<(), QueueEnvelopeError> {
        if self.repo_url.scheme() != "https"
            || self.repo_url.host_str().is_none()
            || !self.repo_url.username().is_empty()
            || self.repo_url.password().is_some()
        {
            return Err(QueueEnvelopeError::InvalidRepositoryUrl);
        }
        if self.git_ref.trim().is_empty()
            || self.git_ref.len() > 255
            || self.git_ref.chars().any(char::is_control)
        {
            return Err(QueueEnvelopeError::InvalidGitRef);
        }
        if self
            .task_prompt
            .as_deref()
            .unwrap_or_default()
            .trim()
            .is_empty()
            && self.startup_check != Some(true)
        {
            return Err(QueueEnvelopeError::MissingTaskPrompt);
        }
        Ok(())
    }
}

fn looks_like_uuid(value: &str) -> bool {
    uuid::Uuid::parse_str(value).is_ok()
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
