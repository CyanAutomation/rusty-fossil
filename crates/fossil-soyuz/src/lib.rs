use async_trait::async_trait;
use fossil_core::contract::{ApiSuccess, ContractVersion, SoyuzRunStatus};
use fossil_core::ids::{ClaimCallbackId, DisplayId, EventId, RunId, WorkerId};
use reqwest::{Client, Method, RequestBuilder, Response, StatusCode};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::fmt;
use std::net::IpAddr;
use std::time::Duration;
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use url::Url;

const MAX_RESPONSE_BYTES: usize = 96 * 1024;
const MAX_EVENT_BYTES: usize = 12 * 1024;

pub use fossil_core::contract::OperationalEventType;

#[derive(Clone)]
pub struct SoyuzClientConfig {
    origin: Url,
    worker_token: SecretString,
    request_timeout: Duration,
}

impl SoyuzClientConfig {
    pub fn new(
        origin: Url,
        worker_token: SecretString,
        request_timeout: Duration,
    ) -> Result<Self, SoyuzClientError> {
        let valid_scheme = origin.scheme() == "https"
            || (origin.scheme() == "http" && is_loopback_origin(&origin));
        if !valid_scheme
            || origin.host_str().is_none()
            || !origin.username().is_empty()
            || origin.password().is_some()
            || !matches!(origin.path(), "" | "/")
            || origin.query().is_some()
            || origin.fragment().is_some()
        {
            return Err(SoyuzClientError::InvalidConfiguration);
        }
        if worker_token.expose_secret().trim().is_empty()
            || worker_token.expose_secret().len() > 4096
            || worker_token
                .expose_secret()
                .chars()
                .any(char::is_whitespace)
            || !(Duration::from_millis(100)..=Duration::from_secs(300)).contains(&request_timeout)
        {
            return Err(SoyuzClientError::InvalidConfiguration);
        }
        Ok(Self {
            origin,
            worker_token,
            request_timeout,
        })
    }
}

impl fmt::Debug for SoyuzClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SoyuzClientConfig")
            .field("origin", &self.origin)
            .field("worker_token", &"[REDACTED]")
            .field("request_timeout", &self.request_timeout)
            .finish()
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SoyuzClientError {
    #[error("Soyuz client configuration is invalid")]
    InvalidConfiguration,
    #[error("request contains invalid or oversized data")]
    InvalidRequest,
    #[error("Soyuz request timed out")]
    Timeout,
    #[error("Soyuz network request failed")]
    Network,
    #[error("Soyuz authentication was rejected")]
    Unauthorized,
    #[error("Soyuz worker token is not authorized for this operation")]
    Forbidden,
    #[error("Soyuz run was not found")]
    NotFound,
    #[error("Soyuz rejected a stale claim fencing identity")]
    StaleClaim,
    #[error("Soyuz rejected a conflicting lifecycle operation")]
    Conflict,
    #[error("Soyuz rate limited the worker")]
    RateLimited,
    #[error("Soyuz is unavailable")]
    Unavailable,
    #[error("Soyuz returned an HTTP error ({status}, code {code})")]
    Remote { status: u16, code: String },
    #[error("Soyuz returned an invalid or oversized response")]
    Protocol,
}

impl SoyuzClientError {
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Timeout | Self::Network | Self::RateLimited | Self::Unavailable
        )
    }
}

#[derive(Debug, Clone)]
pub struct SoyuzClient {
    client: Client,
    origin: Url,
    worker_token: SecretString,
}

impl SoyuzClient {
    pub fn new(config: SoyuzClientConfig) -> Result<Self, SoyuzClientError> {
        let client = Client::builder()
            .timeout(config.request_timeout)
            .build()
            .map_err(|_| SoyuzClientError::InvalidConfiguration)?;
        Ok(Self {
            client,
            origin: config.origin,
            worker_token: config.worker_token,
        })
    }

    fn request(&self, method: Method, path: &str) -> RequestBuilder {
        let origin = self.origin.as_str().trim_end_matches('/');
        self.client
            .request(method, format!("{origin}{path}"))
            .bearer_auth(self.worker_token.expose_secret())
            .header(reqwest::header::ACCEPT, "application/json")
    }

    async fn send<T: DeserializeOwned>(
        &self,
        request: RequestBuilder,
    ) -> Result<(T, String), SoyuzClientError> {
        let response = request.send().await.map_err(classify_transport)?;
        let status = response.status();
        let body = read_limited_body(response).await?;
        if !status.is_success() {
            return Err(classify_response_error(status, &body));
        }
        let envelope: ApiSuccess<T> =
            serde_json::from_slice(&body).map_err(|_| SoyuzClientError::Protocol)?;
        Ok((envelope.data, envelope.request_id))
    }

    fn run_path(run_id: RunId, suffix: &str) -> String {
        format!("/v1/worker/runs/{run_id}{suffix}")
    }

    async fn operation<B: Serialize>(
        &self,
        path: String,
        body: &B,
    ) -> Result<OperationReceipt, SoyuzClientError> {
        let request = self.request(Method::POST, &path).json(body);
        let (_data, request_id): (Value, String) = self.send(request).await?;
        Ok(OperationReceipt { request_id })
    }

    pub async fn reconcile_started(
        &self,
        run_id: RunId,
        callback_id: EventId,
        worker_id: WorkerId,
        claim_callback_id: ClaimCallbackId,
    ) -> Result<StartedAuthorization, SoyuzClientError> {
        let run = self.get_run(run_id).await?;
        if !matches!(
            run.status,
            SoyuzRunStatus::Running | SoyuzRunStatus::CancelRequested
        ) || run.worker_id.as_ref() != Some(&worker_id)
            || run.claim_callback_id != Some(claim_callback_id)
        {
            return Err(SoyuzClientError::Conflict);
        }
        Ok(StartedAuthorization {
            run_id,
            callback_id,
            worker_id,
            claim_callback_id,
            source: StartAuthorizationSource::CanonicalStateReconciled,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationalHealth {
    Unknown,
    Healthy,
    SuspectStalled,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SoyuzRunRead {
    #[serde(rename = "runId")]
    pub run_id: RunId,
    #[serde(rename = "displayId")]
    pub display_id: DisplayId,
    pub status: SoyuzRunStatus,
    #[serde(rename = "cancelRequestedAt", default)]
    pub cancel_requested_at: Option<String>,
    #[serde(rename = "claimExpiresAt", default)]
    pub claim_expires_at: Option<String>,
    #[serde(rename = "workerId", default)]
    pub worker_id: Option<WorkerId>,
    #[serde(rename = "claimCallbackId", default)]
    pub claim_callback_id: Option<ClaimCallbackId>,
    #[serde(default)]
    pub stage: Option<String>,
    #[serde(rename = "updatedAt")]
    pub updated_at: String,
    #[serde(rename = "lastHeartbeatAt", default)]
    pub last_heartbeat_at: Option<String>,
    #[serde(rename = "operationalHealth")]
    pub operational_health: OperationalHealth,
}

impl SoyuzRunRead {
    pub fn cancellation_requested(&self) -> bool {
        self.cancel_requested_at.is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartAuthorizationSource {
    StartedCallbackAccepted,
    CanonicalStateReconciled,
}

/// Proof that Soyuz accepted a start or its canonical run read confirms it.
/// Private fields prevent controller code from manufacturing authorization locally.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartedAuthorization {
    run_id: RunId,
    callback_id: EventId,
    worker_id: WorkerId,
    claim_callback_id: ClaimCallbackId,
    source: StartAuthorizationSource,
}

impl StartedAuthorization {
    pub fn run_id(&self) -> RunId {
        self.run_id
    }

    pub fn callback_id(&self) -> EventId {
        self.callback_id
    }

    pub fn worker_id(&self) -> &WorkerId {
        &self.worker_id
    }

    pub fn claim_callback_id(&self) -> ClaimCallbackId {
        self.claim_callback_id
    }

    pub fn source(&self) -> StartAuthorizationSource {
        self.source
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationReceipt {
    pub request_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WorkerClaimRequest {
    pub run_id: RunId,
    pub callback_id: ClaimCallbackId,
    pub worker_id: WorkerId,
    pub lease_seconds: Option<u16>,
}

impl WorkerClaimRequest {
    fn body(&self) -> Result<ClaimBody<'_>, SoyuzClientError> {
        if self
            .lease_seconds
            .is_some_and(|seconds| !(30..=300).contains(&seconds))
        {
            return Err(SoyuzClientError::InvalidRequest);
        }
        Ok(ClaimBody {
            contract_version: ContractVersion::V1,
            callback_id: self.callback_id,
            worker_id: &self.worker_id,
            lease_seconds: self.lease_seconds,
        })
    }
}

#[derive(Serialize)]
struct ClaimBody<'a> {
    #[serde(rename = "contractVersion")]
    contract_version: ContractVersion,
    #[serde(rename = "callbackId")]
    callback_id: ClaimCallbackId,
    #[serde(rename = "workerId")]
    worker_id: &'a WorkerId,
    #[serde(rename = "leaseSeconds", skip_serializing_if = "Option::is_none")]
    lease_seconds: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StartedRequest {
    pub run_id: RunId,
    pub callback_id: EventId,
    pub worker_id: WorkerId,
    pub claim_callback_id: Option<ClaimCallbackId>,
    pub started_at: Option<String>,
    pub stage: Option<String>,
}

#[derive(Serialize)]
struct StartedBody<'a> {
    #[serde(rename = "contractVersion")]
    contract_version: ContractVersion,
    #[serde(rename = "callbackId")]
    callback_id: EventId,
    #[serde(rename = "workerId")]
    worker_id: &'a WorkerId,
    #[serde(rename = "claimCallbackId", skip_serializing_if = "Option::is_none")]
    claim_callback_id: Option<ClaimCallbackId>,
    #[serde(rename = "startedAt", skip_serializing_if = "Option::is_none")]
    started_at: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stage: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OperationalEvent {
    pub run_id: RunId,
    pub event_id: EventId,
    pub worker_id: WorkerId,
    pub claim_callback_id: Option<ClaimCallbackId>,
    pub event_type: OperationalEventType,
    pub stage: Option<String>,
    pub step: Option<String>,
    pub timestamp: Option<String>,
    pub payload: Option<Value>,
}

#[derive(Serialize)]
struct OperationalEventBody<'a> {
    #[serde(rename = "contractVersion")]
    contract_version: ContractVersion,
    #[serde(rename = "eventId")]
    event_id: EventId,
    #[serde(rename = "workerId")]
    worker_id: &'a WorkerId,
    #[serde(rename = "claimCallbackId", skip_serializing_if = "Option::is_none")]
    claim_callback_id: Option<ClaimCallbackId>,
    #[serde(rename = "type")]
    event_type: OperationalEventType,
    #[serde(skip_serializing_if = "Option::is_none")]
    stage: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    step: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timestamp: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload: Option<&'a Value>,
}

impl OperationalEvent {
    fn body(&self) -> Result<OperationalEventBody<'_>, SoyuzClientError> {
        if self
            .stage
            .as_deref()
            .is_some_and(|value| !valid_label(value))
            || self
                .step
                .as_deref()
                .is_some_and(|value| !valid_label(value))
        {
            return Err(SoyuzClientError::InvalidRequest);
        }
        if let Some(timestamp) = self.timestamp.as_deref() {
            OffsetDateTime::parse(timestamp, &Rfc3339)
                .map_err(|_| SoyuzClientError::InvalidRequest)?;
        }
        if let Some(payload) = &self.payload {
            let encoded =
                serde_json::to_vec(payload).map_err(|_| SoyuzClientError::InvalidRequest)?;
            if encoded.len() > MAX_EVENT_BYTES {
                return Err(SoyuzClientError::InvalidRequest);
            }
        }
        Ok(OperationalEventBody {
            contract_version: ContractVersion::V1,
            event_id: self.event_id,
            worker_id: &self.worker_id,
            claim_callback_id: self.claim_callback_id,
            event_type: self.event_type,
            stage: self.stage.as_deref(),
            step: self.step.as_deref(),
            timestamp: self.timestamp.as_deref(),
            payload: self.payload.as_ref(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CompletedCallback {
    pub run_id: RunId,
    pub callback_id: EventId,
    pub worker_id: WorkerId,
    pub claim_callback_id: Option<ClaimCallbackId>,
    pub completed_at: Option<String>,
    pub exit_code: Option<i32>,
    pub summary: Option<String>,
    pub published_url: Option<Url>,
    pub changed_file_count: Option<u32>,
}

#[derive(Serialize)]
struct CompletedBody<'a> {
    #[serde(rename = "contractVersion")]
    contract_version: ContractVersion,
    #[serde(rename = "callbackId")]
    callback_id: EventId,
    #[serde(rename = "workerId")]
    worker_id: &'a WorkerId,
    #[serde(rename = "claimCallbackId", skip_serializing_if = "Option::is_none")]
    claim_callback_id: Option<ClaimCallbackId>,
    #[serde(rename = "completedAt", skip_serializing_if = "Option::is_none")]
    completed_at: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    summary: Option<&'a str>,
    #[serde(rename = "publishedUrl", skip_serializing_if = "Option::is_none")]
    published_url: Option<&'a Url>,
    #[serde(rename = "changedFileCount", skip_serializing_if = "Option::is_none")]
    changed_file_count: Option<u32>,
}

impl CompletedCallback {
    fn body(&self) -> Result<CompletedBody<'_>, SoyuzClientError> {
        if self.exit_code.is_some_and(|value| value != 0)
            || self
                .summary
                .as_deref()
                .is_some_and(|value| !valid_message(value, 1000))
            || self
                .published_url
                .as_ref()
                .is_some_and(|url| url.scheme() != "https")
        {
            return Err(SoyuzClientError::InvalidRequest);
        }
        validate_timestamp(self.completed_at.as_deref())?;
        Ok(CompletedBody {
            contract_version: ContractVersion::V1,
            callback_id: self.callback_id,
            worker_id: &self.worker_id,
            claim_callback_id: self.claim_callback_id,
            completed_at: self.completed_at.as_deref(),
            exit_code: self.exit_code,
            summary: self.summary.as_deref(),
            published_url: self.published_url.as_ref(),
            changed_file_count: self.changed_file_count,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FailedCallback {
    pub run_id: RunId,
    pub callback_id: EventId,
    pub worker_id: WorkerId,
    pub claim_callback_id: Option<ClaimCallbackId>,
    pub completed_at: Option<String>,
    pub exit_code: Option<i32>,
    pub failure_class: String,
    pub failure_message: String,
}

#[derive(Serialize)]
struct FailedBody<'a> {
    #[serde(rename = "contractVersion")]
    contract_version: ContractVersion,
    #[serde(rename = "callbackId")]
    callback_id: EventId,
    #[serde(rename = "workerId")]
    worker_id: &'a WorkerId,
    #[serde(rename = "claimCallbackId", skip_serializing_if = "Option::is_none")]
    claim_callback_id: Option<ClaimCallbackId>,
    #[serde(rename = "completedAt", skip_serializing_if = "Option::is_none")]
    completed_at: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exit_code: Option<i32>,
    #[serde(rename = "failureClass")]
    failure_class: &'a str,
    #[serde(rename = "failureMessage")]
    failure_message: &'a str,
}

impl FailedCallback {
    fn body(&self) -> Result<FailedBody<'_>, SoyuzClientError> {
        if !valid_message(&self.failure_class, 128)
            || !self
                .failure_class
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
            || !valid_message(&self.failure_message, 1000)
        {
            return Err(SoyuzClientError::InvalidRequest);
        }
        validate_timestamp(self.completed_at.as_deref())?;
        Ok(FailedBody {
            contract_version: ContractVersion::V1,
            callback_id: self.callback_id,
            worker_id: &self.worker_id,
            claim_callback_id: self.claim_callback_id,
            completed_at: self.completed_at.as_deref(),
            exit_code: self.exit_code,
            failure_class: &self.failure_class,
            failure_message: &self.failure_message,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CancelledCallback {
    pub run_id: RunId,
    pub callback_id: EventId,
    pub worker_id: WorkerId,
    pub claim_callback_id: Option<ClaimCallbackId>,
    pub completed_at: Option<String>,
    pub exit_code: Option<i32>,
    pub reason: Option<String>,
}

#[derive(Serialize)]
struct CancelledBody<'a> {
    #[serde(rename = "contractVersion")]
    contract_version: ContractVersion,
    #[serde(rename = "callbackId")]
    callback_id: EventId,
    #[serde(rename = "workerId")]
    worker_id: &'a WorkerId,
    #[serde(rename = "claimCallbackId", skip_serializing_if = "Option::is_none")]
    claim_callback_id: Option<ClaimCallbackId>,
    #[serde(rename = "completedAt", skip_serializing_if = "Option::is_none")]
    completed_at: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'a str>,
}

impl CancelledCallback {
    fn body(&self) -> Result<CancelledBody<'_>, SoyuzClientError> {
        if self
            .reason
            .as_deref()
            .is_some_and(|value| !valid_message(value, 255))
        {
            return Err(SoyuzClientError::InvalidRequest);
        }
        validate_timestamp(self.completed_at.as_deref())?;
        Ok(CancelledBody {
            contract_version: ContractVersion::V1,
            callback_id: self.callback_id,
            worker_id: &self.worker_id,
            claim_callback_id: self.claim_callback_id,
            completed_at: self.completed_at.as_deref(),
            exit_code: self.exit_code,
            reason: self.reason.as_deref(),
        })
    }
}

#[async_trait]
pub trait SoyuzApi: Send + Sync {
    async fn get_run(&self, run_id: RunId) -> Result<SoyuzRunRead, SoyuzClientError>;
    async fn claim(
        &self,
        request: WorkerClaimRequest,
    ) -> Result<OperationReceipt, SoyuzClientError>;
    async fn started(
        &self,
        request: StartedRequest,
    ) -> Result<StartedAuthorization, SoyuzClientError>;
    async fn event(&self, event: OperationalEvent) -> Result<OperationReceipt, SoyuzClientError>;
    async fn heartbeat(
        &self,
        event: OperationalEvent,
    ) -> Result<OperationReceipt, SoyuzClientError>;
    async fn completed(
        &self,
        callback: CompletedCallback,
    ) -> Result<OperationReceipt, SoyuzClientError>;
    async fn failed(&self, callback: FailedCallback) -> Result<OperationReceipt, SoyuzClientError>;
    async fn cancelled(
        &self,
        callback: CancelledCallback,
    ) -> Result<OperationReceipt, SoyuzClientError>;
}

#[async_trait]
impl SoyuzApi for SoyuzClient {
    async fn get_run(&self, run_id: RunId) -> Result<SoyuzRunRead, SoyuzClientError> {
        let path = Self::run_path(run_id, "");
        let request = self.request(Method::GET, &path);
        let (run, _request_id): (SoyuzRunRead, String) = self.send(request).await?;
        if run.run_id != run_id {
            return Err(SoyuzClientError::Protocol);
        }
        Ok(run)
    }

    async fn claim(
        &self,
        request: WorkerClaimRequest,
    ) -> Result<OperationReceipt, SoyuzClientError> {
        let path = Self::run_path(request.run_id, "/claim");
        self.operation(path, &request.body()?).await
    }

    async fn started(
        &self,
        request: StartedRequest,
    ) -> Result<StartedAuthorization, SoyuzClientError> {
        let claim_callback_id = request
            .claim_callback_id
            .ok_or(SoyuzClientError::InvalidRequest)?;
        validate_timestamp(request.started_at.as_deref())?;
        if request
            .stage
            .as_deref()
            .is_some_and(|stage| !valid_label(stage))
        {
            return Err(SoyuzClientError::InvalidRequest);
        }
        let body = StartedBody {
            contract_version: ContractVersion::V1,
            callback_id: request.callback_id,
            worker_id: &request.worker_id,
            claim_callback_id: request.claim_callback_id,
            started_at: request.started_at.as_deref(),
            stage: request.stage.as_deref(),
        };
        let (_data, _request_id): (Value, String) = self
            .send(
                self.request(Method::POST, &Self::run_path(request.run_id, "/started"))
                    .json(&body),
            )
            .await?;
        Ok(StartedAuthorization {
            run_id: request.run_id,
            callback_id: request.callback_id,
            worker_id: request.worker_id,
            claim_callback_id,
            source: StartAuthorizationSource::StartedCallbackAccepted,
        })
    }

    async fn event(&self, event: OperationalEvent) -> Result<OperationReceipt, SoyuzClientError> {
        let body = event.body()?;
        self.operation(Self::run_path(event.run_id, "/events"), &body)
            .await
    }

    async fn heartbeat(
        &self,
        mut event: OperationalEvent,
    ) -> Result<OperationReceipt, SoyuzClientError> {
        event.event_type = OperationalEventType::Heartbeat;
        event.stage = None;
        event.step = None;
        event.payload = None;
        self.event(event).await
    }

    async fn completed(
        &self,
        callback: CompletedCallback,
    ) -> Result<OperationReceipt, SoyuzClientError> {
        let body = callback.body()?;
        self.operation(Self::run_path(callback.run_id, "/completed"), &body)
            .await
    }

    async fn failed(&self, callback: FailedCallback) -> Result<OperationReceipt, SoyuzClientError> {
        let body = callback.body()?;
        self.operation(Self::run_path(callback.run_id, "/failed"), &body)
            .await
    }

    async fn cancelled(
        &self,
        callback: CancelledCallback,
    ) -> Result<OperationReceipt, SoyuzClientError> {
        let body = callback.body()?;
        self.operation(Self::run_path(callback.run_id, "/cancelled"), &body)
            .await
    }
}

fn valid_label(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128 && !value.chars().any(char::is_control)
}

fn valid_message(value: &str, max_bytes: usize) -> bool {
    !value.is_empty() && value.len() <= max_bytes && !value.chars().any(char::is_control)
}

fn validate_timestamp(value: Option<&str>) -> Result<(), SoyuzClientError> {
    if let Some(value) = value {
        OffsetDateTime::parse(value, &Rfc3339).map_err(|_| SoyuzClientError::InvalidRequest)?;
    }
    Ok(())
}

fn is_loopback_origin(url: &Url) -> bool {
    match url.host_str() {
        Some("localhost") => true,
        Some(host) => host
            .parse::<IpAddr>()
            .map(|address| address.is_loopback())
            .unwrap_or(false),
        None => false,
    }
}

async fn read_limited_body(mut response: Response) -> Result<Vec<u8>, SoyuzClientError> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
    {
        return Err(SoyuzClientError::Protocol);
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(classify_transport)? {
        if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err(SoyuzClientError::Protocol);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn classify_transport(error: reqwest::Error) -> SoyuzClientError {
    if error.is_timeout() {
        SoyuzClientError::Timeout
    } else {
        SoyuzClientError::Network
    }
}

fn classify_response_error(status: StatusCode, body: &[u8]) -> SoyuzClientError {
    let code = serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|value| value.get("error")?.get("code")?.as_str().map(str::to_owned));
    match status {
        StatusCode::UNAUTHORIZED => SoyuzClientError::Unauthorized,
        StatusCode::FORBIDDEN => SoyuzClientError::Forbidden,
        StatusCode::NOT_FOUND => SoyuzClientError::NotFound,
        StatusCode::CONFLICT if code.as_deref() == Some("STALE_CLAIM") => {
            SoyuzClientError::StaleClaim
        }
        StatusCode::CONFLICT => SoyuzClientError::Conflict,
        StatusCode::TOO_MANY_REQUESTS => SoyuzClientError::RateLimited,
        StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT => {
            SoyuzClientError::Unavailable
        }
        StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => {
            SoyuzClientError::InvalidRequest
        }
        _ if status.is_server_error() => SoyuzClientError::Unavailable,
        _ => SoyuzClientError::Remote {
            status: status.as_u16(),
            code: code.unwrap_or_else(|| "UNKNOWN".to_owned()),
        },
    }
}
