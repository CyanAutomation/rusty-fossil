//! Restricted model-access broker for the Pi coding worker.
//!
//! The broker accepts only short-lived, supervisor-signed run capabilities and
//! forwards a bounded OpenAI-compatible request to one configured Cloudflare
//! AI Gateway URL. The Cloudflare credential and provider key stay here; they
//! are never copied into the worker container.

use async_trait::async_trait;
use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{HeaderMap, HeaderValue, Response, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use fossil_core::ids::{ClaimCallbackId, EventId, RunId, WorkerId};
use reqwest::redirect::Policy;
use ring::hmac;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;
use thiserror::Error;
use url::Url;

pub const DEFAULT_BIND_ADDRESS: &str = "0.0.0.0:8787";
pub const MAX_REQUEST_BODY_BYTES: usize = 512 * 1024;
pub const MAX_RESPONSE_BODY_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_RUN_TOKEN_TTL_MS: i64 = 3 * 60 * 60 * 1_000;
const MAX_FUTURE_CLOCK_SKEW_MS: i64 = 30_000;
const GATEWAY_RATE_WINDOW_MS: i64 = 60_000;
const MAX_REQUESTS_PER_RATE_WINDOW: i64 = 10;

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum BrokerError {
    #[error("gateway broker configuration is invalid")]
    InvalidConfiguration,
    #[error("run-scoped gateway token is invalid or expired")]
    InvalidToken,
    #[error("model request is invalid")]
    InvalidRequest,
    #[error("model request limit was reached")]
    RateLimited,
    #[error("model gateway request failed")]
    ProviderFailure,
    #[error("gateway request ledger is unavailable")]
    LedgerUnavailable,
    #[error("system clock is unavailable")]
    ClockUnavailable,
}

/// The exact Soyuz fences to which a worker's model capability is bound.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GatewayTokenClaims {
    pub version: u8,
    pub run_id: RunId,
    pub worker_id: WorkerId,
    pub claim_callback_id: ClaimCallbackId,
    pub start_callback_id: EventId,
    pub issued_at_ms: i64,
    pub expires_at_ms: i64,
}

/// HMAC signer shared only by the supervisor and broker. The signed capability
/// given to Pi cannot be used to sign a different run or start fence.
pub struct GatewayTokenSigner {
    key: SecretString,
}

impl GatewayTokenSigner {
    pub fn new(key: SecretString) -> Result<Self, BrokerError> {
        let raw = key.expose_secret();
        if !(32..=512).contains(&raw.len())
            || !raw.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
        {
            return Err(BrokerError::InvalidConfiguration);
        }
        Ok(Self { key })
    }

    pub fn issue(&self, claims: &GatewayTokenClaims) -> Result<String, BrokerError> {
        if !valid_claims(claims, claims.issued_at_ms) {
            return Err(BrokerError::InvalidToken);
        }
        let payload = serde_json::to_vec(claims).map_err(|_| BrokerError::InvalidToken)?;
        let key = hmac::Key::new(hmac::HMAC_SHA256, self.key.expose_secret().as_bytes());
        let signature = hmac::sign(&key, &payload);
        Ok(format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(payload),
            URL_SAFE_NO_PAD.encode(signature.as_ref())
        ))
    }

    pub fn verify(&self, token: &str, now_ms: i64) -> Result<GatewayTokenClaims, BrokerError> {
        if token.len() > 4096 {
            return Err(BrokerError::InvalidToken);
        }
        let (encoded_payload, encoded_signature) = token
            .split_once('.')
            .filter(|(left, right)| !left.is_empty() && !right.is_empty())
            .ok_or(BrokerError::InvalidToken)?;
        if encoded_signature.contains('.') {
            return Err(BrokerError::InvalidToken);
        }
        let payload = URL_SAFE_NO_PAD
            .decode(encoded_payload)
            .map_err(|_| BrokerError::InvalidToken)?;
        let signature = URL_SAFE_NO_PAD
            .decode(encoded_signature)
            .map_err(|_| BrokerError::InvalidToken)?;
        let key = hmac::Key::new(hmac::HMAC_SHA256, self.key.expose_secret().as_bytes());
        hmac::verify(&key, &payload, &signature).map_err(|_| BrokerError::InvalidToken)?;
        let claims: GatewayTokenClaims =
            serde_json::from_slice(&payload).map_err(|_| BrokerError::InvalidToken)?;
        if !valid_claims(&claims, now_ms) {
            return Err(BrokerError::InvalidToken);
        }
        Ok(claims)
    }
}

fn valid_claims(claims: &GatewayTokenClaims, now_ms: i64) -> bool {
    claims.version == 1
        && claims.issued_at_ms >= 0
        && claims.expires_at_ms > claims.issued_at_ms
        && claims.expires_at_ms.saturating_sub(claims.issued_at_ms) <= MAX_RUN_TOKEN_TTL_MS
        && claims.issued_at_ms <= now_ms.saturating_add(MAX_FUTURE_CLOCK_SKEW_MS)
        && claims.expires_at_ms > now_ms
}

/// Validated URL for Cloudflare's OpenAI-compatible universal endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayEndpoint(Url);

impl GatewayEndpoint {
    pub fn parse(value: &str) -> Result<Self, BrokerError> {
        let url = Url::parse(value).map_err(|_| BrokerError::InvalidConfiguration)?;
        if url.scheme() != "https"
            || url.host_str() != Some("gateway.ai.cloudflare.com")
            || url.port().is_some()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(BrokerError::InvalidConfiguration);
        }
        let segments = url
            .path_segments()
            .ok_or(BrokerError::InvalidConfiguration)?
            .collect::<Vec<_>>();
        if segments.len() != 6
            || segments[0] != "v1"
            || !valid_cloudflare_account_id(segments[1])
            || !valid_gateway_id(segments[2])
            || segments[3..] != ["compat", "chat", "completions"]
        {
            return Err(BrokerError::InvalidConfiguration);
        }
        Ok(Self(url))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

fn valid_cloudflare_account_id(value: &str) -> bool {
    value.len() == 32 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn valid_gateway_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

/// Validate the model and the fields that can affect request cost. Unknown
/// payload fields are preserved for provider compatibility, but cannot change
/// the upstream URL or selected model.
pub fn validate_completion_request(
    body: &[u8],
    expected_model: &str,
    maximum_output_tokens: u32,
) -> Result<Value, BrokerError> {
    if body.is_empty()
        || body.len() > MAX_REQUEST_BODY_BYTES
        || !valid_model_id(expected_model)
        || maximum_output_tokens == 0
        || maximum_output_tokens > 16_384
    {
        return Err(BrokerError::InvalidRequest);
    }
    let mut value: Value = serde_json::from_slice(body).map_err(|_| BrokerError::InvalidRequest)?;
    let object = value.as_object_mut().ok_or(BrokerError::InvalidRequest)?;
    if object.get("model").and_then(Value::as_str) != Some(expected_model) {
        return Err(BrokerError::InvalidRequest);
    }
    let messages = object
        .get("messages")
        .and_then(Value::as_array)
        .filter(|messages| !messages.is_empty() && messages.len() <= 128)
        .ok_or(BrokerError::InvalidRequest)?;
    if messages.iter().any(|message| {
        message
            .as_object()
            .and_then(|message| message.get("role"))
            .and_then(Value::as_str)
            .is_none_or(|role| {
                !matches!(role, "system" | "developer" | "user" | "assistant" | "tool")
            })
    }) {
        return Err(BrokerError::InvalidRequest);
    }
    if object
        .get("stream")
        .is_some_and(|stream| !stream.is_boolean())
    {
        return Err(BrokerError::InvalidRequest);
    }
    if object
        .get("tools")
        .is_some_and(|tools| tools.as_array().is_none_or(|tools| tools.len() > 64))
    {
        return Err(BrokerError::InvalidRequest);
    }
    if object
        .get("n")
        .is_some_and(|value| value.as_u64() != Some(1))
    {
        return Err(BrokerError::InvalidRequest);
    }

    let has_max_tokens = object.contains_key("max_tokens");
    let has_max_completion_tokens = object.contains_key("max_completion_tokens");
    if has_max_tokens && has_max_completion_tokens {
        return Err(BrokerError::InvalidRequest);
    }
    let token_key = if has_max_completion_tokens {
        "max_completion_tokens"
    } else {
        "max_tokens"
    };
    if let Some(value) = object.get(token_key) {
        if value
            .as_u64()
            .is_none_or(|tokens| tokens == 0 || tokens > u64::from(maximum_output_tokens))
        {
            return Err(BrokerError::InvalidRequest);
        }
    } else {
        object.insert(token_key.to_owned(), Value::from(maximum_output_tokens));
    }
    Ok(value)
}

fn valid_model_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 200
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'@' | b'/' | b'-')
        })
}

/// SQLite-backed request accounting. Only run IDs, fences, expiries and
/// counters are stored; prompt and response contents never enter the ledger.
pub struct GatewayRequestLedger {
    connection: Mutex<Connection>,
}

struct ExistingRunBudget {
    worker_id: String,
    claim_callback_id: String,
    start_callback_id: String,
    expires_at_ms: i64,
    request_count: i64,
    request_bytes: i64,
    rate_window_started_at_ms: i64,
    rate_window_request_count: i64,
}

impl GatewayRequestLedger {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, BrokerError> {
        let path = private_ledger_path(path.as_ref())?;
        let connection =
            Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE)
                .map_err(|_| BrokerError::LedgerUnavailable)?;
        Self::from_connection(connection)
    }

    pub fn open_in_memory() -> Result<Self, BrokerError> {
        let connection =
            Connection::open_in_memory().map_err(|_| BrokerError::LedgerUnavailable)?;
        Self::from_connection(connection)
    }

    fn from_connection(connection: Connection) -> Result<Self, BrokerError> {
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(|_| BrokerError::LedgerUnavailable)?;
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 CREATE TABLE IF NOT EXISTS gateway_run_budget (
                    run_id TEXT PRIMARY KEY,
                    worker_id TEXT NOT NULL,
                    claim_callback_id TEXT NOT NULL,
                    start_callback_id TEXT NOT NULL,
                    expires_at_ms INTEGER NOT NULL,
                    request_count INTEGER NOT NULL,
                    request_bytes INTEGER NOT NULL,
                    rate_window_started_at_ms INTEGER NOT NULL,
                    rate_window_request_count INTEGER NOT NULL
                 );",
            )
            .map_err(|_| BrokerError::LedgerUnavailable)?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    pub fn reserve(
        &self,
        claims: &GatewayTokenClaims,
        request_bytes: usize,
        maximum_requests: u32,
        maximum_bytes: u64,
    ) -> Result<(), BrokerError> {
        if request_bytes == 0
            || request_bytes > MAX_REQUEST_BODY_BYTES
            || maximum_requests == 0
            || maximum_bytes < request_bytes as u64
        {
            return Err(BrokerError::RateLimited);
        }
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| BrokerError::LedgerUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| BrokerError::LedgerUnavailable)?;
        let now = now_ms()?;
        ensure_unexpired_at(claims, now)?;
        transaction
            .execute(
                "DELETE FROM gateway_run_budget WHERE expires_at_ms <= ?1",
                [now],
            )
            .map_err(|_| BrokerError::LedgerUnavailable)?;
        let current: Option<ExistingRunBudget> = transaction
            .query_row(
                "SELECT worker_id, claim_callback_id, start_callback_id,
                        expires_at_ms, request_count, request_bytes,
                        rate_window_started_at_ms, rate_window_request_count
                 FROM gateway_run_budget WHERE run_id = ?1",
                [claims.run_id.to_string()],
                |row| {
                    Ok(ExistingRunBudget {
                        worker_id: row.get(0)?,
                        claim_callback_id: row.get(1)?,
                        start_callback_id: row.get(2)?,
                        expires_at_ms: row.get(3)?,
                        request_count: row.get(4)?,
                        request_bytes: row.get(5)?,
                        rate_window_started_at_ms: row.get(6)?,
                        rate_window_request_count: row.get(7)?,
                    })
                },
            )
            .optional()
            .map_err(|_| BrokerError::LedgerUnavailable)?;

        match current {
            Some(existing) => {
                if existing.worker_id != claims.worker_id.to_string()
                    || existing.claim_callback_id != claims.claim_callback_id.to_string()
                    || existing.start_callback_id != claims.start_callback_id.to_string()
                    || existing.expires_at_ms != claims.expires_at_ms
                {
                    return Err(BrokerError::InvalidToken);
                }
                if existing.request_count < 0
                    || existing.request_bytes < 0
                    || existing.rate_window_request_count < 0
                {
                    return Err(BrokerError::LedgerUnavailable);
                }
                let (next_window_started, next_window_count) = if now
                    .saturating_sub(existing.rate_window_started_at_ms)
                    >= GATEWAY_RATE_WINDOW_MS
                {
                    (now, 1)
                } else if now < existing.rate_window_started_at_ms
                    || existing.rate_window_request_count >= MAX_REQUESTS_PER_RATE_WINDOW
                {
                    return Err(BrokerError::RateLimited);
                } else {
                    (
                        existing.rate_window_started_at_ms,
                        existing.rate_window_request_count + 1,
                    )
                };
                let current_bytes = u64::try_from(existing.request_bytes)
                    .map_err(|_| BrokerError::LedgerUnavailable)?;
                let next_bytes = current_bytes.saturating_add(request_bytes as u64);
                if existing.request_count >= i64::from(maximum_requests)
                    || next_bytes > maximum_bytes
                {
                    return Err(BrokerError::RateLimited);
                }
                transaction
                    .execute(
                        "UPDATE gateway_run_budget SET request_count = ?2, request_bytes = ?3,
                         rate_window_started_at_ms = ?4, rate_window_request_count = ?5
                         WHERE run_id = ?1",
                        params![
                            claims.run_id.to_string(),
                            existing.request_count + 1,
                            i64::try_from(next_bytes).map_err(|_| BrokerError::RateLimited)?,
                            next_window_started,
                            next_window_count
                        ],
                    )
                    .map_err(|_| BrokerError::LedgerUnavailable)?;
            }
            None => {
                transaction
                    .execute(
                        "INSERT INTO gateway_run_budget (
                            run_id, worker_id, claim_callback_id, start_callback_id,
                            expires_at_ms, request_count, request_bytes,
                            rate_window_started_at_ms, rate_window_request_count
                         ) VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6, ?7, 1)",
                        params![
                            claims.run_id.to_string(),
                            claims.worker_id.to_string(),
                            claims.claim_callback_id.to_string(),
                            claims.start_callback_id.to_string(),
                            claims.expires_at_ms,
                            request_bytes as i64,
                            now,
                        ],
                    )
                    .map_err(|_| BrokerError::LedgerUnavailable)?;
            }
        }
        transaction
            .commit()
            .map_err(|_| BrokerError::LedgerUnavailable)
    }
}

fn private_ledger_path(path: &Path) -> Result<std::path::PathBuf, BrokerError> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

    if !path.is_absolute() {
        return Err(BrokerError::LedgerUnavailable);
    }
    let requested_parent = path.parent().ok_or(BrokerError::LedgerUnavailable)?;
    let parent =
        std::fs::canonicalize(requested_parent).map_err(|_| BrokerError::LedgerUnavailable)?;
    let directory =
        std::fs::symlink_metadata(&parent).map_err(|_| BrokerError::LedgerUnavailable)?;
    if !directory.is_dir()
        || directory.permissions().mode() & 0o077 != 0
        || directory.uid() != unsafe { libc::geteuid() }
    {
        return Err(BrokerError::LedgerUnavailable);
    }
    let filename = path.file_name().ok_or(BrokerError::LedgerUnavailable)?;
    let path = parent.join(filename);
    let options = || {
        let mut options = std::fs::OpenOptions::new();
        options
            .read(true)
            .write(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        options
    };
    match options().create_new(true).mode(0o600).open(&path) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(_) => return Err(BrokerError::LedgerUnavailable),
    }
    let metadata = options()
        .open(&path)
        .and_then(|file| file.metadata())
        .map_err(|_| BrokerError::LedgerUnavailable)?;
    if !metadata.is_file()
        || metadata.permissions().mode() & 0o077 != 0
        || metadata.uid() != unsafe { libc::geteuid() }
    {
        return Err(BrokerError::LedgerUnavailable);
    }
    Ok(path)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GatewayRequestLimits {
    pub maximum_requests_per_run: u32,
    pub maximum_request_bytes_per_run: u64,
    pub maximum_output_tokens: u32,
}

impl GatewayRequestLimits {
    pub fn new(
        maximum_requests_per_run: u32,
        maximum_request_bytes_per_run: u64,
        maximum_output_tokens: u32,
    ) -> Result<Self, BrokerError> {
        if !(1..=512).contains(&maximum_requests_per_run)
            || !(MAX_REQUEST_BODY_BYTES as u64..=64 * 1024 * 1024)
                .contains(&maximum_request_bytes_per_run)
            || !(1..=16_384).contains(&maximum_output_tokens)
        {
            return Err(BrokerError::InvalidConfiguration);
        }
        Ok(Self {
            maximum_requests_per_run,
            maximum_request_bytes_per_run,
            maximum_output_tokens,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayUpstreamResponse {
    pub status: u16,
    pub content_type: String,
    pub body: Vec<u8>,
}

/// Testable network boundary for the one configured model gateway.
#[async_trait]
pub trait GatewayUpstream: Send + Sync {
    async fn send(
        &self,
        endpoint: &str,
        gateway_auth_token: &SecretString,
        request_body: Vec<u8>,
    ) -> Result<GatewayUpstreamResponse, BrokerError>;
}

struct ReqwestGatewayUpstream {
    client: reqwest::Client,
}

impl ReqwestGatewayUpstream {
    fn new() -> Result<Self, BrokerError> {
        let client = reqwest::Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(120))
            .build()
            .map_err(|_| BrokerError::InvalidConfiguration)?;
        Ok(Self { client })
    }
}

#[async_trait]
impl GatewayUpstream for ReqwestGatewayUpstream {
    async fn send(
        &self,
        endpoint: &str,
        gateway_auth_token: &SecretString,
        request_body: Vec<u8>,
    ) -> Result<GatewayUpstreamResponse, BrokerError> {
        let response = self
            .client
            .post(endpoint)
            .header(CONTENT_TYPE.as_str(), "application/json")
            .header(
                "cf-aig-authorization",
                format!("Bearer {}", gateway_auth_token.expose_secret()),
            )
            .body(request_body)
            .send()
            .await
            .map_err(|_| BrokerError::ProviderFailure)?;
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        if !(200..300).contains(&status) {
            return Ok(GatewayUpstreamResponse {
                status,
                content_type,
                body: Vec::new(),
            });
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_RESPONSE_BODY_BYTES as u64)
        {
            return Err(BrokerError::ProviderFailure);
        }
        let mut stream = response.bytes_stream();
        let mut body = Vec::new();
        while let Some(chunk) = futures_util::StreamExt::next(&mut stream).await {
            let chunk = chunk.map_err(|_| BrokerError::ProviderFailure)?;
            if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BODY_BYTES {
                return Err(BrokerError::ProviderFailure);
            }
            body.extend_from_slice(&chunk);
        }
        Ok(GatewayUpstreamResponse {
            status,
            content_type,
            body,
        })
    }
}

/// Fixed model gateway client and authenticated HTTP endpoint.
pub struct GatewayBroker {
    endpoint: GatewayEndpoint,
    model: String,
    gateway_auth_token: SecretString,
    signer: GatewayTokenSigner,
    ledger: GatewayRequestLedger,
    limits: GatewayRequestLimits,
    upstream: std::sync::Arc<dyn GatewayUpstream>,
}

impl GatewayBroker {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        endpoint: GatewayEndpoint,
        model: String,
        gateway_auth_token: SecretString,
        signer: GatewayTokenSigner,
        ledger: GatewayRequestLedger,
        limits: GatewayRequestLimits,
    ) -> Result<Self, BrokerError> {
        let upstream = std::sync::Arc::new(ReqwestGatewayUpstream::new()?);
        Self::with_upstream(
            endpoint,
            model,
            gateway_auth_token,
            signer,
            ledger,
            limits,
            upstream,
        )
    }

    fn with_upstream(
        endpoint: GatewayEndpoint,
        model: String,
        gateway_auth_token: SecretString,
        signer: GatewayTokenSigner,
        ledger: GatewayRequestLedger,
        limits: GatewayRequestLimits,
        upstream: std::sync::Arc<dyn GatewayUpstream>,
    ) -> Result<Self, BrokerError> {
        if !valid_model_id(&model)
            || !(32..=4096).contains(&gateway_auth_token.expose_secret().len())
            || !gateway_auth_token
                .expose_secret()
                .bytes()
                .all(|byte| (0x21..=0x7e).contains(&byte))
        {
            return Err(BrokerError::InvalidConfiguration);
        }
        Ok(Self {
            endpoint,
            model,
            gateway_auth_token,
            signer,
            ledger,
            limits,
            upstream,
        })
    }

    pub fn router(self: std::sync::Arc<Self>) -> Router {
        Router::new()
            .route("/healthz", get(health))
            .route("/v1/chat/completions", post(infer))
            .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
            .with_state(self)
    }

    async fn forward(
        &self,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<Response<Body>, BrokerError> {
        let token = bearer_token(headers).ok_or(BrokerError::InvalidToken)?;
        let now = now_ms()?;
        let claims = self.signer.verify(token, now)?;
        let request =
            validate_completion_request(body, &self.model, self.limits.maximum_output_tokens)?;
        let request_body = serde_json::to_vec(&request).map_err(|_| BrokerError::InvalidRequest)?;
        self.ledger.reserve(
            &claims,
            request_body.len(),
            self.limits.maximum_requests_per_run,
            self.limits.maximum_request_bytes_per_run,
        )?;

        let response = self
            .upstream
            .send(
                self.endpoint.as_str(),
                &self.gateway_auth_token,
                request_body,
            )
            .await?;
        if !(200..300).contains(&response.status) {
            return Err(BrokerError::ProviderFailure);
        }
        if response.body.len() > MAX_RESPONSE_BODY_BYTES {
            return Err(BrokerError::ProviderFailure);
        }
        if !(response.content_type.starts_with("application/json")
            || response.content_type.starts_with("text/event-stream"))
        {
            return Err(BrokerError::ProviderFailure);
        }
        let mut builder = Response::builder()
            .status(StatusCode::from_u16(response.status).unwrap_or(StatusCode::BAD_GATEWAY));
        builder = builder.header(CONTENT_TYPE, response.content_type);
        builder
            .body(Body::from(response.body))
            .map_err(|_| BrokerError::ProviderFailure)
    }
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .filter(|token| {
            !token.is_empty() && token.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
        })
}

async fn health() -> impl IntoResponse {
    StatusCode::NO_CONTENT
}

async fn infer(
    State(broker): State<std::sync::Arc<GatewayBroker>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    match broker.forward(&headers, &body).await {
        Ok(response) => response,
        Err(error) => {
            let status = match error {
                BrokerError::InvalidToken => StatusCode::UNAUTHORIZED,
                BrokerError::InvalidRequest => StatusCode::BAD_REQUEST,
                BrokerError::RateLimited => StatusCode::TOO_MANY_REQUESTS,
                BrokerError::InvalidConfiguration
                | BrokerError::LedgerUnavailable
                | BrokerError::ClockUnavailable
                | BrokerError::ProviderFailure => StatusCode::BAD_GATEWAY,
            };
            if matches!(error, BrokerError::ProviderFailure) {
                tracing::warn!("model gateway request failed");
            }
            let body = if status == StatusCode::BAD_GATEWAY {
                r#"{"error":{"type":"upstream_error","message":"model gateway request failed"}}"#
            } else {
                r#"{"error":{"type":"request_rejected","message":"model request rejected"}}"#
            };
            Response::builder()
                .status(status)
                .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
                .body(Body::from(body))
                .unwrap_or_else(|_| Response::new(Body::empty()))
        }
    }
}

fn now_ms() -> Result<i64, BrokerError> {
    checked_epoch_millis(std::time::SystemTime::now())
}

fn checked_epoch_millis(now: std::time::SystemTime) -> Result<i64, BrokerError> {
    let duration = now
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| BrokerError::ClockUnavailable)?;
    checked_duration_millis(duration)
}

fn checked_duration_millis(duration: Duration) -> Result<i64, BrokerError> {
    i64::try_from(duration.as_millis()).map_err(|_| BrokerError::ClockUnavailable)
}

fn ensure_unexpired_at(claims: &GatewayTokenClaims, now: i64) -> Result<(), BrokerError> {
    if claims.expires_at_ms <= now {
        return Err(BrokerError::InvalidToken);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use fossil_core::ids::{ClaimCallbackId, EventId, RunId, WorkerId};
    use std::sync::Mutex;
    use std::time::UNIX_EPOCH;

    struct MockGateway {
        expected_endpoint: String,
        expected_auth_token: String,
        response: GatewayUpstreamResponse,
        seen_request: Mutex<Option<Vec<u8>>>,
    }

    #[async_trait]
    impl GatewayUpstream for MockGateway {
        async fn send(
            &self,
            endpoint: &str,
            gateway_auth_token: &SecretString,
            request_body: Vec<u8>,
        ) -> Result<GatewayUpstreamResponse, BrokerError> {
            assert_eq!(endpoint, self.expected_endpoint);
            assert_eq!(gateway_auth_token.expose_secret(), self.expected_auth_token);
            *self
                .seen_request
                .lock()
                .map_err(|_| BrokerError::ProviderFailure)? = Some(request_body);
            Ok(self.response.clone())
        }
    }

    fn claims(now_ms: i64) -> GatewayTokenClaims {
        GatewayTokenClaims {
            version: 1,
            run_id: RunId::parse("018f6f7b-3a1c-7cc1-8df2-2f16a7a66d01").unwrap(),
            worker_id: WorkerId::parse("pilot-worker").unwrap(),
            claim_callback_id: ClaimCallbackId::parse("cc1c1b68-5f60-4d7e-8f50-5deabc37000a")
                .unwrap(),
            start_callback_id: EventId::parse("cc1c1b68-5f60-4d7e-8f50-5deabc37000b").unwrap(),
            issued_at_ms: now_ms,
            expires_at_ms: now_ms + 60_000,
        }
    }

    #[test]
    fn system_clock_conversion_returns_errors_instead_of_expiry_sentinels() {
        let before_epoch = UNIX_EPOCH.checked_sub(Duration::from_millis(1)).unwrap();
        assert_eq!(
            checked_epoch_millis(before_epoch),
            Err(BrokerError::ClockUnavailable)
        );
        assert_eq!(
            checked_duration_millis(Duration::from_millis(i64::MAX as u64 + 1)),
            Err(BrokerError::ClockUnavailable)
        );
        assert_eq!(checked_epoch_millis(UNIX_EPOCH), Ok(0));
    }

    #[test]
    fn token_expiry_is_checked_at_the_transaction_timestamp() {
        let request_claims = claims(1_000);
        assert_eq!(
            ensure_unexpired_at(&request_claims, request_claims.expires_at_ms),
            Err(BrokerError::InvalidToken)
        );
        assert_eq!(ensure_unexpired_at(&request_claims, 60_999), Ok(()));
    }

    fn test_broker(
        response_status: u16,
        response_content_type: &str,
        response_body: &[u8],
    ) -> (GatewayBroker, std::sync::Arc<MockGateway>, String) {
        let endpoint = GatewayEndpoint::parse(
            "https://gateway.ai.cloudflare.com/v1/0123456789abcdef0123456789abcdef/pilot/compat/chat/completions",
        )
        .unwrap();
        let signer = GatewayTokenSigner::new(SecretString::from("s".repeat(48))).unwrap();
        let now = now_ms().unwrap();
        let token = signer.issue(&claims(now)).unwrap();
        let mock = std::sync::Arc::new(MockGateway {
            expected_endpoint: endpoint.as_str().to_owned(),
            expected_auth_token: "cloudflare-auth-token-for-broker-only".to_owned(),
            response: GatewayUpstreamResponse {
                status: response_status,
                content_type: response_content_type.to_owned(),
                body: response_body.to_vec(),
            },
            seen_request: Mutex::new(None),
        });
        let broker = GatewayBroker::with_upstream(
            endpoint,
            "workers-ai/@cf/meta/test-model".to_owned(),
            SecretString::from(mock.expected_auth_token.clone()),
            signer,
            GatewayRequestLedger::open_in_memory().unwrap(),
            GatewayRequestLimits::new(4, MAX_REQUEST_BODY_BYTES as u64, 128).unwrap(),
            mock.clone(),
        )
        .unwrap();
        (broker, mock, token)
    }

    fn request_headers(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        headers
    }

    #[tokio::test]
    async fn broker_forwards_only_the_fixed_model_endpoint_and_broker_credential() {
        let body = br#"{"model":"workers-ai/@cf/meta/test-model","messages":[{"role":"user","content":"hello"}],"stream":true}"#;
        let event_stream = b"data: [DONE]\n\n";
        let (broker, mock, token) = test_broker(200, "text/event-stream", event_stream);
        let response = broker
            .forward(&request_headers(&token), body)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[CONTENT_TYPE], "text/event-stream");
        assert_eq!(
            axum::body::to_bytes(response.into_body(), 1024)
                .await
                .unwrap()
                .as_ref(),
            event_stream
        );
        let forwarded: Value =
            serde_json::from_slice(mock.seen_request.lock().unwrap().as_ref().unwrap()).unwrap();
        assert_eq!(forwarded["max_tokens"], 128);
        assert_eq!(forwarded["model"], "workers-ai/@cf/meta/test-model");
    }

    #[tokio::test]
    async fn provider_errors_and_unexpected_content_types_never_become_success() {
        let body = br#"{"model":"workers-ai/@cf/meta/test-model","messages":[{"role":"user","content":"hello"}],"max_tokens":64}"#;
        let (broker, _, token) = test_broker(429, "application/json", br#"{"error":"private"}"#);
        assert!(matches!(
            broker.forward(&request_headers(&token), body).await,
            Err(BrokerError::ProviderFailure)
        ));

        let (broker, _, token) = test_broker(200, "text/html", b"<html>unexpected</html>");
        assert!(matches!(
            broker.forward(&request_headers(&token), body).await,
            Err(BrokerError::ProviderFailure)
        ));
    }
}
