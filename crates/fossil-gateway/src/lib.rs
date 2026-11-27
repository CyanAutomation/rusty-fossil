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

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum BrokerError {
    #[error("gateway broker configuration is invalid")]
    InvalidConfiguration,
    #[error("run-scoped gateway token is invalid or expired")]
    InvalidToken,
    #[error("model request is invalid")]
    InvalidRequest,
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

/// Validate the configured model and supported request fields. Token-count
/// options are removed so client defaults cannot impose an inference budget;
/// unknown fields cannot change the upstream URL or selected model.
pub fn validate_completion_request(
    body: &[u8],
    expected_model: &str,
) -> Result<Value, BrokerError> {
    if body.is_empty() || body.len() > MAX_REQUEST_BODY_BYTES || !valid_model_id(expected_model) {
        return Err(BrokerError::InvalidRequest);
    }
    let mut value: Value = serde_json::from_slice(body).map_err(|_| BrokerError::InvalidRequest)?;
    let object = value.as_object_mut().ok_or(BrokerError::InvalidRequest)?;
    const ALLOWED_FIELDS: &[&str] = &[
        "model",
        "messages",
        "stream",
        "stream_options",
        "tools",
        "tool_choice",
        "parallel_tool_calls",
        "max_tokens",
        "max_completion_tokens",
        "n",
        "temperature",
        "top_p",
        "frequency_penalty",
        "presence_penalty",
        "stop",
        "user",
        "seed",
        "response_format",
        "logit_bias",
        "logprobs",
        "top_logprobs",
    ];
    if object
        .keys()
        .any(|key| !ALLOWED_FIELDS.contains(&key.as_str()))
    {
        return Err(BrokerError::InvalidRequest);
    }
    if object.get("model").and_then(Value::as_str) != Some(expected_model) {
        return Err(BrokerError::InvalidRequest);
    }
    let messages = object
        .get("messages")
        .and_then(Value::as_array)
        .filter(|messages| !messages.is_empty() && messages.len() <= 128)
        .ok_or(BrokerError::InvalidRequest)?;
    for message in messages {
        let message = message.as_object().ok_or(BrokerError::InvalidRequest)?;
        let role = message
            .get("role")
            .and_then(Value::as_str)
            .filter(|role| {
                matches!(
                    *role,
                    "system" | "developer" | "user" | "assistant" | "tool"
                )
            })
            .ok_or(BrokerError::InvalidRequest)?;
        if let Some(content) = message.get("content")
            && !is_bounded_text_content(content, role)
        {
            return Err(BrokerError::InvalidRequest);
        }
    }
    if object
        .get("stream")
        .is_some_and(|stream| !stream.is_boolean())
    {
        return Err(BrokerError::InvalidRequest);
    }
    if let Some(tools) = object.get("tools") {
        let tools = tools.as_array().ok_or(BrokerError::InvalidRequest)?;
        if tools.len() > 64 || tools.iter().any(|tool| !is_function_tool(tool)) {
            return Err(BrokerError::InvalidRequest);
        }
    }
    object.remove("max_tokens");
    object.remove("max_completion_tokens");
    Ok(value)
}

fn is_bounded_text_content(content: &Value, role: &str) -> bool {
    match content {
        Value::String(_) => true,
        Value::Null => role == "assistant",
        Value::Array(parts) => {
            !parts.is_empty()
                && parts.iter().all(|part| {
                    part.as_object().is_some_and(|part| {
                        part.get("type").and_then(Value::as_str) == Some("text")
                            && part.get("text").and_then(Value::as_str).is_some()
                            && part
                                .keys()
                                .all(|key| matches!(key.as_str(), "type" | "text"))
                    })
                })
        }
        _ => false,
    }
}

fn is_function_tool(tool: &Value) -> bool {
    let Some(tool) = tool.as_object() else {
        return false;
    };
    if tool.len() != 2 || tool.get("type").and_then(Value::as_str) != Some("function") {
        return false;
    }
    let Some(function) = tool.get("function").and_then(Value::as_object) else {
        return false;
    };
    if function.keys().any(|key| {
        !matches!(
            key.as_str(),
            "name" | "description" | "parameters" | "strict"
        )
    }) {
        return false;
    }
    function
        .get("name")
        .and_then(Value::as_str)
        .is_some_and(|name| !name.is_empty() && name.len() <= 128)
        && function
            .get("description")
            .is_none_or(|description| description.as_str().is_some())
        && function
            .get("parameters")
            .is_none_or(|parameters| parameters.is_object())
        && function
            .get("strict")
            .is_none_or(|strict| strict.is_boolean())
}

fn valid_model_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 200
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'@' | b'/' | b'-')
        })
}

/// SQLite-backed correlation and observed-cost ledger. It stores only run
/// fences, gateway log IDs, and cost values; prompts and token counts stay out.
pub struct GatewayRequestLedger {
    connection: Mutex<Connection>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayCostReport {
    pub estimated_cost_usd: f64,
    pub currency: &'static str,
    pub source: &'static str,
    pub complete: bool,
    pub request_count: u64,
    pub priced_request_count: u64,
    pub pending_request_count: u64,
}

impl GatewayCostReport {
    fn new(estimated_cost_usd: f64, request_count: u64, priced_request_count: u64) -> Self {
        let pending_request_count = request_count.saturating_sub(priced_request_count);
        Self {
            estimated_cost_usd,
            currency: "USD",
            source: "cloudflare_ai_gateway",
            complete: pending_request_count == 0,
            request_count,
            priced_request_count,
            pending_request_count,
        }
    }
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
                 DROP TABLE IF EXISTS gateway_run_budget;
                 CREATE TABLE IF NOT EXISTS gateway_run_cost (
                    run_id TEXT PRIMARY KEY,
                    worker_id TEXT NOT NULL,
                    claim_callback_id TEXT NOT NULL,
                    start_callback_id TEXT NOT NULL,
                    expires_at_ms INTEGER NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS gateway_request_cost (
                    request_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES gateway_run_cost(run_id),
                    log_id TEXT UNIQUE,
                    cost_usd REAL
                 );",
            )
            .map_err(|_| BrokerError::LedgerUnavailable)?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    /// Persist the run fence and request identity before calling Cloudflare.
    /// This records usage; it never rejects a request based on count, bytes,
    /// rate, token count, or cost.
    pub fn begin_request(
        &self,
        claims: &GatewayTokenClaims,
        request_id: EventId,
    ) -> Result<(), BrokerError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| BrokerError::LedgerUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| BrokerError::LedgerUnavailable)?;
        let now = now_ms()?;
        ensure_unexpired_at(claims, now)?;
        let existing: Option<(String, String, String, i64)> = transaction
            .query_row(
                "SELECT worker_id, claim_callback_id, start_callback_id, expires_at_ms
                 FROM gateway_run_cost WHERE run_id = ?1",
                [claims.run_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(|_| BrokerError::LedgerUnavailable)?;
        match existing {
            Some((worker_id, claim_id, start_id, expires_at_ms)) => {
                if worker_id != claims.worker_id.to_string()
                    || claim_id != claims.claim_callback_id.to_string()
                    || start_id != claims.start_callback_id.to_string()
                    || expires_at_ms != claims.expires_at_ms
                {
                    return Err(BrokerError::InvalidToken);
                }
            }
            None => {
                transaction
                    .execute(
                        "INSERT INTO gateway_run_cost (
                            run_id, worker_id, claim_callback_id, start_callback_id, expires_at_ms
                         ) VALUES (?1, ?2, ?3, ?4, ?5)",
                        params![
                            claims.run_id.to_string(),
                            claims.worker_id.to_string(),
                            claims.claim_callback_id.to_string(),
                            claims.start_callback_id.to_string(),
                            claims.expires_at_ms,
                        ],
                    )
                    .map_err(|_| BrokerError::LedgerUnavailable)?;
            }
        }
        transaction
            .execute(
                "INSERT INTO gateway_request_cost (request_id, run_id) VALUES (?1, ?2)",
                params![request_id.to_string(), claims.run_id.to_string()],
            )
            .map_err(|_| BrokerError::LedgerUnavailable)?;
        transaction
            .commit()
            .map_err(|_| BrokerError::LedgerUnavailable)
    }

    pub fn record_gateway_log(
        &self,
        claims: &GatewayTokenClaims,
        request_id: EventId,
        log_id: &str,
    ) -> Result<(), BrokerError> {
        if log_id.is_empty()
            || log_id.len() > 128
            || !log_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(BrokerError::InvalidRequest);
        }
        let connection = self
            .connection
            .lock()
            .map_err(|_| BrokerError::LedgerUnavailable)?;
        let updated = connection
            .execute(
                "UPDATE gateway_request_cost
                 SET log_id = ?1
                 WHERE request_id = ?2 AND run_id = ?3
                   AND EXISTS (
                     SELECT 1 FROM gateway_run_cost
                     WHERE run_id = ?3 AND worker_id = ?4 AND claim_callback_id = ?5
                       AND start_callback_id = ?6 AND expires_at_ms = ?7
                   )",
                params![
                    log_id,
                    request_id.to_string(),
                    claims.run_id.to_string(),
                    claims.worker_id.to_string(),
                    claims.claim_callback_id.to_string(),
                    claims.start_callback_id.to_string(),
                    claims.expires_at_ms,
                ],
            )
            .map_err(|_| BrokerError::LedgerUnavailable)?;
        if updated != 1 {
            return Err(BrokerError::InvalidToken);
        }
        Ok(())
    }

    pub fn pending_log_ids(&self, claims: &GatewayTokenClaims) -> Result<Vec<String>, BrokerError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| BrokerError::LedgerUnavailable)?;
        ensure_run_fence(&connection, claims)?;
        let mut statement = connection
            .prepare(
                "SELECT log_id FROM gateway_request_cost
                 WHERE run_id = ?1 AND log_id IS NOT NULL AND cost_usd IS NULL
                 ORDER BY request_id",
            )
            .map_err(|_| BrokerError::LedgerUnavailable)?;
        statement
            .query_map([claims.run_id.to_string()], |row| row.get(0))
            .map_err(|_| BrokerError::LedgerUnavailable)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| BrokerError::LedgerUnavailable)
    }

    pub fn record_gateway_cost(&self, log_id: &str, cost_usd: f64) -> Result<(), BrokerError> {
        if !cost_usd.is_finite() || cost_usd < 0.0 {
            return Err(BrokerError::InvalidRequest);
        }
        let connection = self
            .connection
            .lock()
            .map_err(|_| BrokerError::LedgerUnavailable)?;
        let updated = connection
            .execute(
                "UPDATE gateway_request_cost SET cost_usd = ?1 WHERE log_id = ?2",
                params![cost_usd, log_id],
            )
            .map_err(|_| BrokerError::LedgerUnavailable)?;
        if updated != 1 {
            return Err(BrokerError::LedgerUnavailable);
        }
        Ok(())
    }

    pub fn cost_report(
        &self,
        claims: &GatewayTokenClaims,
    ) -> Result<GatewayCostReport, BrokerError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| BrokerError::LedgerUnavailable)?;
        ensure_run_fence(&connection, claims)?;
        connection
            .query_row(
                "SELECT COUNT(*), COUNT(cost_usd), COALESCE(SUM(cost_usd), 0.0)
                 FROM gateway_request_cost WHERE run_id = ?1",
                [claims.run_id.to_string()],
                |row| {
                    let request_count = row.get::<_, i64>(0)?;
                    let priced_request_count = row.get::<_, i64>(1)?;
                    let total_cost_usd = row.get::<_, f64>(2)?;
                    Ok((request_count, priced_request_count, total_cost_usd))
                },
            )
            .map_err(|_| BrokerError::LedgerUnavailable)
            .and_then(|(request_count, priced_request_count, total_cost_usd)| {
                let request_count =
                    u64::try_from(request_count).map_err(|_| BrokerError::LedgerUnavailable)?;
                let priced_request_count = u64::try_from(priced_request_count)
                    .map_err(|_| BrokerError::LedgerUnavailable)?;
                if !total_cost_usd.is_finite() {
                    return Err(BrokerError::LedgerUnavailable);
                }
                Ok(GatewayCostReport::new(
                    total_cost_usd,
                    request_count,
                    priced_request_count,
                ))
            })
    }
}

fn ensure_run_fence(
    connection: &Connection,
    claims: &GatewayTokenClaims,
) -> Result<(), BrokerError> {
    let matches: bool = connection
        .query_row(
            "SELECT EXISTS (
                SELECT 1 FROM gateway_run_cost
                WHERE run_id = ?1 AND worker_id = ?2 AND claim_callback_id = ?3
                  AND start_callback_id = ?4 AND expires_at_ms = ?5
             )",
            params![
                claims.run_id.to_string(),
                claims.worker_id.to_string(),
                claims.claim_callback_id.to_string(),
                claims.start_callback_id.to_string(),
                claims.expires_at_ms,
            ],
            |row| row.get(0),
        )
        .map_err(|_| BrokerError::LedgerUnavailable)?;
    if matches {
        Ok(())
    } else {
        Err(BrokerError::InvalidToken)
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayUpstreamResponse {
    pub status: u16,
    pub content_type: String,
    pub log_id: Option<String>,
    pub body: Vec<u8>,
}

/// Testable network boundary for the one configured model gateway.
#[async_trait]
pub trait GatewayUpstream: Send + Sync {
    async fn send(
        &self,
        endpoint: &str,
        gateway_auth_token: &SecretString,
        run_id: RunId,
        request_id: EventId,
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
        run_id: RunId,
        request_id: EventId,
        request_body: Vec<u8>,
    ) -> Result<GatewayUpstreamResponse, BrokerError> {
        let metadata = serde_json::to_string(&serde_json::json!({
            "run_id": run_id.to_string(),
            "request_id": request_id.to_string(),
        }))
        .map_err(|_| BrokerError::InvalidRequest)?;
        let response = self
            .client
            .post(endpoint)
            .header(CONTENT_TYPE.as_str(), "application/json")
            .header(
                "cf-aig-authorization",
                format!("Bearer {}", gateway_auth_token.expose_secret()),
            )
            .header("cf-aig-collect-log-payload", "false")
            .header("cf-aig-event-id", request_id.to_string())
            .header("cf-aig-metadata", metadata)
            .body(request_body)
            .send()
            .await
            .map_err(|_| BrokerError::ProviderFailure)?;
        let status = response.status().as_u16();
        let log_id = response
            .headers()
            .get("cf-aig-log-id")
            .or_else(|| response.headers().get("cf-aig-event-id"))
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
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
                log_id,
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
            log_id,
            body,
        })
    }
}

#[async_trait]
pub trait GatewayLogReader: Send + Sync {
    /// Read the Cloudflare AI Gateway's estimated USD cost for one log entry.
    /// Missing cost means Cloudflare has not supplied an estimate yet.
    async fn cost_usd(&self, log_id: &str) -> Result<Option<f64>, BrokerError>;
}

struct ReqwestGatewayLogReader {
    client: reqwest::Client,
    token: SecretString,
    logs_url: Url,
}

impl ReqwestGatewayLogReader {
    fn new(endpoint: &GatewayEndpoint, token: SecretString) -> Result<Self, BrokerError> {
        let mut segments = endpoint
            .0
            .path_segments()
            .ok_or(BrokerError::InvalidConfiguration)?;
        let account_id = segments.nth(1).ok_or(BrokerError::InvalidConfiguration)?;
        let gateway_id = segments.next().ok_or(BrokerError::InvalidConfiguration)?;
        let mut logs_url = Url::parse("https://api.cloudflare.com/client/v4/")
            .map_err(|_| BrokerError::InvalidConfiguration)?;
        logs_url
            .path_segments_mut()
            .map_err(|_| BrokerError::InvalidConfiguration)?
            .extend([
                "accounts",
                account_id,
                "ai-gateway",
                "gateways",
                gateway_id,
                "logs",
            ]);
        let client = reqwest::Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(20))
            .build()
            .map_err(|_| BrokerError::InvalidConfiguration)?;
        Ok(Self {
            client,
            token,
            logs_url,
        })
    }
}

#[derive(Deserialize)]
struct CloudflareLogEnvelope {
    success: bool,
    result: CloudflareLogResult,
}

#[derive(Deserialize)]
struct CloudflareLogResult {
    cost: Option<f64>,
}

#[async_trait]
impl GatewayLogReader for ReqwestGatewayLogReader {
    async fn cost_usd(&self, log_id: &str) -> Result<Option<f64>, BrokerError> {
        if log_id.is_empty()
            || log_id.len() > 128
            || !log_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(BrokerError::InvalidRequest);
        }
        let mut url = self.logs_url.clone();
        url.path_segments_mut()
            .map_err(|_| BrokerError::InvalidConfiguration)?
            .push(log_id);
        let response = self
            .client
            .get(url)
            .bearer_auth(self.token.expose_secret())
            .send()
            .await
            .map_err(|_| BrokerError::ProviderFailure)?;
        if !response.status().is_success()
            || response
                .content_length()
                .is_some_and(|length| length > 128 * 1024)
        {
            return Err(BrokerError::ProviderFailure);
        }
        let body = response
            .bytes()
            .await
            .map_err(|_| BrokerError::ProviderFailure)?;
        if body.len() > 128 * 1024 {
            return Err(BrokerError::ProviderFailure);
        }
        let envelope: CloudflareLogEnvelope =
            serde_json::from_slice(&body).map_err(|_| BrokerError::ProviderFailure)?;
        if !envelope.success {
            return Err(BrokerError::ProviderFailure);
        }
        let Some(cost) = envelope.result.cost else {
            return Ok(None);
        };
        if !cost.is_finite() || cost < 0.0 {
            return Err(BrokerError::ProviderFailure);
        }
        Ok(Some(cost))
    }
}

/// Fixed model gateway client and authenticated HTTP endpoint.
pub struct GatewayBroker {
    endpoint: GatewayEndpoint,
    model: String,
    gateway_auth_token: SecretString,
    signer: GatewayTokenSigner,
    ledger: GatewayRequestLedger,
    log_reader: std::sync::Arc<dyn GatewayLogReader>,
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
    ) -> Result<Self, BrokerError> {
        let upstream = std::sync::Arc::new(ReqwestGatewayUpstream::new()?);
        let log_reader = std::sync::Arc::new(ReqwestGatewayLogReader::new(
            &endpoint,
            gateway_auth_token.clone(),
        )?);
        Self::with_clients(
            endpoint,
            model,
            gateway_auth_token,
            signer,
            ledger,
            log_reader,
            upstream,
        )
    }

    fn with_clients(
        endpoint: GatewayEndpoint,
        model: String,
        gateway_auth_token: SecretString,
        signer: GatewayTokenSigner,
        ledger: GatewayRequestLedger,
        log_reader: std::sync::Arc<dyn GatewayLogReader>,
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
            log_reader,
            upstream,
        })
    }

    pub fn router(self: std::sync::Arc<Self>) -> Router {
        Router::new()
            .route("/healthz", get(health))
            .route("/v1/chat/completions", post(infer))
            .route("/v1/cost", get(cost_report))
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
        let request = validate_completion_request(body, &self.model)?;
        let request_body = serde_json::to_vec(&request).map_err(|_| BrokerError::InvalidRequest)?;
        let request_id = EventId::new();
        self.ledger.begin_request(&claims, request_id)?;

        let response = self
            .upstream
            .send(
                self.endpoint.as_str(),
                &self.gateway_auth_token,
                claims.run_id,
                request_id,
                request_body,
            )
            .await?;
        if let Some(log_id) = response.log_id.as_deref() {
            self.ledger
                .record_gateway_log(&claims, request_id, log_id)?;
        }
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

    async fn refresh_cost_report(
        &self,
        headers: &HeaderMap,
    ) -> Result<GatewayCostReport, BrokerError> {
        let token = bearer_token(headers).ok_or(BrokerError::InvalidToken)?;
        let claims = self.signer.verify(token, now_ms()?)?;
        for log_id in self.ledger.pending_log_ids(&claims)? {
            if let Some(cost_usd) = self.log_reader.cost_usd(&log_id).await? {
                self.ledger.record_gateway_cost(&log_id, cost_usd)?;
            }
        }
        self.ledger.cost_report(&claims)
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

async fn cost_report(
    State(broker): State<std::sync::Arc<GatewayBroker>>,
    headers: HeaderMap,
) -> Response<Body> {
    match broker.refresh_cost_report(&headers).await {
        Ok(report) => {
            let body = serde_json::to_vec(&report).unwrap_or_else(|_| b"{}".to_vec());
            Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
                .body(Body::from(body))
                .unwrap_or_else(|_| Response::new(Body::empty()))
        }
        Err(error) => {
            let status = match error {
                BrokerError::InvalidToken => StatusCode::UNAUTHORIZED,
                BrokerError::InvalidRequest => StatusCode::BAD_REQUEST,
                BrokerError::InvalidConfiguration
                | BrokerError::LedgerUnavailable
                | BrokerError::ClockUnavailable
                | BrokerError::ProviderFailure => StatusCode::BAD_GATEWAY,
            };
            Response::builder()
                .status(status)
                .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
                .body(Body::from(
                    r#"{"error":{"type":"cost_unavailable","message":"gateway cost report is unavailable"}}"#,
                ))
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
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::UNIX_EPOCH;

    struct MockGateway {
        expected_endpoint: String,
        expected_auth_token: String,
        response: GatewayUpstreamResponse,
        seen_request: Mutex<Option<Vec<u8>>>,
        calls: AtomicU64,
    }

    #[async_trait]
    impl GatewayUpstream for MockGateway {
        async fn send(
            &self,
            endpoint: &str,
            gateway_auth_token: &SecretString,
            run_id: RunId,
            request_id: EventId,
            request_body: Vec<u8>,
        ) -> Result<GatewayUpstreamResponse, BrokerError> {
            let call = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
            assert_eq!(endpoint, self.expected_endpoint);
            assert_eq!(gateway_auth_token.expose_secret(), self.expected_auth_token);
            assert_eq!(run_id, claims(0).run_id);
            assert_ne!(
                request_id,
                EventId::parse("cc1c1b68-5f60-4d7e-8f50-5deabc37000b").unwrap()
            );
            *self
                .seen_request
                .lock()
                .map_err(|_| BrokerError::ProviderFailure)? = Some(request_body);
            let mut response = self.response.clone();
            response.log_id = Some(format!("log-{call}"));
            Ok(response)
        }
    }

    struct MockLogReader;

    #[async_trait]
    impl GatewayLogReader for MockLogReader {
        async fn cost_usd(&self, log_id: &str) -> Result<Option<f64>, BrokerError> {
            assert!(log_id.starts_with("log-"));
            Ok(Some(0.00025))
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
                log_id: None,
                body: response_body.to_vec(),
            },
            seen_request: Mutex::new(None),
            calls: AtomicU64::new(0),
        });
        let broker = GatewayBroker::with_clients(
            endpoint,
            "workers-ai/@cf/meta/test-model".to_owned(),
            SecretString::from(mock.expected_auth_token.clone()),
            signer,
            GatewayRequestLedger::open_in_memory().unwrap(),
            std::sync::Arc::new(MockLogReader),
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
    async fn broker_forwards_unlimited_requests_and_reports_observed_cost() {
        let body = br#"{"model":"workers-ai/@cf/meta/test-model","messages":[{"role":"user","content":"hello"}],"max_completion_tokens":1000000,"stream":true}"#;
        let event_stream = b"data: [DONE]\n\n";
        let (broker, mock, token) = test_broker(200, "text/event-stream", event_stream);
        for index in 0..100 {
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
            assert_eq!(mock.calls.load(Ordering::Relaxed), index + 1);
        }
        let forwarded: Value =
            serde_json::from_slice(mock.seen_request.lock().unwrap().as_ref().unwrap()).unwrap();
        assert!(forwarded.get("max_completion_tokens").is_none());
        assert!(forwarded.get("max_tokens").is_none());
        assert_eq!(forwarded["model"], "workers-ai/@cf/meta/test-model");
        let report = broker
            .refresh_cost_report(&request_headers(&token))
            .await
            .unwrap();
        assert_eq!(report.request_count, 100);
        assert_eq!(report.priced_request_count, 100);
        assert_eq!(report.pending_request_count, 0);
        assert!(report.complete);
        assert!((report.estimated_cost_usd - 0.025).abs() < 0.000_001);
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
