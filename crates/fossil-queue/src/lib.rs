use fossil_core::ids::QueueLeaseId;
use fossil_core::queue::{
    CloudflareAckRequest, CloudflarePullRequest, CloudflarePullResponse, CloudflareQueueMessage,
};
use reqwest::{Client, Method, Response, StatusCode};
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::watch;

const API_ORIGIN: &str = "https://api.cloudflare.com/client/v4";
const MAX_RESPONSE_BYTES: usize = 256 * 1024;
const MAX_QUEUE_ID_BYTES: usize = 32;
const MAX_RETRY_DELAY_SECONDS: u32 = 375;

#[derive(Clone)]
pub struct CloudflareQueueConfig {
    account_id: String,
    queue_id: String,
    api_token: SecretString,
    request_timeout: Duration,
    visibility_timeout_ms: u32,
}

impl CloudflareQueueConfig {
    pub fn new(
        account_id: impl Into<String>,
        queue_id: impl Into<String>,
        api_token: SecretString,
        request_timeout: Duration,
        visibility_timeout_ms: u32,
    ) -> Result<Self, QueueError> {
        let account_id = account_id.into();
        let queue_id = queue_id.into();
        if !valid_resource_id(&account_id)
            || !valid_resource_id(&queue_id)
            || api_token.expose_secret().trim().is_empty()
            || api_token.expose_secret().len() > 4096
            || api_token.expose_secret().chars().any(char::is_whitespace)
            || !(Duration::from_millis(100)..=Duration::from_secs(300)).contains(&request_timeout)
            || !(1_000..=43_200_000).contains(&visibility_timeout_ms)
        {
            return Err(QueueError::InvalidConfiguration);
        }
        Ok(Self {
            account_id,
            queue_id,
            api_token,
            request_timeout,
            visibility_timeout_ms,
        })
    }
}

impl fmt::Debug for CloudflareQueueConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CloudflareQueueConfig")
            .field("account_id", &self.account_id)
            .field("queue_id", &self.queue_id)
            .field("api_token", &"[REDACTED]")
            .field("request_timeout", &self.request_timeout)
            .field("visibility_timeout_ms", &self.visibility_timeout_ms)
            .finish()
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum QueueError {
    #[error("Cloudflare Queue client configuration is invalid")]
    InvalidConfiguration,
    #[error("Cloudflare Queue request timed out")]
    Timeout,
    #[error("Cloudflare Queue network request failed")]
    Network,
    #[error("Cloudflare rejected Queue credentials")]
    Unauthorized,
    #[error("Cloudflare Queue token lacks permission for this operation")]
    Forbidden,
    #[error("Cloudflare rate limited Queue operations")]
    RateLimited,
    #[error("Cloudflare Queue service is unavailable")]
    Unavailable,
    #[error("Cloudflare Queue request was rejected")]
    Rejected,
    #[error("Cloudflare Queue response was invalid or exceeded its size limit")]
    Protocol,
    #[error("Cloudflare did not confirm the requested Queue lease operation")]
    LeaseNotApplied,
}

impl QueueError {
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Timeout | Self::Network | Self::RateLimited | Self::Unavailable
        )
    }
}

#[derive(Default)]
struct QueueMetrics {
    pull_successes: AtomicU64,
    pull_failures: AtomicU64,
    ack_attempts: AtomicU64,
    ack_failures: AtomicU64,
    retry_attempts: AtomicU64,
    retry_failures: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueMetricsSnapshot {
    pub pull_successes: u64,
    pub pull_failures: u64,
    pub ack_attempts: u64,
    pub ack_failures: u64,
    pub retry_attempts: u64,
    pub retry_failures: u64,
}

#[derive(Clone)]
pub struct CloudflareQueueClient {
    client: Client,
    pull_url: String,
    ack_url: String,
    api_token: SecretString,
    visibility_timeout_ms: u32,
    metrics: Arc<QueueMetrics>,
}

impl CloudflareQueueClient {
    pub fn new(config: CloudflareQueueConfig) -> Result<Self, QueueError> {
        let client = Client::builder()
            .timeout(config.request_timeout)
            .build()
            .map_err(|_| QueueError::InvalidConfiguration)?;
        let message_url = format!(
            "{API_ORIGIN}/accounts/{}/queues/{}/messages",
            config.account_id, config.queue_id
        );
        Ok(Self {
            client,
            pull_url: format!("{message_url}/pull"),
            ack_url: format!("{message_url}/ack"),
            api_token: config.api_token,
            visibility_timeout_ms: config.visibility_timeout_ms,
            metrics: Arc::new(QueueMetrics::default()),
        })
    }

    /// Pulls at most one message so callers cannot create a local backlog.
    pub async fn pull_one(&self) -> Result<Option<CloudflareQueueMessage>, QueueError> {
        let request = CloudflarePullRequest::new(1, self.visibility_timeout_ms)
            .ok_or(QueueError::InvalidConfiguration)?;
        let response = match self
            .client
            .request(Method::POST, &self.pull_url)
            .bearer_auth(self.api_token.expose_secret())
            .json(&request)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                self.metrics.pull_failures.fetch_add(1, Ordering::Relaxed);
                return Err(classify_transport(error));
            }
        };
        let bytes = match successful_response_bytes(response).await {
            Ok(bytes) => bytes,
            Err(error) => {
                self.metrics.pull_failures.fetch_add(1, Ordering::Relaxed);
                return Err(error);
            }
        };
        let envelope: CloudflarePullResponse = match serde_json::from_slice(&bytes) {
            Ok(envelope) => envelope,
            Err(_) => {
                self.metrics.pull_failures.fetch_add(1, Ordering::Relaxed);
                return Err(QueueError::Protocol);
            }
        };
        if !envelope.success {
            self.metrics.pull_failures.fetch_add(1, Ordering::Relaxed);
            return Err(QueueError::Rejected);
        }
        let mut messages = envelope
            .result
            .map(|result| result.messages)
            .unwrap_or_default()
            .into_iter();
        let message = messages.next();
        if messages.next().is_some() {
            self.metrics.pull_failures.fetch_add(1, Ordering::Relaxed);
            return Err(QueueError::Protocol);
        }
        self.metrics.pull_successes.fetch_add(1, Ordering::Relaxed);
        Ok(message)
    }

    pub async fn acknowledge(&self, lease_id: QueueLeaseId) -> Result<(), QueueError> {
        self.metrics.ack_attempts.fetch_add(1, Ordering::Relaxed);
        let request = CloudflareAckRequest::acknowledge([lease_id]);
        let response: CloudflareAckResponse = match self.post_ack(&request).await {
            Ok(response) => response,
            Err(error) => {
                self.metrics.ack_failures.fetch_add(1, Ordering::Relaxed);
                return Err(error);
            }
        };
        if response.result.and_then(|result| result.ack_count) != Some(1) {
            self.metrics.ack_failures.fetch_add(1, Ordering::Relaxed);
            return Err(QueueError::LeaseNotApplied);
        }
        Ok(())
    }

    pub async fn retry(
        &self,
        lease_id: QueueLeaseId,
        delay_seconds: u32,
    ) -> Result<(), QueueError> {
        if delay_seconds > MAX_RETRY_DELAY_SECONDS {
            return Err(QueueError::InvalidConfiguration);
        }
        self.metrics.retry_attempts.fetch_add(1, Ordering::Relaxed);
        let request = CloudflareAckRequest {
            acks: Vec::new(),
            retries: vec![fossil_core::queue::CloudflareRetryItem {
                lease_id,
                delay_seconds: Some(delay_seconds),
            }],
        };
        let response = match self.post_ack(&request).await {
            Ok(response) => response,
            Err(error) => {
                self.metrics.retry_failures.fetch_add(1, Ordering::Relaxed);
                return Err(error);
            }
        };
        if response.result.and_then(|result| result.retry_count) != Some(1) {
            self.metrics.retry_failures.fetch_add(1, Ordering::Relaxed);
            return Err(QueueError::LeaseNotApplied);
        }
        Ok(())
    }

    pub fn metrics(&self) -> QueueMetricsSnapshot {
        QueueMetricsSnapshot {
            pull_successes: self.metrics.pull_successes.load(Ordering::Relaxed),
            pull_failures: self.metrics.pull_failures.load(Ordering::Relaxed),
            ack_attempts: self.metrics.ack_attempts.load(Ordering::Relaxed),
            ack_failures: self.metrics.ack_failures.load(Ordering::Relaxed),
            retry_attempts: self.metrics.retry_attempts.load(Ordering::Relaxed),
            retry_failures: self.metrics.retry_failures.load(Ordering::Relaxed),
        }
    }

    /// Polls only while `can_pull` permits it. Shutdown is observed between
    /// pulls; any already-pulled message is fully handled before the loop exits.
    pub async fn run<F, Fut, C>(
        &self,
        poll_interval: Duration,
        mut shutdown: watch::Receiver<bool>,
        can_pull: C,
        mut process: F,
    ) where
        F: FnMut(CloudflareQueueMessage) -> Fut,
        Fut: Future<Output = QueueDisposition>,
        C: Fn() -> bool,
    {
        let mut failure_count = 0_u32;
        loop {
            if *shutdown.borrow() {
                return;
            }
            if !wait_for_poll(&mut shutdown, poll_interval).await {
                return;
            }
            if !can_pull() {
                continue;
            }
            match self.pull_one().await {
                Ok(Some(message)) => {
                    failure_count = 0;
                    let lease_id = message.lease_id.clone();
                    match process(message).await {
                        QueueDisposition::Acknowledge => {
                            let _ = self.acknowledge(lease_id).await;
                        }
                        QueueDisposition::Retry { delay_seconds } => {
                            let _ = self.retry(lease_id, delay_seconds).await;
                        }
                    }
                }
                Ok(None) => failure_count = 0,
                Err(error) => {
                    failure_count = failure_count.saturating_add(1);
                    // Invalid credentials, malformed API responses, and other
                    // permanent failures stop this poller instead of hot-looping.
                    // Queue delivery remains leased and will become visible again.
                    if !error.is_retryable() {
                        return;
                    }
                    if !wait_for_poll(
                        &mut shutdown,
                        Duration::from_secs(u64::from(retry_delay_seconds(failure_count))),
                    )
                    .await
                    {
                        return;
                    }
                }
            }
        }
    }

    async fn post_ack(
        &self,
        request: &CloudflareAckRequest,
    ) -> Result<CloudflareAckResponse, QueueError> {
        let response = self
            .client
            .request(Method::POST, &self.ack_url)
            .bearer_auth(self.api_token.expose_secret())
            .json(request)
            .send()
            .await
            .map_err(classify_transport)?;
        let bytes = successful_response_bytes(response).await?;
        let response: CloudflareAckResponse =
            serde_json::from_slice(&bytes).map_err(|_| QueueError::Protocol)?;
        if !response.success {
            return Err(QueueError::Rejected);
        }
        Ok(response)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueDisposition {
    Acknowledge,
    Retry { delay_seconds: u32 },
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CloudflareAckResponse {
    success: bool,
    result: Option<CloudflareAckResult>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CloudflareAckResult {
    ack_count: Option<u64>,
    retry_count: Option<u64>,
}

fn valid_resource_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_QUEUE_ID_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

fn classify_transport(error: reqwest::Error) -> QueueError {
    if error.is_timeout() {
        QueueError::Timeout
    } else {
        QueueError::Network
    }
}

async fn successful_response_bytes(response: Response) -> Result<Vec<u8>, QueueError> {
    let status = response.status();
    if !status.is_success() {
        return Err(classify_status(status));
    }
    read_limited_body(response).await
}

fn classify_status(status: StatusCode) -> QueueError {
    match status {
        StatusCode::UNAUTHORIZED => QueueError::Unauthorized,
        StatusCode::FORBIDDEN => QueueError::Forbidden,
        StatusCode::TOO_MANY_REQUESTS => QueueError::RateLimited,
        StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT => QueueError::Unavailable,
        _ if status.is_server_error() => QueueError::Unavailable,
        _ => QueueError::Rejected,
    }
}

async fn read_limited_body(mut response: Response) -> Result<Vec<u8>, QueueError> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
    {
        return Err(QueueError::Protocol);
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(classify_transport)? {
        if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err(QueueError::Protocol);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

async fn wait_for_poll(shutdown: &mut watch::Receiver<bool>, duration: Duration) -> bool {
    if *shutdown.borrow() {
        return false;
    }
    tokio::select! {
        changed = shutdown.changed() => changed.is_ok() && !*shutdown.borrow(),
        _ = tokio::time::sleep(duration) => !*shutdown.borrow(),
    }
}

/// Exponential retry delay with bounded ±25% jitter, capped at 375 seconds.
pub fn retry_delay_seconds(attempt: u32) -> u32 {
    let exponent = attempt.min(9);
    let base = (1_u32 << exponent).min(300);
    let jitter_seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    let percent = 750 + jitter_seed % 501;
    (base.saturating_mul(percent) / 1_000).clamp(1, MAX_RETRY_DELAY_SECONDS)
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn mocked_client(server: &MockServer) -> CloudflareQueueClient {
        let client = Client::builder()
            .timeout(Duration::from_secs(1))
            .build()
            .unwrap();
        let message_url = format!(
            "{}/client/v4/accounts/account-123/queues/queue-123/messages",
            server.uri()
        );
        CloudflareQueueClient {
            client,
            pull_url: format!("{message_url}/pull"),
            ack_url: format!("{message_url}/ack"),
            api_token: SecretString::from("dedicated-cloudflare-token".to_owned()),
            visibility_timeout_ms: 10_000,
            metrics: Arc::new(QueueMetrics::default()),
        }
    }

    #[tokio::test]
    async fn pull_requests_exactly_one_message_and_keeps_lease_identity_separate() {
        let server = MockServer::start().await;
        let response = include_str!("../../fossil-core/tests/fixtures/cloudflare-pull-v1.json");
        Mock::given(method("POST"))
            .and(path(
                "/client/v4/accounts/account-123/queues/queue-123/messages/pull",
            ))
            .and(header("authorization", "Bearer dedicated-cloudflare-token"))
            .and(body_json(serde_json::json!({
                "batch_size": 1,
                "visibility_timeout_ms": 10_000
            })))
            .respond_with(ResponseTemplate::new(200).set_body_string(response))
            .expect(1)
            .mount(&server)
            .await;

        let client = mocked_client(&server);
        let message = client.pull_one().await.unwrap().unwrap();
        assert_eq!(message.id.as_str(), "msg-0001");
        assert_eq!(message.lease_id.as_str(), "opaque-lease-token");
        assert_eq!(
            message.decode_envelope().unwrap().run_id.to_string(),
            "f8a0c4c5-d628-7b93-a7cf-4b4629a28853"
        );
        assert_eq!(client.metrics().pull_successes, 1);
    }

    #[tokio::test]
    async fn acknowledgement_and_retry_use_cloudflare_lease_operations() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(
                "/client/v4/accounts/account-123/queues/queue-123/messages/ack",
            ))
            .and(body_json(serde_json::json!({
                "acks": [{"lease_id": "lease-1"}],
                "retries": []
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "success": true,
                "result": {"ackCount": 1, "retryCount": 0}
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(
                "/client/v4/accounts/account-123/queues/queue-123/messages/ack",
            ))
            .and(body_json(serde_json::json!({
                "acks": [],
                "retries": [{"lease_id": "lease-2", "delay_seconds": 30}]
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "success": true,
                "result": {"ackCount": 0, "retryCount": 1}
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = mocked_client(&server);
        client
            .acknowledge(QueueLeaseId::parse("lease-1").unwrap())
            .await
            .unwrap();
        client
            .retry(QueueLeaseId::parse("lease-2").unwrap(), 30)
            .await
            .unwrap();
        assert_eq!(client.metrics().ack_attempts, 1);
        assert_eq!(client.metrics().retry_attempts, 1);
    }

    #[tokio::test]
    async fn cloudflare_http_errors_are_classified_without_exposing_remote_body() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(
                "/client/v4/accounts/account-123/queues/queue-123/messages/pull",
            ))
            .respond_with(ResponseTemplate::new(401).set_body_string("secret response details"))
            .mount(&server)
            .await;

        let client = mocked_client(&server);
        let error = client.pull_one().await.unwrap_err();
        assert_eq!(error, QueueError::Unauthorized);
        assert!(!error.to_string().contains("secret response details"));
        assert_eq!(client.metrics().pull_failures, 1);
    }

    #[tokio::test]
    async fn an_unconfirmed_lease_operation_is_not_reported_as_success() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(
                "/client/v4/accounts/account-123/queues/queue-123/messages/ack",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "success": true,
                "result": {"ackCount": 0, "retryCount": 0}
            })))
            .mount(&server)
            .await;

        let client = mocked_client(&server);
        assert_eq!(
            client
                .acknowledge(QueueLeaseId::parse("lease-1").unwrap())
                .await,
            Err(QueueError::LeaseNotApplied)
        );
    }

    #[tokio::test]
    async fn polling_loop_processes_before_ack_and_observes_shutdown() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(
                "/client/v4/accounts/account-123/queues/queue-123/messages/pull",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_string(include_str!(
                "../../fossil-core/tests/fixtures/cloudflare-pull-v1.json"
            )))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(
                "/client/v4/accounts/account-123/queues/queue-123/messages/ack",
            ))
            .and(body_json(serde_json::json!({
                "acks": [{"lease_id": "opaque-lease-token"}],
                "retries": []
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "success": true,
                "result": {"ackCount": 1, "retryCount": 0}
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = mocked_client(&server);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        client
            .run(
                Duration::from_millis(1),
                shutdown_rx,
                || true,
                move |_message| {
                    let _ = shutdown_tx.send(true);
                    async { QueueDisposition::Acknowledge }
                },
            )
            .await;
        assert_eq!(client.metrics().pull_successes, 1);
        assert_eq!(client.metrics().ack_attempts, 1);
        assert_eq!(client.metrics().ack_failures, 0);
    }

    #[test]
    fn client_config_redacts_tokens_and_rejects_path_unsafe_identifiers() {
        let config = CloudflareQueueConfig::new(
            "account-123",
            "queue-123",
            SecretString::from("secret-cloudflare-token".to_owned()),
            Duration::from_secs(5),
            10_000,
        )
        .unwrap();
        assert!(!format!("{config:?}").contains("secret-cloudflare-token"));
        assert!(
            CloudflareQueueConfig::new(
                "account/123",
                "queue-123",
                SecretString::from("secret-cloudflare-token".to_owned()),
                Duration::from_secs(5),
                10_000,
            )
            .is_err()
        );
    }

    #[test]
    fn retry_delay_is_exponential_and_bounded_with_jitter() {
        for attempt in 0..32 {
            let delay = retry_delay_seconds(attempt);
            assert!((1..=375).contains(&delay));
        }
    }
}
