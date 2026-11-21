use fossil_core::ids::WorkerId;
use fossil_queue::CloudflareQueueConfig;
use fossil_senku::{SenkuClient, SenkuClientConfig};
use secrecy::{ExposeSecret, SecretString};
use std::collections::HashMap;
use std::fmt;
use std::io::Read;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;
use thiserror::Error;
use url::Url;

const MAX_SECRET_BYTES: usize = 4096;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("configuration value {0} is invalid")]
    InvalidValue(&'static str),
    #[error("required execution configuration is missing: {0}")]
    MissingRequiredConfiguration(String),
    #[error("secret file configured by {0} could not be read safely")]
    InvalidSecretFile(&'static str),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum DeploymentMode {
    Development,
    Production,
}

impl DeploymentMode {
    fn parse(value: Option<&str>) -> Result<Self, ConfigError> {
        match value.unwrap_or("production") {
            "development" => Ok(Self::Development),
            "production" => Ok(Self::Production),
            _ => Err(ConfigError::InvalidValue("RUSTY_FOSSIL_ENV")),
        }
    }
}

impl fmt::Debug for DeploymentMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Development => "Development",
            Self::Production => "Production",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl LogLevel {
    fn parse(value: Option<&str>) -> Result<Self, ConfigError> {
        match value.unwrap_or("info") {
            "error" => Ok(Self::Error),
            "warn" => Ok(Self::Warn),
            "info" => Ok(Self::Info),
            "debug" => Ok(Self::Debug),
            "trace" => Ok(Self::Trace),
            _ => Err(ConfigError::InvalidValue("RUSTY_FOSSIL_LOG_LEVEL")),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug",
            Self::Trace => "trace",
        }
    }
}

#[derive(Clone)]
pub struct AppConfig {
    pub mode: DeploymentMode,
    pub soyuz_api_origin: Option<Url>,
    pub worker_api_token: Option<SecretString>,
    pub cloudflare_account_id: Option<String>,
    pub cloudflare_queue_id: Option<String>,
    pub cloudflare_queue_token: Option<SecretString>,
    pub cloudflare_queue_visibility_timeout_ms: u32,
    pub worker_id: Option<WorkerId>,
    pub senku_api_origin: Option<Url>,
    pub senku_api_token: Option<SecretString>,
    pub polling_interval: Duration,
    pub request_timeout: Duration,
    pub state_dir: PathBuf,
    pub log_level: LogLevel,
    pub bind: SocketAddr,
}

impl fmt::Debug for AppConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppConfig")
            .field("mode", &self.mode)
            .field(
                "soyuz_api_origin_configured",
                &self.soyuz_api_origin.is_some(),
            )
            .field(
                "worker_api_token",
                &self.worker_api_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field(
                "cloudflare_account_id_configured",
                &self.cloudflare_account_id.is_some(),
            )
            .field(
                "cloudflare_queue_id_configured",
                &self.cloudflare_queue_id.is_some(),
            )
            .field(
                "cloudflare_queue_token",
                &self.cloudflare_queue_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("worker_id_configured", &self.worker_id.is_some())
            .field(
                "senku_api_origin_configured",
                &self.senku_api_origin.is_some(),
            )
            .field(
                "senku_api_token",
                &self.senku_api_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field(
                "cloudflare_queue_visibility_timeout_ms",
                &self.cloudflare_queue_visibility_timeout_ms,
            )
            .field("polling_interval", &self.polling_interval)
            .field("request_timeout", &self.request_timeout)
            .field(
                "state_dir_configured",
                &!self.state_dir.as_os_str().is_empty(),
            )
            .field("log_level", &self.log_level)
            .field("bind", &self.bind)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadinessReport {
    pub ready: bool,
    pub execution_ready: bool,
    pub execution_handoff_ready: bool,
    pub state_store_ready: bool,
    pub soyuz_configured: bool,
    pub queue_configured: bool,
    pub queue_transport_healthy: Option<bool>,
    pub recovery_backlog_healthy: bool,
    pub outbox_backpressured: bool,
    pub callbacks_pending: u64,
    pub callbacks_blocked: u64,
    pub oldest_pending_callback_age_ms: Option<u64>,
    pub senku_configured: bool,
    pub senku_advisory_healthy: Option<bool>,
    pub missing_dependencies: Vec<String>,
    pub limitations: Vec<String>,
}

impl AppConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_pairs(std::env::vars())
    }

    pub fn from_pairs(
        pairs: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, ConfigError> {
        let values: HashMap<String, String> = pairs.into_iter().collect();
        let get = |key: &str| {
            values
                .get(key)
                .map(String::as_str)
                .filter(|v| !v.trim().is_empty())
        };
        let mode = DeploymentMode::parse(get("RUSTY_FOSSIL_ENV"))?;
        let soyuz_api_origin = get("SOYUZ_API_ORIGIN")
            .map(|value| parse_api_origin(value, mode))
            .transpose()?;
        let worker_api_token = read_secret(
            &values,
            "SOYUZ_WORKER_API_TOKEN_FILE",
            "SOYUZ_WORKER_API_TOKEN",
        )?;
        let cloudflare_queue_token = read_secret(
            &values,
            "CLOUDFLARE_QUEUE_TOKEN_FILE",
            "CLOUDFLARE_QUEUE_TOKEN",
        )?;
        let cloudflare_account_id = get("CLOUDFLARE_ACCOUNT_ID")
            .map(|value| parse_resource_id(value, "CLOUDFLARE_ACCOUNT_ID"))
            .transpose()?;
        let cloudflare_queue_id = get("CLOUDFLARE_QUEUE_ID")
            .map(|value| parse_resource_id(value, "CLOUDFLARE_QUEUE_ID"))
            .transpose()?;
        let worker_id = get("SOYUZ_WORKER_ID")
            .map(|value| {
                WorkerId::parse(value).map_err(|_| ConfigError::InvalidValue("SOYUZ_WORKER_ID"))
            })
            .transpose()?;
        let senku_api_origin = get("SENKU_API_ORIGIN")
            .map(|value| parse_senku_api_origin(value, mode))
            .transpose()?;
        let senku_api_token = read_secret(&values, "SENKU_API_TOKEN_FILE", "SENKU_API_TOKEN")?;
        if senku_api_token.as_ref().is_some_and(|token| {
            let token = token.expose_secret();
            !(32..=1_024).contains(&token.len())
                || !token.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
        }) {
            return Err(ConfigError::InvalidValue("SENKU_API_TOKEN"));
        }
        let cloudflare_queue_visibility_timeout_ms = parse_bounded_u32(
            get("CLOUDFLARE_QUEUE_VISIBILITY_TIMEOUT_MS"),
            60_000,
            1_000,
            43_200_000,
            "CLOUDFLARE_QUEUE_VISIBILITY_TIMEOUT_MS",
        )?;
        let polling_interval = parse_seconds(
            get("RUSTY_FOSSIL_POLL_INTERVAL_SECONDS"),
            5,
            1,
            3600,
            "RUSTY_FOSSIL_POLL_INTERVAL_SECONDS",
        )?;
        let request_timeout = parse_seconds(
            get("RUSTY_FOSSIL_REQUEST_TIMEOUT_SECONDS"),
            10,
            1,
            300,
            "RUSTY_FOSSIL_REQUEST_TIMEOUT_SECONDS",
        )?;
        let state_dir = PathBuf::from(get("RUSTY_FOSSIL_STATE_DIR").unwrap_or("./state"));
        let log_level = LogLevel::parse(get("RUSTY_FOSSIL_LOG_LEVEL"))?;
        let bind = get("RUSTY_FOSSIL_BIND")
            .unwrap_or("127.0.0.1:8080")
            .parse::<SocketAddr>()
            .map_err(|_| ConfigError::InvalidValue("RUSTY_FOSSIL_BIND"))?;

        Ok(Self {
            mode,
            soyuz_api_origin,
            worker_api_token,
            cloudflare_account_id,
            cloudflare_queue_id,
            cloudflare_queue_token,
            cloudflare_queue_visibility_timeout_ms,
            worker_id,
            senku_api_origin,
            senku_api_token,
            polling_interval,
            request_timeout,
            state_dir,
            log_level,
            bind,
        })
    }

    pub fn readiness_report(&self, state_store_ready: bool) -> ReadinessReport {
        self.readiness_report_with_state(state_store_ready, false, None)
    }

    pub fn cloudflare_queue_config(&self) -> Result<Option<CloudflareQueueConfig>, ConfigError> {
        let (Some(account_id), Some(queue_id), Some(api_token)) = (
            self.cloudflare_account_id.as_ref(),
            self.cloudflare_queue_id.as_ref(),
            self.cloudflare_queue_token.as_ref(),
        ) else {
            return Ok(None);
        };
        CloudflareQueueConfig::new(
            account_id.clone(),
            queue_id.clone(),
            api_token.clone(),
            self.request_timeout,
            self.cloudflare_queue_visibility_timeout_ms,
        )
        .map(Some)
        .map_err(|_| ConfigError::InvalidValue("Cloudflare Queue configuration"))
    }

    pub fn senku_client(&self) -> Result<Option<SenkuClient>, ConfigError> {
        let (Some(origin), Some(token)) = (&self.senku_api_origin, &self.senku_api_token) else {
            return Ok(None);
        };
        let config = SenkuClientConfig::new(origin.clone(), token.clone(), self.request_timeout)
            .map_err(|_| ConfigError::InvalidValue("SENKU_API_CONFIGURATION"))?;
        SenkuClient::new(config)
            .map(Some)
            .map_err(|_| ConfigError::InvalidValue("SENKU_API_CONFIGURATION"))
    }

    pub fn readiness_report_with_state(
        &self,
        state_store_ready: bool,
        recovery_backlog_healthy: bool,
        queue_transport_healthy: Option<bool>,
    ) -> ReadinessReport {
        let soyuz_configured = self.soyuz_api_origin.is_some()
            && self.worker_api_token.is_some()
            && self.worker_id.is_some();
        let queue_configured = self.cloudflare_account_id.is_some()
            && self.cloudflare_queue_id.is_some()
            && self.cloudflare_queue_token.is_some();
        let mut missing_dependencies = Vec::new();
        for (configured, name) in [
            (self.soyuz_api_origin.is_some(), "soyuz_api_origin"),
            (self.worker_api_token.is_some(), "soyuz_worker_api_token"),
            (self.worker_id.is_some(), "soyuz_worker_id"),
            (
                self.cloudflare_account_id.is_some(),
                "cloudflare_account_id",
            ),
            (self.cloudflare_queue_id.is_some(), "cloudflare_queue_id"),
            (
                self.cloudflare_queue_token.is_some(),
                "cloudflare_queue_token",
            ),
        ] {
            if !configured {
                missing_dependencies.push(name.to_owned());
            }
        }
        if !state_store_ready {
            missing_dependencies.push("state_store".to_owned());
        }
        let limitations = vec![
            "production_queue_intake_disabled".to_owned(),
            "queue_transport_not_probed".to_owned(),
            "execution_runtime_not_implemented".to_owned(),
        ];
        let execution_handoff_ready = state_store_ready
            && soyuz_configured
            && queue_configured
            && recovery_backlog_healthy
            && queue_transport_healthy == Some(true)
            && !limitations.contains(&"production_queue_intake_disabled".to_owned());
        let execution_ready = execution_handoff_ready
            && !limitations.contains(&"execution_runtime_not_implemented".to_owned());
        let ready = state_store_ready && execution_ready;
        ReadinessReport {
            ready,
            execution_ready,
            execution_handoff_ready,
            state_store_ready,
            soyuz_configured,
            queue_configured,
            queue_transport_healthy,
            recovery_backlog_healthy,
            outbox_backpressured: false,
            callbacks_pending: 0,
            callbacks_blocked: 0,
            oldest_pending_callback_age_ms: None,
            senku_configured: self.senku_api_origin.is_some() && self.senku_api_token.is_some(),
            senku_advisory_healthy: None,
            missing_dependencies,
            limitations,
        }
    }

    pub fn require_execution_configuration(&self) -> Result<(), ConfigError> {
        let report = self.readiness_report(true);
        if report.missing_dependencies.is_empty() {
            Ok(())
        } else {
            Err(ConfigError::MissingRequiredConfiguration(
                report.missing_dependencies.join(", "),
            ))
        }
    }
}

fn parse_api_origin(value: &str, mode: DeploymentMode) -> Result<Url, ConfigError> {
    let url = Url::parse(value).map_err(|_| ConfigError::InvalidValue("SOYUZ_API_ORIGIN"))?;
    let https = url.scheme() == "https";
    let development_loopback =
        mode == DeploymentMode::Development && url.scheme() == "http" && is_loopback(&url);
    if (!https && !development_loopback)
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || !matches!(url.path(), "" | "/")
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ConfigError::InvalidValue("SOYUZ_API_ORIGIN"));
    }
    Ok(url)
}

fn parse_senku_api_origin(value: &str, mode: DeploymentMode) -> Result<Url, ConfigError> {
    let url = Url::parse(value).map_err(|_| ConfigError::InvalidValue("SENKU_API_ORIGIN"))?;
    let https = url.scheme() == "https";
    let development_loopback =
        mode == DeploymentMode::Development && url.scheme() == "http" && is_loopback(&url);
    if (!https && !development_loopback)
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || !matches!(url.path(), "" | "/")
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ConfigError::InvalidValue("SENKU_API_ORIGIN"));
    }
    Ok(url)
}

fn is_loopback(url: &Url) -> bool {
    match url.host_str() {
        Some("localhost") => true,
        Some(host) => host
            .parse::<IpAddr>()
            .map(|address| address.is_loopback())
            .unwrap_or(false),
        None => false,
    }
}

fn parse_resource_id(value: &str, name: &'static str) -> Result<String, ConfigError> {
    if value.is_empty()
        || value.len() > 32
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return Err(ConfigError::InvalidValue(name));
    }
    Ok(value.to_owned())
}

fn parse_seconds(
    value: Option<&str>,
    default: u64,
    min: u64,
    max: u64,
    name: &'static str,
) -> Result<Duration, ConfigError> {
    let parsed = value
        .map(str::parse::<u64>)
        .transpose()
        .map_err(|_| ConfigError::InvalidValue(name))?
        .unwrap_or(default);
    if !(min..=max).contains(&parsed) {
        return Err(ConfigError::InvalidValue(name));
    }
    Ok(Duration::from_secs(parsed))
}

fn parse_bounded_u32(
    value: Option<&str>,
    default: u32,
    minimum: u32,
    maximum: u32,
    name: &'static str,
) -> Result<u32, ConfigError> {
    let Some(value) = value else {
        return Ok(default);
    };
    let parsed = value
        .parse::<u32>()
        .map_err(|_| ConfigError::InvalidValue(name))?;
    if !(minimum..=maximum).contains(&parsed) {
        return Err(ConfigError::InvalidValue(name));
    }
    Ok(parsed)
}

fn read_secret(
    values: &HashMap<String, String>,
    file_key: &'static str,
    env_key: &'static str,
) -> Result<Option<SecretString>, ConfigError> {
    if let Some(path) = values.get(file_key).filter(|path| !path.trim().is_empty()) {
        let file =
            std::fs::File::open(path).map_err(|_| ConfigError::InvalidSecretFile(file_key))?;
        return read_secret_contents(file, file_key);
    }
    values
        .get(env_key)
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .map(|value| normalize_secret(value, env_key))
        .transpose()
        .map(Option::flatten)
}

fn read_secret_contents(
    reader: impl Read,
    file_key: &'static str,
) -> Result<Option<SecretString>, ConfigError> {
    let mut value = String::new();
    reader
        .take((MAX_SECRET_BYTES + 1) as u64)
        .read_to_string(&mut value)
        .map_err(|_| ConfigError::InvalidSecretFile(file_key))?;
    if value.len() > MAX_SECRET_BYTES {
        return Err(ConfigError::InvalidSecretFile(file_key));
    }
    normalize_secret(value, file_key)
}

fn normalize_secret(
    value: String,
    name: &'static str,
) -> Result<Option<SecretString>, ConfigError> {
    let value = value.trim().to_owned();
    if value.is_empty() {
        return Ok(None);
    }
    if value.len() > 4096 || value.chars().any(char::is_whitespace) {
        return Err(ConfigError::InvalidValue(name));
    }
    Ok(Some(SecretString::from(value)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    struct CountingReader {
        remaining: usize,
        bytes_read: usize,
    }

    impl Read for CountingReader {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            let count = self.remaining.min(buffer.len());
            buffer[..count].fill(b'x');
            self.remaining -= count;
            self.bytes_read += count;
            Ok(count)
        }
    }

    #[test]
    fn oversized_secret_stream_is_rejected_after_a_bounded_read() {
        let mut reader = CountingReader {
            remaining: MAX_SECRET_BYTES * 8,
            bytes_read: 0,
        };

        let result = read_secret_contents(&mut reader, "SOYUZ_WORKER_API_TOKEN_FILE");

        assert!(matches!(
            result,
            Err(ConfigError::InvalidSecretFile(
                "SOYUZ_WORKER_API_TOKEN_FILE"
            ))
        ));
        assert_eq!(reader.bytes_read, MAX_SECRET_BYTES + 1);
    }

    #[test]
    fn queue_visibility_timeout_uses_a_bounded_configurable_default() {
        let default = AppConfig::from_pairs([]).unwrap();
        assert_eq!(default.cloudflare_queue_visibility_timeout_ms, 60_000);

        let configured = AppConfig::from_pairs([(
            "CLOUDFLARE_QUEUE_VISIBILITY_TIMEOUT_MS".to_owned(),
            "90000".to_owned(),
        )])
        .unwrap();
        assert_eq!(configured.cloudflare_queue_visibility_timeout_ms, 90_000);
        assert!(matches!(
            AppConfig::from_pairs([(
                "CLOUDFLARE_QUEUE_VISIBILITY_TIMEOUT_MS".to_owned(),
                "999".to_owned(),
            )]),
            Err(ConfigError::InvalidValue(
                "CLOUDFLARE_QUEUE_VISIBILITY_TIMEOUT_MS"
            ))
        ));
    }

    #[test]
    fn queue_transport_configuration_uses_the_dedicated_token_and_timeout() {
        let config = AppConfig::from_pairs([
            ("CLOUDFLARE_ACCOUNT_ID".to_owned(), "account-123".to_owned()),
            ("CLOUDFLARE_QUEUE_ID".to_owned(), "queue-123".to_owned()),
            (
                "CLOUDFLARE_QUEUE_TOKEN".to_owned(),
                "separate-queue-token".to_owned(),
            ),
            (
                "CLOUDFLARE_QUEUE_VISIBILITY_TIMEOUT_MS".to_owned(),
                "90000".to_owned(),
            ),
        ])
        .unwrap();
        let queue_config = config.cloudflare_queue_config().unwrap().unwrap();
        let debug = format!("{queue_config:?}");
        assert!(debug.contains("visibility_timeout_ms: 90000"));
        assert!(!debug.contains("separate-queue-token"));
        assert!(
            AppConfig::from_pairs([])
                .unwrap()
                .cloudflare_queue_config()
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn senku_configuration_is_optional_separate_and_redacted() {
        let absent = AppConfig::from_pairs([]).unwrap();
        assert!(!absent.readiness_report(true).senku_configured);
        assert!(absent.senku_client().unwrap().is_none());

        let configured = AppConfig::from_pairs([
            (
                "SENKU_API_ORIGIN".to_owned(),
                "https://senku.example.test".to_owned(),
            ),
            (
                "SENKU_API_TOKEN".to_owned(),
                "synthetic-senku-token-000000000000".to_owned(),
            ),
        ])
        .unwrap();
        assert!(configured.readiness_report(true).senku_configured);
        assert!(configured.senku_client().unwrap().is_some());
        assert!(!format!("{configured:?}").contains("synthetic-senku-token"));
        assert!(
            !configured
                .readiness_report(true)
                .missing_dependencies
                .contains(&"senku".to_owned())
        );
    }

    #[test]
    fn senku_rejects_remote_loopback_http_and_malformed_credentials() {
        assert!(matches!(
            AppConfig::from_pairs([(
                "SENKU_API_ORIGIN".to_owned(),
                "http://senku.example.test".to_owned(),
            )]),
            Err(ConfigError::InvalidValue("SENKU_API_ORIGIN"))
        ));
        assert!(matches!(
            AppConfig::from_pairs([("SENKU_API_TOKEN".to_owned(), "too-short".to_owned(),)]),
            Err(ConfigError::InvalidValue("SENKU_API_TOKEN"))
        ));
        assert!(
            AppConfig::from_pairs([
                ("RUSTY_FOSSIL_ENV".to_owned(), "development".to_owned()),
                (
                    "SENKU_API_ORIGIN".to_owned(),
                    "http://127.0.0.1:8787".to_owned()
                ),
            ])
            .is_ok()
        );
    }
}
