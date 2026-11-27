use fossil_gateway::{
    DEFAULT_BIND_ADDRESS, GatewayBroker, GatewayEndpoint, GatewayRequestLedger, GatewayTokenSigner,
};
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use hyper_util::service::TowerToHyperService;
use rustls::pki_types::pem::PemObject;
use secrecy::SecretString;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let endpoint = GatewayEndpoint::parse(&required("FOSSIL_GATEWAY_ENDPOINT")?)?;
    let model = required("FOSSIL_GATEWAY_MODEL")?;
    let gateway_auth_token = read_secret_file("FOSSIL_GATEWAY_AUTH_TOKEN_FILE", 32, 4096)?;
    let signing_key = read_secret_file("FOSSIL_GATEWAY_SIGNING_KEY_FILE", 32, 512)?;
    let tls_certificate_path = PathBuf::from(required("FOSSIL_GATEWAY_TLS_CERT_FILE")?);
    let tls_key_path = PathBuf::from(required("FOSSIL_GATEWAY_TLS_KEY_FILE")?);
    let tls_certificate =
        read_tls_file(&tls_certificate_path, "FOSSIL_GATEWAY_TLS_CERT_FILE", false)?;
    let tls_key = read_tls_file(&tls_key_path, "FOSSIL_GATEWAY_TLS_KEY_FILE", true)?;
    let tls_config = tls_server_config(&tls_certificate, &tls_key)?;
    let ledger_path = PathBuf::from(
        std::env::var_os("FOSSIL_GATEWAY_LEDGER_PATH")
            .unwrap_or_else(|| "/var/lib/fossil-gateway/ledger.sqlite3".into()),
    );
    if !ledger_path.is_absolute() {
        return Err("FOSSIL_GATEWAY_LEDGER_PATH must be absolute".into());
    }
    let bind: SocketAddr = std::env::var("FOSSIL_GATEWAY_BIND")
        .unwrap_or_else(|_| DEFAULT_BIND_ADDRESS.to_owned())
        .parse()?;

    let ledger = GatewayRequestLedger::open(&ledger_path)?;
    let signer = GatewayTokenSigner::new(signing_key)?;
    let broker = std::sync::Arc::new(GatewayBroker::new(
        endpoint,
        model,
        gateway_auth_token,
        signer,
        ledger,
    )?);
    let listener = TcpListener::bind(bind).await?;
    tracing::info!(address = %bind, "restricted model gateway broker listening with TLS");
    let tls_acceptor = TlsAcceptor::from(tls_config);
    let router = broker.router();
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let tls_acceptor = tls_acceptor.clone();
                let router = router.clone();
                tokio::spawn(async move {
                    let Ok(stream) = tls_acceptor.accept(stream).await else {
                        tracing::debug!("gateway TLS handshake failed");
                        return;
                    };
                    let service = TowerToHyperService::new(router);
                    if http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await
                        .is_err()
                    {
                        tracing::debug!("gateway HTTPS connection ended with an error");
                    }
                });
            }
        }
    }
    Ok(())
}

fn tls_server_config(
    certificate_pem: &[u8],
    private_key_pem: &[u8],
) -> Result<Arc<rustls::ServerConfig>, Box<dyn std::error::Error>> {
    let certificates = rustls::pki_types::CertificateDer::pem_slice_iter(certificate_pem)
        .collect::<Result<Vec<_>, _>>()?;
    if certificates.is_empty() {
        return Err("TLS certificate file contains no certificates".into());
    }
    let private_key = rustls::pki_types::PrivateKeyDer::from_pem_slice(private_key_pem)?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(certificates, private_key)?;
    Ok(Arc::new(config))
}

fn read_tls_file(
    path: &std::path::Path,
    variable: &'static str,
    private: bool,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    use std::{fs::OpenOptions, io::Read};

    const MAX_TLS_FILE_BYTES: usize = 64 * 1024;
    if !path.is_absolute() {
        return Err(format!("{variable} must be an absolute path").into());
    }
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.len() == 0
        || metadata.len() > MAX_TLS_FILE_BYTES as u64
        || (private && metadata.permissions().mode() & 0o077 != 0)
    {
        return Err(format!(
            "{variable} must reference a bounded regular file with safe permissions"
        )
        .into());
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.by_ref()
        .take(MAX_TLS_FILE_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.is_empty() || bytes.len() > MAX_TLS_FILE_BYTES {
        return Err(format!("{variable} contains invalid TLS material").into());
    }
    Ok(bytes)
}

fn required(name: &'static str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("missing required configuration {name}").into())
}

fn read_secret_file(
    variable: &'static str,
    minimum: usize,
    maximum: usize,
) -> Result<SecretString, Box<dyn std::error::Error>> {
    let path = PathBuf::from(required(variable)?);
    read_private_secret_file(&path, variable, minimum, maximum)
}

fn read_private_secret_file(
    path: &std::path::Path,
    variable: &'static str,
    minimum: usize,
    maximum: usize,
) -> Result<SecretString, Box<dyn std::error::Error>> {
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::fs::PermissionsExt;
    use std::{fs::OpenOptions, io::Read};

    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.permissions().mode() & 0o077 != 0
        || metadata.len() > maximum as u64
    {
        return Err(format!("{variable} must reference a private regular file").into());
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.by_ref()
        .take(maximum as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > maximum {
        return Err(format!("{variable} contains an invalid secret").into());
    }
    let mut value = String::from_utf8(bytes)?;
    while value.ends_with('\n') || value.ends_with('\r') {
        value.pop();
    }
    if !(minimum..=maximum).contains(&value.len())
        || !value.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
    {
        return Err(format!("{variable} contains an invalid secret").into());
    }
    Ok(SecretString::from(value))
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        let mut signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("register SIGTERM handler");
        signal.recv().await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use super::{read_private_secret_file, tls_server_config};
    use secrecy::ExposeSecret;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn reads_only_bounded_private_regular_secret_files_without_following_symlinks() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("secret");
        std::fs::write(&path, "secret-value-0123456789abcdef0123\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o400)).unwrap();
        let secret = read_private_secret_file(&path, "TEST_SECRET", 32, 64).unwrap();
        assert_eq!(secret.expose_secret(), "secret-value-0123456789abcdef0123");

        let public = directory.path().join("public");
        std::fs::write(&public, "secret-value-0123456789abcdef0123").unwrap();
        std::fs::set_permissions(&public, std::fs::Permissions::from_mode(0o444)).unwrap();
        assert!(read_private_secret_file(&public, "TEST_SECRET", 32, 64).is_err());

        let target = directory.path().join("target");
        std::fs::write(&target, "secret-value-0123456789abcdef0123").unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o400)).unwrap();
        let link = directory.path().join("link");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        assert!(read_private_secret_file(&link, "TEST_SECRET", 32, 64).is_err());

        let oversized = directory.path().join("oversized");
        std::fs::write(&oversized, "x".repeat(65)).unwrap();
        std::fs::set_permissions(&oversized, std::fs::Permissions::from_mode(0o400)).unwrap();
        assert!(read_private_secret_file(&oversized, "TEST_SECRET", 32, 64).is_err());
    }

    #[test]
    fn tls_server_config_rejects_non_pem_test_material() {
        assert!(
            tls_server_config(b"test certificate placeholder", b"test key placeholder").is_err()
        );
    }
}
