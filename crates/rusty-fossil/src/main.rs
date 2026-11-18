use axum::serve;
use clap::Parser;
use fossil_storage::SqliteStore;
use rusty_fossil::api::{ApiState, router};
use rusty_fossil::config::AppConfig;
use std::net::SocketAddr;
use std::path::PathBuf;
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "rusty-fossil",
    about = "Soyuz-authorized repository worker controller"
)]
struct Args {
    /// Override the diagnostics bind address. Defaults to loopback.
    #[arg(long)]
    bind: Option<SocketAddr>,
    /// Override the durable local state directory.
    #[arg(long)]
    state_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let mut config = AppConfig::from_env()?;
    if let Some(bind) = args.bind {
        config.bind = bind;
    }
    if let Some(state_dir) = args.state_dir {
        config.state_dir = state_dir;
    }

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(config.log_level.as_str()))
        .with_target(false)
        .init();

    let store = SqliteStore::open(config.state_dir.join("fossil.sqlite3"))?;
    let listener = TcpListener::bind(config.bind).await?;
    tracing::info!(address = %config.bind, "diagnostic service listening");

    serve(listener, router(ApiState::new(config, store)))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
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
