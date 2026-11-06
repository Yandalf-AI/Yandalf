use anyhow::Result;
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;
use yandalf::app::Settings;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "yandalf=info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let path = std::env::var_os("YANDALF_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|| "yandalf.toml".into());
    let app = Settings::load(&path)?.build().await?;
    let stop = CancellationToken::new();
    let signal_stop = stop.clone();
    let signal = tokio::spawn(async move {
        let result = shutdown_signal().await;
        signal_stop.cancel();
        result
    });
    let result = app.run(stop).await;
    signal.abort();
    result
}
async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}
