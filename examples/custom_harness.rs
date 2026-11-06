use anyhow::Result;
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;
use yandalf::{
    codex::CodexConfig,
    config::Config,
    input::Stdin,
    plugin::{App, Plugin, PluginContext},
    plugins::{CodexServer, Sqlite, TaskRunner},
    runtime::TaskCompleted,
};

struct ResultLog;
impl Plugin for ResultLog {
    fn name(&self) -> &str {
        "result-log"
    }
    fn register(&self, ctx: &mut PluginContext) -> Result<()> {
        ctx.observe::<TaskCompleted, _>(|_, completed| {
            Box::pin(async move {
                println!(
                    "{}: {:?}",
                    completed.job.event.subject, completed.result.status
                );
                Ok(())
            })
        });
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let endpoint = std::env::var("YANDALF_CODEX_ENDPOINT")?;
    let cwd = std::env::current_dir()?;
    let app = App::builder()
        .plugin(Stdin)
        .plugin(ResultLog)
        .plugin(TaskRunner(Config {
            cwd,
            ..Config::default()
        }))
        .plugin(CodexServer(CodexConfig {
            endpoint,
            ..CodexConfig::default()
        }))
        .plugin(Sqlite(PathBuf::from("data/custom-harness.sqlite")))
        .build()
        .await?;
    let stop = CancellationToken::new();
    let signal = stop.clone();
    let watcher = tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        signal.cancel();
    });
    let result = app.run(stop).await;
    watcher.abort();
    result
}
