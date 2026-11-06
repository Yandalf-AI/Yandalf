use anyhow::Result;
use std::path::PathBuf;
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;
use yandalf::{
    channel::{Channel, ChannelPlugin, Inbox},
    codex::CodexConfig,
    config::Config,
    domain::Event,
    plugin::{App, Plugin, PluginContext},
    plugins::{CodexServer, Sqlite, TaskRunner},
    runtime::TaskCompleted,
};

struct ProjectCommand {
    id: String,
    project: String,
    text: String,
}

struct Commands {
    source: &'static str,
    receiver: Mutex<mpsc::Receiver<ProjectCommand>>,
}

#[async_trait::async_trait]
impl Channel for Commands {
    async fn run(&self, inbox: Inbox, stop: CancellationToken) -> Result<()> {
        let mut receiver = self.receiver.lock().await;
        loop {
            let command = tokio::select! {
                _ = stop.cancelled() => return Ok(()),
                command = receiver.recv() => match command {
                    Some(command) => command,
                    None => return Ok(()),
                },
            };
            inbox
                .accept(&Event {
                    id: command.id,
                    source: self.source.into(),
                    subject: format!("project:{}", command.project),
                    title: None,
                    text: command.text,
                    context: serde_json::Value::Null,
                })
                .await?;
        }
    }
}

struct ResultLog;
impl Plugin for ResultLog {
    fn name(&self) -> &str {
        "result-log"
    }
    fn register(&self, ctx: &mut PluginContext) -> Result<()> {
        ctx.observe::<TaskCompleted, _>(|_, completed| {
            Box::pin(async move {
                tracing::info!(
                    subject = completed.job.event.subject,
                    status = ?completed.result.status,
                    "task completed"
                );
                Ok(())
            })
        });
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let endpoint = std::env::var("YANDALF_CODEX_ENDPOINT")?;
    let cwd = std::env::current_dir()?;
    let (application, application_commands) = mpsc::channel(32);
    let (operations, operations_commands) = mpsc::channel(32);
    let app = App::builder()
        .plugin(ChannelPlugin::new(
            "application-commands",
            Commands {
                source: "application",
                receiver: Mutex::new(application_commands),
            },
        ))
        .plugin(ChannelPlugin::new(
            "operations-commands",
            Commands {
                source: "operations",
                receiver: Mutex::new(operations_commands),
            },
        ))
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
    application
        .send(ProjectCommand {
            id: "startup-summary".into(),
            project: "example".into(),
            text: "Summarize the project's structure without changing files.".into(),
        })
        .await?;
    operations
        .send(ProjectCommand {
            id: "startup-checks".into(),
            project: "example".into(),
            text: "List the project's development checks without running them.".into(),
        })
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
