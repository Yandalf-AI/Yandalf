pub mod schedule;
pub use schedule::Schedule;

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_util::sync::CancellationToken;
use yandalf_core::{
    channel::{Channel, ChannelPlugin, Inbox},
    domain::Event,
    plugin::{Plugin, PluginContext},
};

pub struct Stdin;
#[async_trait]
impl Channel for Stdin {
    async fn run(&self, inbox: Inbox, stop: CancellationToken) -> Result<()> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Input {
            id: String,
            subject: String,
            text: String,
            #[serde(default)]
            title: Option<String>,
            #[serde(default)]
            context: Value,
        }
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        loop {
            let line =
                tokio::select! { _=stop.cancelled()=>return Ok(()),line=lines.next_line()=>line? };
            let Some(line) = line else { return Ok(()) };
            if line.trim().is_empty() {
                continue;
            }
            let input: Input = serde_json::from_str(&line).context("invalid stdin event")?;
            let job = inbox
                .accept(&Event {
                    id: input.id,
                    source: "stdin".into(),
                    subject: input.subject,
                    title: input.title,
                    text: input.text,
                    context: input.context,
                })
                .await?;
            tracing::info!(job_id=%job.id,"event accepted");
        }
    }
}
impl Plugin for Stdin {
    fn name(&self) -> &str {
        "stdin"
    }
    fn register(&self, ctx: &mut PluginContext) -> Result<()> {
        ChannelPlugin::new("stdin", Stdin).register(ctx)
    }
}
