mod config;
mod executor;
mod transport;
mod wire;

pub use config::CodexConfig;
pub use executor::Codex;

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

pub struct CodexServer(pub CodexConfig);

#[async_trait]
impl yandalf_core::executor::ExecutorProvider for CodexServer {
    async fn connect(&self) -> Result<yandalf_core::executor::ExecutorConnection> {
        let client = Arc::new(Codex::connect(&self.0).await?);
        let observer = client.clone();
        Ok(yandalf_core::executor::ExecutorConnection {
            executor: client,
            closed: Box::pin(async move { observer.wait_closed().await }),
        })
    }
}
impl yandalf_core::plugin::Plugin for CodexServer {
    fn name(&self) -> &str {
        "codex"
    }
    fn register(&self, ctx: &mut yandalf_core::plugin::PluginContext) -> Result<()> {
        self.0.validate()?;
        ctx.provide::<dyn yandalf_core::executor::ExecutorProvider>(Arc::new(Self(self.0.clone())))
    }
}
