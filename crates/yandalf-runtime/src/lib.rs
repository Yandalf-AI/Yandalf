pub mod config;
pub mod runtime;

use anyhow::Result;
pub use config::Config;
pub use runtime::Runtime;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
pub use yandalf_core::task;
use yandalf_core::{
    executor::ExecutorProvider,
    plugin::{Plugin, PluginContext, Services},
    store::Store,
};

pub struct TaskRunner(pub Config);
impl Plugin for TaskRunner {
    fn name(&self) -> &str {
        "task-runner"
    }
    fn register(&self, ctx: &mut PluginContext) -> Result<()> {
        self.0.validate()?;
        ctx.require::<dyn Store>();
        ctx.require::<dyn ExecutorProvider>();
        let config = Arc::new(self.0.clone());
        ctx.task("execute tasks", move |services, stop| async move {
            serve(config, services, stop).await
        });
        Ok(())
    }
}

async fn serve(config: Arc<Config>, services: Services, stop: CancellationToken) -> Result<()> {
    let store = services.get::<dyn Store>()?;
    let provider = services.get::<dyn ExecutorProvider>()?;
    let mut retry_secs = 1;
    loop {
        let connection = tokio::select! {
            _ = stop.cancelled() => return Ok(()),
            result = provider.connect() => result,
        };
        match connection {
            Ok(connection) => {
                retry_secs = 1;
                let runtime = Arc::new(
                    Runtime::new(config.clone(), store.clone(), connection.executor)?
                        .with_services(services.clone()),
                );
                let connection_stop = stop.child_token();
                let run = runtime.run(connection_stop.clone(), false);
                tokio::pin!(run);
                tokio::select! {
                    result = &mut run => return result,
                    reason = connection.closed => {
                        tracing::warn!(reason, "executor disconnected; preserving tasks before reconnect");
                        connection_stop.cancel();
                        run.await?;
                    }
                }
            }
            Err(error) => {
                tracing::warn!(error = %format!("{error:#}"), "executor connection failed; inbox remains active")
            }
        }
        tokio::select! {
            _ = stop.cancelled() => return Ok(()),
            _ = tokio::time::sleep(Duration::from_secs(retry_secs)) => {},
        }
        retry_secs = (retry_secs * 2).min(30);
    }
}
