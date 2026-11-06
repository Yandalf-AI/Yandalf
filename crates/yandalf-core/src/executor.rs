use crate::domain::{Inspection, SessionSpec, ThreadInspection, TurnInput, TurnResult};
use anyhow::Result;
use async_trait::async_trait;
use futures_util::future::BoxFuture;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

#[async_trait]
pub trait Executor: Send + Sync {
    async fn create_thread(&self, cwd: &Path, creation_id: &str) -> Result<String>;
    async fn inspect_thread(&self, creation_id: &str) -> Result<ThreadInspection>;
    /// Refuse preparation while the backend reports unrelated active work in this thread.
    async fn thread_cwd(&self, thread_id: &str) -> Result<PathBuf>;
    async fn configure_thread(&self, thread_id: &str, spec: &SessionSpec) -> Result<()>;
    async fn archive_thread(&self, _thread_id: &str) -> Result<()> {
        anyhow::bail!("executor does not support session archiving")
    }
    async fn start_turn(
        &self,
        thread_id: &str,
        client_message_id: &str,
        input: &TurnInput,
    ) -> Result<String>;
    async fn wait_turn(&self, thread_id: &str, turn_id: &str) -> Result<TurnResult>;
    async fn inspect(
        &self,
        thread_id: &str,
        client_message_id: &str,
        turn_id: Option<&str>,
    ) -> Result<Inspection>;
    async fn interrupt(&self, thread_id: &str, turn_id: &str) -> Result<()>;
}

pub struct ExecutorConnection {
    pub executor: Arc<dyn Executor>,
    pub closed: BoxFuture<'static, String>,
}

#[async_trait]
pub trait ExecutorProvider: Send + Sync {
    async fn connect(&self) -> Result<ExecutorConnection>;
}
