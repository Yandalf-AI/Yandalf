use crate::{
    domain::{Event, Job},
    plugin::{Plugin, PluginContext},
    store::Store,
};
use anyhow::{Result, ensure};
use async_trait::async_trait;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct Inbox {
    store: Arc<dyn Store>,
}

impl Inbox {
    pub fn new(store: Arc<dyn Store>) -> Self {
        Self { store }
    }

    pub(crate) fn validate(event: &Event) -> Result<()> {
        ensure!(
            !event.id.is_empty() && !event.source.is_empty() && !event.subject.is_empty(),
            "event identity must not be empty"
        );
        ensure!(
            !event.text.trim().is_empty(),
            "event text must not be empty"
        );
        Ok(())
    }

    pub async fn accept(&self, event: &Event) -> Result<Job> {
        Self::validate(event)?;
        self.store.enqueue(event).await
    }

    pub async fn accept_scheduled(&self, event: &Event, next: i64) -> Result<Job> {
        Self::validate(event)?;
        self.store.accept_scheduled(event, next).await
    }

    pub async fn source_cursor(&self, source: &str, initial: i64) -> Result<i64> {
        self.store.source_cursor(source, initial).await
    }

    pub async fn advance_source_cursor(&self, source: &str, next: i64) -> Result<()> {
        self.store.advance_source_cursor(source, next).await
    }

    pub async fn last_event(&self, source: &str, subject: &str) -> Result<Option<Event>> {
        self.store.last_event(source, subject).await
    }

    pub async fn last_events(&self, source: &str) -> Result<Vec<Event>> {
        self.store.last_events(source).await
    }
}

#[async_trait]
pub trait Channel: Send + Sync {
    async fn run(&self, inbox: Inbox, stop: CancellationToken) -> Result<()>;
}

pub struct ChannelPlugin {
    name: String,
    channel: Arc<dyn Channel>,
}

impl ChannelPlugin {
    pub fn new(name: impl Into<String>, channel: impl Channel + 'static) -> Self {
        Self {
            name: name.into(),
            channel: Arc::new(channel),
        }
    }
}

impl Plugin for ChannelPlugin {
    fn name(&self) -> &str {
        &self.name
    }

    fn register(&self, ctx: &mut PluginContext) -> Result<()> {
        ctx.require::<dyn Store>();
        let channel = self.channel.clone();
        ctx.task("receive events", move |services, stop| async move {
            channel
                .run(Inbox::new(services.get::<dyn Store>()?), stop)
                .await
        });
        Ok(())
    }
}
