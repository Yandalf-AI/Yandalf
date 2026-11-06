use anyhow::Result;
use async_trait::async_trait;

use crate::domain::Event;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decision {
    Run,
    Archive,
    Skip { reason: String },
}

#[async_trait]
pub trait DecisionProvider: Send + Sync {
    async fn decide(&self, event: &Event) -> Result<Decision>;
}
