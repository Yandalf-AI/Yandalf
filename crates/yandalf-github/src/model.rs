use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use yandalf_core::domain::Event;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChangeKind {
    Opened,
    Revision,
    ReadyForReview,
    Reopened,
    Edited,
    ReviewRequested,
    Commented,
    Reviewed,
}

impl ChangeKind {
    pub fn defaults() -> Vec<Self> {
        vec![
            Self::Opened,
            Self::Revision,
            Self::ReadyForReview,
            Self::Reopened,
            Self::Edited,
            Self::ReviewRequested,
            Self::Commented,
            Self::Reviewed,
        ]
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct PullRequest {
    pub(crate) number: u64,
    pub(crate) url: String,
    pub(crate) title: String,
    pub(crate) body: String,
    pub(crate) state: String,
    pub(crate) draft: bool,
    pub(crate) base_sha: String,
    pub(crate) head_sha: String,
    pub(crate) base_ref: String,
    pub(crate) head_ref: String,
    pub(crate) updated_at: String,
    pub(crate) created_at: String,
    pub(crate) stack: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Activity {
    pub(crate) id: String,
    pub(crate) kind: ChangeKind,
    pub(crate) actor: String,
    pub(crate) actor_type: String,
    pub(crate) target: String,
    pub(crate) body: String,
    pub(crate) state: String,
    pub(crate) updated_at: String,
    pub(crate) url: String,
    pub(crate) path: Value,
    pub(crate) line: Value,
    pub(crate) in_reply_to_id: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Change {
    pub(crate) kind: ChangeKind,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub(crate) activity: Option<Activity>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub(crate) struct GithubContext {
    pub(crate) pr: Option<u64>,
    pub(crate) base_sha: Option<String>,
    pub(crate) head_sha: Option<String>,
    pub(crate) pull: Option<PullSnapshot>,
    pub(crate) activity: Option<BTreeMap<String, String>>,
    pub(crate) changes: Option<Vec<Change>>,
    pub(crate) viewer: Option<String>,
}

impl GithubContext {
    pub(crate) fn decode(event: &Event) -> Result<Self> {
        serde_json::from_value(event.context.clone()).context("decode GitHub event context")
    }

    pub(crate) fn state(&self) -> Option<&str> {
        self.pull.as_ref().and_then(|pull| pull.state.as_deref())
    }

    pub(crate) fn is_draft(&self) -> bool {
        self.pull.as_ref().and_then(|pull| pull.draft) == Some(true)
    }

    pub(crate) fn head_ref(&self) -> Option<&str> {
        self.pull.as_ref().and_then(|pull| pull.head_ref.as_deref())
    }

    pub(crate) fn matches_pull(&self, pull: &PullRequest) -> Result<bool> {
        match &self.pull {
            Some(previous) => previous.matches(pull),
            None => Ok(false),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct PullSnapshot {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) draft: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) body: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) base_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) head_ref: Option<String>,
    #[serde(flatten)]
    remaining: BTreeMap<String, Value>,
}

impl PullSnapshot {
    fn matches(&self, pull: &PullRequest) -> Result<bool> {
        Ok(serde_json::to_value(self)? == serde_json::to_value(pull)?)
    }

    pub(crate) fn complete(&self) -> Result<PullRequest> {
        serde_json::from_value(serde_json::to_value(self)?)
            .context("decode persisted closed pull request")
    }
}

pub(crate) struct StoredObservation {
    pub(crate) event: Event,
    pub(crate) context: GithubContext,
}

impl StoredObservation {
    pub(crate) fn decode(event: Event) -> Result<Self> {
        let context = GithubContext::decode(&event)?;
        Ok(Self { event, context })
    }
}
