use std::collections::BTreeMap;

use anyhow::{Context, Result};
use async_trait::async_trait;
use yandalf_core::{
    decision::{Decision, DecisionProvider},
    domain::Event,
};

use crate::{
    api::canonical_repository,
    model::{Change, ChangeKind, GithubContext},
};

pub struct GithubDecisions {
    sources: BTreeMap<String, Vec<ChangeKind>>,
}

impl GithubDecisions {
    pub fn new(repository: &str, on: Vec<ChangeKind>) -> Result<Self> {
        let mut sources = BTreeMap::new();
        sources.insert(format!("github:{}", canonical_repository(repository)?), on);
        Ok(Self { sources })
    }

    pub(crate) fn decide_context(&self, source: &str, context: &GithubContext) -> Result<Decision> {
        let Some(enabled) = self.sources.get(source) else {
            return Ok(Decision::Run);
        };
        match context.state() {
            Some("closed") => return Ok(Decision::Archive),
            Some("open") => {}
            _ => {
                return Ok(Decision::Skip {
                    reason: "Неизвестное состояние PR".into(),
                });
            }
        }
        if context.is_draft() {
            return Ok(Decision::Skip {
                reason: "PR в draft".into(),
            });
        }
        let changes = context
            .changes
            .as_ref()
            .context("missing GitHub changes for decision")?;
        let viewer = context
            .viewer
            .as_deref()
            .context("missing GitHub identity")?;
        if changes
            .iter()
            .any(|change| should_run(change, enabled, viewer))
        {
            return Ok(Decision::Run);
        }
        Ok(Decision::Skip {
            reason: "Нет изменений, требующих turn по настроенному правилу".into(),
        })
    }
}

#[async_trait]
impl DecisionProvider for GithubDecisions {
    async fn decide(&self, event: &Event) -> Result<Decision> {
        if !self.sources.contains_key(&event.source) {
            return Ok(Decision::Run);
        }
        self.decide_context(&event.source, &GithubContext::decode(event)?)
    }
}

fn should_run(change: &Change, enabled: &[ChangeKind], viewer: &str) -> bool {
    if !enabled.contains(&change.kind) {
        return false;
    }
    let Some(activity) = &change.activity else {
        return true;
    };
    if activity.actor.is_empty()
        || activity.actor.eq_ignore_ascii_case(viewer)
        || activity.actor_type == "Bot"
        || activity.actor.ends_with("[bot]")
    {
        return false;
    }
    change.kind != ChangeKind::ReviewRequested || activity.target.eq_ignore_ascii_case(viewer)
}

pub(crate) fn supersedes(older: &GithubContext, current: &GithubContext) -> bool {
    (older.state() == Some("closed") && current.state() != Some("closed"))
        || current.state() == Some("closed")
        || current.is_draft()
        || known_change(older.head_ref(), current.head_ref())
        || known_change(older.base_sha.as_deref(), current.base_sha.as_deref())
        || known_change(older.head_sha.as_deref(), current.head_sha.as_deref())
}

fn known_change(older: Option<&str>, current: Option<&str>) -> bool {
    matches!((older, current), (Some(older), Some(current)) if older != current)
}
