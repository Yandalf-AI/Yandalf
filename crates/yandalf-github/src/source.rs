use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use yandalf_channels::schedule::Timing;
use yandalf_core::{
    channel::{Channel, ChannelPlugin, Inbox},
    domain::now_ms,
    plugin::{Plugin, PluginContext},
    store::Store,
    task::TaskDeciding,
};

use crate::{
    api::{self, canonical_repository},
    model::{ChangeKind, GithubContext, PullRequest, StoredObservation},
    observation::{changed_event, closed_event, subject},
    policy::{GithubDecisions, supersedes},
};

#[derive(Clone)]
pub struct Github {
    repository: String,
    source: String,
    timing: Timing,
    instruction: Option<String>,
    on: Vec<ChangeKind>,
}

impl Github {
    pub fn new(repository: &str, interval: Option<&str>, cron: Option<&str>) -> Result<Self> {
        let repository = canonical_repository(repository)?;
        let timing = Timing::new(interval, cron)?;
        timing.next_tick(now_ms())?;
        Ok(Self {
            source: format!("github:{repository}"),
            repository,
            timing,
            instruction: None,
            on: ChangeKind::defaults(),
        })
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn with_triggers(mut self, on: Vec<ChangeKind>) -> Self {
        self.on = on;
        self
    }

    pub fn with_instruction(mut self, instruction: Option<&str>) -> Result<Self> {
        if let Some(instruction) = instruction {
            ensure!(
                !instruction.trim().is_empty(),
                "GitHub task text must not be empty"
            );
            self.instruction = Some(instruction.to_owned());
        }
        Ok(self)
    }

    async fn poll(
        &self,
        inbox: &Inbox,
        viewer: &str,
        watch_started_at: i64,
    ) -> Result<(usize, usize)> {
        let poll_started_at = now_ms();
        let cursor_source = format!("{}:activity", self.source);
        let cursor = inbox
            .source_cursor(&cursor_source, watch_started_at)
            .await?;
        let (dirty, feed_failed) = self.dirty_pulls(cursor).await?;
        let previous = inbox
            .last_events(&self.source)
            .await?
            .into_iter()
            .map(|event| {
                let key = event.subject.clone();
                Ok((key, StoredObservation::decode(event)?))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let (pulls, pull_failed) = self.refresh_pulls(&previous).await?;
        let (admitted, activity_failed) = self
            .observe_pulls(inbox, &pulls, &previous, &dirty, viewer, watch_started_at)
            .await?;
        ensure!(
            !feed_failed && !pull_failed && !activity_failed,
            "some GitHub PRs could not be refreshed; retaining activity cursor for retry"
        );
        inbox
            .advance_source_cursor(&cursor_source, poll_started_at)
            .await?;
        Ok((pulls.len(), admitted))
    }

    async fn dirty_pulls(&self, cursor: i64) -> Result<(BTreeSet<u64>, bool)> {
        let since = chrono::DateTime::from_timestamp_millis(cursor.saturating_sub(1000))
            .context("invalid GitHub activity cursor")?
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let mut dirty = BTreeSet::new();
        let mut failed = false;
        for (path, field) in [
            ("issues/comments", "issue_url"),
            ("pulls/comments", "pull_request_url"),
        ] {
            match api::comment_numbers(&self.repository, path, field, &since, &mut dirty).await {
                Ok(()) => {}
                Err(error) => {
                    failed = true;
                    tracing::warn!(
                        feed = path,
                        error = %format!("{error:#}"),
                        "GitHub comment feed refresh failed"
                    );
                }
            }
        }
        Ok((dirty, failed))
    }

    async fn refresh_pulls(
        &self,
        previous: &BTreeMap<String, StoredObservation>,
    ) -> Result<(Vec<PullRequest>, bool)> {
        let mut pulls = api::open_pulls(&self.repository).await?;
        let present: BTreeSet<_> = pulls.iter().map(|pull| pull.number).collect();
        let mut failed = false;
        for previous in previous.values() {
            let Some(number) = previous.context.pr else {
                continue;
            };
            if present.contains(&number) {
                continue;
            }
            match previous.context.state() {
                Some("closed") => {
                    let pull = previous.context.pull.as_ref().unwrap().complete()?;
                    ensure!(
                        pull.number == number,
                        "persisted pull request number mismatch"
                    );
                    pulls.push(pull);
                }
                state if state == Some("open") || previous.context.pull.is_none() => {
                    match api::pull(&self.repository, number).await {
                        Ok(pull) => pulls.push(pull),
                        Err(error) => {
                            failed = true;
                            tracing::warn!(
                                pull_request = number,
                                error = %format!("{error:#}"),
                                "GitHub PR refresh failed"
                            );
                        }
                    }
                }
                _ => {}
            }
        }
        Ok((pulls, failed))
    }

    async fn observe_pulls(
        &self,
        inbox: &Inbox,
        pulls: &[PullRequest],
        previous: &BTreeMap<String, StoredObservation>,
        dirty: &BTreeSet<u64>,
        viewer: &str,
        watch_started_at: i64,
    ) -> Result<(usize, bool)> {
        let mut admitted = 0;
        let mut failed = false;
        for pull in pulls {
            let previous = previous.get(&subject(&self.repository, pull.number));
            if pull.state == "closed" {
                admitted += self.admit_closed(inbox, pull, previous).await?;
                continue;
            }
            if !dirty.contains(&pull.number)
                && let Some(previous) = previous
                && previous.context.matches_pull(pull)?
                && previous.context.viewer.as_deref() == Some(viewer)
            {
                continue;
            }
            let activities = match api::activities(&self.repository, pull.number).await {
                Ok(activities) => activities,
                Err(error) => {
                    failed = true;
                    tracing::warn!(
                        pull_request = pull.number,
                        error = %format!("{error:#}"),
                        "GitHub PR activity refresh failed"
                    );
                    continue;
                }
            };
            let Some(mut event) = changed_event(
                &self.repository,
                pull,
                &activities,
                previous.map(|previous| &previous.event),
                viewer,
                watch_started_at,
                pulls,
            )?
            else {
                continue;
            };
            if let Some(instruction) = &self.instruction {
                event.text.push_str("\n\n");
                event.text.push_str(instruction);
            }
            let changes = GithubContext::decode(&event)?
                .changes
                .map_or(0, |changes| changes.len());
            let job = inbox.accept(&event).await?;
            admitted += 1;
            tracing::info!(
                repository = %self.repository,
                pull_request = pull.number,
                job_id = %job.id,
                changes,
                "GitHub observation accepted"
            );
        }
        Ok((admitted, failed))
    }

    async fn admit_closed(
        &self,
        inbox: &Inbox,
        pull: &PullRequest,
        previous: Option<&StoredObservation>,
    ) -> Result<usize> {
        let event = closed_event(&self.repository, pull)?;
        if previous.is_some_and(|previous| previous.event.id == event.id) {
            return Ok(0);
        }
        let job = inbox.accept(&event).await?;
        tracing::info!(
            repository = %self.repository,
            pull_request = pull.number,
            job_id = %job.id,
            "GitHub session archive queued"
        );
        Ok(1)
    }

    async fn poll_with_identity(
        &self,
        inbox: &Inbox,
        viewer: &mut Option<String>,
        watch_started_at: i64,
    ) -> Result<(usize, usize)> {
        if viewer.is_none() {
            *viewer = Some(api::viewer().await?);
        }
        self.poll(inbox, viewer.as_deref().unwrap(), watch_started_at)
            .await
    }

    async fn wait_for_tick(&self, stop: &CancellationToken) -> Result<bool> {
        let next = self.timing.next_tick(now_ms())?;
        loop {
            let delay = next.saturating_sub(now_ms()).max(0) as u64;
            tokio::select! {
                _ = stop.cancelled() => return Ok(false),
                _ = tokio::time::sleep(Duration::from_millis(delay.min(60_000))) => {}
            }
            if now_ms() >= next {
                return Ok(true);
            }
        }
    }
}

#[async_trait]
impl Channel for Github {
    async fn run(&self, inbox: Inbox, stop: CancellationToken) -> Result<()> {
        let watch_started_at = inbox
            .source_cursor(&format!("{}:watch", self.source), now_ms())
            .await?;
        let mut viewer = None;
        loop {
            let result = tokio::select! {
                _ = stop.cancelled() => return Ok(()),
                result = self.poll_with_identity(&inbox, &mut viewer, watch_started_at) => result
            };
            match result {
                Ok((observed, admitted)) => {
                    tracing::info!(
                        repository = %self.repository,
                        observed,
                        admitted,
                        "GitHub poll completed"
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        repository = %self.repository,
                        error = %format!("{error:#}"),
                        "GitHub poll failed; retrying on the next tick"
                    );
                }
            }
            if !self.wait_for_tick(&stop).await? {
                return Ok(());
            }
        }
    }
}

impl Plugin for Github {
    fn name(&self) -> &str {
        self.source()
    }

    fn register(&self, ctx: &mut PluginContext) -> Result<()> {
        ChannelPlugin::new(self.source(), self.clone()).register(ctx)?;
        let policy = Arc::new(GithubDecisions::new(&self.repository, self.on.clone())?);
        let source = self.source.clone();
        ctx.on::<TaskDeciding, _>(move |services, task| {
            let policy = policy.clone();
            let source = source.clone();
            Box::pin(async move {
                if task.job.event.source != source {
                    return Ok(());
                }
                let event = &task.job.event;
                let context = GithubContext::decode(event)?;
                let store = services.get::<dyn Store>()?;
                if let Some(latest) = store.last_event(&source, &event.subject).await?
                    && latest.id != event.id
                    && supersedes(&context, &GithubContext::decode(&latest)?)
                {
                    task.decision = yandalf_core::decision::Decision::Skip {
                        reason: "Наблюдение заменено более новым состоянием PR".into(),
                    };
                    return Ok(());
                }
                let decision = policy.decide_context(&event.source, &context)?;
                if !matches!(decision, yandalf_core::decision::Decision::Run) {
                    task.decision = decision;
                }
                Ok(())
            })
        });
        Ok(())
    }
}
