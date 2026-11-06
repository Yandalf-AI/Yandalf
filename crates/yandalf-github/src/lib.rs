use std::{
    collections::{BTreeMap, BTreeSet},
    process::Stdio,
    time::Duration,
};
use yandalf_channels::schedule::Timing;

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{process::Command, time::timeout};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use yandalf_core::{
    channel::{Channel, Inbox},
    decision::{Decision, DecisionProvider},
    domain::{Event, now_ms},
};

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

pub struct GithubDecisions {
    sources: BTreeMap<String, Vec<ChangeKind>>,
}

impl GithubDecisions {
    pub fn new(repository: &str, on: Vec<ChangeKind>) -> Result<Self> {
        let mut sources = BTreeMap::new();
        sources.insert(format!("github:{}", canonical_repository(repository)?), on);
        Ok(Self { sources })
    }
}

#[async_trait]
impl DecisionProvider for GithubDecisions {
    async fn decide(&self, event: &Event) -> Result<Decision> {
        let Some(enabled) = self.sources.get(&event.source) else {
            return Ok(Decision::Run);
        };
        let pull = &event.context["pull"];
        if pull["state"] == "closed" {
            return Ok(Decision::Archive);
        }
        if pull["state"] != "open" {
            return Ok(Decision::Skip {
                reason: "Неизвестное состояние PR".into(),
            });
        }
        if pull["draft"] == true {
            return Ok(Decision::Skip {
                reason: "PR в draft".into(),
            });
        }
        let changes: Vec<Change> = serde_json::from_value(event.context["changes"].clone())
            .context("decode GitHub changes for decision")?;
        let viewer = event.context["viewer"]
            .as_str()
            .context("missing GitHub identity")?;
        for change in changes {
            if !enabled.contains(&change.kind) {
                continue;
            }
            if let Some(activity) = &change.activity {
                if activity.actor.is_empty()
                    || activity.actor.eq_ignore_ascii_case(viewer)
                    || activity.actor_type == "Bot"
                    || activity.actor.ends_with("[bot]")
                {
                    continue;
                }
                if change.kind == ChangeKind::ReviewRequested
                    && !activity.target.eq_ignore_ascii_case(viewer)
                {
                    continue;
                }
            }
            return Ok(Decision::Run);
        }
        Ok(Decision::Skip {
            reason: "Нет изменений, требующих turn по настроенному правилу".into(),
        })
    }
}

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
        let since = chrono::DateTime::from_timestamp_millis(cursor.saturating_sub(1000))
            .context("invalid GitHub activity cursor")?
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let mut dirty = BTreeSet::new();
        let mut failed = false;
        for (path, field) in [
            ("issues/comments", "issue_url"),
            ("pulls/comments", "pull_request_url"),
        ] {
            let result = async {
                let pages = api(&format!(
                    "repos/{}/{path}?sort=updated&direction=asc&since={since}&per_page=100",
                    self.repository
                ))
                .await?;
                for page in pages {
                    for entry in page
                        .as_array()
                        .context("expected GitHub comment feed array")?
                    {
                        if let Some(number) = entry[field]
                            .as_str()
                            .and_then(|url| url.rsplit('/').next())
                            .and_then(|number| number.parse::<u64>().ok())
                        {
                            dirty.insert(number);
                        }
                    }
                }
                anyhow::Ok(())
            }
            .await;
            if let Err(error) = result {
                failed = true;
                tracing::warn!(feed = path, error = %error, "GitHub comment feed refresh failed");
            }
        }
        let previous: BTreeMap<_, _> = inbox
            .last_events(&self.source)
            .await?
            .into_iter()
            .map(|event| (event.subject.clone(), event))
            .collect();
        let mut pulls = parse_pulls(
            &self.repository,
            api(&format!(
                "repos/{}/pulls?state=open&sort=created&direction=asc&per_page=100",
                self.repository
            ))
            .await?,
        )?;
        let present: BTreeSet<_> = pulls.iter().map(|pull| pull.number).collect();
        for event in previous.values() {
            let Some(number) = event.context["pr"].as_u64() else {
                continue;
            };
            if present.contains(&number) {
                continue;
            }
            if event.context["pull"]["state"] == "closed" {
                let pull: PullRequest = serde_json::from_value(event.context["pull"].clone())
                    .context("decode persisted closed pull request")?;
                ensure!(
                    pull.number == number,
                    "persisted pull request number mismatch"
                );
                pulls.push(pull);
                continue;
            }
            let was_open =
                event.context["pull"]["state"] == "open" || event.context["pull"].is_null();
            if !was_open {
                continue;
            }
            let result = async {
                let pages = api(&format!("repos/{}/pulls/{number}", self.repository)).await?;
                ensure!(pages.len() == 1, "unexpected pull request pagination");
                parse_pull(&self.repository, pages.into_iter().next().unwrap())
            }
            .await;
            match result {
                Ok(pull) => pulls.push(pull),
                Err(error) => {
                    failed = true;
                    tracing::warn!(pull_request = number, error = %error, "GitHub PR refresh failed");
                }
            }
        }
        let mut admitted = 0;
        for pull in &pulls {
            let previous = previous.get(&subject(&self.repository, pull.number));
            if pull.state == "closed" {
                let event = closed_event(&self.repository, pull)?;
                if previous.is_none_or(|previous| previous.id != event.id) {
                    let job = inbox.accept(&event).await?;
                    admitted += 1;
                    tracing::info!(repository = %self.repository, pull_request = pull.number, job_id = %job.id,
                        "GitHub session archive queued");
                }
                continue;
            }
            let serialized = serde_json::to_value(pull)?;
            if !dirty.contains(&pull.number)
                && previous.is_some_and(|event| {
                    event.context["pull"] == serialized && event.context["viewer"] == viewer
                })
            {
                continue;
            }
            let activity_result = async {
                let timeline = api(&format!(
                    "repos/{}/issues/{}/timeline?per_page=100",
                    self.repository, pull.number
                ))
                .await?;
                let comments = api(&format!(
                    "repos/{}/pulls/{}/comments?per_page=100",
                    self.repository, pull.number
                ))
                .await?;
                parse_activities(timeline, comments)
            }
            .await;
            let activities = match activity_result {
                Ok(activities) => activities,
                Err(error) => {
                    failed = true;
                    tracing::warn!(pull_request = pull.number, error = %error, "GitHub PR activity refresh failed");
                    continue;
                }
            };
            if let Some(mut event) = changed_event(
                &self.repository,
                pull,
                &activities,
                previous,
                viewer,
                watch_started_at,
                &pulls,
            )? {
                if let Some(instruction) = &self.instruction {
                    event.text.push_str("\n\n");
                    event.text.push_str(instruction);
                }
                let job = inbox.accept(&event).await?;
                admitted += 1;
                tracing::info!(repository = %self.repository, pull_request = pull.number, job_id = %job.id,
                    changes = event.context["changes"].as_array().map_or(0, Vec::len), "GitHub observation accepted");
            }
        }
        ensure!(
            !failed,
            "some GitHub PRs could not be refreshed; retaining activity cursor for retry"
        );
        inbox
            .advance_source_cursor(&cursor_source, poll_started_at)
            .await?;
        Ok((pulls.len(), admitted))
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
                result = async {
                    if viewer.is_none() {
                        let pages = api("user").await?;
                        let login = pages.first().and_then(|user| user["login"].as_str()).context("missing authenticated GitHub login")?;
                        ensure!(!login.is_empty(), "empty authenticated GitHub login");
                        viewer = Some(login.to_owned());
                    }
                    self.poll(&inbox, viewer.as_deref().unwrap(), watch_started_at).await
                } => result,
            };
            match result {
                Ok((observed, admitted)) => {
                    tracing::info!(repository = %self.repository, observed, admitted, "GitHub poll completed")
                }
                Err(error) => {
                    tracing::warn!(repository = %self.repository, error = %error, "GitHub poll failed; retrying on the next tick")
                }
            }
            let next = self.timing.next_tick(now_ms())?;
            loop {
                let delay = next.saturating_sub(now_ms()).max(0) as u64;
                tokio::select! {
                    _ = stop.cancelled() => return Ok(()),
                    _ = tokio::time::sleep(Duration::from_millis(delay.min(60_000))) => {},
                }
                if now_ms() >= next {
                    break;
                }
            }
        }
    }
}

async fn api(endpoint: &str) -> Result<Vec<Value>> {
    let output = timeout(
        Duration::from_secs(60),
        Command::new("gh")
            .args(["api", "--method", "GET", "--paginate", endpoint])
            .env("GH_PROMPT_DISABLED", "1")
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .context("GitHub request timed out")?
    .context("start gh api")?;
    ensure!(
        output.status.success(),
        "gh api failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    decode_pages(&output.stdout)
}

fn decode_pages(bytes: &[u8]) -> Result<Vec<Value>> {
    let pages = serde_json::Deserializer::from_slice(bytes)
        .into_iter::<Value>()
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("decode GitHub pages")?;
    ensure!(!pages.is_empty(), "GitHub returned no response pages");
    Ok(pages)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PullRequest {
    number: u64,
    url: String,
    title: String,
    body: String,
    state: String,
    draft: bool,
    base_sha: String,
    head_sha: String,
    base_ref: String,
    head_ref: String,
    updated_at: String,
    created_at: String,
    stack: Value,
}

#[derive(Deserialize)]
struct RawPullRequest {
    number: u64,
    html_url: String,
    title: String,
    state: String,
    draft: bool,
    #[serde(default)]
    body: Option<String>,
    #[serde(default)]
    updated_at: String,
    #[serde(default)]
    created_at: String,
    #[serde(default)]
    stack: Value,
    base: GitRef,
    head: GitRef,
}

#[derive(Deserialize)]
struct GitRef {
    sha: String,
    #[serde(rename = "ref")]
    name: String,
    repo: Option<Repository>,
}
#[derive(Deserialize)]
struct Repository {
    url: String,
}

fn parse_pulls(repository: &str, pages: Vec<Value>) -> Result<Vec<PullRequest>> {
    let mut pulls = Vec::new();
    let mut seen = BTreeSet::new();
    for page in pages {
        for raw in page
            .as_array()
            .context("expected GitHub pull request array")?
        {
            let pull = parse_pull(repository, raw.clone())?;
            ensure!(
                seen.insert(pull.number),
                "duplicate pull request during GitHub pagination"
            );
            pulls.push(pull);
        }
    }
    Ok(pulls)
}

fn parse_pull(repository: &str, raw: Value) -> Result<PullRequest> {
    let pull: RawPullRequest = serde_json::from_value(raw).context("decode GitHub pull request")?;
    ensure!(pull.number > 0, "invalid pull request number");
    ensure!(
        pull.base.repo.as_ref().is_some_and(|repo| repo
            .url
            .eq_ignore_ascii_case(&format!("https://api.github.com/repos/{repository}"))),
        "GitHub returned a different base repository"
    );
    ensure!(
        pull.html_url.eq_ignore_ascii_case(&format!(
            "https://github.com/{repository}/pull/{}",
            pull.number
        )),
        "GitHub returned a different pull request URL"
    );
    validate_sha(&pull.base.sha)?;
    validate_sha(&pull.head.sha)?;
    Ok(PullRequest {
        number: pull.number,
        url: pull.html_url,
        title: pull.title,
        body: pull.body.unwrap_or_default(),
        state: pull.state,
        draft: pull.draft,
        base_sha: pull.base.sha.to_ascii_lowercase(),
        head_sha: pull.head.sha.to_ascii_lowercase(),
        base_ref: pull.base.name,
        head_ref: pull.head.name,
        updated_at: pull.updated_at,
        created_at: pull.created_at,
        stack: pull.stack,
    })
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Activity {
    id: String,
    kind: ChangeKind,
    actor: String,
    actor_type: String,
    target: String,
    body: String,
    state: String,
    updated_at: String,
    url: String,
    path: Value,
    line: Value,
    in_reply_to_id: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Change {
    kind: ChangeKind,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    activity: Option<Activity>,
}

fn parse_activities(timeline: Vec<Value>, comments: Vec<Value>) -> Result<Vec<Activity>> {
    let mut result = BTreeMap::new();
    for (pages, inline) in [(timeline, false), (comments, true)] {
        for page in pages {
            for entry in page.as_array().context("expected GitHub activity array")? {
                let kind = if inline {
                    ChangeKind::Commented
                } else {
                    match entry["event"].as_str() {
                        Some("commented") => ChangeKind::Commented,
                        Some("reviewed") => ChangeKind::Reviewed,
                        Some("review_requested") => ChangeKind::ReviewRequested,
                        _ => continue,
                    }
                };
                let number = entry["id"].as_u64().context("missing GitHub activity id")?;
                let id = format!(
                    "{}:{number}",
                    if inline {
                        "inline"
                    } else {
                        entry["event"].as_str().unwrap()
                    }
                );
                let actor = if entry["user"].is_object() {
                    &entry["user"]
                } else {
                    &entry["actor"]
                };
                result.insert(
                    id.clone(),
                    Activity {
                        id,
                        kind,
                        actor: actor["login"].as_str().unwrap_or_default().into(),
                        actor_type: actor["type"].as_str().unwrap_or_default().into(),
                        target: entry["requested_reviewer"]["login"]
                            .as_str()
                            .unwrap_or_default()
                            .into(),
                        body: entry["body"].as_str().unwrap_or_default().into(),
                        state: entry["state"].as_str().unwrap_or_default().into(),
                        updated_at: entry["updated_at"]
                            .as_str()
                            .or_else(|| entry["created_at"].as_str())
                            .or_else(|| entry["submitted_at"].as_str())
                            .unwrap_or_default()
                            .into(),
                        url: entry["html_url"]
                            .as_str()
                            .or_else(|| entry["url"].as_str())
                            .unwrap_or_default()
                            .into(),
                        path: entry["path"].clone(),
                        line: entry["line"].clone(),
                        in_reply_to_id: entry["in_reply_to_id"].clone(),
                    },
                );
            }
        }
    }
    Ok(result.into_values().collect())
}

fn subject(repository: &str, number: u64) -> String {
    format!("github:{repository}/pr/{number}")
}

fn fingerprint(value: &impl Serialize) -> Result<String> {
    let mut value = serde_json::to_value(value)?;
    value.sort_all_objects();
    Ok(Uuid::new_v5(&Uuid::NAMESPACE_URL, &serde_json::to_vec(&value)?).to_string())
}

fn closed_event(repository: &str, pull: &PullRequest) -> Result<Event> {
    let title = format!("PR #{} — {}", pull.number, pull.title);
    let mut event = Event {
        id: String::new(),
        source: format!("github:{repository}"),
        subject: subject(repository, pull.number),
        title: Some(title.clone()),
        text: format!("{title}\n{}\n\nPR закрыт; архивировать сессию.", pull.url),
        context: json!({"repository":repository,"pr":pull.number,"url":pull.url,
            "base_sha":pull.base_sha,"head_sha":pull.head_sha,"pull":pull}),
    };
    event.id = fingerprint(&event)?;
    Ok(event)
}

fn changed_event(
    repository: &str,
    pull: &PullRequest,
    activities: &[Activity],
    previous: Option<&Event>,
    viewer: &str,
    watch_started_at: i64,
    all_pulls: &[PullRequest],
) -> Result<Option<Event>> {
    if pull.state == "closed" {
        let event = closed_event(repository, pull)?;
        return Ok(previous
            .is_none_or(|previous| previous.id != event.id)
            .then_some(event));
    }
    let activity: BTreeMap<_, _> = activities
        .iter()
        .map(|item| Ok((item.id.clone(), fingerprint(item)?)))
        .collect::<Result<_>>()?;
    let serialized = serde_json::to_value(pull)?;
    if previous.is_some_and(|event| {
        event.context["pull"] == serialized
            && event.context["activity"] == json!(activity)
            && event.context["viewer"] == viewer
    }) {
        return Ok(None);
    }
    let mut changes = Vec::new();
    let mut add = |kind| {
        changes.push(Change {
            kind,
            activity: None,
        })
    };
    if let Some(previous) = previous {
        if previous.context["base_sha"] != pull.base_sha
            || previous.context["head_sha"] != pull.head_sha
        {
            add(ChangeKind::Revision);
        }
        if let Some(old) = previous.context["pull"].as_object() {
            if old.get("draft") == Some(&json!(true)) && !pull.draft {
                add(ChangeKind::ReadyForReview);
            }
            if old.get("state") == Some(&json!("closed")) && pull.state == "open" {
                add(ChangeKind::Reopened);
            }
            if old.get("title") != Some(&json!(pull.title))
                || old.get("body") != Some(&json!(pull.body))
                || old.get("base_ref") != Some(&json!(pull.base_ref))
                || old.get("head_ref") != Some(&json!(pull.head_ref))
            {
                add(ChangeKind::Edited);
            }
        }
    } else if chrono::DateTime::parse_from_rfc3339(&pull.created_at)
        .is_ok_and(|created| created.timestamp() >= watch_started_at.div_euclid(1000))
    {
        add(ChangeKind::Opened);
    }
    let previous_activity = previous.and_then(|event| event.context["activity"].as_object());
    for item in activities {
        let changed = if let Some(old) = previous_activity {
            old.get(&item.id) != activity.get(&item.id).map(|hash| json!(hash)).as_ref()
        } else {
            chrono::DateTime::parse_from_rfc3339(&item.updated_at)
                .is_ok_and(|updated| updated.timestamp() >= watch_started_at.div_euclid(1000))
        };
        if changed {
            changes.push(Change {
                kind: item.kind,
                activity: Some(item.clone()),
            });
        }
    }
    let source = format!("github:{repository}");
    let subject = subject(repository, pull.number);
    let revision = json!({"base_sha":pull.base_sha,"head_sha":pull.head_sha});
    let previous_revision = previous.map(
        |event| json!({"base_sha":event.context["base_sha"],"head_sha":event.context["head_sha"]}),
    );
    let id = fingerprint(
        &json!({"source":source,"subject":subject,"previous":previous.map(|event| &event.id),"pull":serialized,"activity":activity,"viewer":viewer}),
    )?;
    let stack = pull.stack["number"].as_u64().map(|number| {
        let mut members: Vec<_> = all_pulls.iter().filter(|member| member.stack["number"] == number).collect();
        members.sort_by_key(|member| member.stack["position"].as_u64());
        let summaries: Vec<_> = members.iter().map(|member| json!({
            "number":member.number,"url":member.url,"title":member.title,"state":member.state,"draft":member.draft,
            "base_sha":member.base_sha,"head_sha":member.head_sha,"base_ref":member.base_ref,"head_ref":member.head_ref,
            "position":member.stack["position"]
        })).collect();
        json!({"number":number,"base":pull.stack["base"],"size":pull.stack["size"],"complete":pull.stack["size"].as_u64() == Some(members.len() as u64),"pull_requests":summaries})
    });
    let title = format!("PR #{} — {}", pull.number, pull.title);
    let mut summaries = Vec::new();
    for change in &changes {
        let summary = match change.kind {
            ChangeKind::Opened => "открыт новый PR",
            ChangeKind::Revision => "обновлены коммиты PR или его базы",
            ChangeKind::ReadyForReview => "PR переведён из draft в готовый к ревью",
            ChangeKind::Reopened => "PR открыт повторно",
            ChangeKind::Edited => "изменены название, описание или базовая ветка PR",
            ChangeKind::ReviewRequested => "запрошено ревью",
            ChangeKind::Commented => "добавлен или изменён комментарий",
            ChangeKind::Reviewed => "добавлено или изменено ревью",
        };
        if !summaries.contains(&summary) {
            summaries.push(summary);
        }
    }
    let summary = if summaries.is_empty() {
        "снимок PR без изменений для запуска".to_owned()
    } else {
        summaries.join("; ")
    };
    let text = format!(
        "{title}\n{}\n\nРепозиторий: {repository}\nВетка PR: {} ({})\nБаза: {} ({})\nИзменения: {summary}.",
        pull.url,
        pull.head_ref,
        &pull.head_sha[..12],
        pull.base_ref,
        &pull.base_sha[..12]
    );
    Ok(Some(Event {
        id,
        source,
        subject,
        title: Some(title),
        text,
        context: json!({"repository":repository,"pr":pull.number,"url":pull.url,"base_sha":pull.base_sha,"head_sha":pull.head_sha,
            "previous_revision":previous_revision,"revision":revision,"pull":serialized,"activity":activity,"changes":changes,
            "viewer":viewer,"watch_started_at":watch_started_at,"stack":stack}),
    }))
}

fn validate_sha(sha: &str) -> Result<()> {
    ensure!(
        sha.len() == 40 && sha.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "invalid GitHub revision SHA"
    );
    Ok(())
}

fn canonical_repository(repository: &str) -> Result<String> {
    let parts: Vec<_> = repository.split('/').collect();
    ensure!(parts.len() == 2, "GitHub repository must be owner/name");
    ensure!(
        !parts[0].is_empty()
            && parts[0].len() <= 39
            && !parts[0].starts_with('-')
            && !parts[0].ends_with('-')
            && parts[0]
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'),
        "invalid GitHub repository owner"
    );
    ensure!(
        !parts[1].is_empty()
            && parts[1].len() <= 100
            && parts[1] != "."
            && parts[1] != ".."
            && parts[1]
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte)),
        "invalid GitHub repository name"
    );
    Ok(repository.to_ascii_lowercase())
}

impl yandalf_core::plugin::Plugin for Github {
    fn name(&self) -> &str {
        self.source()
    }
    fn register(&self, ctx: &mut yandalf_core::plugin::PluginContext) -> Result<()> {
        use std::sync::Arc;
        use yandalf_core::{
            channel::ChannelPlugin, decision::DecisionProvider, store::Store, task::TaskDeciding,
        };
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
                let store = services.get::<dyn Store>()?;
                if let Some(latest) = store.last_event(&source, &task.job.event.subject).await?
                    && latest.id != task.job.event.id
                    && ((task.job.event.context["pull"]["state"] == "closed"
                        && latest.context["pull"]["state"] != "closed")
                        || latest.context["pull"]["state"] == "closed"
                        || latest.context["pull"]["draft"] == true
                        || (latest.context["pull"]["head_ref"].is_string()
                            && task.job.event.context["pull"]["head_ref"].is_string()
                            && latest.context["pull"]["head_ref"]
                                != task.job.event.context["pull"]["head_ref"])
                        || ["base_sha", "head_sha"].iter().any(|key| {
                            latest.context[*key].is_string()
                                && task.job.event.context[*key].is_string()
                                && latest.context[*key] != task.job.event.context[*key]
                        }))
                {
                    task.decision = crate::Decision::Skip {
                        reason: "Наблюдение заменено более новым состоянием PR".into(),
                    };
                    return Ok(());
                }
                let decision = policy.decide(&task.job.event).await?;
                if !matches!(decision, crate::Decision::Run) {
                    task.decision = decision;
                }
                Ok(())
            })
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const REPOSITORY: &str = "ydb-platform/ydb-js-sdk";
    const VIEWER: &str = "YandalfRed";
    const START: &str = "2026-11-04T00:00:00Z";

    fn watch_started() -> i64 {
        chrono::DateTime::parse_from_rfc3339(START)
            .unwrap()
            .timestamp_millis()
    }

    fn raw_pull(number: u64) -> Value {
        json!({
            "number": number,
            "html_url": format!("https://github.com/{REPOSITORY}/pull/{number}"),
            "title": "Handle cancellation",
            "body": "Preserve the caller's cancellation reason.",
            "state": "open", "draft": false,
            "created_at": "2026-11-03T10:00:00Z", "updated_at": "2026-11-03T12:00:00Z",
            "base": {"sha": "a".repeat(40), "ref": "main", "repo": {"url": format!("https://api.github.com/repos/{REPOSITORY}")}},
            "head": {"sha": "b".repeat(40), "ref": "fix-cancellation", "repo": null}
        })
    }

    fn pull(number: u64) -> PullRequest {
        parse_pull(REPOSITORY, raw_pull(number)).unwrap()
    }

    fn observation(pull: &PullRequest, activity: &[Activity], previous: Option<&Event>) -> Event {
        changed_event(
            REPOSITORY,
            pull,
            activity,
            previous,
            VIEWER,
            watch_started(),
            std::slice::from_ref(pull),
        )
        .unwrap()
        .unwrap()
    }

    fn changes(event: &Event) -> Vec<ChangeKind> {
        serde_json::from_value::<Vec<Change>>(event.context["changes"].clone())
            .unwrap()
            .into_iter()
            .map(|change| change.kind)
            .collect()
    }

    #[test]
    fn decodes_paginated_json_without_requiring_gh_slurp_support() {
        assert_eq!(
            decode_pages(
                br#"[{"number":1}]
[{"number":2}]"#
            )
            .unwrap(),
            vec![json!([{"number":1}]), json!([{"number":2}])]
        );
        assert_eq!(
            decode_pages(br#"{"login":"bot"}"#).unwrap(),
            vec![json!({"login":"bot"})]
        );
        assert_eq!(decode_pages(b"[]").unwrap(), vec![json!([])]);
        assert!(decode_pages(b" ").is_err());
        assert!(decode_pages(b"[] [{").is_err());
    }

    fn policy(on: Vec<ChangeKind>) -> GithubDecisions {
        GithubDecisions::new(REPOSITORY, on).unwrap()
    }

    async fn should_run(event: &Event) -> bool {
        policy(ChangeKind::defaults()).decide(event).await.unwrap() == Decision::Run
    }

    fn activity(event: &str, actor: &str, actor_type: &str, target: &str) -> Vec<Activity> {
        parse_activities(vec![json!([{
            "id": 42, "event": event, "actor": {"login": actor, "type": actor_type},
            "requested_reviewer": {"login": target}, "body": "Please recheck cancellation.",
            "created_at": "2026-11-04T01:00:00Z", "html_url": "https://github.com/example/activity/42"
        }])], vec![json!([])]).unwrap()
    }

    #[test]
    fn new_pr_message_starts_with_its_identity_and_revision() {
        let mut pull = pull(800);
        pull.created_at = "2026-11-04T00:01:00Z".into();
        let event = observation(&pull, &[], None);
        assert_eq!(
            event.title.as_deref(),
            Some("PR #800 — Handle cancellation")
        );
        assert!(event.text.starts_with(
            "PR #800 — Handle cancellation\nhttps://github.com/ydb-platform/ydb-js-sdk/pull/800\n"
        ));
        assert!(event.text.contains("Репозиторий: ydb-platform/ydb-js-sdk"));
        assert!(
            event
                .text
                .contains("Ветка PR: fix-cancellation (bbbbbbbbbbbb)")
        );
        assert!(event.text.contains("База: main (aaaaaaaaaaaa)"));
        assert!(event.text.ends_with("Изменения: открыт новый PR."));
        assert_eq!(event.context["pull"]["body"], pull.body);
    }

    #[test]
    fn updated_pr_message_identifies_code_changes_and_discussion() {
        let original = pull(715);
        let previous = observation(&original, &[], None);
        let mut pull = original;
        pull.head_sha = "c".repeat(40);
        pull.title = "Keep the cancellation reason".into();
        let comments = activity("commented", "polRk", "User", "");
        let event = observation(&pull, &comments, Some(&previous));
        assert_eq!(
            event.title.as_deref(),
            Some("PR #715 — Keep the cancellation reason")
        );
        assert!(
            event
                .text
                .starts_with("PR #715 — Keep the cancellation reason\n")
        );
        assert!(
            event
                .text
                .contains("Ветка PR: fix-cancellation (cccccccccccc)")
        );
        assert!(event.text.contains("обновлены коммиты PR или его базы"));
        assert!(event.text.contains("добавлен или изменён комментарий"));
        assert!(!event.text.contains("открыт новый PR"));
    }

    #[tokio::test]
    async fn existing_pr_is_baselined_but_new_pr_triggers_opened() {
        let old = pull(715);
        let mut old_comments = activity("commented", "polRk", "User", "");
        old_comments[0].updated_at = "2026-11-03T23:00:00Z".into();
        let initial = observation(&old, &old_comments, None);
        assert!(changes(&initial).is_empty());
        assert!(!should_run(&initial).await);

        let fresh_comment = activity("commented", "polRk", "User", "");
        let during_bootstrap = observation(&old, &fresh_comment, None);
        assert_eq!(changes(&during_bootstrap), [ChangeKind::Commented]);
        assert!(should_run(&during_bootstrap).await);

        let mut created = pull(800);
        created.created_at = "2026-11-04T00:01:00Z".into();
        let initial = observation(&created, &[], None);
        assert_eq!(changes(&initial), [ChangeKind::Opened]);
        assert!(should_run(&initial).await);
    }

    #[tokio::test]
    async fn initial_events_in_watch_start_second_are_not_lost_to_timestamp_precision() {
        let mut created = pull(800);
        created.created_at = START.into();
        let event = changed_event(
            REPOSITORY,
            &created,
            &[],
            None,
            VIEWER,
            watch_started() + 500,
            std::slice::from_ref(&created),
        )
        .unwrap()
        .unwrap();
        assert_eq!(changes(&event), [ChangeKind::Opened]);
        assert!(should_run(&event).await);

        let old = pull(715);
        let mut comment = activity("commented", "polRk", "User", "");
        comment[0].updated_at = START.into();
        let event = changed_event(
            REPOSITORY,
            &old,
            &comment,
            None,
            VIEWER,
            watch_started() + 500,
            std::slice::from_ref(&old),
        )
        .unwrap()
        .unwrap();
        assert_eq!(changes(&event), [ChangeKind::Commented]);
        assert!(should_run(&event).await);
    }

    #[tokio::test]
    async fn changed_revision_runs_and_identical_snapshot_does_not_emit_again() {
        let initial_pull = pull(715);
        let initial = observation(&initial_pull, &[], None);
        let mut updated = initial_pull.clone();
        updated.head_sha = "c".repeat(40);
        let event = observation(&updated, &[], Some(&initial));
        assert_eq!(changes(&event), [ChangeKind::Revision]);
        assert!(should_run(&event).await);
        assert_eq!(event.subject, initial.subject);
        assert_eq!(
            event.context["previous_revision"]["head_sha"],
            initial_pull.head_sha
        );
        assert!(
            changed_event(
                REPOSITORY,
                &updated,
                &[],
                Some(&event),
                VIEWER,
                watch_started(),
                &[updated.clone()]
            )
            .unwrap()
            .is_none()
        );

        let returned = observation(&initial_pull, &[], Some(&event));
        assert!(should_run(&returned).await);
        assert_ne!(returned.id, initial.id);
        assert_eq!(
            returned.id,
            observation(&initial_pull, &[], Some(&event)).id
        );
    }

    #[tokio::test]
    async fn base_revision_and_title_or_body_edits_trigger_independently() {
        let original = pull(715);
        let initial = observation(&original, &[], None);
        let mut rebase = original.clone();
        rebase.base_sha = "d".repeat(40);
        assert_eq!(
            changes(&observation(&rebase, &[], Some(&initial))),
            [ChangeKind::Revision]
        );
        for field in ["title", "body", "base_ref", "head_ref"] {
            let mut edited = original.clone();
            match field {
                "title" => edited.title = "Changed cancellation contract".into(),
                "body" => edited.body = "The caller now retains the original reason.".into(),
                "base_ref" => edited.base_ref = "release".into(),
                _ => edited.head_ref = "renamed-cancellation".into(),
            }
            let event = observation(&edited, &[], Some(&initial));
            assert_eq!(changes(&event), [ChangeKind::Edited]);
            assert!(should_run(&event).await);
        }
    }

    #[tokio::test]
    async fn drafts_skip_and_closed_prs_archive_but_ready_and_reopened_prs_run() {
        let original = pull(715);
        let initial = observation(&original, &[], None);
        let mut draft = original.clone();
        draft.draft = true;
        draft.head_sha = "c".repeat(40);
        let draft_event = observation(&draft, &[], Some(&initial));
        assert!(!should_run(&draft_event).await);
        draft.draft = false;
        let ready = observation(&draft, &[], Some(&draft_event));
        assert_eq!(changes(&ready), [ChangeKind::ReadyForReview]);
        assert!(should_run(&ready).await);

        let mut closed = original.clone();
        closed.state = "closed".into();
        closed.head_sha = "e".repeat(40);
        let closed_event = observation(&closed, &[], Some(&initial));
        assert!(!should_run(&closed_event).await);
        closed.state = "open".into();
        let reopened = observation(&closed, &[], Some(&closed_event));
        assert_eq!(changes(&reopened), [ChangeKind::Reopened]);
        assert!(should_run(&reopened).await);
    }

    #[tokio::test]
    async fn closure_archives_without_review_triggers_or_activity_context() {
        let mut closed = pull(715);
        closed.state = "closed".into();
        closed.draft = true;
        let event = closed_event(REPOSITORY, &closed).unwrap();
        assert_eq!(
            policy(Vec::new()).decide(&event).await.unwrap(),
            Decision::Archive
        );
    }

    #[test]
    fn closure_identity_survives_restart_and_reclosing_creates_another_event() {
        let mut closed = pull(715);
        closed.state = "closed".into();
        let mut legacy = closed_event(REPOSITORY, &closed).unwrap();
        legacy.id = "old-skipped-observation".into();
        legacy.context["viewer"] = VIEWER.into();
        let upgraded = observation(&closed, &[], Some(&legacy));
        let restored: Event =
            serde_json::from_str(&serde_json::to_string(&upgraded).unwrap()).unwrap();
        let restored_pull = serde_json::from_value(restored.context["pull"].clone()).unwrap();
        assert!(
            changed_event(
                REPOSITORY,
                &restored_pull,
                &[],
                Some(&restored),
                "another-account",
                now_ms(),
                &[]
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(
            serde_json::to_value(closed_event(REPOSITORY, &restored_pull).unwrap()).unwrap(),
            serde_json::to_value(&restored).unwrap()
        );
        closed.updated_at = "2026-11-05T00:00:00Z".into();
        assert_ne!(closed_event(REPOSITORY, &closed).unwrap().id, restored.id);
    }

    #[tokio::test]
    async fn review_requests_require_human_actor_and_target_current_bot() {
        let pull = pull(715);
        let initial = observation(&pull, &[], None);
        for (actor, actor_type, target, expected) in [
            ("polRk", "User", VIEWER, true),
            ("polRk", "User", "another-reviewer", false),
            ("yandalfred", "User", VIEWER, false),
            ("helper", "Bot", VIEWER, false),
            ("helper[bot]", "User", VIEWER, false),
            ("", "User", VIEWER, false),
        ] {
            let events = activity("review_requested", actor, actor_type, target);
            let event = observation(&pull, &events, Some(&initial));
            assert_eq!(changes(&event), [ChangeKind::ReviewRequested]);
            assert_eq!(
                should_run(&event).await,
                expected,
                "actor={actor}, target={target}"
            );
        }
    }

    #[tokio::test]
    async fn human_reviews_comments_and_edited_inline_replies_trigger() {
        let pull = pull(715);
        let initial = observation(&pull, &[], None);
        for kind in ["reviewed", "commented"] {
            let events = activity(kind, "polRk", "User", "");
            assert!(should_run(&observation(&pull, &events, Some(&initial))).await);
        }
        let inline = json!({
            "id": 15, "user": {"login": "polRk", "type": "User"},
            "body": "Fixed the cancellation path.", "path": "src/query.ts", "line": 12,
            "in_reply_to_id": 8, "created_at": "2026-11-04T01:00:00Z",
            "updated_at": "2026-11-04T01:00:00Z", "html_url": "https://github.com/example/pull/715#discussion_r15"
        });
        let activities = parse_activities(vec![json!([])], vec![json!([inline.clone()])]).unwrap();
        let reply = observation(&pull, &activities, Some(&initial));
        assert!(should_run(&reply).await);
        assert_eq!(reply.context["changes"][0]["activity"]["in_reply_to_id"], 8);
        assert_eq!(
            reply.context["changes"][0]["activity"]["path"],
            "src/query.ts"
        );

        let mut edited = inline;
        edited["body"] = "The fix is in the next commit; please check it.".into();
        edited["updated_at"] = "2026-11-04T02:00:00Z".into();
        let activities = parse_activities(vec![json!([])], vec![json!([edited])]).unwrap();
        assert!(should_run(&observation(&pull, &activities, Some(&reply))).await);
    }

    #[tokio::test]
    async fn own_activity_and_timestamp_only_updates_are_persisted_without_feedback_turns() {
        let mut pull = pull(715);
        let initial = observation(&pull, &[], None);
        for kind in ["reviewed", "commented"] {
            let own_activity = activity(kind, VIEWER, "User", "");
            pull.updated_at = "2026-11-04T02:00:00Z".into();
            let ignored = observation(&pull, &own_activity, Some(&initial));
            assert!(!should_run(&ignored).await);
            assert!(
                changed_event(
                    REPOSITORY,
                    &pull,
                    &own_activity,
                    Some(&ignored),
                    VIEWER,
                    watch_started(),
                    &[pull.clone()]
                )
                .unwrap()
                .is_none()
            );
        }
        let timestamp = observation(&pull, &[], Some(&initial));
        assert!(changes(&timestamp).is_empty());
        assert!(!should_run(&timestamp).await);
    }

    #[tokio::test]
    async fn configured_trigger_set_filters_changes_but_does_not_affect_other_sources() {
        let pull = pull(715);
        let initial = observation(&pull, &[], None);
        let activities = activity("commented", "polRk", "User", "");
        let comment = observation(&pull, &activities, Some(&initial));
        let revisions_only = policy(vec![ChangeKind::Revision]);
        assert!(matches!(
            revisions_only.decide(&comment).await.unwrap(),
            Decision::Skip { .. }
        ));
        let mut external = comment;
        external.source = "schedule:example".into();
        assert_eq!(
            revisions_only.decide(&external).await.unwrap(),
            Decision::Run
        );
    }

    #[tokio::test]
    async fn stack_metadata_is_context_not_a_trigger_or_subject_partition() {
        let original = pull(715);
        let initial = observation(&original, &[], None);
        let mut stacked = original;
        stacked.stack = json!({"number": 730, "position": 2, "size": 2, "base": "main"});
        let mut lower = pull(721);
        lower.stack = json!({"number": 730, "position": 1, "size": 2, "base": "main"});
        let event = changed_event(
            REPOSITORY,
            &stacked,
            &[],
            Some(&initial),
            VIEWER,
            watch_started(),
            &[stacked.clone(), lower],
        )
        .unwrap()
        .unwrap();
        assert_eq!(event.subject, initial.subject);
        assert!(changes(&event).is_empty());
        assert!(!should_run(&event).await);
        assert_eq!(event.context["stack"]["pull_requests"][0]["number"], 721);
        assert_eq!(event.context["stack"]["pull_requests"][1]["number"], 715);
    }

    #[test]
    fn repository_pages_are_complete_and_reject_duplicate_or_foreign_prs() {
        let parsed = parse_pulls(
            REPOSITORY,
            vec![json!([raw_pull(715)]), json!([raw_pull(800)])],
        )
        .unwrap();
        assert_eq!(
            parsed.iter().map(|pull| pull.number).collect::<Vec<_>>(),
            [715, 800]
        );
        assert!(
            parse_pulls(
                REPOSITORY,
                vec![json!([raw_pull(715)]), json!([raw_pull(715)])]
            )
            .is_err()
        );
        let mut foreign = raw_pull(715);
        foreign["base"]["repo"]["url"] = "https://api.github.com/elsewhere/another".into();
        assert!(parse_pull(REPOSITORY, foreign).is_err());
        let mut foreign_url = raw_pull(715);
        foreign_url["html_url"] = "https://github.com/elsewhere/another/pull/715".into();
        assert!(parse_pull(REPOSITORY, foreign_url).is_err());
        let mut truncated = raw_pull(715);
        truncated["head"]["sha"] = "1234567".into();
        assert!(parse_pull(REPOSITORY, truncated).is_err());
        assert_eq!(
            canonical_repository("YDB-Platform/YDB-JS-SDK").unwrap(),
            REPOSITORY
        );
        for invalid in [
            "../repo",
            "owner/repo?state=all",
            "owner/../repo",
            "https://github.com/owner/repo",
        ] {
            assert!(canonical_repository(invalid).is_err());
        }
    }

    #[test]
    fn activity_pages_preserve_inline_and_timeline_ids_without_collision() {
        let timeline = vec![
            json!([{"id": 7, "event": "commented", "user": {"login": "author", "type": "User"}, "body": "Question"}]),
            json!([{"id": 8, "event": "reviewed", "user": {"login": "reviewer", "type": "User"}, "body": "Found a bug", "state": "changes_requested"}]),
        ];
        let inline = vec![
            json!([{"id": 7, "user": {"login": "author", "type": "User"}, "body": "Fixed", "in_reply_to_id": 3}]),
        ];
        let activities = parse_activities(timeline, inline).unwrap();
        assert_eq!(activities.len(), 3);
        assert!(
            activities
                .iter()
                .any(|entry| entry.id == "commented:7" && entry.actor == "author")
        );
        assert!(
            activities
                .iter()
                .any(|entry| entry.id == "inline:7" && entry.in_reply_to_id == 3)
        );
        assert!(
            activities
                .iter()
                .any(|entry| entry.id == "reviewed:8" && entry.state == "changes_requested")
        );
    }
}
