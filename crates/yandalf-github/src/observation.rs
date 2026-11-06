use std::collections::BTreeMap;

use anyhow::Result;
use serde::Serialize;
use serde_json::{Value, json};
use uuid::Uuid;
use yandalf_core::domain::Event;

use crate::model::{Activity, Change, ChangeKind, GithubContext, PullRequest};

pub(crate) fn subject(repository: &str, number: u64) -> String {
    format!("github:{repository}/pr/{number}")
}

fn fingerprint(value: &impl Serialize) -> Result<String> {
    let mut value = serde_json::to_value(value)?;
    value.sort_all_objects();
    Ok(Uuid::new_v5(&Uuid::NAMESPACE_URL, &serde_json::to_vec(&value)?).to_string())
}

pub(crate) fn closed_event(repository: &str, pull: &PullRequest) -> Result<Event> {
    let title = title(pull);
    let mut event = Event {
        id: String::new(),
        source: format!("github:{repository}"),
        subject: subject(repository, pull.number),
        title: Some(title.clone()),
        text: format!("{title}\n{}\n\nPR закрыт; архивировать сессию.", pull.url),
        context: json!({
            "repository": repository,
            "pr": pull.number,
            "url": pull.url,
            "base_sha": pull.base_sha,
            "head_sha": pull.head_sha,
            "pull": pull
        }),
    };
    event.id = fingerprint(&event)?;
    Ok(event)
}

pub(crate) fn changed_event(
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
    let previous_context = previous.map(GithubContext::decode).transpose()?;
    let activity = activity_fingerprints(activities)?;
    if let Some(previous) = &previous_context
        && previous.matches_pull(pull)?
        && previous.activity.as_ref() == Some(&activity)
        && previous.viewer.as_deref() == Some(viewer)
    {
        return Ok(None);
    }
    let changes = changes(
        pull,
        activities,
        previous_context.as_ref(),
        &activity,
        watch_started_at,
    );
    let source = format!("github:{repository}");
    let subject = subject(repository, pull.number);
    let id = fingerprint(&json!({
        "source": source,
        "subject": subject,
        "previous": previous.map(|event| &event.id),
        "pull": pull,
        "activity": activity,
        "viewer": viewer
    }))?;
    let previous_revision = previous_context.as_ref().map(|previous| {
        json!({
            "base_sha": previous.base_sha,
            "head_sha": previous.head_sha
        })
    });
    Ok(Some(Event {
        id,
        source,
        subject,
        title: Some(title(pull)),
        text: message(repository, pull, &changes),
        context: json!({
            "repository": repository,
            "pr": pull.number,
            "url": pull.url,
            "base_sha": pull.base_sha,
            "head_sha": pull.head_sha,
            "previous_revision": previous_revision,
            "revision": {
                "base_sha": pull.base_sha,
                "head_sha": pull.head_sha
            },
            "pull": pull,
            "activity": activity,
            "changes": changes,
            "viewer": viewer,
            "watch_started_at": watch_started_at,
            "stack": stack_context(pull, all_pulls)
        }),
    }))
}

fn activity_fingerprints(activities: &[Activity]) -> Result<BTreeMap<String, String>> {
    activities
        .iter()
        .map(|item| Ok((item.id.clone(), fingerprint(item)?)))
        .collect()
}

fn changes(
    pull: &PullRequest,
    activities: &[Activity],
    previous: Option<&GithubContext>,
    activity: &BTreeMap<String, String>,
    watch_started_at: i64,
) -> Vec<Change> {
    let mut changes = pull_changes(pull, previous, watch_started_at);
    let previous_activity = previous.and_then(|context| context.activity.as_ref());
    for item in activities {
        let changed = match previous_activity {
            Some(old) => old.get(&item.id) != activity.get(&item.id),
            None => observed_since(&item.updated_at, watch_started_at),
        };
        if changed {
            changes.push(Change {
                kind: item.kind,
                activity: Some(item.clone()),
            });
        }
    }
    changes
}

fn pull_changes(
    pull: &PullRequest,
    previous: Option<&GithubContext>,
    watch_started_at: i64,
) -> Vec<Change> {
    let mut changes = Vec::new();
    let mut add = |kind| {
        changes.push(Change {
            kind,
            activity: None,
        });
    };
    if let Some(previous) = previous {
        if previous.base_sha.as_deref() != Some(&pull.base_sha)
            || previous.head_sha.as_deref() != Some(&pull.head_sha)
        {
            add(ChangeKind::Revision);
        }
        if let Some(old) = &previous.pull {
            if old.draft == Some(true) && !pull.draft {
                add(ChangeKind::ReadyForReview);
            }
            if old.state.as_deref() == Some("closed") && pull.state == "open" {
                add(ChangeKind::Reopened);
            }
            if old.title.as_deref() != Some(&pull.title)
                || old.body.as_deref() != Some(&pull.body)
                || old.base_ref.as_deref() != Some(&pull.base_ref)
                || old.head_ref.as_deref() != Some(&pull.head_ref)
            {
                add(ChangeKind::Edited);
            }
        }
    } else if observed_since(&pull.created_at, watch_started_at) {
        add(ChangeKind::Opened);
    }
    changes
}

fn observed_since(timestamp: &str, watch_started_at: i64) -> bool {
    chrono::DateTime::parse_from_rfc3339(timestamp)
        .is_ok_and(|time| time.timestamp() >= watch_started_at.div_euclid(1000))
}

fn stack_context(pull: &PullRequest, all_pulls: &[PullRequest]) -> Option<Value> {
    let number = pull.stack["number"].as_u64()?;
    let mut members: Vec<_> = all_pulls
        .iter()
        .filter(|member| member.stack["number"] == number)
        .collect();
    members.sort_by_key(|member| member.stack["position"].as_u64());
    let summaries: Vec<_> = members
        .iter()
        .map(|member| {
            json!({
                "number": member.number,
                "url": member.url,
                "title": member.title,
                "state": member.state,
                "draft": member.draft,
                "base_sha": member.base_sha,
                "head_sha": member.head_sha,
                "base_ref": member.base_ref,
                "head_ref": member.head_ref,
                "position": member.stack["position"]
            })
        })
        .collect();
    Some(json!({
        "number": number,
        "base": pull.stack["base"],
        "size": pull.stack["size"],
        "complete": pull.stack["size"].as_u64() == Some(members.len() as u64),
        "pull_requests": summaries
    }))
}

fn title(pull: &PullRequest) -> String {
    format!("PR #{} — {}", pull.number, pull.title)
}

fn message(repository: &str, pull: &PullRequest, changes: &[Change]) -> String {
    let mut summaries = Vec::new();
    for change in changes {
        let summary = change.kind.summary();
        if !summaries.contains(&summary) {
            summaries.push(summary);
        }
    }
    let summary = if summaries.is_empty() {
        "снимок PR без изменений для запуска".to_owned()
    } else {
        summaries.join("; ")
    };
    let title = title(pull);
    format!(
        "{title}\n{}\n\nРепозиторий: {repository}\nВетка PR: {} ({})\nБаза: {} ({})\nИзменения: {summary}.",
        pull.url,
        pull.head_ref,
        &pull.head_sha[..12],
        pull.base_ref,
        &pull.base_sha[..12]
    )
}

impl ChangeKind {
    fn summary(self) -> &'static str {
        match self {
            Self::Opened => "открыт новый PR",
            Self::Revision => "обновлены коммиты PR или его базы",
            Self::ReadyForReview => "PR переведён из draft в готовый к ревью",
            Self::Reopened => "PR открыт повторно",
            Self::Edited => "изменены название, описание или базовая ветка PR",
            Self::ReviewRequested => "запрошено ревью",
            Self::Commented => "добавлен или изменён комментарий",
            Self::Reviewed => "добавлено или изменено ревью",
        }
    }
}
