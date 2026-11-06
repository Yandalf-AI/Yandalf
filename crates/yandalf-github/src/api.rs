use std::{
    collections::{BTreeMap, BTreeSet},
    process::Stdio,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use serde_json::Value;
use tokio::{process::Command, time::timeout};

use crate::model::{Activity, ChangeKind, PullRequest};

pub(crate) async fn viewer() -> Result<String> {
    let pages = api("user").await?;
    let login = pages
        .first()
        .and_then(|user| user["login"].as_str())
        .context("missing authenticated GitHub login")?;
    ensure!(!login.is_empty(), "empty authenticated GitHub login");
    Ok(login.to_owned())
}

pub(crate) async fn open_pulls(repository: &str) -> Result<Vec<PullRequest>> {
    let pages = api(&format!(
        "repos/{repository}/pulls?state=open&sort=created&direction=asc&per_page=100"
    ))
    .await?;
    parse_pulls(repository, pages)
}

pub(crate) async fn pull(repository: &str, number: u64) -> Result<PullRequest> {
    let pages = api(&format!("repos/{repository}/pulls/{number}")).await?;
    ensure!(pages.len() == 1, "unexpected pull request pagination");
    parse_pull(repository, pages.into_iter().next().unwrap())
}

pub(crate) async fn activities(repository: &str, number: u64) -> Result<Vec<Activity>> {
    let timeline = api(&format!(
        "repos/{repository}/issues/{number}/timeline?per_page=100"
    ))
    .await?;
    let comments = api(&format!(
        "repos/{repository}/pulls/{number}/comments?per_page=100"
    ))
    .await?;
    parse_activities(timeline, comments)
}

pub(crate) async fn comment_numbers(
    repository: &str,
    path: &str,
    field: &str,
    since: &str,
    numbers: &mut BTreeSet<u64>,
) -> Result<()> {
    let pages = api(&format!(
        "repos/{repository}/{path}?sort=updated&direction=asc&since={since}&per_page=100"
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
                numbers.insert(number);
            }
        }
    }
    Ok(())
}

pub(crate) async fn api(endpoint: &str) -> Result<Vec<Value>> {
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

pub(crate) fn decode_pages(bytes: &[u8]) -> Result<Vec<Value>> {
    let pages = serde_json::Deserializer::from_slice(bytes)
        .into_iter::<Value>()
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("decode GitHub pages")?;
    ensure!(!pages.is_empty(), "GitHub returned no response pages");
    Ok(pages)
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

pub(crate) fn parse_pulls(repository: &str, pages: Vec<Value>) -> Result<Vec<PullRequest>> {
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

pub(crate) fn parse_pull(repository: &str, raw: Value) -> Result<PullRequest> {
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

pub(crate) fn parse_activities(
    timeline: Vec<Value>,
    comments: Vec<Value>,
) -> Result<Vec<Activity>> {
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

pub(crate) fn validate_sha(sha: &str) -> Result<()> {
    ensure!(
        sha.len() == 40 && sha.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "invalid GitHub revision SHA"
    );
    Ok(())
}

pub(crate) fn canonical_repository(repository: &str) -> Result<String> {
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
