use std::path::{Component, PathBuf};

use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use yandalf_core::{domain::Event, plugin::PluginContext, task::TaskPreparing};
use yandalf_git_worktree::{WorktreeRequest, WorktreeService};

use crate::{
    api::{canonical_repository, validate_sha},
    observation::subject,
};

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorktreeConfig {
    pub checkout: PathBuf,
    pub root: PathBuf,
}

impl WorktreeConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        for path in [&self.checkout, &self.root] {
            ensure!(path.is_absolute(), "GitHub worktree paths must be absolute");
            ensure!(
                !path
                    .components()
                    .any(|component| matches!(component, Component::ParentDir)),
                "GitHub worktree paths must not contain parent traversal"
            );
        }
        Ok(())
    }

    pub(crate) fn register(&self, ctx: &mut PluginContext, repository: &str, source: &str) {
        ctx.require::<WorktreeService>();
        let config = self.clone();
        let repository = repository.to_owned();
        let source = source.to_owned();
        ctx.on::<TaskPreparing, _>(move |services, task| {
            let config = config.clone();
            let repository = repository.clone();
            let source = source.clone();
            Box::pin(async move {
                if task.job.event.source != source {
                    return Ok(());
                }
                let pull = PullContext::decode(&repository, &task.job.event)?;
                let worktrees = services.get::<WorktreeService>()?;
                let origin = worktrees.origin(&config.checkout).await?;
                ensure!(
                    origin_repository(&origin).as_deref() == Some(repository.as_str()),
                    "configured checkout origin does not match GitHub repository"
                );
                let request = config.request(pull, task, origin);
                let prepared = worktrees.prepare(&request).await?;
                task.task.session.cwd = prepared.cwd;
                task.task.session.git = Some(prepared.git);
                Ok(())
            })
        });
    }

    fn request(
        &self,
        context: PullContext,
        task: &TaskPreparing,
        origin: String,
    ) -> WorktreeRequest {
        WorktreeRequest {
            checkout: self.checkout.clone(),
            path: self.root.join(format!("pr-{}", context.pr)),
            existing_path: task
                .existing_session
                .as_ref()
                .map(|_| task.task.session.cwd.clone()),
            owner: task.job.event.subject.clone(),
            revision: context.head_sha.to_ascii_lowercase(),
            branch: context.pull.head_ref,
            fallback_branch: format!("review/pr-{}", context.pr),
            required_revisions: vec![context.base_sha.to_ascii_lowercase()],
            fetch_refs: vec![format!("refs/pull/{}/head", context.pr)],
            expected_origin: origin,
        }
    }
}

#[derive(Deserialize)]
struct PullContext {
    repository: String,
    pr: u64,
    base_sha: String,
    head_sha: String,
    pull: PullBranch,
}

impl PullContext {
    fn decode(repository: &str, event: &Event) -> Result<Self> {
        let context: Self = serde_json::from_value(event.context.clone())
            .context("decode GitHub worktree context")?;
        ensure!(
            canonical_repository(&context.repository)? == repository,
            "GitHub worktree repository mismatch"
        );
        ensure!(context.pr > 0, "invalid pull request number");
        ensure!(
            event.subject == subject(repository, context.pr),
            "GitHub worktree subject does not identify this pull request"
        );
        validate_sha(&context.head_sha)?;
        validate_sha(&context.base_sha)?;
        ensure!(
            !context.pull.head_ref.trim().is_empty(),
            "pull request head branch must not be empty"
        );
        Ok(context)
    }
}

#[derive(Deserialize)]
struct PullBranch {
    head_ref: String,
}

fn origin_repository(origin: &str) -> Option<String> {
    let path = origin
        .strip_prefix("https://github.com/")
        .or_else(|| origin.strip_prefix("git@github.com:"))
        .or_else(|| origin.strip_prefix("ssh://git@github.com/"))?;
    let path = path.trim_end_matches('/');
    canonical_repository(path.strip_suffix(".git").unwrap_or(path)).ok()
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};
    use yandalf_core::domain::{Job, JobStatus, PreparedTask, Session, SessionSpec, TurnInput};

    use super::*;

    fn config() -> WorktreeConfig {
        WorktreeConfig {
            checkout: "/configured/checkout".into(),
            root: "/configured/worktrees".into(),
        }
    }

    fn task() -> TaskPreparing {
        TaskPreparing {
            job: Job {
                id: "job-42".into(),
                event: Event {
                    id: "event-42".into(),
                    source: "github:owner/repo".into(),
                    subject: "github:owner/repo/pr/42".into(),
                    title: None,
                    text: "Review".into(),
                    context: json!({
                        "repository": "owner/repo", "pr": 42,
                        "base_sha": "A".repeat(40), "head_sha": "B".repeat(40),
                        "pull": {"head_ref": "feature"},
                        "cwd": "/untrusted/event/path"
                    }),
                },
                status: JobStatus::Preparing,
                thread_id: None,
                turn_id: None,
                cancel_requested: false,
                error: None,
                created_at: 0,
                updated_at: 0,
            },
            existing_session: None,
            task: PreparedTask {
                session: SessionSpec {
                    cwd: "/executor/current".into(),
                    title: None,
                    git: None,
                },
                input: TurnInput {
                    text: "Review".into(),
                    context: Value::Null,
                },
            },
        }
    }

    #[test]
    fn request_uses_configured_paths_and_pinned_pull_identity() {
        let config = config();
        let task = task();
        let pull = PullContext::decode("owner/repo", &task.job.event).unwrap();
        let origin = "git@github.com:owner/repo.git";
        let request = config.request(pull, &task, origin.into());

        assert_eq!(request.checkout, config.checkout);
        assert_eq!(request.path, config.root.join("pr-42"));
        assert_eq!(request.existing_path, None);
        assert_eq!(request.owner, task.job.event.subject);
        assert_eq!(request.revision, "b".repeat(40));
        assert_eq!(request.branch, "feature");
        assert_eq!(request.fallback_branch, "review/pr-42");
        assert_eq!(request.required_revisions, ["a".repeat(40)]);
        assert_eq!(request.fetch_refs, ["refs/pull/42/head"]);
        assert_eq!(request.expected_origin, origin);
    }

    #[test]
    fn followup_uses_the_current_executor_cwd() {
        let mut task = task();
        task.existing_session = Some(Session {
            subject: task.job.event.subject.clone(),
            cwd: "/previous/binding".into(),
            thread_id: "thread-42".into(),
        });
        let pull = PullContext::decode("owner/repo", &task.job.event).unwrap();
        let request = config().request(pull, &task, "https://github.com/owner/repo".into());

        assert_eq!(request.existing_path, Some(task.task.session.cwd));
    }

    #[test]
    fn rejects_incomplete_or_mismatched_pull_context() {
        for (pointer, value) in [
            ("/repository", json!("other/repo")),
            ("/pr", json!(0)),
            ("/pr", json!(43)),
            ("/pr", Value::Null),
            ("/base_sha", json!("not-a-sha")),
            ("/head_sha", Value::Null),
            ("/pull/head_ref", Value::Null),
            ("/pull/head_ref", json!(" ")),
        ] {
            let mut task = task();
            *task.job.event.context.pointer_mut(pointer).unwrap() = value;
            assert!(
                PullContext::decode("owner/repo", &task.job.event).is_err(),
                "{pointer}"
            );
        }
        let mut task = task();
        task.job.event.subject = "github:owner/repo/pr/7".into();
        assert!(PullContext::decode("owner/repo", &task.job.event).is_err());
    }

    #[test]
    fn accepts_only_public_github_repository_origins() {
        for origin in [
            "https://github.com/Owner/Repo.git/",
            "git@github.com:Owner/Repo.git",
            "ssh://git@github.com/Owner/Repo",
        ] {
            assert_eq!(origin_repository(origin).as_deref(), Some("owner/repo"));
        }
        for origin in [
            "https://github.com.evil/owner/repo",
            "https://token@github.com/owner/repo",
            "https://github.com/owner/repo/extra",
            "/local/repository",
        ] {
            assert_eq!(origin_repository(origin), None, "{origin}");
        }
    }

    #[test]
    fn configuration_requires_absolute_paths_without_parent_traversal() {
        config().validate().unwrap();
        for path in ["relative", "/configured/../outside"] {
            let mut config = config();
            config.checkout = path.into();
            assert!(config.validate().is_err());
            config.checkout = "/configured/checkout".into();
            config.root = path.into();
            assert!(config.validate().is_err());
        }
        assert!(
            serde_json::from_value::<WorktreeConfig>(json!({
                "checkout": "/checkout", "root": "/worktrees", "unknown": true
            }))
            .is_err()
        );
    }
}
