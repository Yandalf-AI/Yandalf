#![cfg(feature = "git-worktree")]

use std::{
    ffi::OsStr,
    path::Path,
    process::Command,
    sync::{Arc, Mutex},
};

use anyhow::{Result, ensure};
use async_trait::async_trait;
use serde_json::{Value, json};
use yandalf_core::{
    domain::{Event, PreparedTask, SessionSpec, TurnInput},
    plugin::{App, Plugin, PluginContext},
    store::Store,
    task::TaskPreparing,
};
use yandalf_git_worktree::{
    CommandOutput, CommandRequest, CommandRunner, GitWorktree, LocalCommandRunner,
};
use yandalf_github::{Github, WorktreeConfig};
use yandalf_sqlite::Sqlite;

fn git(cwd: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "commit.gpgsign=false",
        ])
        .args(args)
        .current_dir(cwd)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

fn commit(cwd: &Path, message: &str) -> String {
    std::fs::write(cwd.join("file"), message).unwrap();
    git(cwd, &["add", "file"]);
    git(
        cwd,
        &[
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "-m",
            message,
        ],
    );
    git(cwd, &["rev-parse", "HEAD"])
}

fn repository(directory: &Path, name: &str) -> (WorktreeConfig, Event) {
    let checkout = directory.join(name);
    let repository = format!("owner/{name}");
    let origin = format!("https://github.com/{repository}.git");
    std::fs::create_dir(&checkout).unwrap();
    git(&checkout, &["init", "--initial-branch=main"]);
    git(&checkout, &["remote", "add", "origin", &origin]);
    let base = commit(&checkout, &format!("{name} base"));
    let head = commit(&checkout, &format!("{name} head"));
    let event = Event {
        id: "event-42".into(),
        source: format!("github:{repository}"),
        subject: format!("github:{repository}/pr/42"),
        title: Some("PR #42".into()),
        text: "Review".into(),
        context: json!({
            "repository": repository, "pr": 42,
            "base_sha": base, "head_sha": head,
            "pull": {"head_ref": "feature"}
        }),
    };
    (
        WorktreeConfig {
            checkout,
            root: directory.join(format!("{name}-worktrees")),
        },
        event,
    )
}

async fn preparing(store: &dyn Store, event: &Event, cwd: &Path) -> TaskPreparing {
    TaskPreparing {
        job: store.enqueue(event).await.unwrap(),
        existing_session: None,
        task: PreparedTask {
            session: SessionSpec {
                cwd: cwd.to_owned(),
                title: event.title.clone(),
                git: None,
            },
            input: TurnInput {
                text: event.text.clone(),
                context: event.context.clone(),
            },
        },
    }
}

#[derive(Default)]
struct Commands {
    calls: Mutex<Vec<CommandRequest>>,
}

#[async_trait]
impl CommandRunner for Commands {
    async fn run(&self, request: CommandRequest) -> Result<CommandOutput> {
        ensure!(
            !request.args.iter().any(|arg| arg == OsStr::new("fetch")),
            "test must use locally available revisions"
        );
        self.calls.lock().unwrap().push(request.clone());
        LocalCommandRunner.run(request).await
    }
}

struct TestCommands(Arc<Commands>);

impl Plugin for TestCommands {
    fn name(&self) -> &str {
        "test-commands"
    }

    fn register(&self, ctx: &mut PluginContext) -> Result<()> {
        ctx.provide::<dyn CommandRunner>(self.0.clone())
    }
}

#[tokio::test]
async fn requires_the_worktree_service_only_when_configured() {
    let directory = tempfile::tempdir().unwrap();
    let github = Github::new("owner/repo", Some("30s"), None).unwrap();
    let app = App::builder()
        .plugin(github.clone())
        .plugin(Sqlite(directory.path().join("state.sqlite")))
        .build()
        .await
        .unwrap();
    app.shutdown().await.unwrap();

    let configured = github
        .with_worktree(WorktreeConfig {
            checkout: directory.path().join("checkout"),
            root: directory.path().join("worktrees"),
        })
        .unwrap();
    let error = App::builder()
        .plugin(configured)
        .plugin(Sqlite(directory.path().join("state.sqlite")))
        .build()
        .await
        .err()
        .unwrap();
    assert!(format!("{error:#}").contains("requires missing service"));
    assert!(format!("{error:#}").contains("WorktreeService"));
}

#[tokio::test]
async fn prepares_only_its_channel_and_preserves_the_admitted_event() {
    let directory = tempfile::tempdir().unwrap();
    let checkout = directory.path().join("checkout");
    let root = directory.path().join("worktrees");
    std::fs::create_dir(&checkout).unwrap();
    git(&checkout, &["init", "--initial-branch=main"]);
    git(
        &checkout,
        &[
            "remote",
            "add",
            "origin",
            "https://github.com/owner/repo.git",
        ],
    );
    let base = commit(&checkout, "base");
    let head = commit(&checkout, "head");
    let commands = Arc::new(Commands::default());
    let app = App::builder()
        .plugin(
            Github::new("owner/repo", Some("30s"), None)
                .unwrap()
                .with_worktree(WorktreeConfig {
                    checkout: checkout.clone(),
                    root: root.clone(),
                })
                .unwrap(),
        )
        .plugin(
            Github::new("other/repo", Some("30s"), None)
                .unwrap()
                .with_worktree(WorktreeConfig {
                    checkout: checkout.clone(),
                    root: directory.path().join("other-worktrees"),
                })
                .unwrap(),
        )
        .plugin(Sqlite(directory.path().join("state.sqlite")))
        .plugin(TestCommands(commands.clone()))
        .plugin(GitWorktree::default())
        .build()
        .await
        .unwrap();
    let services = app.services();
    let store = services.get::<dyn Store>().unwrap();
    let event = Event {
        id: "event-42".into(),
        source: "github:owner/repo".into(),
        subject: "github:owner/repo/pr/42".into(),
        title: Some("PR #42".into()),
        text: "Review".into(),
        context: json!({
            "repository": "owner/repo", "pr": 42,
            "base_sha": base, "head_sha": head,
            "pull": {"head_ref": "feature"}
        }),
    };
    let mut task = preparing(store.as_ref(), &event, &checkout).await;

    services.dispatch(&mut task).await.unwrap();

    assert_eq!(
        task.task.session.cwd,
        root.canonicalize().unwrap().join("pr-42")
    );
    let metadata = task.task.session.git.as_ref().unwrap();
    assert_eq!(metadata.sha, head);
    assert_eq!(metadata.branch.as_deref(), Some("feature"));
    assert_eq!(
        metadata.origin_url.as_deref(),
        Some("https://github.com/owner/repo.git")
    );
    assert_eq!(task.job.event, event);
    assert_eq!(store.get_job(&task.job.id).await.unwrap().event, event);

    let count = commands.calls.lock().unwrap().len();
    task.job.event.source = "schedule:task".into();
    task.job.event.context = Value::Null;
    services.dispatch(&mut task).await.unwrap();
    assert_eq!(commands.calls.lock().unwrap().len(), count);

    task.job.event = event.clone();
    task.job.event.context["pr"] = 7.into();
    assert!(services.dispatch(&mut task).await.is_err());
    assert_eq!(commands.calls.lock().unwrap().len(), count);

    git(
        &checkout,
        &[
            "remote",
            "set-url",
            "origin",
            "https://github.com/other/repo",
        ],
    );
    task.job.event = event;
    let error = services.dispatch(&mut task).await.unwrap_err();
    assert!(format!("{error:#}").contains("origin does not match GitHub repository"));
    assert!(
        commands.calls.lock().unwrap()[count..]
            .iter()
            .all(|request| !request.args.iter().any(|arg| arg == OsStr::new("worktree")))
    );
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn two_channels_prepare_independent_repositories_with_the_same_pull_and_branch() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().canonicalize().unwrap();
    let (first_config, first_event) = repository(&path, "first");
    let (second_config, second_event) = repository(&path, "second");
    let commands = Arc::new(Commands::default());
    let app = App::builder()
        .plugin(
            Github::new("owner/first", Some("30s"), None)
                .unwrap()
                .with_worktree(first_config.clone())
                .unwrap(),
        )
        .plugin(
            Github::new("owner/second", Some("30s"), None)
                .unwrap()
                .with_worktree(second_config.clone())
                .unwrap(),
        )
        .plugin(Sqlite(path.join("state.sqlite")))
        .plugin(TestCommands(commands.clone()))
        .plugin(GitWorktree::default())
        .build()
        .await
        .unwrap();
    let services = app.services();
    let store = services.get::<dyn Store>().unwrap();
    let mut first = preparing(store.as_ref(), &first_event, &first_config.checkout).await;
    let mut second = preparing(store.as_ref(), &second_event, &second_config.checkout).await;

    services.dispatch(&mut first).await.unwrap();
    let first_path = first.task.session.cwd.clone();
    std::fs::write(first_path.join("local.txt"), "preserve local work").unwrap();
    let first_head = git(&first_path, &["rev-parse", "HEAD"]);
    let calls_before_second = commands.calls.lock().unwrap().len();

    services.dispatch(&mut second).await.unwrap();

    assert_eq!(first_path, first_config.root.join("pr-42"));
    assert_eq!(second.task.session.cwd, second_config.root.join("pr-42"));
    assert_ne!(first_path, second.task.session.cwd);
    for (task, event, origin) in [
        (&first, &first_event, "https://github.com/owner/first.git"),
        (
            &second,
            &second_event,
            "https://github.com/owner/second.git",
        ),
    ] {
        let metadata = task.task.session.git.as_ref().unwrap();
        assert_eq!(metadata.sha, event.context["head_sha"]);
        assert_eq!(metadata.branch.as_deref(), Some("feature"));
        assert_eq!(metadata.origin_url.as_deref(), Some(origin));
        assert_eq!(
            git(&task.task.session.cwd, &["rev-parse", "HEAD"]),
            metadata.sha
        );
        assert_eq!(
            git(&task.task.session.cwd, &["remote", "get-url", "origin"]),
            origin
        );
    }
    assert_eq!(git(&first_path, &["rev-parse", "HEAD"]), first_head);
    assert_eq!(git(&first_path, &["branch", "--show-current"]), "feature");
    assert_eq!(
        std::fs::read_to_string(first_path.join("local.txt")).unwrap(),
        "preserve local work"
    );
    assert!(
        commands.calls.lock().unwrap()[calls_before_second..]
            .iter()
            .all(|request| {
                !request.cwd.starts_with(&first_config.checkout)
                    && !request.cwd.starts_with(&first_config.root)
            })
    );
    app.shutdown().await.unwrap();
}
