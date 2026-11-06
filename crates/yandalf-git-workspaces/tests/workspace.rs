#![cfg(unix)]

use std::{
    ffi::OsStr,
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Mutex},
};

use anyhow::Result;
use async_trait::async_trait;
use serde_json::json;
use tempfile::TempDir;
use yandalf_core::{
    domain::{Event, Job, JobStatus, PreparedTask, Session, SessionSpec, TurnInput},
    plugin::{App, Plugin, PluginContext},
    task::TaskPreparing,
};
use yandalf_git_workspaces::{
    CommandOutput, CommandRequest, CommandRunner, GitWorkspaces, LocalCommandRunner,
};

fn git<I, S>(cwd: &Path, args: I) -> String
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = Command::new("git")
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "commit.gpgsign=false",
        ])
        .args(args)
        .current_dir(cwd)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

struct Fixture {
    _directory: TempDir,
    root: PathBuf,
    author: PathBuf,
    origin: PathBuf,
    checkout: PathBuf,
    workspaces: PathBuf,
    base: String,
    head: String,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let author = root.join("author");
        fs::create_dir(&author).unwrap();
        git(&author, ["init", "-b", "main"]);
        git(&author, ["config", "user.name", "Workspace Test"]);
        git(
            &author,
            ["config", "user.email", "workspace@example.invalid"],
        );
        fs::write(author.join("value.txt"), "base\n").unwrap();
        fs::write(author.join(".gitignore"), "ignored/\n").unwrap();
        git(&author, ["add", "."]);
        git(&author, ["commit", "-m", "base"]);
        let base = git(&author, ["rev-parse", "HEAD"]);
        let origin = root.join("origin.git");
        git(
            &root,
            [
                OsStr::new("clone"),
                OsStr::new("--bare"),
                author.as_os_str(),
                origin.as_os_str(),
            ],
        );
        let checkout = root.join("checkout");
        git(
            &root,
            [
                OsStr::new("clone"),
                origin.as_os_str(),
                checkout.as_os_str(),
            ],
        );
        git(
            &checkout,
            [
                "remote",
                "set-url",
                "origin",
                "https://github.com/acme/sdk.git",
            ],
        );
        git(&author, ["switch", "-c", "topic"]);
        fs::write(author.join("value.txt"), "first PR revision\n").unwrap();
        git(&author, ["commit", "-am", "first"]);
        let head = git(&author, ["rev-parse", "HEAD"]);
        git(
            &author,
            [
                OsStr::new("push"),
                origin.as_os_str(),
                OsStr::new("HEAD:refs/pull/731/head"),
            ],
        );
        let workspaces = root.join("workspaces");
        Self {
            _directory: directory,
            root,
            author,
            origin,
            checkout,
            workspaces,
            base,
            head,
        }
    }

    fn advance(&self, number: u64, content: &str) -> String {
        fs::write(self.author.join("value.txt"), content).unwrap();
        git(&self.author, ["commit", "-am", "next"]);
        git(
            &self.author,
            [
                OsStr::new("push"),
                self.origin.as_os_str(),
                OsStr::new(&format!("HEAD:refs/pull/{number}/head")),
            ],
        );
        git(&self.author, ["rev-parse", "HEAD"])
    }

    fn event(&self, number: u64, head: &str, existing: Option<&Path>) -> TaskPreparing {
        let subject = format!("github:acme/sdk/pr/{number}");
        let cwd = existing.unwrap_or(&self.checkout).to_owned();
        TaskPreparing {
            job: Job {
                id: format!("job-{number}-{head}"),
                event: Event {
                    id: format!("event-{head}"),
                    source: "github:acme/sdk".into(),
                    subject: subject.clone(),
                    title: Some(format!("PR #{number}")),
                    text: "Review the pinned revision".into(),
                    context: json!({"repository":"acme/sdk","pr":number,"base_sha":self.base,"head_sha":head,
                        "pull":{"head_ref":format!("feature/pr-{number}")}}),
                },
                status: JobStatus::Preparing,
                thread_id: None,
                turn_id: None,
                cancel_requested: false,
                error: None,
                created_at: 1,
                updated_at: 1,
            },
            existing_session: existing.map(|_| Session {
                subject,
                cwd: cwd.clone(),
                thread_id: "same-thread".into(),
            }),
            task: PreparedTask {
                session: SessionSpec {
                    cwd,
                    title: Some(format!("PR #{number}")),
                    git: None,
                },
                input: TurnInput {
                    text: "Review the pinned revision".into(),
                    context: json!({"stack":{"number":730}}),
                },
            },
        }
    }

    async fn app(&self) -> (App, Arc<FixtureRunner>) {
        let runner = Arc::new(FixtureRunner {
            origin: self.origin.clone(),
            calls: Mutex::new(Vec::new()),
        });
        let app = App::builder()
            .plugin(
                GitWorkspaces::new("acme/sdk", self.checkout.clone(), self.workspaces.clone())
                    .unwrap(),
            )
            .plugin(FixtureCommands(runner.clone()))
            .build()
            .await
            .unwrap();
        (app, runner)
    }
}

struct FixtureRunner {
    origin: PathBuf,
    calls: Mutex<Vec<CommandRequest>>,
}

#[async_trait]
impl CommandRunner for FixtureRunner {
    async fn run(&self, mut request: CommandRequest) -> Result<CommandOutput> {
        self.calls.lock().unwrap().push(request.clone());
        if request.program == "git" && request.args.first().is_some_and(|arg| arg == "fetch") {
            assert_eq!(request.args.get(2).unwrap(), "origin");
            request.args[2] = self.origin.as_os_str().to_owned();
        }
        LocalCommandRunner.run(request).await
    }
}

struct FixtureCommands(Arc<FixtureRunner>);
impl Plugin for FixtureCommands {
    fn name(&self) -> &str {
        "fixture.commands"
    }
    fn register(&self, ctx: &mut PluginContext) -> Result<()> {
        ctx.provide::<dyn CommandRunner>(self.0.clone())
    }
}

#[tokio::test]
async fn prepares_independent_named_pr_worktrees_at_pinned_heads_before_execution() {
    let fixture = Fixture::new();
    let second_head = fixture.advance(732, "another pull request\n");
    let (app, runner) = fixture.app().await;
    let mut first = fixture.event(731, &fixture.head, None);
    let mut second = fixture.event(732, &second_head, None);
    let services = app.services();
    let (first_result, second_result) = tokio::join!(
        services.dispatch(&mut first),
        services.dispatch(&mut second)
    );
    first_result.unwrap();
    second_result.unwrap();

    assert_eq!(first.task.session.cwd, fixture.workspaces.join("pr-731"));
    assert_eq!(second.task.session.cwd, fixture.workspaces.join("pr-732"));
    assert_eq!(
        git(&first.task.session.cwd, ["rev-parse", "HEAD"]),
        fixture.head
    );
    assert_eq!(
        git(&second.task.session.cwd, ["rev-parse", "HEAD"]),
        second_head
    );
    assert_eq!(
        first.task.session.git.as_ref().unwrap().branch.as_deref(),
        Some("feature/pr-731")
    );
    assert_eq!(
        second.task.session.git.as_ref().unwrap().branch.as_deref(),
        Some("feature/pr-732")
    );
    assert_eq!(first.task.input.context, json!({"stack":{"number":730}}));
    assert_eq!(git(&fixture.checkout, ["rev-parse", "HEAD"]), fixture.base);
    assert_eq!(git(&fixture.checkout, ["branch", "--show-current"]), "main");
    assert_eq!(
        fs::read_to_string(fixture.checkout.join("value.txt")).unwrap(),
        "base\n"
    );
    assert!(
        runner
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|call| call.args.first().is_some_and(|arg| arg == "fetch"))
    );
    app.shutdown().await.unwrap();
    assert!(first.task.session.cwd.exists());
}

#[tokio::test]
async fn reuses_owned_worktree_after_crash_before_session_binding_and_advances_clean_revision() {
    let fixture = Fixture::new();
    let (app, _) = fixture.app().await;
    let mut first = fixture.event(731, &fixture.head, None);
    app.services().dispatch(&mut first).await.unwrap();
    let path = first.task.session.cwd.clone();
    fs::create_dir(path.join("ignored")).unwrap();
    fs::write(path.join("ignored/build-cache"), "preserve").unwrap();
    app.shutdown().await.unwrap();

    let (restarted, _) = fixture.app().await;
    let mut retry = fixture.event(731, &fixture.head, None);
    restarted.services().dispatch(&mut retry).await.unwrap();
    assert_eq!(retry.task.session.cwd, path);
    let new_head = fixture.advance(731, "updated PR\n");
    let mut update = fixture.event(731, &new_head, Some(&path));
    restarted.services().dispatch(&mut update).await.unwrap();
    assert_eq!(update.task.session.cwd, path);
    assert_eq!(git(&path, ["rev-parse", "HEAD"]), new_head);
    assert_eq!(git(&path, ["branch", "--show-current"]), "feature/pr-731");
    assert_eq!(
        update.task.session.git.as_ref().unwrap().branch.as_deref(),
        Some("feature/pr-731")
    );
    assert_eq!(
        fs::read_to_string(path.join("ignored/build-cache")).unwrap(),
        "preserve"
    );
    assert_eq!(
        git(&fixture.checkout, ["worktree", "list", "--porcelain"])
            .matches("worktree ")
            .count(),
        2
    );
    restarted.shutdown().await.unwrap();
}

#[tokio::test]
async fn renames_owned_review_branch_in_place_and_preserves_local_changes() {
    let fixture = Fixture::new();
    let (app, _) = fixture.app().await;
    let mut first = fixture.event(731, &fixture.head, None);
    app.services().dispatch(&mut first).await.unwrap();
    let path = first.task.session.cwd;
    git(&path, ["branch", "-m", "review/pr-731"]);
    git(
        &path,
        ["config", "--unset", "branch.review/pr-731.yandalf-subject"],
    );
    fs::write(path.join("value.txt"), "local investigation\n").unwrap();

    let mut update = fixture.event(731, &fixture.head, Some(&path));
    app.services().dispatch(&mut update).await.unwrap();

    assert_eq!(update.task.session.cwd, path);
    assert_eq!(update.existing_session.unwrap().thread_id, "same-thread");
    assert_eq!(git(&path, ["branch", "--show-current"]), "feature/pr-731");
    assert_eq!(
        update.task.session.git.unwrap().branch.as_deref(),
        Some("feature/pr-731")
    );
    assert_eq!(git(&path, ["rev-parse", "HEAD"]), fixture.head);
    assert_eq!(
        fs::read_to_string(path.join("value.txt")).unwrap(),
        "local investigation\n"
    );
    assert_eq!(
        git(&fixture.checkout, ["worktree", "list", "--porcelain"])
            .matches("worktree ")
            .count(),
        2
    );
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn follows_upstream_branch_renames_with_the_same_session_and_preserves_conflicting_refs() {
    for advance in [false, true] {
        let fixture = Fixture::new();
        let (app, _) = fixture.app().await;
        let mut first = fixture.event(731, &fixture.head, None);
        app.services().dispatch(&mut first).await.unwrap();
        let path = first.task.session.cwd;
        let head = if advance {
            fixture.advance(731, "updated while renaming the branch\n")
        } else {
            fixture.head.clone()
        };
        let mut rename = fixture.event(731, &head, Some(&path));
        rename.job.event.context["pull"]["head_ref"] = json!("renamed/topic");

        app.services().dispatch(&mut rename).await.unwrap();

        assert_eq!(rename.task.session.cwd, path);
        assert_eq!(rename.existing_session.unwrap().thread_id, "same-thread");
        assert_eq!(git(&path, ["branch", "--show-current"]), "renamed/topic");
        assert_eq!(git(&path, ["rev-parse", "HEAD"]), head);
        assert_eq!(
            rename.task.session.git.unwrap().branch.as_deref(),
            Some("renamed/topic")
        );
        assert_eq!(
            git(
                &path,
                ["config", "--get", "branch.renamed/topic.yandalf-subject"]
            ),
            "github:acme/sdk/pr/731"
        );
        git(&fixture.checkout, ["branch", "occupied", &fixture.base]);
        let mut conflict = fixture.event(731, &head, Some(&path));
        conflict.job.event.context["pull"]["head_ref"] = json!("occupied");

        app.services().dispatch(&mut conflict).await.unwrap();

        assert_eq!(conflict.task.session.cwd, path);
        assert_eq!(git(&path, ["branch", "--show-current"]), "review/pr-731");
        assert_eq!(git(&path, ["rev-parse", "HEAD"]), head);
        assert_eq!(
            git(&fixture.checkout, ["rev-parse", "occupied"]),
            fixture.base
        );
        assert_eq!(
            conflict.task.session.git.unwrap().branch.as_deref(),
            Some("review/pr-731")
        );
        app.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn never_moves_a_branch_marked_as_owned_by_another_subject() {
    let fixture = Fixture::new();
    let (app, _) = fixture.app().await;
    let mut first = fixture.event(731, &fixture.head, None);
    app.services().dispatch(&mut first).await.unwrap();
    let path = first.task.session.cwd;
    git(&path, ["switch", "-c", "foreign"]);
    git(
        &path,
        [
            "config",
            "branch.foreign.yandalf-subject",
            "github:acme/sdk/pr/999",
        ],
    );
    let head = fixture.advance(731, "new revision\n");
    let mut update = fixture.event(731, &head, Some(&path));
    update.job.event.context["pull"]["head_ref"] = json!("foreign");

    let error = app.services().dispatch(&mut update).await.unwrap_err();

    assert!(format!("{error:#}").contains("owned by another task"));
    assert_eq!(git(&path, ["rev-parse", "HEAD"]), fixture.head);
    assert_eq!(git(&path, ["branch", "--show-current"]), "foreign");
    assert_eq!(
        git(&path, ["config", "--get", "branch.foreign.yandalf-subject"]),
        "github:acme/sdk/pr/999"
    );
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn conflicting_local_refs_are_preserved_while_the_review_branch_advances() {
    for conflict in ["checked-out", "unused", "prefix"] {
        let fixture = Fixture::new();
        let (head_ref, foreign_ref) = match conflict {
            "checked-out" => ("main", "main"),
            "unused" => {
                git(&fixture.checkout, ["branch", "feature/pr-731"]);
                ("feature/pr-731", "feature/pr-731")
            }
            "prefix" => {
                git(&fixture.checkout, ["branch", "feature"]);
                ("feature/pr-731", "feature")
            }
            _ => unreachable!(),
        };
        let (app, _) = fixture.app().await;
        let mut first = fixture.event(731, &fixture.head, None);
        first.job.event.context["pull"]["head_ref"] = json!(head_ref);
        app.services().dispatch(&mut first).await.unwrap();
        let path = first.task.session.cwd;
        let new_head = fixture.advance(731, "new PR revision\n");
        let mut update = fixture.event(731, &new_head, Some(&path));
        update.job.event.context["pull"]["head_ref"] = json!(head_ref);

        app.services().dispatch(&mut update).await.unwrap();

        assert_eq!(git(&path, ["rev-parse", "HEAD"]), new_head);
        assert_eq!(git(&path, ["branch", "--show-current"]), "review/pr-731");
        assert_eq!(
            update.task.session.git.unwrap().branch.as_deref(),
            Some("review/pr-731")
        );
        assert_eq!(
            git(&fixture.checkout, ["rev-parse", foreign_ref]),
            fixture.base
        );
        assert_eq!(git(&fixture.checkout, ["branch", "--show-current"]), "main");
        app.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn separate_pull_requests_with_the_same_branch_name_keep_independent_worktrees() {
    let fixture = Fixture::new();
    let second_head = fixture.advance(732, "another fork's branch\n");
    let (app, _) = fixture.app().await;
    let mut first = fixture.event(731, &fixture.head, None);
    app.services().dispatch(&mut first).await.unwrap();
    let mut second = fixture.event(732, &second_head, None);
    second.job.event.context["pull"]["head_ref"] = json!("feature/pr-731");

    app.services().dispatch(&mut second).await.unwrap();

    assert_eq!(
        git(&first.task.session.cwd, ["branch", "--show-current"]),
        "feature/pr-731"
    );
    assert_eq!(
        git(&first.task.session.cwd, ["rev-parse", "HEAD"]),
        fixture.head
    );
    assert_eq!(
        git(&second.task.session.cwd, ["branch", "--show-current"]),
        "review/pr-732"
    );
    assert_eq!(
        git(&second.task.session.cwd, ["rev-parse", "HEAD"]),
        second_head
    );
    assert_eq!(
        second.task.session.git.unwrap().branch.as_deref(),
        Some("review/pr-732")
    );
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn rejects_missing_invalid_and_expanded_head_branch_names_before_workspace_mutations() {
    let fixture = Fixture::new();
    git(&fixture.checkout, ["switch", "-c", "previous"]);
    git(&fixture.checkout, ["switch", "main"]);
    let (app, runner) = fixture.app().await;
    for head_ref in [
        serde_json::Value::Null,
        json!(""),
        json!("../outside"),
        json!("--detach"),
        json!("HEAD"),
        json!("@{-1}"),
        json!("bad\nbranch"),
    ] {
        let mut event = fixture.event(731, &fixture.head, None);
        event.job.event.context["pull"]["head_ref"] = head_ref;
        let error = app.services().dispatch(&mut event).await.unwrap_err();
        assert!(format!("{error:#}").contains("pull request head branch"));
    }
    assert!(!fixture.workspaces.exists());
    assert_eq!(git(&fixture.checkout, ["branch", "--show-current"]), "main");
    assert!(runner.calls.lock().unwrap().iter().all(|call| {
        call.program == "git"
            && call
                .args
                .first()
                .is_some_and(|arg| arg == "rev-parse" || arg == "check-ref-format")
    }));
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn preserves_dirty_same_revision_and_refuses_to_discard_changes_for_an_update() {
    let fixture = Fixture::new();
    let (app, _) = fixture.app().await;
    let mut first = fixture.event(731, &fixture.head, None);
    app.services().dispatch(&mut first).await.unwrap();
    let path = first.task.session.cwd;
    fs::write(path.join("value.txt"), "uncommitted work\n").unwrap();
    fs::write(path.join("scratch.txt"), "local reproduction\n").unwrap();
    let mut same_revision = fixture.event(731, &fixture.head, Some(&path));
    app.services().dispatch(&mut same_revision).await.unwrap();

    let new_head = fixture.advance(731, "updated PR\n");
    let mut update = fixture.event(731, &new_head, Some(&path));
    let error = app.services().dispatch(&mut update).await.unwrap_err();
    assert!(format!("{error:#}").contains("local changes"));
    assert_eq!(git(&path, ["rev-parse", "HEAD"]), fixture.head);
    assert_eq!(
        fs::read_to_string(path.join("value.txt")).unwrap(),
        "uncommitted work\n"
    );
    assert_eq!(
        fs::read_to_string(path.join("scratch.txt")).unwrap(),
        "local reproduction\n"
    );
    git(&path, ["restore", "value.txt"]);
    let mut still_dirty = fixture.event(731, &new_head, Some(&path));
    assert!(app.services().dispatch(&mut still_dirty).await.is_err());
    assert_eq!(git(&path, ["rev-parse", "HEAD"]), fixture.head);
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn adopts_existing_detached_session_worktree_without_moving_or_attaching_it() {
    let fixture = Fixture::new();
    git(
        &fixture.checkout,
        [
            OsStr::new("fetch"),
            fixture.origin.as_os_str(),
            OsStr::new(&fixture.head),
        ],
    );
    let legacy = fixture.root.join("legacy-session-worktree");
    git(
        &fixture.checkout,
        [
            OsStr::new("worktree"),
            OsStr::new("add"),
            OsStr::new("--detach"),
            legacy.as_os_str(),
            OsStr::new(&fixture.head),
        ],
    );
    let (app, _) = fixture.app().await;
    let head = fixture.advance(731, "new revision\n");
    let mut update = fixture.event(731, &head, Some(&legacy));
    app.services().dispatch(&mut update).await.unwrap();
    assert_eq!(update.task.session.cwd, legacy);
    assert_eq!(update.task.session.git.unwrap().branch, None);
    assert_eq!(git(&legacy, ["rev-parse", "HEAD"]), head);
    assert!(!fixture.workspaces.exists());
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn refuses_foreign_bound_repositories_and_unowned_destination_paths() {
    let fixture = Fixture::new();
    let foreign = fixture.root.join("foreign");
    fs::create_dir(&foreign).unwrap();
    git(&foreign, ["init", "-b", "main"]);
    let (app, _) = fixture.app().await;
    let mut event = fixture.event(731, &fixture.head, Some(&foreign));
    let error = app.services().dispatch(&mut event).await.unwrap_err();
    assert!(format!("{error:#}").contains("different repository"));

    let collision = fixture.workspaces.join("pr-731");
    fs::create_dir_all(&collision).unwrap();
    fs::write(collision.join("user-file"), "untouched").unwrap();
    let mut event = fixture.event(731, &fixture.head, None);
    assert!(app.services().dispatch(&mut event).await.is_err());
    assert_eq!(
        fs::read_to_string(collision.join("user-file")).unwrap(),
        "untouched"
    );
    assert_eq!(
        git(&fixture.checkout, ["worktree", "list", "--porcelain"])
            .matches("worktree ")
            .count(),
        1
    );
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn leaves_other_sources_untouched_and_rejects_mismatched_pr_identity() {
    let fixture = Fixture::new();
    let (app, runner) = fixture.app().await;
    let mut event = fixture.event(731, &fixture.head, None);
    event.job.event.source = "scheduler".into();
    let before = event.task.clone();
    app.services().dispatch(&mut event).await.unwrap();
    assert_eq!(event.task, before);
    assert!(runner.calls.lock().unwrap().is_empty());

    event.job.event.source = "github:acme/sdk".into();
    event.job.event.subject = "github:acme/sdk/pr/732".into();
    assert!(app.services().dispatch(&mut event).await.is_err());
    assert!(runner.calls.lock().unwrap().is_empty());
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn finishes_partially_created_worktree_and_keeps_the_observed_sha_after_remote_push() {
    let fixture = Fixture::new();
    let newer = fixture.advance(731, "remote advanced before admission\n");
    let (app, _) = fixture.app().await;
    let mut event = fixture.event(731, &fixture.head, None);
    app.services().dispatch(&mut event).await.unwrap();
    assert_ne!(fixture.head, newer);
    assert_eq!(
        git(&event.task.session.cwd, ["rev-parse", "HEAD"]),
        fixture.head
    );

    let partial = fixture.workspaces.join("pr-732");
    git(
        &fixture.checkout,
        [
            OsStr::new("worktree"),
            OsStr::new("add"),
            OsStr::new("--detach"),
            OsStr::new("--lock"),
            OsStr::new("--reason"),
            OsStr::new("yandalf:github:acme/sdk/pr/732"),
            partial.as_os_str(),
            OsStr::new(&fixture.head),
        ],
    );
    let mut resumed = fixture.event(732, &fixture.head, None);
    app.services().dispatch(&mut resumed).await.unwrap();
    assert_eq!(resumed.task.session.cwd, partial);
    assert_eq!(
        resumed.task.session.git.unwrap().branch.as_deref(),
        Some("feature/pr-732")
    );
    assert_eq!(git(&fixture.checkout, ["rev-parse", "HEAD"]), fixture.base);
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn never_retargets_primary_checkout_even_when_reference_checkout_is_linked() {
    let fixture = Fixture::new();
    let reference = fixture.root.join("configured-linked-checkout");
    git(
        &fixture.checkout,
        [
            OsStr::new("worktree"),
            OsStr::new("add"),
            OsStr::new("--detach"),
            reference.as_os_str(),
            OsStr::new(&fixture.base),
        ],
    );
    let runner = Arc::new(FixtureRunner {
        origin: fixture.origin.clone(),
        calls: Mutex::new(Vec::new()),
    });
    let app = App::builder()
        .plugin(GitWorkspaces::new("acme/sdk", reference, fixture.workspaces.clone()).unwrap())
        .plugin(FixtureCommands(runner))
        .build()
        .await
        .unwrap();
    let mut event = fixture.event(731, &fixture.head, Some(&fixture.checkout));
    app.services().dispatch(&mut event).await.unwrap();
    assert_eq!(event.task.session.cwd, fixture.workspaces.join("pr-731"));
    assert_eq!(git(&fixture.checkout, ["rev-parse", "HEAD"]), fixture.base);
    assert_eq!(git(&fixture.checkout, ["branch", "--show-current"]), "main");
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn concurrent_local_edit_between_clean_check_and_checkout_is_never_overwritten() {
    struct EditingRunner {
        inner: FixtureRunner,
    }
    #[async_trait]
    impl CommandRunner for EditingRunner {
        async fn run(&self, request: CommandRequest) -> Result<CommandOutput> {
            if request.program == "git" && request.args.first().is_some_and(|arg| arg == "reset") {
                fs::write(request.cwd.join("value.txt"), "concurrent user edit\n").unwrap();
            }
            self.inner.run(request).await
        }
    }
    let fixture = Fixture::new();
    let (app, _) = fixture.app().await;
    let mut first = fixture.event(731, &fixture.head, None);
    app.services().dispatch(&mut first).await.unwrap();
    let path = first.task.session.cwd;
    app.shutdown().await.unwrap();
    let next = fixture.advance(731, "new committed revision\n");
    let runner = EditingRunner {
        inner: FixtureRunner {
            origin: fixture.origin.clone(),
            calls: Mutex::new(Vec::new()),
        },
    };
    let plugin = GitWorkspaces::new(
        "acme/sdk",
        fixture.checkout.clone(),
        fixture.workspaces.clone(),
    )
    .unwrap();
    let mut update = fixture.event(731, &next, Some(&path));
    assert!(plugin.prepare(&runner, &mut update).await.is_err());
    assert_eq!(
        fs::read_to_string(path.join("value.txt")).unwrap(),
        "concurrent user edit\n"
    );
    assert_eq!(git(&path, ["rev-parse", "HEAD"]), fixture.head);
}

#[tokio::test]
async fn missing_bound_worktree_gets_owned_replacement_without_pruning_old_registration() {
    for remove_registration in [false, true] {
        let fixture = Fixture::new();
        git(
            &fixture.checkout,
            [
                OsStr::new("fetch"),
                fixture.origin.as_os_str(),
                OsStr::new(&fixture.head),
            ],
        );
        let missing = fixture.root.join("removed-legacy-worktree");
        git(
            &fixture.checkout,
            [
                OsStr::new("worktree"),
                OsStr::new("add"),
                OsStr::new("--detach"),
                missing.as_os_str(),
                OsStr::new(&fixture.head),
            ],
        );
        if remove_registration {
            git(
                &fixture.checkout,
                [
                    OsStr::new("worktree"),
                    OsStr::new("remove"),
                    missing.as_os_str(),
                ],
            );
        } else {
            fs::remove_dir_all(&missing).unwrap();
        }
        let (app, _) = fixture.app().await;
        let mut event = fixture.event(731, &fixture.head, Some(&missing));
        if remove_registration {
            let error = app.services().dispatch(&mut event).await.unwrap_err();
            assert!(format!("{error:#}").contains("no Git worktree registration"));
            assert!(!fixture.workspaces.exists());
            app.shutdown().await.unwrap();
            continue;
        }
        app.services().dispatch(&mut event).await.unwrap();
        assert_eq!(event.task.session.cwd, fixture.workspaces.join("pr-731"));
        assert_eq!(
            event.existing_session.as_ref().unwrap().thread_id,
            "same-thread"
        );
        assert_eq!(event.existing_session.as_ref().unwrap().cwd, missing);
        assert_eq!(
            event.task.session.git.unwrap().branch.as_deref(),
            Some("feature/pr-731")
        );
        assert_eq!(
            git(&event.task.session.cwd, ["rev-parse", "HEAD"]),
            fixture.head
        );
        assert!(!missing.exists());
        let trees = git(&fixture.checkout, ["worktree", "list", "--porcelain"]);
        assert_eq!(
            trees.contains(missing.to_str().unwrap()),
            !remove_registration
        );
        assert_eq!(git(&fixture.checkout, ["rev-parse", "HEAD"]), fixture.base);
        app.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn an_unknown_directory_probe_result_does_not_replace_the_bound_worktree() {
    struct FailedProbe(FixtureRunner);
    #[async_trait]
    impl CommandRunner for FailedProbe {
        async fn run(&self, request: CommandRequest) -> Result<CommandOutput> {
            if request.program == "test" && request.args.first().is_some_and(|arg| arg == "-d") {
                return Ok(CommandOutput {
                    status: Some(2),
                    stdout: Vec::new(),
                    stderr: b"probe failed".to_vec(),
                });
            }
            self.0.run(request).await
        }
    }
    let fixture = Fixture::new();
    let plugin = GitWorkspaces::new(
        "acme/sdk",
        fixture.checkout.clone(),
        fixture.workspaces.clone(),
    )
    .unwrap();
    let runner = FailedProbe(FixtureRunner {
        origin: fixture.origin.clone(),
        calls: Mutex::new(Vec::new()),
    });
    let mut event = fixture.event(731, &fixture.head, Some(&fixture.root.join("missing")));
    let error = plugin.prepare(&runner, &mut event).await.unwrap_err();
    assert!(format!("{error:#}").contains("cannot inspect bound session directory"));
    assert!(!fixture.workspaces.exists());
}

#[tokio::test]
async fn command_start_error_identifies_program_and_cwd_without_logging_arguments() {
    let directory = tempfile::tempdir().unwrap();
    let missing = directory.path().join("missing-cwd");
    let request = CommandRequest {
        cwd: missing.clone(),
        program: "git".into(),
        args: vec!["sensitive-argument".into()],
        timeout: std::time::Duration::from_secs(1),
    };
    let error = LocalCommandRunner.run(request).await.unwrap_err();
    let text = format!("{error:#}");
    assert!(text.contains("git"));
    assert!(text.contains(missing.to_str().unwrap()));
    assert!(!text.contains("sensitive-argument"));
}
