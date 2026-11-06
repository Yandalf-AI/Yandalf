#![cfg(unix)]

use std::{
    ffi::{OsStr, OsString},
    fs,
    os::unix::{ffi::OsStringExt, fs::PermissionsExt},
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Mutex},
};

use anyhow::Result;
use async_trait::async_trait;
use tempfile::TempDir;
use yandalf_core::plugin::{App, Plugin, PluginContext};
use yandalf_git_worktree::{
    CommandOutput, CommandRequest, CommandRunner, GitWorktree, LocalCommandRunner,
    PreparedWorktree, WorktreeRequest, WorktreeService,
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

fn git_args(request: &CommandRequest) -> &[OsString] {
    match request.args.as_slice() {
        [option, value, args @ ..] if option == "-c" && value == "core.hooksPath=/dev/null" => args,
        args => args,
    }
}

struct Fixture {
    _directory: TempDir,
    root: PathBuf,
    author: PathBuf,
    origin: PathBuf,
    checkout: PathBuf,
    worktrees: PathBuf,
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
        git(&author, ["config", "user.name", "WorktreeLocation Test"]);
        git(
            &author,
            ["config", "user.email", "worktree@example.invalid"],
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
        git(&author, ["switch", "-c", "topic"]);
        fs::write(author.join("value.txt"), "first revision\n").unwrap();
        git(&author, ["commit", "-am", "first"]);
        let head = git(&author, ["rev-parse", "HEAD"]);
        git(
            &author,
            [
                OsStr::new("push"),
                origin.as_os_str(),
                OsStr::new("HEAD:refs/heads/task-a"),
            ],
        );
        Self {
            _directory: directory,
            worktrees: root.join("worktrees"),
            root,
            author,
            origin,
            checkout,
            base,
            head,
        }
    }

    fn advance(&self, name: &str, content: &str) -> String {
        fs::write(self.author.join("value.txt"), content).unwrap();
        git(&self.author, ["commit", "-am", "next"]);
        git(
            &self.author,
            [
                OsStr::new("push"),
                self.origin.as_os_str(),
                OsStr::new(&format!("HEAD:refs/heads/{name}")),
            ],
        );
        git(&self.author, ["rev-parse", "HEAD"])
    }

    fn request(&self, name: &str, revision: &str, existing: Option<&Path>) -> WorktreeRequest {
        WorktreeRequest {
            checkout: self.checkout.clone(),
            path: self.worktrees.join(name),
            existing_path: existing.map(Path::to_owned),
            owner: format!("source:{name}"),
            revision: revision.into(),
            branch: format!("feature/{name}"),
            fallback_branch: format!("worktree/{name}"),
            required_revisions: vec![self.base.clone()],
            fetch_refs: vec![format!("refs/heads/{name}")],
            expected_origin: self.origin.to_string_lossy().into_owned(),
        }
    }

    async fn app(&self, allow_hooks: bool) -> (App, Arc<RecordingRunner>, Arc<WorktreeService>) {
        let runner = Arc::new(RecordingRunner::default());
        let app = build_app(runner.clone(), allow_hooks).await;
        let service = app.services().get::<WorktreeService>().unwrap();
        (app, runner, service)
    }

    fn detached(&self, name: &str) -> PathBuf {
        git(
            &self.checkout,
            [
                OsStr::new("fetch"),
                self.origin.as_os_str(),
                OsStr::new(&self.head),
            ],
        );
        let path = self.root.join(name);
        git(
            &self.checkout,
            [
                OsStr::new("worktree"),
                OsStr::new("add"),
                OsStr::new("--detach"),
                path.as_os_str(),
                OsStr::new(&self.head),
            ],
        );
        path
    }
}

#[derive(Default)]
struct RecordingRunner {
    calls: Mutex<Vec<CommandRequest>>,
    edit_on_reset: bool,
    fail_directory_probe: bool,
}

#[async_trait]
impl CommandRunner for RecordingRunner {
    async fn run(&self, request: CommandRequest) -> Result<CommandOutput> {
        self.calls.lock().unwrap().push(request.clone());
        if self.fail_directory_probe
            && request.program == "test"
            && request.args.first().is_some_and(|arg| arg == "-d")
        {
            return Ok(CommandOutput {
                status: Some(2),
                stdout: vec![],
                stderr: b"probe failed".to_vec(),
            });
        }
        if self.edit_on_reset
            && request.program == "git"
            && git_args(&request).first().is_some_and(|arg| arg == "reset")
        {
            fs::write(request.cwd.join("value.txt"), "concurrent user edit\n").unwrap();
        }
        LocalCommandRunner.run(request).await
    }
}

struct Commands(Arc<dyn CommandRunner>);
impl Plugin for Commands {
    fn name(&self) -> &str {
        "test.commands"
    }
    fn register(&self, ctx: &mut PluginContext) -> Result<()> {
        ctx.provide::<dyn CommandRunner>(self.0.clone())
    }
}

async fn build_app(runner: Arc<dyn CommandRunner>, allow_hooks: bool) -> App {
    let plugin = if allow_hooks {
        GitWorktree::default().with_allow_hooks(true)
    } else {
        GitWorktree::default()
    };
    App::builder()
        .plugin(plugin)
        .plugin(Commands(runner))
        .build()
        .await
        .unwrap()
}

async fn prepare(service: &WorktreeService, request: WorktreeRequest) -> PreparedWorktree {
    service.prepare(&request).await.unwrap()
}

#[tokio::test]
async fn prepares_independent_worktrees_at_pinned_revisions_without_touching_primary() {
    let f = Fixture::new();
    let next = f.advance("task-b", "another task\n");
    let (app, runner, service) = f.app(false).await;
    let first = f.request("task-a", &f.head, None);
    let second = f.request("task-b", &next, None);
    let (first, second) = tokio::join!(service.prepare(&first), service.prepare(&second));
    let (first, second) = (first.unwrap(), second.unwrap());
    for (prepared, name, revision) in [(&first, "task-a", &f.head), (&second, "task-b", &next)] {
        assert_eq!(prepared.cwd, f.worktrees.join(name));
        assert_eq!(prepared.git.sha, *revision);
        assert_eq!(
            prepared.git.branch.as_deref(),
            Some(format!("feature/{name}").as_str())
        );
        assert_eq!(git(&prepared.cwd, ["rev-parse", "HEAD"]), *revision);
    }
    assert_eq!(git(&f.checkout, ["rev-parse", "HEAD"]), f.base);
    assert_eq!(git(&f.checkout, ["branch", "--show-current"]), "main");
    assert!(
        runner
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|c| git_args(c).first().is_some_and(|a| a == "fetch"))
    );
    app.shutdown().await.unwrap();
    assert!(first.cwd.exists());
}

#[tokio::test]
async fn reuses_owned_worktree_after_restart_and_preserves_ignored_cache_on_update() {
    let f = Fixture::new();
    let (app, _, service) = f.app(false).await;
    let first = prepare(&service, f.request("task-a", &f.head, None)).await;
    fs::create_dir(first.cwd.join("ignored")).unwrap();
    fs::write(first.cwd.join("ignored/cache"), "keep").unwrap();
    app.shutdown().await.unwrap();
    let (app, _, service) = f.app(false).await;
    let retry = prepare(&service, f.request("task-a", &f.head, None)).await;
    assert_eq!(retry.cwd, first.cwd);
    let next = f.advance("task-a", "updated\n");
    let update = prepare(&service, f.request("task-a", &next, Some(&first.cwd))).await;
    assert_eq!(update.cwd, first.cwd);
    assert_eq!(update.git.sha, next);
    assert_eq!(
        fs::read_to_string(first.cwd.join("ignored/cache")).unwrap(),
        "keep"
    );
    assert_eq!(
        git(&f.checkout, ["worktree", "list", "--porcelain"])
            .matches("worktree ")
            .count(),
        2
    );
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn tracked_checkout_hooks_require_explicit_opt_in_without_changing_repo_configuration() {
    for allow_hooks in [false, true] {
        let f = Fixture::new();
        let hook = f.author.join(".hooks/post-checkout");
        fs::create_dir(hook.parent().unwrap()).unwrap();
        fs::write(&hook, "#!/bin/sh\nprintf executed > ../hook-ran\n").unwrap();
        fs::set_permissions(&hook, fs::Permissions::from_mode(0o755)).unwrap();
        git(&f.author, ["add", ".hooks/post-checkout"]);
        let head = f.advance("task-a", "tracked hook\n");
        git(&f.checkout, ["config", "core.hooksPath", ".hooks"]);
        let (app, runner, service) = f.app(allow_hooks).await;
        prepare(&service, f.request("task-a", &head, None)).await;
        assert_eq!(f.worktrees.join("hook-ran").exists(), allow_hooks);
        assert_eq!(
            git(
                &f.checkout,
                ["config", "--local", "--get", "core.hooksPath"]
            ),
            ".hooks"
        );
        for call in runner
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|call| call.program == "git")
        {
            assert_eq!(git_args(call).len() == call.args.len(), allow_hooks);
        }
        app.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn renames_owned_fallback_branch_without_moving_worktree_or_discarding_local_edits() {
    let f = Fixture::new();
    let (app, _, service) = f.app(false).await;
    let first = prepare(&service, f.request("task-a", &f.head, None)).await;
    git(&first.cwd, ["branch", "-m", "worktree/task-a"]);
    git(
        &first.cwd,
        [
            "config",
            "--unset",
            "branch.worktree/task-a.yandalf-subject",
        ],
    );
    fs::write(first.cwd.join("value.txt"), "local investigation\n").unwrap();
    let updated = prepare(&service, f.request("task-a", &f.head, Some(&first.cwd))).await;
    assert_eq!(updated.cwd, first.cwd);
    assert_eq!(updated.git.branch.as_deref(), Some("feature/task-a"));
    assert_eq!(
        fs::read_to_string(first.cwd.join("value.txt")).unwrap(),
        "local investigation\n"
    );
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn follows_requested_branch_rename_with_same_or_changed_revision_and_preserves_conflicts() {
    for advance in [false, true] {
        let f = Fixture::new();
        let (app, _, service) = f.app(false).await;
        let first = prepare(&service, f.request("task-a", &f.head, None)).await;
        let head = if advance {
            f.advance("task-a", "renamed and updated\n")
        } else {
            f.head.clone()
        };
        let mut request = f.request("task-a", &head, Some(&first.cwd));
        request.branch = "renamed/topic".into();
        let renamed = prepare(&service, request.clone()).await;
        assert_eq!(renamed.cwd, first.cwd);
        assert_eq!(renamed.git.sha, head);
        assert_eq!(renamed.git.branch.as_deref(), Some("renamed/topic"));
        assert_eq!(
            git(
                &renamed.cwd,
                ["config", "--get", "branch.renamed/topic.yandalf-subject"]
            ),
            "source:task-a"
        );
        git(&f.checkout, ["branch", "occupied", &f.base]);
        request.branch = "occupied".into();
        let fallback = prepare(&service, request).await;
        assert_eq!(fallback.cwd, first.cwd);
        assert_eq!(fallback.git.branch.as_deref(), Some("worktree/task-a"));
        assert_eq!(git(&f.checkout, ["rev-parse", "occupied"]), f.base);
        app.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn conflicting_local_refs_are_preserved_while_owned_revision_advances() {
    for conflict in ["checked-out", "unused", "prefix"] {
        let f = Fixture::new();
        let (branch, foreign) = match conflict {
            "checked-out" => ("main", "main"),
            "unused" => {
                git(&f.checkout, ["branch", "feature/task-a"]);
                ("feature/task-a", "feature/task-a")
            }
            _ => {
                git(&f.checkout, ["branch", "feature"]);
                ("feature/task-a", "feature")
            }
        };
        let (app, _, service) = f.app(false).await;
        let mut request = f.request("task-a", &f.head, None);
        request.branch = branch.into();
        let first = prepare(&service, request.clone()).await;
        request.existing_path = Some(first.cwd.clone());
        request.revision = f.advance("task-a", "new revision\n");
        let updated = prepare(&service, request).await;
        assert_eq!(updated.git.branch.as_deref(), Some("worktree/task-a"));
        assert_eq!(git(&f.checkout, ["rev-parse", foreign]), f.base);
        app.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn independent_owners_with_same_requested_branch_get_separate_worktrees() {
    let f = Fixture::new();
    let next = f.advance("task-b", "other owner\n");
    let (app, _, service) = f.app(false).await;
    let first = prepare(&service, f.request("task-a", &f.head, None)).await;
    let mut request = f.request("task-b", &next, None);
    request.branch = "feature/task-a".into();
    request.owner = "scheduler:daily-build".into();
    let second = prepare(&service, request).await;
    assert_eq!(first.git.branch.as_deref(), Some("feature/task-a"));
    assert_eq!(second.git.branch.as_deref(), Some("worktree/task-b"));
    assert_eq!(git(&first.cwd, ["rev-parse", "HEAD"]), f.head);
    assert_eq!(second.git.sha, next);
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn never_moves_a_branch_marked_as_owned_by_someone_else() {
    let f = Fixture::new();
    let (app, _, service) = f.app(false).await;
    let first = prepare(&service, f.request("task-a", &f.head, None)).await;
    git(&first.cwd, ["switch", "-c", "foreign"]);
    git(
        &first.cwd,
        ["config", "branch.foreign.yandalf-subject", "other:owner"],
    );
    let next = f.advance("task-a", "new revision\n");
    let mut request = f.request("task-a", &next, Some(&first.cwd));
    request.branch = "foreign".into();
    assert!(
        format!("{:#}", service.prepare(&request).await.unwrap_err())
            .contains("owned by another task")
    );
    assert_eq!(git(&first.cwd, ["rev-parse", "HEAD"]), f.head);
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_requests_are_rejected_before_mutations() {
    let f = Fixture::new();
    git(&f.checkout, ["switch", "-c", "previous"]);
    git(&f.checkout, ["switch", "main"]);
    let (app, runner, service) = f.app(false).await;
    for invalid in ["", "../outside", "--detach", "HEAD", "@{-1}", "bad\nbranch"] {
        let mut request = f.request("task-a", &f.head, None);
        request.branch = invalid.into();
        assert!(service.prepare(&request).await.is_err());
    }
    for invalid in [
        "refs/../outside",
        "--upload-pack=x",
        "main",
        "refs/heads/bad\nref",
    ] {
        let mut request = f.request("task-a", &f.head, None);
        request.fetch_refs = vec![invalid.into()];
        assert!(service.prepare(&request).await.is_err());
    }
    for kind in ["path", "existing", "nul", "sha", "required", "owner"] {
        let mut request = f.request("task-a", &f.head, None);
        match kind {
            "path" => request.path = f.root.join("../outside"),
            "existing" => request.existing_path = Some(PathBuf::from("relative")),
            "nul" => request.path = PathBuf::from(OsString::from_vec(b"/invalid\0path".to_vec())),
            "sha" => request.revision = "short".into(),
            "required" => request.required_revisions = vec!["invalid".into()],
            _ => request.owner.clear(),
        };
        assert!(service.prepare(&request).await.is_err());
    }
    assert!(!f.worktrees.exists());
    assert!(runner.calls.lock().unwrap().iter().all(|c| {
        c.program == "git"
            && git_args(c)
                .first()
                .is_some_and(|a| a == "rev-parse" || a == "remote" || a == "check-ref-format")
    }));
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn dirty_same_revision_is_preserved_but_updates_refuse_tracked_and_untracked_edits() {
    let f = Fixture::new();
    let (app, _, service) = f.app(false).await;
    let first = prepare(&service, f.request("task-a", &f.head, None)).await;
    fs::write(first.cwd.join("value.txt"), "uncommitted\n").unwrap();
    fs::write(first.cwd.join("scratch"), "repro").unwrap();
    prepare(&service, f.request("task-a", &f.head, Some(&first.cwd))).await;
    let next = f.advance("task-a", "updated\n");
    let request = f.request("task-a", &next, Some(&first.cwd));
    assert!(service.prepare(&request).await.is_err());
    git(&first.cwd, ["restore", "value.txt"]);
    assert!(service.prepare(&request).await.is_err());
    assert_eq!(git(&first.cwd, ["rev-parse", "HEAD"]), f.head);
    assert_eq!(
        fs::read_to_string(first.cwd.join("scratch")).unwrap(),
        "repro"
    );
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn authoritative_existing_detached_worktree_is_reused_without_attachment() {
    let f = Fixture::new();
    let existing = f.detached("native-worktree");
    let next = f.advance("task-a", "updated\n");
    let (app, _, service) = f.app(false).await;
    let prepared = prepare(&service, f.request("task-a", &next, Some(&existing))).await;
    assert_eq!(prepared.cwd, existing);
    assert_eq!(prepared.git.branch, None);
    assert_eq!(prepared.git.sha, next);
    assert!(!f.worktrees.exists());
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn refuses_foreign_existing_repositories_and_unowned_destination_paths() {
    let f = Fixture::new();
    let foreign = f.root.join("foreign");
    fs::create_dir(&foreign).unwrap();
    git(&foreign, ["init", "-b", "main"]);
    let (app, _, service) = f.app(false).await;
    assert!(
        format!(
            "{:#}",
            service
                .prepare(&f.request("task-a", &f.head, Some(&foreign)))
                .await
                .unwrap_err()
        )
        .contains("different repository")
    );
    let path = f.worktrees.join("task-a");
    fs::create_dir_all(&path).unwrap();
    fs::write(path.join("user-file"), "keep").unwrap();
    assert!(
        service
            .prepare(&f.request("task-a", &f.head, None))
            .await
            .is_err()
    );
    assert_eq!(fs::read_to_string(path.join("user-file")).unwrap(), "keep");
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn origin_is_read_only_and_exact_mismatch_prevents_fetch_and_directory_creation() {
    let f = Fixture::new();
    let (app, runner, service) = f.app(false).await;
    let origin = service.origin(&f.checkout).await.unwrap();
    assert_eq!(origin, f.origin.to_string_lossy());
    let mut request = f.request("task-a", &f.head, None);
    request.expected_origin.push('/');
    assert!(
        format!("{:#}", service.prepare(&request).await.unwrap_err()).contains("expected origin")
    );
    assert!(!f.worktrees.exists());
    assert!(runner.calls.lock().unwrap().iter().all(|c| {
        c.program == "git"
            && git_args(c)
                .first()
                .is_some_and(|a| a == "rev-parse" || a == "remote")
    }));
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn finishes_partially_created_worktree_at_observed_commit_after_remote_moves() {
    let f = Fixture::new();
    let newer = f.advance("task-a", "remote advanced\n");
    let (app, _, service) = f.app(false).await;
    let first = prepare(&service, f.request("task-a", &f.head, None)).await;
    assert_ne!(first.git.sha, newer);
    assert_eq!(first.git.sha, f.head);
    let request = f.request("task-b", &f.head, None);
    git(
        &f.checkout,
        [
            OsStr::new("worktree"),
            OsStr::new("add"),
            OsStr::new("--detach"),
            OsStr::new("--lock"),
            OsStr::new("--reason"),
            OsStr::new("yandalf:source:task-b"),
            request.path.as_os_str(),
            OsStr::new(&f.head),
        ],
    );
    let prepared = prepare(&service, request).await;
    assert_eq!(prepared.cwd, f.worktrees.join("task-b"));
    assert_eq!(prepared.git.branch.as_deref(), Some("feature/task-b"));
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn never_retargets_primary_when_configured_reference_checkout_is_linked() {
    let f = Fixture::new();
    let reference = f.detached("reference");
    let (app, _, service) = f.app(false).await;
    let mut request = f.request("task-a", &f.head, Some(&f.checkout));
    request.checkout = reference;
    let prepared = prepare(&service, request).await;
    assert_eq!(prepared.cwd, f.worktrees.join("task-a"));
    assert_eq!(git(&f.checkout, ["rev-parse", "HEAD"]), f.base);
    assert_eq!(git(&f.checkout, ["branch", "--show-current"]), "main");
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn concurrent_local_edit_between_clean_check_and_reset_is_not_overwritten() {
    let f = Fixture::new();
    let (app, _, service) = f.app(false).await;
    let first = prepare(&service, f.request("task-a", &f.head, None)).await;
    app.shutdown().await.unwrap();
    let next = f.advance("task-a", "new committed content\n");
    let app = build_app(
        Arc::new(RecordingRunner {
            edit_on_reset: true,
            ..Default::default()
        }),
        false,
    )
    .await;
    let service = app.services().get::<WorktreeService>().unwrap();
    assert!(
        service
            .prepare(&f.request("task-a", &next, Some(&first.cwd)))
            .await
            .is_err()
    );
    assert_eq!(git(&first.cwd, ["rev-parse", "HEAD"]), f.head);
    assert_eq!(
        fs::read_to_string(first.cwd.join("value.txt")).unwrap(),
        "concurrent user edit\n"
    );
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn missing_registered_worktree_gets_replacement_without_pruning_old_registration() {
    for remove_registration in [false, true] {
        let f = Fixture::new();
        let missing = f.detached("removed-native-worktree");
        if remove_registration {
            git(
                &f.checkout,
                [
                    OsStr::new("worktree"),
                    OsStr::new("remove"),
                    missing.as_os_str(),
                ],
            );
        } else {
            fs::remove_dir_all(&missing).unwrap();
        }
        let (app, _, service) = f.app(false).await;
        let request = f.request("task-a", &f.head, Some(&missing));
        let result = service.prepare(&request).await;
        if remove_registration {
            assert!(format!("{:#}", result.unwrap_err()).contains("no Git worktree registration"));
            assert!(!f.worktrees.exists());
        } else {
            assert_eq!(result.unwrap().cwd, f.worktrees.join("task-a"));
            assert!(
                git(&f.checkout, ["worktree", "list", "--porcelain"])
                    .contains(missing.to_str().unwrap())
            );
        }
        app.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn unknown_directory_probe_does_not_replace_authoritative_path() {
    let f = Fixture::new();
    let app = build_app(
        Arc::new(RecordingRunner {
            fail_directory_probe: true,
            ..Default::default()
        }),
        false,
    )
    .await;
    let service = app.services().get::<WorktreeService>().unwrap();
    let request = f.request("task-a", &f.head, Some(&f.root.join("missing")));
    assert!(
        format!("{:#}", service.prepare(&request).await.unwrap_err())
            .contains("cannot inspect bound worktree directory")
    );
    assert!(!f.worktrees.exists());
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn arbitrary_fetch_refs_and_required_commits_work_with_a_local_origin() {
    for fetch_reference in [false, true] {
        let f = Fixture::new();
        let required = f.advance("task-a", "required commit\n");
        git(
            &f.author,
            [
                OsStr::new("push"),
                f.origin.as_os_str(),
                OsStr::new("HEAD:refs/changes/build-input"),
            ],
        );
        let (app, runner, service) = f.app(false).await;
        let mut request = f.request("task-a", &f.head, None);
        request.owner = "build-input:987".into();
        request.required_revisions.push(required.clone());
        request.fetch_refs = if fetch_reference {
            vec!["refs/changes/build-input".into()]
        } else {
            vec![]
        };
        request.revision = request.revision.to_ascii_uppercase();
        let prepared = prepare(&service, request).await;
        assert_eq!(prepared.git.sha, f.head);
        assert_eq!(
            prepared.git.origin_url.as_deref(),
            Some(f.origin.to_str().unwrap())
        );
        assert_eq!(git(&f.checkout, ["cat-file", "-t", &required]), "commit");
        if fetch_reference {
            assert!(runner.calls.lock().unwrap().iter().any(|call| {
                git_args(call)
                    .last()
                    .is_some_and(|arg| arg == "refs/changes/build-input")
            }));
        }
        app.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn command_start_errors_identify_program_and_cwd_without_argument_values() {
    let directory = tempfile::tempdir().unwrap();
    let missing = directory.path().join("missing-cwd");
    let request = CommandRequest {
        cwd: missing.clone(),
        program: "git".into(),
        args: vec!["sensitive-argument".into()],
        timeout: std::time::Duration::from_secs(1),
    };
    let text = format!("{:#}", LocalCommandRunner.run(request).await.unwrap_err());
    assert!(text.contains("git"));
    assert!(text.contains(missing.to_str().unwrap()));
    assert!(!text.contains("sensitive-argument"));
}
