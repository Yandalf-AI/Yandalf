use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    path::{Component, Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use tokio::{process::Command, sync::Mutex, time::timeout};

use yandalf_core::{
    domain::GitInfo,
    plugin::{Plugin, PluginContext},
    task::TaskPreparing,
};

#[derive(Clone, Debug)]
pub struct CommandRequest {
    pub cwd: PathBuf,
    pub program: OsString,
    pub args: Vec<OsString>,
    pub timeout: Duration,
}

#[derive(Clone, Debug)]
pub struct CommandOutput {
    pub status: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

impl CommandOutput {
    fn success(&self) -> bool {
        self.status == Some(0)
    }

    fn text(self) -> Result<String> {
        ensure!(
            self.success(),
            "workspace command failed: {}",
            String::from_utf8_lossy(&self.stderr).trim()
        );
        String::from_utf8(self.stdout).context("workspace command returned non-UTF-8 data")
    }
}

#[async_trait]
pub trait CommandRunner: Send + Sync {
    async fn run(&self, request: CommandRequest) -> Result<CommandOutput>;
}

pub struct LocalCommandRunner;

#[async_trait]
impl CommandRunner for LocalCommandRunner {
    async fn run(&self, request: CommandRequest) -> Result<CommandOutput> {
        let output = timeout(
            request.timeout,
            Command::new(&request.program)
                .args(&request.args)
                .current_dir(&request.cwd)
                .env_remove("GIT_DIR")
                .env_remove("GIT_WORK_TREE")
                .env_remove("GIT_COMMON_DIR")
                .env_remove("GIT_INDEX_FILE")
                .env_remove("GIT_NAMESPACE")
                .env("GIT_TERMINAL_PROMPT", "0")
                .stdin(Stdio::null())
                .kill_on_drop(true)
                .output(),
        )
        .await
        .with_context(|| {
            format!(
                "workspace command {:?} timed out in {}",
                request.program,
                request.cwd.display()
            )
        })?
        .with_context(|| {
            format!(
                "start workspace command {:?} in {}",
                request.program,
                request.cwd.display()
            )
        })?;
        Ok(CommandOutput {
            status: output.status.code(),
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }
}

pub struct LocalCommands;

impl Plugin for LocalCommands {
    fn name(&self) -> &str {
        "commands.local"
    }

    fn register(&self, ctx: &mut PluginContext) -> Result<()> {
        ctx.provide::<dyn CommandRunner>(Arc::new(LocalCommandRunner))
    }
}

#[derive(Clone)]
pub struct GitWorkspaces {
    name: String,
    repository: String,
    checkout: PathBuf,
    root: PathBuf,
    allow_hooks: bool,
    lock: Arc<Mutex<()>>,
}

impl GitWorkspaces {
    pub fn new(repository: &str, checkout: PathBuf, root: PathBuf) -> Result<Self> {
        let repository = canonical_repository(repository)?;
        for path in [&checkout, &root] {
            ensure!(path.is_absolute(), "Git workspace paths must be absolute");
            ensure!(
                !path
                    .components()
                    .any(|component| matches!(component, Component::ParentDir)),
                "Git workspace paths must not contain parent traversal"
            );
        }
        Ok(Self {
            name: format!("workspaces.git:{repository}"),
            repository,
            checkout,
            root,
            allow_hooks: false,
            lock: Arc::new(Mutex::new(())),
        })
    }

    pub fn with_allow_hooks(mut self, allow_hooks: bool) -> Self {
        self.allow_hooks = allow_hooks;
        self
    }

    pub async fn prepare(
        &self,
        runner: &dyn CommandRunner,
        event: &mut TaskPreparing,
    ) -> Result<()> {
        let Some(pull) = PullRequest::decode(&self.repository, event)? else {
            return Ok(());
        };
        let _lock = self.lock.lock().await;
        let git = Git {
            runner,
            allow_hooks: self.allow_hooks,
        };
        let mut repository =
            Repository::inspect(&git, &self.checkout, &self.repository, &pull).await?;
        let workspace = self
            .resolve_workspace(&git, &repository, event, &pull)
            .await?;
        git.prepare_worktree(&mut repository, &workspace, &pull)
            .await?;
        let branch = BranchPlan::inspect(&git, &repository, &workspace, &pull).await?;
        git.update_worktree(&workspace.path, &branch, &pull).await?;
        event.task.session.git = Some(git.metadata(&workspace.path, &repository, &pull).await?);
        event.task.session.cwd = workspace.path;
        Ok(())
    }

    async fn resolve_workspace(
        &self,
        git: &Git<'_>,
        repository: &Repository,
        event: &TaskPreparing,
        pull: &PullRequest,
    ) -> Result<Workspace> {
        if let Some(path) = Workspace::bound(git, repository, event).await? {
            return Ok(Workspace {
                path,
                adopted: true,
            });
        }
        git.command(
            &repository.checkout,
            "mkdir",
            [OsStr::new("-p"), OsStr::new("--"), self.root.as_os_str()],
        )
        .await?
        .text()?;
        let root = PathBuf::from(git.command(&self.root, "pwd", ["-P"]).await?.text()?.trim());
        ensure!(
            root.is_absolute(),
            "workspace root did not resolve to an absolute path"
        );
        let path = root.join(format!("pr-{}", pull.number));
        ensure!(
            path != repository.checkout,
            "review workspace cannot be the configured checkout"
        );
        if let Some(tree) = repository.trees.get(&path) {
            ensure!(
                tree.locked.as_deref() == Some(pull.lock_reason.as_str()),
                "workspace path is already owned by another task"
            );
        }
        Ok(Workspace {
            path,
            adopted: false,
        })
    }
}

struct PullRequest {
    number: u64,
    subject: String,
    head: String,
    base: String,
    head_ref: String,
    review_branch: String,
    lock_reason: String,
}

impl PullRequest {
    fn decode(repository: &str, event: &TaskPreparing) -> Result<Option<Self>> {
        let event = &event.job.event;
        if event.source != format!("github:{repository}") {
            return Ok(None);
        }
        let context = &event.context;
        ensure!(
            context["repository"] == repository,
            "Git workspace repository mismatch"
        );
        let number = context["pr"]
            .as_u64()
            .filter(|number| *number > 0)
            .context("missing pull request number")?;
        ensure!(
            event.subject == format!("github:{repository}/pr/{number}"),
            "Git workspace subject does not identify this pull request"
        );
        let head = revision(context["head_sha"].as_str())?;
        let base = revision(context["base_sha"].as_str())?;
        let head_ref = context["pull"]["head_ref"]
            .as_str()
            .filter(|name| !name.is_empty())
            .context("missing pull request head branch")?
            .to_owned();
        Ok(Some(Self {
            number,
            subject: event.subject.clone(),
            head,
            base,
            head_ref,
            review_branch: format!("review/pr-{number}"),
            lock_reason: format!("yandalf:{}", event.subject),
        }))
    }
}

struct Repository {
    checkout: PathBuf,
    origin: String,
    common: PathBuf,
    trees: BTreeMap<PathBuf, Worktree>,
}

impl Repository {
    async fn inspect(
        git: &Git<'_>,
        checkout: &Path,
        expected: &str,
        pull: &PullRequest,
    ) -> Result<Self> {
        let checkout = git.top(checkout).await?;
        let checked_branch = git
            .output(&checkout, ["check-ref-format", "--branch", &pull.head_ref])
            .await?;
        ensure!(
            checked_branch.success() && checked_branch.text()?.trim() == pull.head_ref,
            "invalid pull request head branch"
        );
        let origin = git.text(&checkout, ["remote", "get-url", "origin"]).await?;
        ensure!(
            origin_repository(&origin).as_deref() == Some(expected),
            "configured checkout origin does not match GitHub repository"
        );
        Ok(Self {
            common: git.common(&checkout).await?,
            trees: git.worktrees(&checkout).await?,
            checkout,
            origin,
        })
    }
}

struct Workspace {
    path: PathBuf,
    adopted: bool,
}

impl Workspace {
    async fn bound(
        git: &Git<'_>,
        repository: &Repository,
        event: &TaskPreparing,
    ) -> Result<Option<PathBuf>> {
        if event.existing_session.is_none() {
            return Ok(None);
        }
        let cwd = &event.task.session.cwd;
        let directory = git
            .command(
                &repository.checkout,
                "test",
                [OsStr::new("-d"), cwd.as_os_str()],
            )
            .await?;
        ensure!(
            matches!(directory.status, Some(0 | 1)),
            "cannot inspect bound session directory"
        );
        if !directory.success() {
            let tree = repository
                .trees
                .get(cwd)
                .context("missing bound session directory has no Git worktree registration")?;
            ensure!(
                !tree.primary && tree.prunable,
                "bound session directory is not a confirmed missing worktree"
            );
            return Ok(None);
        }
        let path = git.top(cwd).await?;
        ensure!(
            git.common(&path).await? == repository.common,
            "bound session belongs to a different repository"
        );
        let tree = repository
            .trees
            .get(&path)
            .context("bound session is not a registered Git worktree")?;
        Ok((path != repository.checkout && !tree.primary).then_some(path))
    }
}

struct BranchPlan {
    current: Option<String>,
    target: Option<String>,
    owned: bool,
}

impl BranchPlan {
    async fn inspect(
        git: &Git<'_>,
        repository: &Repository,
        workspace: &Workspace,
        pull: &PullRequest,
    ) -> Result<Self> {
        let tree = repository
            .trees
            .get(&workspace.path)
            .context("Git did not register the requested worktree")?;
        let current = git.branch(&workspace.path).await?;
        let owned = tree.locked.as_deref() == Some(pull.lock_reason.as_str());
        if !owned {
            return Ok(Self {
                current,
                target: None,
                owned,
            });
        }
        let marked = if let Some(current) = &current {
            git.branch_owner(&workspace.path, current, &pull.subject)
                .await?
        } else {
            false
        };
        ensure!(
            marked
                || current.as_deref().is_none_or(|current| {
                    current == pull.head_ref || current == pull.review_branch
                }),
            "worktree is attached to a different branch"
        );
        let references = git
            .text(
                &repository.checkout,
                ["for-each-ref", "--format=%(refname)", "refs/heads/"],
            )
            .await?;
        let available = |name: &str| {
            let reference = format!("refs/heads/{name}");
            !references.lines().any(|existing| {
                existing == reference
                    || existing.starts_with(&format!("{reference}/"))
                    || reference.starts_with(&format!("{existing}/"))
            }) && !repository.trees.iter().any(|(other_path, tree)| {
                other_path != &workspace.path && tree.branch.as_deref() == Some(reference.as_str())
            })
        };
        let target =
            if current.as_deref() == Some(pull.head_ref.as_str()) || available(&pull.head_ref) {
                pull.head_ref.clone()
            } else {
                ensure!(
                    current.as_deref() == Some(pull.review_branch.as_str())
                        || available(&pull.review_branch),
                    "pull request and review branch names are already in use"
                );
                pull.review_branch.clone()
            };
        Ok(Self {
            current,
            target: Some(target),
            owned,
        })
    }
}

impl Plugin for GitWorkspaces {
    fn name(&self) -> &str {
        &self.name
    }

    fn register(&self, ctx: &mut PluginContext) -> Result<()> {
        ctx.require::<dyn CommandRunner>();
        let workspace = self.clone();
        ctx.on::<TaskPreparing, _>(move |services, event| {
            let workspace = workspace.clone();
            Box::pin(async move {
                let runner = services.get::<dyn CommandRunner>()?;
                workspace.prepare(runner.as_ref(), event).await
            })
        });
        Ok(())
    }
}

struct Git<'a> {
    runner: &'a dyn CommandRunner,
    allow_hooks: bool,
}

impl Git<'_> {
    async fn command<I, S>(&self, cwd: &Path, program: &str, args: I) -> Result<CommandOutput>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.runner
            .run(CommandRequest {
                cwd: cwd.to_owned(),
                program: program.into(),
                args: args
                    .into_iter()
                    .map(|arg| arg.as_ref().to_owned())
                    .collect(),
                timeout: Duration::from_secs(120),
            })
            .await
    }

    async fn output<I, S>(&self, cwd: &Path, args: I) -> Result<CommandOutput>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let args = args.into_iter().map(|arg| arg.as_ref().to_owned());
        let options = if self.allow_hooks {
            Vec::new()
        } else {
            vec![
                OsString::from("-c"),
                OsString::from("core.hooksPath=/dev/null"),
            ]
        };
        self.command(cwd, "git", options.into_iter().chain(args))
            .await
    }

    async fn text<I, S>(&self, cwd: &Path, args: I) -> Result<String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        Ok(self.output(cwd, args).await?.text()?.trim().to_owned())
    }

    async fn top(&self, path: &Path) -> Result<PathBuf> {
        let top = PathBuf::from(
            self.text(
                path,
                ["rev-parse", "--path-format=absolute", "--show-toplevel"],
            )
            .await?,
        );
        ensure!(top.is_absolute(), "Git working tree root must be absolute");
        Ok(top)
    }

    async fn common(&self, path: &Path) -> Result<PathBuf> {
        let common = PathBuf::from(
            self.text(
                path,
                ["rev-parse", "--path-format=absolute", "--git-common-dir"],
            )
            .await?,
        );
        ensure!(
            common.is_absolute(),
            "Git common directory must be absolute"
        );
        Ok(common)
    }

    async fn branch(&self, path: &Path) -> Result<Option<String>> {
        let output = self
            .output(path, ["symbolic-ref", "--quiet", "--short", "HEAD"])
            .await?;
        if output.status == Some(1) {
            return Ok(None);
        }
        Ok(Some(output.text()?.trim().to_owned()))
    }

    async fn branch_owner(&self, path: &Path, branch: &str, subject: &str) -> Result<bool> {
        let owner = self
            .output(
                path,
                [
                    "config",
                    "--get",
                    &format!("branch.{branch}.yandalf-subject"),
                ],
            )
            .await?;
        if !owner.success() {
            ensure!(owner.status == Some(1), "cannot inspect branch ownership");
            return Ok(false);
        }
        ensure!(
            owner.text()?.trim() == subject,
            "worktree branch is owned by another task"
        );
        Ok(true)
    }

    async fn prepare_worktree(
        &self,
        repository: &mut Repository,
        workspace: &Workspace,
        pull: &PullRequest,
    ) -> Result<()> {
        let checkout = &repository.checkout;
        let path = &workspace.path;
        self.ensure_revisions(checkout, pull.number, &pull.head, &pull.base)
            .await?;
        if !repository.trees.contains_key(path) {
            ensure!(!workspace.adopted, "bound worktree disappeared");
            let exists = self
                .command(checkout, "test", [OsStr::new("-e"), path.as_os_str()])
                .await?;
            let symlink = self
                .command(checkout, "test", [OsStr::new("-L"), path.as_os_str()])
                .await?;
            ensure!(
                matches!(exists.status, Some(0 | 1)) && matches!(symlink.status, Some(0 | 1)),
                "cannot inspect worktree destination"
            );
            ensure!(
                !exists.success() && !symlink.success(),
                "worktree destination exists without a matching registration"
            );
            self.text(
                checkout,
                [
                    OsStr::new("worktree"),
                    OsStr::new("add"),
                    OsStr::new("--detach"),
                    OsStr::new("--lock"),
                    OsStr::new("--reason"),
                    OsStr::new(&pull.lock_reason),
                    path.as_os_str(),
                    OsStr::new(&pull.head),
                ],
            )
            .await?;
            repository.trees = self.worktrees(checkout).await?;
        }
        let tree = repository
            .trees
            .get(path)
            .context("Git did not register the requested worktree")?;
        ensure!(
            !tree.primary,
            "review workspace cannot be the primary checkout"
        );
        ensure!(
            self.common(path).await? == repository.common,
            "prepared worktree belongs to another repository"
        );
        if !workspace.adopted {
            ensure!(
                tree.locked.as_deref() == Some(pull.lock_reason.as_str()),
                "Git did not preserve worktree ownership"
            );
        }
        Ok(())
    }

    async fn update_worktree(
        &self,
        path: &Path,
        branch: &BranchPlan,
        pull: &PullRequest,
    ) -> Result<()> {
        let current_head = self.text(path, ["rev-parse", "HEAD"]).await?;
        if current_head != pull.head {
            ensure!(
                branch.current.is_none() || branch.owned,
                "refusing to move a non-review branch"
            );
            ensure!(
                self.is_clean(path).await?,
                "worktree has local changes; refusing to replace its revision"
            );
        }
        if branch.owned
            && let Some(name) = branch.current.as_deref().or(branch.target.as_deref())
        {
            self.text(
                path,
                [
                    "config",
                    &format!("branch.{name}.yandalf-subject"),
                    &pull.subject,
                ],
            )
            .await?;
        }
        if current_head != pull.head {
            self.text(path, ["reset", "--keep", &pull.head]).await?;
            ensure!(
                self.is_clean(path).await?,
                "worktree changed while updating its revision"
            );
        }
        if branch.owned
            && let Some(target) = &branch.target
        {
            match branch.current.as_deref() {
                Some(current) if current != target => {
                    self.text(path, ["branch", "-m", "--", target]).await?;
                }
                None => {
                    self.text(path, ["switch", "-c", target]).await?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn is_clean(&self, path: &Path) -> Result<bool> {
        Ok(self
            .text(
                path,
                ["status", "--porcelain=v1", "--untracked-files=normal"],
            )
            .await?
            .is_empty())
    }

    async fn metadata(
        &self,
        path: &Path,
        repository: &Repository,
        pull: &PullRequest,
    ) -> Result<GitInfo> {
        let actual_head = self.text(path, ["rev-parse", "HEAD"]).await?;
        ensure!(
            actual_head == pull.head,
            "worktree revision changed while preparing the task"
        );
        Ok(GitInfo {
            sha: actual_head,
            branch: self.branch(path).await?,
            origin_url: Some(repository.origin.clone()),
        })
    }

    async fn worktrees(&self, checkout: &Path) -> Result<BTreeMap<PathBuf, Worktree>> {
        let output = self
            .output(checkout, ["worktree", "list", "--porcelain", "-z"])
            .await?
            .text()?;
        let mut result = BTreeMap::new();
        let mut path = None;
        let mut tree = Worktree::default();
        for field in output.split('\0') {
            if field.is_empty() {
                if let Some(path) = path.take() {
                    tree.primary = result.is_empty();
                    ensure!(
                        result.insert(path, std::mem::take(&mut tree)).is_none(),
                        "Git returned duplicate worktree paths"
                    );
                }
            } else if let Some(value) = field.strip_prefix("worktree ") {
                path = Some(PathBuf::from(value));
            } else if let Some(value) = field.strip_prefix("branch ") {
                tree.branch = Some(value.into());
            } else if let Some(value) = field.strip_prefix("locked ") {
                tree.locked = Some(value.into());
            } else if field == "locked" {
                tree.locked = Some(String::new());
            } else if field == "prunable" || field.starts_with("prunable ") {
                tree.prunable = true;
            }
        }
        Ok(result)
    }

    async fn has_commit(&self, checkout: &Path, sha: &str) -> Result<bool> {
        Ok(self
            .output(checkout, ["cat-file", "-e", &format!("{sha}^{{commit}}")])
            .await?
            .success())
    }

    async fn ensure_revisions(
        &self,
        checkout: &Path,
        number: u64,
        head: &str,
        base: &str,
    ) -> Result<()> {
        if !self.has_commit(checkout, head).await? {
            self.text(
                checkout,
                [
                    "fetch",
                    "--no-tags",
                    "origin",
                    &format!("refs/pull/{number}/head"),
                ],
            )
            .await?;
        }
        for sha in [head, base] {
            if !self.has_commit(checkout, sha).await? {
                self.text(checkout, ["fetch", "--no-tags", "origin", sha])
                    .await?;
                ensure!(
                    self.has_commit(checkout, sha).await?,
                    "Git did not fetch the pinned revision"
                );
            }
        }
        Ok(())
    }
}

#[derive(Default)]
struct Worktree {
    primary: bool,
    prunable: bool,
    branch: Option<String>,
    locked: Option<String>,
}

fn revision(value: Option<&str>) -> Result<String> {
    let value = value.context("missing pinned Git revision")?;
    ensure!(
        value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "invalid pinned Git revision"
    );
    Ok(value.to_ascii_lowercase())
}

fn canonical_repository(repository: &str) -> Result<String> {
    let Some((owner, name)) = repository.split_once('/') else {
        bail!("GitHub repository must be owner/name");
    };
    ensure!(
        !owner.is_empty()
            && !name.is_empty()
            && !name.contains('/')
            && owner
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
            && ![".", ".."].contains(&name),
        "invalid GitHub repository"
    );
    Ok(repository.to_ascii_lowercase())
}

fn origin_repository(origin: &str) -> Option<String> {
    let path = origin
        .strip_prefix("https://github.com/")
        .or_else(|| origin.strip_prefix("git@github.com:"))
        .or_else(|| origin.strip_prefix("ssh://git@github.com/"))?;
    canonical_repository(
        path.trim_end_matches('/')
            .strip_suffix(".git")
            .unwrap_or(path.trim_end_matches('/')),
    )
    .ok()
}
