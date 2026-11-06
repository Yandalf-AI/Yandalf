use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    path::{Component, Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use tokio::{process::Command, sync::Mutex, time::timeout};

use yandalf_core::{
    domain::GitInfo,
    plugin::{Plugin, PluginContext},
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
            "worktree command failed: {}",
            String::from_utf8_lossy(&self.stderr).trim()
        );
        String::from_utf8(self.stdout).context("worktree command returned non-UTF-8 data")
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
                "worktree command {:?} timed out in {}",
                request.program,
                request.cwd.display()
            )
        })?
        .with_context(|| {
            format!(
                "start worktree command {:?} in {}",
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

#[derive(Clone, Default)]
pub struct GitWorktree {
    allow_hooks: bool,
}

impl GitWorktree {
    pub fn with_allow_hooks(mut self, allow_hooks: bool) -> Self {
        self.allow_hooks = allow_hooks;
        self
    }
}

impl Plugin for GitWorktree {
    fn name(&self) -> &str {
        "git.worktree"
    }

    fn register(&self, ctx: &mut PluginContext) -> Result<()> {
        ctx.require::<dyn CommandRunner>();
        let allow_hooks = self.allow_hooks;
        ctx.provide_with::<WorktreeService, _, _>(move |services| async move {
            Ok(Arc::new(WorktreeService {
                runner: services.get::<dyn CommandRunner>()?,
                allow_hooks,
                lock: Mutex::new(()),
            }))
        })
    }
}

/// A worktree allocation selected by trusted application configuration.
/// Paths are absolute, and `existing_path` takes precedence when it identifies a registered worktree.
/// The stable `owner` controls reuse; revisions are full commit SHAs, and `fetch_refs` are fully qualified refs fetched from `origin` when commits are missing.
#[derive(Clone, Debug)]
pub struct WorktreeRequest {
    pub checkout: PathBuf,
    pub path: PathBuf,
    pub existing_path: Option<PathBuf>,
    pub owner: String,
    pub revision: String,
    pub branch: String,
    pub fallback_branch: String,
    pub required_revisions: Vec<String>,
    pub fetch_refs: Vec<String>,
    pub expected_origin: String,
}

impl WorktreeRequest {
    fn validated(&self) -> Result<Self> {
        validate_path(&self.checkout)?;
        validate_path(&self.path)?;
        if let Some(path) = &self.existing_path {
            validate_path(path)?;
        }
        ensure!(
            !self.owner.is_empty()
                && self.owner.trim() == self.owner
                && !self.owner.chars().any(char::is_control),
            "invalid worktree owner"
        );
        ensure!(
            !self.expected_origin.is_empty() && !self.expected_origin.contains(['\0', '\n', '\r']),
            "invalid expected Git origin"
        );
        let mut request = self.clone();
        request.revision = revision(Some(&request.revision))?;
        request.required_revisions = request
            .required_revisions
            .iter()
            .map(|value| revision(Some(value)))
            .collect::<Result<_>>()?;
        Ok(request)
    }

    fn lock_reason(&self) -> String {
        format!("yandalf:{}", self.owner)
    }
}

#[derive(Clone, Debug)]
pub struct PreparedWorktree {
    pub cwd: PathBuf,
    pub git: GitInfo,
}

pub struct WorktreeService {
    runner: Arc<dyn CommandRunner>,
    allow_hooks: bool,
    lock: Mutex<()>,
}

impl WorktreeService {
    fn git(&self) -> Git<'_> {
        Git {
            runner: self.runner.as_ref(),
            allow_hooks: self.allow_hooks,
        }
    }

    pub async fn origin(&self, checkout: &Path) -> Result<String> {
        validate_path(checkout)?;
        let git = self.git();
        let checkout = git.top(checkout).await?;
        git.text(&checkout, ["remote", "get-url", "origin"]).await
    }

    /// Requires an exact `expected_origin` match before any fetch or worktree mutation.
    pub async fn prepare(&self, request: &WorktreeRequest) -> Result<PreparedWorktree> {
        let request = request.validated()?;
        let _lock = self.lock.lock().await;
        let git = self.git();
        let mut repository = Repository::inspect(&git, &request).await?;
        let worktree = WorktreeLocation::resolve(&git, &repository, &request).await?;
        git.prepare_worktree(&mut repository, &worktree, &request)
            .await?;
        let branch = BranchPlan::inspect(&git, &repository, &worktree, &request).await?;
        git.update_worktree(&worktree.path, &branch, &request)
            .await?;
        Ok(PreparedWorktree {
            git: git.metadata(&worktree.path, &repository, &request).await?,
            cwd: worktree.path,
        })
    }
}

struct Repository {
    checkout: PathBuf,
    origin: String,
    common: PathBuf,
    trees: BTreeMap<PathBuf, Worktree>,
}

impl Repository {
    async fn inspect(git: &Git<'_>, request: &WorktreeRequest) -> Result<Self> {
        let checkout = git.top(&request.checkout).await?;
        let origin = git.text(&checkout, ["remote", "get-url", "origin"]).await?;
        ensure!(
            origin == request.expected_origin,
            "configured checkout origin does not match expected origin"
        );
        for branch in [&request.branch, &request.fallback_branch] {
            let checked = git
                .output(&checkout, ["check-ref-format", "--branch", branch])
                .await?;
            ensure!(
                checked.success() && checked.text()?.trim() == *branch,
                "invalid worktree branch"
            );
        }
        for reference in &request.fetch_refs {
            ensure!(
                reference.starts_with("refs/"),
                "fetch ref must be fully qualified"
            );
            let checked = git
                .output(&checkout, ["check-ref-format", reference])
                .await?;
            ensure!(checked.success(), "invalid fetch ref");
        }
        Ok(Self {
            common: git.common(&checkout).await?,
            trees: git.worktrees(&checkout).await?,
            checkout,
            origin,
        })
    }
}

struct WorktreeLocation {
    path: PathBuf,
    adopted: bool,
}

impl WorktreeLocation {
    async fn resolve(
        git: &Git<'_>,
        repository: &Repository,
        request: &WorktreeRequest,
    ) -> Result<Self> {
        if let Some(path) = Self::bound(git, repository, request.existing_path.as_deref()).await? {
            return Ok(Self {
                path,
                adopted: true,
            });
        }
        let parent = request
            .path
            .parent()
            .context("worktree path has no parent")?;
        let name = request
            .path
            .file_name()
            .context("worktree path has no name")?;
        git.command(
            &repository.checkout,
            "mkdir",
            [OsStr::new("-p"), OsStr::new("--"), parent.as_os_str()],
        )
        .await?
        .text()?;
        let root = PathBuf::from(git.command(parent, "pwd", ["-P"]).await?.text()?.trim());
        ensure!(
            root.is_absolute(),
            "worktree root did not resolve to an absolute path"
        );
        let path = root.join(name);
        ensure!(
            path != repository.checkout,
            "worktree cannot be the configured checkout"
        );
        if let Some(tree) = repository.trees.get(&path) {
            ensure!(
                tree.locked.as_deref() == Some(request.lock_reason().as_str()),
                "worktree path is already owned by another task"
            );
        }
        Ok(Self {
            path,
            adopted: false,
        })
    }

    async fn bound(
        git: &Git<'_>,
        repository: &Repository,
        existing: Option<&Path>,
    ) -> Result<Option<PathBuf>> {
        let Some(cwd) = existing else { return Ok(None) };
        let directory = git
            .command(
                &repository.checkout,
                "test",
                [OsStr::new("-d"), cwd.as_os_str()],
            )
            .await?;
        ensure!(
            matches!(directory.status, Some(0 | 1)),
            "cannot inspect bound worktree directory"
        );
        if !directory.success() {
            let tree = repository
                .trees
                .get(cwd)
                .context("missing bound worktree directory has no Git worktree registration")?;
            ensure!(
                !tree.primary && tree.prunable,
                "bound worktree directory is not a confirmed missing worktree"
            );
            return Ok(None);
        }
        let path = git.top(cwd).await?;
        ensure!(
            git.common(&path).await? == repository.common,
            "bound worktree belongs to a different repository"
        );
        let tree = repository
            .trees
            .get(&path)
            .context("bound worktree is not a registered Git worktree")?;
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
        worktree: &WorktreeLocation,
        request: &WorktreeRequest,
    ) -> Result<Self> {
        let tree = repository
            .trees
            .get(&worktree.path)
            .context("Git did not register the requested worktree")?;
        let current = git.branch(&worktree.path).await?;
        let owned = tree.locked.as_deref() == Some(request.lock_reason().as_str());
        if !owned {
            return Ok(Self {
                current,
                target: None,
                owned,
            });
        }
        let marked = if let Some(current) = &current {
            git.branch_owner(&worktree.path, current, &request.owner)
                .await?
        } else {
            false
        };
        ensure!(
            marked
                || current.as_deref().is_none_or(|current| {
                    current == request.branch || current == request.fallback_branch
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
                other_path != &worktree.path && tree.branch.as_deref() == Some(reference.as_str())
            })
        };
        let target =
            if current.as_deref() == Some(request.branch.as_str()) || available(&request.branch) {
                request.branch.clone()
            } else {
                ensure!(
                    current.as_deref() == Some(request.fallback_branch.as_str())
                        || available(&request.fallback_branch),
                    "requested and fallback branch names are already in use"
                );
                request.fallback_branch.clone()
            };
        Ok(Self {
            current,
            target: Some(target),
            owned,
        })
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

    async fn branch_owner(&self, path: &Path, branch: &str, expected_owner: &str) -> Result<bool> {
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
            owner.text()?.trim() == expected_owner,
            "worktree branch is owned by another task"
        );
        Ok(true)
    }

    async fn prepare_worktree(
        &self,
        repository: &mut Repository,
        worktree: &WorktreeLocation,
        request: &WorktreeRequest,
    ) -> Result<()> {
        let checkout = &repository.checkout;
        let path = &worktree.path;
        self.ensure_revisions(checkout, request).await?;
        if !repository.trees.contains_key(path) {
            ensure!(!worktree.adopted, "bound worktree disappeared");
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
                    OsStr::new(&request.lock_reason()),
                    path.as_os_str(),
                    OsStr::new(&request.revision),
                ],
            )
            .await?;
            repository.trees = self.worktrees(checkout).await?;
        }
        let tree = repository
            .trees
            .get(path)
            .context("Git did not register the requested worktree")?;
        ensure!(!tree.primary, "worktree cannot be the primary checkout");
        ensure!(
            self.common(path).await? == repository.common,
            "prepared worktree belongs to another repository"
        );
        if !worktree.adopted {
            ensure!(
                tree.locked.as_deref() == Some(request.lock_reason().as_str()),
                "Git did not preserve worktree ownership"
            );
        }
        Ok(())
    }

    async fn update_worktree(
        &self,
        path: &Path,
        branch: &BranchPlan,
        request: &WorktreeRequest,
    ) -> Result<()> {
        let current_head = self.text(path, ["rev-parse", "HEAD"]).await?;
        if current_head != request.revision {
            ensure!(
                branch.current.is_none() || branch.owned,
                "refusing to move an unowned branch"
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
                    &request.owner,
                ],
            )
            .await?;
        }
        if current_head != request.revision {
            self.text(path, ["reset", "--keep", &request.revision])
                .await?;
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
        request: &WorktreeRequest,
    ) -> Result<GitInfo> {
        let actual_head = self.text(path, ["rev-parse", "HEAD"]).await?;
        ensure!(
            actual_head == request.revision,
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

    async fn ensure_revisions(&self, checkout: &Path, request: &WorktreeRequest) -> Result<()> {
        let revisions: Vec<_> = std::iter::once(&request.revision)
            .chain(request.required_revisions.iter())
            .collect();
        let mut missing = false;
        for revision in &revisions {
            missing |= !self.has_commit(checkout, revision).await?;
        }
        if missing {
            for reference in &request.fetch_refs {
                self.text(checkout, ["fetch", "--no-tags", "origin", reference])
                    .await?;
            }
        }
        for revision in revisions {
            if !self.has_commit(checkout, revision).await? {
                self.text(checkout, ["fetch", "--no-tags", "origin", revision])
                    .await?;
                ensure!(
                    self.has_commit(checkout, revision).await?,
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

fn validate_path(path: &Path) -> Result<()> {
    ensure!(path.is_absolute(), "Git worktree paths must be absolute");
    ensure!(
        !path.as_os_str().as_encoded_bytes().contains(&0),
        "Git worktree paths must not contain NUL"
    );
    ensure!(
        !path
            .components()
            .any(|component| matches!(component, Component::ParentDir)),
        "Git worktree paths must not contain parent traversal"
    );
    Ok(())
}
