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
            lock: Arc::new(Mutex::new(())),
        })
    }

    pub async fn prepare(
        &self,
        runner: &dyn CommandRunner,
        event: &mut TaskPreparing,
    ) -> Result<()> {
        if event.job.event.source != format!("github:{}", self.repository) {
            return Ok(());
        }
        let context = &event.job.event.context;
        ensure!(
            context["repository"] == self.repository,
            "Git workspace repository mismatch"
        );
        let number = context["pr"]
            .as_u64()
            .filter(|number| *number > 0)
            .context("missing pull request number")?;
        ensure!(
            event.job.event.subject == format!("github:{}/pr/{number}", self.repository),
            "Git workspace subject does not identify this pull request"
        );
        let head = revision(context["head_sha"].as_str())?;
        let base = revision(context["base_sha"].as_str())?;
        let head_ref = context["pull"]["head_ref"]
            .as_str()
            .filter(|name| !name.is_empty())
            .context("missing pull request head branch")?;
        let _lock = self.lock.lock().await;
        let git = Git { runner };
        let checkout = git.top(&self.checkout).await?;
        let checked_branch = git
            .output(&checkout, ["check-ref-format", "--branch", head_ref])
            .await?;
        ensure!(
            checked_branch.success() && checked_branch.text()?.trim() == head_ref,
            "invalid pull request head branch"
        );
        let origin = git.text(&checkout, ["remote", "get-url", "origin"]).await?;
        ensure!(
            origin_repository(&origin).as_deref() == Some(self.repository.as_str()),
            "configured checkout origin does not match GitHub repository"
        );
        let common = git.common(&checkout).await?;
        let mut trees = git.worktrees(&checkout).await?;
        let review_branch = format!("review/pr-{number}");
        let lock_reason = format!("yandalf:{}", event.job.event.subject);
        let workspace = if event.existing_session.is_some() {
            let directory = git
                .command(
                    &checkout,
                    "test",
                    [OsStr::new("-d"), event.task.session.cwd.as_os_str()],
                )
                .await?;
            ensure!(
                matches!(directory.status, Some(0 | 1)),
                "cannot inspect bound session directory"
            );
            if directory.success() {
                let path = git.top(&event.task.session.cwd).await?;
                ensure!(
                    git.common(&path).await? == common,
                    "bound session belongs to a different repository"
                );
                let tree = trees
                    .get(&path)
                    .context("bound session is not a registered Git worktree")?;
                if path == checkout || tree.primary {
                    None
                } else {
                    Some(path)
                }
            } else {
                let tree = trees
                    .get(&event.task.session.cwd)
                    .context("missing bound session directory has no Git worktree registration")?;
                ensure!(
                    !tree.primary && tree.prunable,
                    "bound session directory is not a confirmed missing worktree"
                );
                None
            }
        } else {
            None
        };
        let (path, adopted) = match workspace {
            Some(path) => (path, true),
            None => {
                git.command(
                    &checkout,
                    "mkdir",
                    [OsStr::new("-p"), OsStr::new("--"), self.root.as_os_str()],
                )
                .await?
                .text()?;
                let root =
                    PathBuf::from(git.command(&self.root, "pwd", ["-P"]).await?.text()?.trim());
                ensure!(
                    root.is_absolute(),
                    "workspace root did not resolve to an absolute path"
                );
                let path = root.join(format!("pr-{number}"));
                ensure!(
                    path != checkout,
                    "review workspace cannot be the configured checkout"
                );
                if let Some(tree) = trees.get(&path) {
                    ensure!(
                        tree.locked.as_deref() == Some(lock_reason.as_str()),
                        "workspace path is already owned by another task"
                    );
                }
                (path, false)
            }
        };

        git.ensure_revisions(&checkout, number, &head, &base)
            .await?;
        if !trees.contains_key(&path) {
            ensure!(!adopted, "bound worktree disappeared");
            let exists = git
                .command(&checkout, "test", [OsStr::new("-e"), path.as_os_str()])
                .await?;
            let symlink = git
                .command(&checkout, "test", [OsStr::new("-L"), path.as_os_str()])
                .await?;
            ensure!(
                matches!(exists.status, Some(0 | 1)) && matches!(symlink.status, Some(0 | 1)),
                "cannot inspect worktree destination"
            );
            ensure!(
                !exists.success() && !symlink.success(),
                "worktree destination exists without a matching registration"
            );
            git.text(
                &checkout,
                [
                    OsStr::new("worktree"),
                    OsStr::new("add"),
                    OsStr::new("--detach"),
                    OsStr::new("--lock"),
                    OsStr::new("--reason"),
                    OsStr::new(&lock_reason),
                    path.as_os_str(),
                    OsStr::new(&head),
                ],
            )
            .await?;
            trees = git.worktrees(&checkout).await?;
        }
        let tree = trees
            .get(&path)
            .context("Git did not register the requested worktree")?;
        ensure!(
            !tree.primary,
            "review workspace cannot be the primary checkout"
        );
        ensure!(
            git.common(&path).await? == common,
            "prepared worktree belongs to another repository"
        );
        if !adopted {
            ensure!(
                tree.locked.as_deref() == Some(lock_reason.as_str()),
                "Git did not preserve worktree ownership"
            );
        }
        let current_branch = git.branch(&path).await?;
        let owned = tree.locked.as_deref() == Some(lock_reason.as_str());
        let branch = if owned {
            let mut marked = false;
            if let Some(current) = &current_branch {
                let owner = git
                    .output(
                        &path,
                        [
                            "config",
                            "--get",
                            &format!("branch.{current}.yandalf-subject"),
                        ],
                    )
                    .await?;
                if owner.success() {
                    ensure!(
                        owner.text()?.trim() == event.job.event.subject,
                        "worktree branch is owned by another task"
                    );
                    marked = true;
                } else {
                    ensure!(owner.status == Some(1), "cannot inspect branch ownership");
                }
            }
            ensure!(
                marked
                    || current_branch
                        .as_deref()
                        .is_none_or(|current| current == head_ref || current == review_branch),
                "worktree is attached to a different branch"
            );
            let references = git
                .text(
                    &checkout,
                    ["for-each-ref", "--format=%(refname)", "refs/heads/"],
                )
                .await?;
            let available = |name: &str| {
                let reference = format!("refs/heads/{name}");
                !references.lines().any(|existing| {
                    existing == reference
                        || existing.starts_with(&format!("{reference}/"))
                        || reference.starts_with(&format!("{existing}/"))
                }) && !trees.iter().any(|(other_path, tree)| {
                    other_path != &path && tree.branch.as_deref() == Some(reference.as_str())
                })
            };
            if current_branch.as_deref() == Some(head_ref) || available(head_ref) {
                Some(head_ref)
            } else {
                ensure!(
                    current_branch.as_deref() == Some(review_branch.as_str())
                        || available(&review_branch),
                    "pull request and review branch names are already in use"
                );
                Some(review_branch.as_str())
            }
        } else {
            current_branch.as_deref()
        };
        let current_head = git.text(&path, ["rev-parse", "HEAD"]).await?;
        if current_head != head {
            ensure!(
                current_branch.is_none() || owned,
                "refusing to move a non-review branch"
            );
            ensure!(
                git.text(
                    &path,
                    ["status", "--porcelain=v1", "--untracked-files=normal"]
                )
                .await?
                .is_empty(),
                "worktree has local changes; refusing to replace its revision"
            );
        }
        if owned && let Some(name) = current_branch.as_deref().or(branch) {
            git.text(
                &path,
                [
                    "config",
                    &format!("branch.{name}.yandalf-subject"),
                    &event.job.event.subject,
                ],
            )
            .await?;
        }
        if current_head != head {
            git.text(&path, ["reset", "--keep", &head]).await?;
            ensure!(
                git.text(
                    &path,
                    ["status", "--porcelain=v1", "--untracked-files=normal"]
                )
                .await?
                .is_empty(),
                "worktree changed while updating its revision"
            );
        }
        if owned && let Some(branch) = branch {
            match current_branch.as_deref() {
                Some(current) if current != branch => {
                    git.text(&path, ["branch", "-m", "--", branch]).await?;
                }
                None => {
                    git.text(&path, ["switch", "-c", branch]).await?;
                }
                _ => {}
            }
        }
        let actual_head = git.text(&path, ["rev-parse", "HEAD"]).await?;
        ensure!(
            actual_head == head,
            "worktree revision changed while preparing the task"
        );
        event.task.session.cwd = path.clone();
        event.task.session.git = Some(GitInfo {
            sha: actual_head,
            branch: git.branch(&path).await?,
            origin_url: Some(origin),
        });
        Ok(())
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
        self.command(cwd, "git", args).await
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
