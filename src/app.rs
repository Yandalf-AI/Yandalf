use crate::{
    codex::CodexConfig,
    config::Config,
    github::{ChangeKind, Github, WorktreeConfig},
    input::Schedule,
    plugin::App,
    plugins::{CodexServer, TaskRunner},
    worktree::{GitWorktree, LocalCommands},
};
use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use serde_json::Value;
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    pub plugins: Vec<PluginConfig>,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum PluginConfig {
    #[cfg(feature = "sqlite")]
    Sqlite {
        path: PathBuf,
    },
    Codex {
        endpoint: String,
        #[serde(default)]
        bearer_token_env: Option<String>,
        #[serde(default = "rpc_timeout")]
        request_timeout_secs: u64,
    },
    Runtime {
        cwd: PathBuf,
        #[serde(default = "concurrency")]
        concurrency: usize,
        #[serde(default = "task_timeout")]
        timeout_secs: u64,
    },
    LocalCommands,
    GitWorktree {
        #[serde(default)]
        allow_hooks: bool,
    },
    Github {
        repository: String,
        #[serde(default)]
        worktree: Option<WorktreeConfig>,
        #[serde(default)]
        text: Option<String>,
        #[serde(default = "ChangeKind::defaults")]
        on: Vec<ChangeKind>,
        #[serde(default)]
        interval: Option<String>,
        #[serde(default)]
        cron: Option<String>,
    },
    Schedule {
        #[serde(default)]
        interval: Option<String>,
        #[serde(default)]
        cron: Option<String>,
        text: String,
        #[serde(default)]
        context: Value,
    },
}
fn rpc_timeout() -> u64 {
    30
}
fn concurrency() -> usize {
    4
}
fn task_timeout() -> u64 {
    1800
}
fn resolve(path: &mut PathBuf, base: &Path) {
    if !path.is_absolute() {
        *path = base.join(&*path);
    }
}

impl Settings {
    pub fn load(path: &Path) -> Result<Self> {
        let path = std::fs::canonicalize(path).context("service config path")?;
        let base = path.parent().context("service config directory")?;
        let mut settings: Self = toml::from_str(&std::fs::read_to_string(&path)?)
            .context("parse plugin configuration")?;
        for plugin in &mut settings.plugins {
            match plugin {
                #[cfg(feature = "sqlite")]
                PluginConfig::Sqlite { path } => resolve(path, base),
                PluginConfig::Runtime { cwd, .. } => resolve(cwd, base),
                PluginConfig::Github {
                    worktree: Some(worktree),
                    ..
                } => {
                    resolve(&mut worktree.checkout, base);
                    resolve(&mut worktree.root, base);
                }
                _ => {}
            }
        }
        Ok(settings)
    }
    pub async fn build(self) -> Result<App> {
        let mut app = App::builder();
        let mut sources = BTreeSet::new();
        let mut runners = 0;
        for plugin in self.plugins {
            match plugin {
                #[cfg(feature = "sqlite")]
                PluginConfig::Sqlite { path } => {
                    app = app.plugin(crate::plugins::Sqlite(path));
                    continue;
                }
                PluginConfig::Codex {
                    endpoint,
                    bearer_token_env,
                    request_timeout_secs,
                } => {
                    app = app.plugin(CodexServer(CodexConfig {
                        endpoint,
                        bearer_token_env,
                        request_timeout_secs,
                    }));
                    continue;
                }
                PluginConfig::Runtime {
                    cwd,
                    concurrency,
                    timeout_secs,
                } => {
                    runners += 1;
                    ensure!(runners == 1, "only one runtime may own this queue");
                    let mut config = Config {
                        cwd,
                        concurrency,
                        timeout_secs,
                    };
                    config.resolve_paths(Path::new("."))?;
                    app = app.plugin(TaskRunner(config));
                    continue;
                }
                PluginConfig::LocalCommands => {
                    app = app.plugin(LocalCommands);
                    continue;
                }
                PluginConfig::GitWorktree { allow_hooks } => {
                    app = app.plugin(GitWorktree::default().with_allow_hooks(allow_hooks));
                    continue;
                }
                PluginConfig::Github {
                    repository,
                    worktree,
                    text,
                    on,
                    interval,
                    cron,
                } => {
                    let mut input = Github::new(&repository, interval.as_deref(), cron.as_deref())?
                        .with_instruction(text.as_deref())?
                        .with_triggers(on);
                    if let Some(worktree) = worktree {
                        input = input.with_worktree(worktree)?;
                    }
                    ensure!(
                        sources.insert(input.source().to_owned()),
                        "duplicate input configuration"
                    );
                    app = app.plugin(input);
                }
                PluginConfig::Schedule {
                    interval,
                    cron,
                    text,
                    context,
                } => {
                    let input =
                        Schedule::new(interval.as_deref(), cron.as_deref(), &text, &context)?;
                    ensure!(
                        sources.insert(input.source().to_owned()),
                        "duplicate input configuration"
                    );
                    app = app.plugin(input);
                }
            };
        }
        ensure!(runners == 1, "configure one runtime plugin");
        app.build().await
    }
}
