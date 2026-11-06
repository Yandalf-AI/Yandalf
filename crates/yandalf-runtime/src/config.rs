use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub cwd: PathBuf,
    pub concurrency: usize,
    pub timeout_secs: u64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            cwd: ".".into(),
            concurrency: 4,
            timeout_secs: 1800,
        }
    }
}
impl Config {
    pub fn resolve_paths(&mut self, base: &Path) -> Result<()> {
        let cwd = if self.cwd.is_absolute() {
            self.cwd.clone()
        } else {
            base.join(&self.cwd)
        };
        self.cwd = std::fs::canonicalize(cwd).context("harness cwd")?;
        self.validate()
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.cwd.is_dir(),
            "harness cwd must be an existing directory"
        );
        ensure!(self.concurrency > 0, "concurrency must be positive");
        ensure!(self.timeout_secs > 0, "execution timeout must be positive");
        Ok(())
    }
}
