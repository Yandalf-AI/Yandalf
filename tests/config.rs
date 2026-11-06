#![cfg(feature = "sqlite")]

use yandalf::{
    app::{PluginConfig, Settings},
    worktree::WorktreeService,
};

#[tokio::test]
async fn multiple_github_channels_resolve_their_paths_and_share_one_worktree_provider() {
    let directory = tempfile::tempdir().unwrap();
    let config = directory.path().join("yandalf.toml");
    std::fs::write(
        &config,
        r#"
[[plugins]]
type = "sqlite"
path = "state.sqlite"

[[plugins]]
type = "runtime"
cwd = "."

[[plugins]]
type = "codex"
endpoint = "unix:///unused/codex.sock"

[[plugins]]
type = "local_commands"

[[plugins]]
type = "git_worktree"

[[plugins]]
type = "github"
repository = "owner/first"
interval = "30s"
[plugins.worktree]
checkout = "repositories/first"
root = "worktrees/first"

[[plugins]]
type = "github"
repository = "owner/second"
interval = "1m"
[plugins.worktree]
checkout = "repositories/second"
root = "worktrees/second"
"#,
    )
    .unwrap();

    let settings = Settings::load(&config).unwrap();
    let base = directory.path().canonicalize().unwrap();
    let paths: Vec<_> = settings
        .plugins
        .iter()
        .filter_map(|plugin| match plugin {
            PluginConfig::Github {
                worktree: Some(worktree),
                ..
            } => Some((worktree.checkout.clone(), worktree.root.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(
        paths,
        [
            (
                base.join("repositories/first"),
                base.join("worktrees/first")
            ),
            (
                base.join("repositories/second"),
                base.join("worktrees/second")
            ),
        ]
    );
    let app = settings.build().await.unwrap();
    app.services().get::<WorktreeService>().unwrap();
    app.shutdown().await.unwrap();
}
