# Yandalf

Yandalf is a Rust library for composing a harness from plugins. Channels admit events into a durable inbox; a task runner binds each subject to an external agent session, limits execution and recovers interrupted connections. Agent servers own the model loop, tools and native memory.

## Build your own harness

Plugins are ordinary Cargo dependencies. A custom plugin needs `yandalf-core`; storage adapters, channels and executors are separate crates. The root `yandalf` crate re-exports the bundled components and provides the service binary.

- `yandalf-core`: plugin registration, typed services, task contracts, events and lifecycle.
- `yandalf-runtime`: durable execution, session binding, preparation checkpoints and result processing.
- `yandalf-codex`: client of an independently running Codex app-server.
- `yandalf-sqlite`: the SQLite implementation of `Store`.
- `yandalf-github`: repository polling and configurable PR-change decisions.
- `yandalf-channels`: scheduled inputs.
- `yandalf-git-workspaces`: Git worktree preparation and the replaceable `CommandRunner` capability.

```rust
use yandalf_core::plugin::App;
use yandalf_runtime::TaskRunner;
use yandalf_sqlite::Sqlite;
use yandalf_codex::CodexServer;

let app = App::builder()
    .plugin(TaskRunner(runtime_config))
    .plugin(my_input)
    .plugin(my_policy)
    .plugin(Sqlite(database_path))
    .plugin(CodexServer(codex_config))
    .build()
    .await?;

app.run(stop).await?;
```

The registration order need not match service dependencies. `build()` validates missing providers, duplicate providers and cycles before activation, then starts provider factories in dependency order. Built-in and user plugins use the same API. See [custom_harness.rs](examples/custom_harness.rs) for a complete composition with a custom result observer.

## Agent skill for harness authors

The [Yandalf harness development skill](skills/yandalf-harness-development/SKILL.md) guides coding agents building a consumer application or plugin with these crates. Copy `skills/yandalf-harness-development` into your agent's skill directory and invoke `$yandalf-harness-development`. Keep its `references/` directory alongside `SKILL.md`.

The skill uses the Yandalf checkout or Cargo dependency selected for your project; it has no VM, account or repository-specific deployment assumptions. It covers composition, typed services and events, replay and standalone verification. Repository contributors should use [AGENTS.md](AGENTS.md).

## Write a plugin

A plugin implements `Plugin::register` and registers the capabilities it owns:

- `require::<T>()` declares a required service.
- `provide::<T>(Arc<T>)` or `provide_with::<T, _, _>(factory)` supplies a typed service, including trait objects.
- `on::<E, _>(handler)` registers a serial handler for a `SerialEvent`. Handlers run in plugin registration order; an error stops preparation.
- `observe::<E, _>(handler)` registers an immutable observer for a `Notification`. All observers are awaited; they may run concurrently.
- `task(name, callback)` starts application-owned background work with a cancellation token.
- `cleanup(callback)` registers asynchronous resource cleanup.

Custom service traits and event types live in the plugin author's crate. Serial events and notifications have separate marker traits; their mode is part of the public contract. `Services::get::<T>()` retrieves a service, `dispatch()` runs serial handlers, and `notify()` awaits observers.

The composition is fixed at build time. On shutdown, the application cancels and awaits its background work, waits for active event dispatches, then performs cleanup in reverse activation order. Call and await `run(stop)` or `shutdown()` to perform asynchronous cleanup. Plugin tasks must cooperate with cancellation. Cleanup releases subscriptions and connections; durable workspaces and external agent servers remain owned by their respective systems.

## Task lifecycle

The runner claims a subject exclusively and emits `TaskDeciding`. Accepted work passes through `TaskPreparing`; a new session also passes through `SessionPreparing`. The resulting `PreparedTask` contains a session specification and turn input, and is saved before creating the session or submitting a turn. Recovery reuses that checkpoint.

Only `subject` identifies a session, independently of the input channel. Internal event IDs deduplicate delivery. Neither is a user configuration knob. The durable backlog is independent of the concurrency limit; active, uncertain and finalizing tasks reserve execution capacity and their subject.

Session creation intent is saved before the external call. A lost acknowledgement is reconciled using its creation marker; an unknown or ambiguous outcome never triggers another creation request. Codex 0.166 hides empty threads from ordinary persisted listings, so an empty thread lost with the server itself may remain uncertain and require operator recovery.

The runner records the raw turn result before emitting `TaskCompleted`. It finishes the job only after result observers succeed. Failed observers leave the job `finalizing`; restart or reconciliation retries those observers without repeating the model turn. Preparation handlers and result observers must tolerate replay after a crash and use the stable job ID when they need external idempotency. There is no general exactly-once guarantee for arbitrary external effects.

A harness restart detaches from active agent work. Explicit cancellation and execution timeout can interrupt a turn. The runtime reconnects to unavailable executors while input plugins continue admitting events through the Store.

## Configure the bundled service

Install Rust 1.93+ to build Yandalf. Run and authenticate Codex independently. Save `yandalf.toml`, or set `YANDALF_CONFIG` to its path:

```toml
[[plugins]]
type = "sqlite"
path = "data/harness.sqlite"

[[plugins]]
type = "codex"
endpoint = "unix:///path/to/codex.sock"
request_timeout_secs = 60

[[plugins]]
type = "runtime"
cwd = "/srv/repos/example"
concurrency = 4
timeout_secs = 3600

[[plugins]]
type = "local_commands"

[[plugins]]
type = "git_workspaces"
repository = "example-org/example-repo"
checkout = "/srv/repos/example"
root = "/srv/workspaces/example"

[[plugins]]
type = "github"
repository = "example-org/example-repo"
interval = "30s"
text = "Используй $github-review. Проверь подготовленную ревизию PR и опубликуй краткое ревью по-русски."
```

Replace the sample endpoint and repository paths with those of your executor host. Paths resolve relative to the configuration file. The executable selects from its bundled plugins; an application with custom crates composes them through the Rust API. Build with `cargo build --release -p yandalf` and run `target/release/yandalf`. It has no command interface. SIGINT and SIGTERM stop the harness without terminating Codex.

The Codex client accepts explicit `unix://`, `ws://` or `wss://` endpoints. Unix sockets carry the same WebSocket protocol. Optional `bearer_token_env` names the environment variable containing a remote listener's token. Models, approvals, authentication, home directories and native memory belong to the server configuration. Interactive approval requests that remain after server policy are declined by the headless client.

A `schedule` input accepts `text`, optional `context`, and exactly one of `interval` or five-field `cron` in UTC. Interval schedules begin immediately; cron schedules begin at the next matching time. Missed schedule ticks remain eligible after restart.

One application can run multiple inputs concurrently: configure each GitHub repository or schedule as another `[[plugins]]` entry. All inputs persist events into the same inbox while the runtime limits execution. Custom inputs implement `Channel` and register through `ChannelPlugin`; their transport can be stdin, a socket, a queue or another event source. The [custom harness example](examples/custom_harness.rs) shows independent inputs sharing one runtime.

## GitHub review workflow

The GitHub plugin watches the entire repository through outgoing `gh api` calls. Existing PRs establish a baseline without a retrospective review. Optional `on` selects `opened`, `revision`, `ready_for_review`, `reopened`, `edited`, `commented`, `reviewed` and `review_requested`; all are enabled by default. Draft PRs, the authenticated user's own activity and bot comments/reviews are skipped. CI status changes do not start turns. Superseded queued revisions are skipped when a newer observation is already stored.

Closing or merging a tracked PR queues session archival after its current work finishes, without starting a model turn. This lifecycle action is independent of `on` and survives restarts. The session binding and worktree remain; reopening the PR restores and continues the same session.

Observations and skip reasons survive restarts. Repository-wide comment feeds detect edits independently of PR timestamps. Stack membership supplies dependency context and never limits the watched PRs. Polling cannot reconstruct intermediate pushes between observations.

`GitWorkspaces` uses the configured command provider before a session or turn is started. For a new PR it creates `root/pr-N`, checks out the observed head SHA using the PR's source branch name, and records ownership so interrupted preparation can safely reuse the directory. A conflicting local branch is preserved; the worktree uses `review/pr-N` instead. Existing owned review branches are renamed in place. Existing sessions retain their actual linked worktrees, including detached checkouts. Revision changes require a clean worktree and never alter the primary checkout or a foreign branch. `CommandRunner` must operate on the executor's host; `LocalCommands` is suitable when harness and executor share that host.

Workspace preparation disables Git hooks by default. Set `allow_hooks = true` in a `git_workspaces` plugin entry, or call `GitWorkspaces::with_allow_hooks(true)`, to permit the checkout's configured hooks. Those hooks execute on the command provider's host and may come from the PR itself. This setting affects workspace preparation; the agent server controls commands executed by its tools.

On Unix, SQLite creates new store directories with mode `0700` and database and lock files with mode `0600`. Opening an existing Yandalf database also restricts its database and journal files; unrelated parent directories keep their permissions.

The prepared cwd, title and Git metadata are applied through the executor interface. The agent receives the PR identity, revisions, change reasons and stack context, and performs the review through its native tools. It does not prepare or change the checkout. The bundled [github-review skill](skills/github-review/SKILL.md) defines review evidence and concise Russian publication through `gh`.

Install that skill under the executor user's `~/.agents/skills/github-review` or another native skill location. Review publication remains an agent tool action; Yandalf has no outbox or publisher.

## Development

```sh
cargo fmt --all --check
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo check -p yandalf-core
```

Tests use external fake app-servers and real temporary Git repositories. They do not invoke models or publish reviews. Individual crates can be built and tested independently.
