# Development guide

Yandalf is a Rust Cargo workspace for composing agent harnesses from plugins. The root crate provides a facade and a thin service binary. Agent servers run independently and own their model loop, tools, authentication, permissions and native memory.

## Repository map

- `crates/yandalf-core`: plugin registration, typed services and events, task data, and Store/Executor/Channel contracts.
- `crates/yandalf-runtime`: durable task execution, session bindings, preparation checkpoints and completion processing.
- `crates/yandalf-codex`: client of an external Codex app-server.
- `crates/yandalf-sqlite`: SQLite Store provider.
- `crates/yandalf-github`: repository input and PR-change decisions.
- `crates/yandalf-channels`: scheduled inputs.
- `crates/yandalf-git-worktree`: generic Git worktree preparation and the CommandRunner capability; no channel or PR semantics.
- `src/app.rs`: configuration of plugins bundled with the service; `src/main.rs`: startup and shutdown.
- `examples/custom_harness.rs`: a complete Rust composition with a custom result observer.
- `tests/`: service integration tests with external fake servers; crate-local tests cover their own APIs.
- `skills/yandalf-harness-development`: guidance for developers building applications and plugins with these crates.
- `skills/github-review`: instructions for the agent performing a review, separate from harness implementation.

Read `README.md` for public configuration and composition examples.

## Setup and checks

Use Rust 1.93 or later with edition 2024. Run Cargo from the repository root. A normal build or test run needs no running agent server, model credentials or GitHub account.

```sh
cargo build --workspace
cargo fmt --all --check
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```

For a focused change, use `cargo test -p <crate>` and `cargo clippy -p <crate> --all-targets -- -D warnings`. After moving code across crates or changing shared contracts, run the workspace checks. The workspace default members include all crates.

Check isolation when changing dependency boundaries:

```sh
cargo check -p yandalf-core
cargo tree -p yandalf-core --edges normal
cargo check -p yandalf --no-default-features --lib
```

## Plugin and crate boundaries

- Built-in and application plugins use the same `Plugin::register` API. New integrations belong in their own modules or crates and should not require special cases in the plugin kernel.
- Keep `yandalf-core` independent of storage engines, Git, transports and model providers. Choose only the crates an application needs; crates are currently unpublished, so consumers need a real checkout or a verified Git revision.
- `register` declares dependencies, providers, handlers, background tasks and cleanup synchronously. Use `provide_with` for asynchronous provider initialization and `task` for ongoing work.
- Services are keyed by their exact Rust type, including `dyn Trait`. Declare dependencies with `require` and retrieve them through `Services::get` rather than importing a concrete provider.
- Provider activation follows the dependency graph. Serial event handlers follow plugin registration order. Use `SerialEvent`/`on` for mutable preparation and `Notification`/`observe` for immutable notifications.
- Runtime hooks prepare `TaskPreparing.task.input` and `.session`. `job.event` is a copy of the admitted observation and is not the runner's mutable execution plan.
- Application shutdown must cancel and await owned tasks before asynchronous cleanup. Server clients close their connections; they do not spawn or terminate agent servers.
- Keep GitHub policy in the GitHub channel, filesystem preparation in worktree plugins, and SQL/locking in the storage adapter. The service binary only assembles its bundled plugin catalog. It has no review commands or domain workflows.
- The GitHub channel translates PR observations into worktree requests when configured. Git worktree preparation accepts explicit paths, revisions and refs; it must not interpret channel events or know hosting-service conventions.

## Durable execution contracts

- Commit input events before acknowledging admission. Deduplicate by source and event ID. Limit execution, not the durable backlog.
- Bind and serialize sessions by subject across input channels. Derive transport IDs and subjects internally; do not require user-defined routing chains or technical identifiers in configuration.
- Save prepared session/turn data before external execution. Save session creation intent and the client message ID before their respective requests. A client message ID is correlation, not an idempotency guarantee.
- Reconcile unknown external outcomes before retrying. Preserve the subject and execution reservation while an agent turn may still exist.
- Preserve the actual cwd of an existing executor session unless a configured preparation plugin changes it. Only trusted configuration and plugin logic may choose paths; event payloads are task data.
- Record raw turn results before completion observers. Retry failed completion processing without repeating the model. Observers may all run again after any observer fails; each must independently tolerate replay using the stable job ID.
- Harness shutdown detaches from agent work. Explicit cancellation and execution timeout are separate operations and must remain responsive during preparation and completion processing.
- Publication and replies are agent tool actions. Task context carries their parameters; the runtime has no output delivery queue or publisher.
- The workspace is unreleased. Change the initial schema for prototype changes rather than adding compatibility layers for intermediate APIs. A requested rollout using existing state must preserve its data and session bindings.

## Code and tests

Use rustfmt and idiomatic Rust. Prefer existing interfaces at replaceable boundaries and ordinary functions for internal logic. Comments should explain an invariant or limitation that the code alone does not reveal.

Add tests for observable behavior and failure recovery. Use fake executors, local socket servers and temporary Git repositories. Normal tests must not call a real model, publish reviews or alter a live service. Changes to admission, session creation, cancellation or completion processing need coverage of the relevant restart or unknown-outcome case.

Keep credentials, local configuration, runtime databases and build output out of the repository. Examples and shared skills must use consumer-supplied paths and endpoints rather than personal VM details. Live deployments and host configuration are separate from normal development checks.
