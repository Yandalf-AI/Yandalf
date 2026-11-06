---
name: yandalf-harness-development
description: Build consumer Rust harnesses and plugin crates with Yandalf's public API, including custom channels, executors and storage providers.
---

# Yandalf harness development

Build the consumer's application in its own Cargo project. Extend Yandalf through its public plugin and service contracts; changes to the framework are needed only when the requested capability exposes a missing shared contract.

## Locate the framework

Find the Yandalf checkout or dependency selected by the user. Read its root `Cargo.toml`, `README.md`, the manifests of the crates you need and their public API. A separately installed copy of this skill may not be next to the framework source.

The current crates are unpublished. Use real `path` dependencies or a user-selected Git revision verified to contain the required crates. Do not invent crates.io versions or assume the latest repository commit has the same API as a local checkout.

For registration signatures, lifecycle events and a minimal plugin example, read [references/plugin-api.md](references/plugin-api.md). Confirm those signatures against the selected source before writing code.

## Choose the composition

Establish the inputs, logical subject identity, executor endpoint, persistent state and any preparation the task needs. Resolve which host owns execution paths before preparing files or worktrees.

- An ordinary plugin depends on `yandalf-core` and its own dependencies.
- Add `yandalf-runtime` when using the durable task runner.
- Add only the storage, executor and input crates the application uses. `yandalf-sqlite`, `yandalf-codex`, `yandalf-github`, `yandalf-channels` and `yandalf-git-worktree` are separate plugins and services.
- The root `yandalf` crate is a facade and a bundled service. Its TOML catalog selects bundled plugins; a consumer composes additional crates with `App::builder().plugin(...)` in Rust.

Use a service trait for a capability with replaceable providers and typed events for interception or enrichment. Keep a small integration in one crate unless its contract, provider and consumers actually need independent ownership. Plugin authors may declare their own service traits and event types.

## Implement and assemble

Implement `Plugin::register` as synchronous registration. Declare required services, register providers or asynchronous provider factories, and attach handlers, background tasks and cleanup through `PluginContext`.

Choose event modes deliberately: serial handlers mutate preparation in registration order; notification observers receive immutable data and may run concurrently. For runner hooks, change `TaskPreparing.task.input` or `.session`; editing its copied `job.event` does not change execution.

Clone captured configuration or `Arc` handles inside each `Fn` handler invocation before entering `Box::pin(async move { ... })`. Retrieve services through `Services::get` using the same exact type, including `dyn Trait`, under which they were provided.

Assemble the host with the required providers and plugins, await `build()`, and run it with an application cancellation token. Background tasks must observe cancellation. Await `run(stop)` or `shutdown()` to complete asynchronous cleanup.

## Preserve execution contracts

- Admit channel events through `Inbox`; acknowledgements follow durable acceptance. Keep polling and intake independent of executor availability.
- Keep one logical subject across input channels that address the same work. Event IDs identify deliveries; subject identifies the session.
- Let the task runner own subject serialization, execution limits, preparation checkpoints and external request correlation. Preparation code ensures the requested resource exists and can be safely reused.
- Agent servers run independently. Executor clients connect to them and release client connections on shutdown. Server-side configuration owns authentication, permissions, home directories and native memory.
- Distinguish a confirmed missing backend resource from an unknown request outcome. A lost acknowledgement does not establish that creation or turn submission failed. Follow the selected runtime's recovery API rather than editing its storage directly.
- Raw turn results are saved before `TaskCompleted`. A failed observer may cause every observer to run again; each side effect needs its own replay handling keyed by the stable job ID. Reprocessing a result must not call the model again.

## Verify the consumer

Compile the consumer as a standalone Cargo project against the chosen dependencies. Run rustfmt, focused tests and clippy in that project. Use fake executors, local socket servers and temporary repositories for behavior checks; live provider tests follow the user's requested scope.

Cover the integration's observable contract: service resolution, changed preparation, cancellation or recovery as relevant. For a new channel or executor, demonstrate that repeated delivery or a lost response does not repeat work. For a result observer, demonstrate replay handling. Report the consumer entry point, selected crates and checks that actually ran.
