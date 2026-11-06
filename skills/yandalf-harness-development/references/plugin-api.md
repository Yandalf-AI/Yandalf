# Plugin API and runner contracts

Use the consumer's selected Yandalf checkout as the source of truth when signatures differ.

## Source map

Paths below are inside the framework checkout, not relative to an installed skill:

- `crates/yandalf-core/src/plugin.rs`: Plugin, PluginContext, Services, App, SerialEvent and Notification.
- `crates/yandalf-core/src/task.rs`: lifecycle events.
- `crates/yandalf-core/src/domain.rs`: events, jobs, session specifications, prepared tasks and results.
- `crates/yandalf-core/src/channel.rs`: Channel, Inbox and ChannelPlugin.
- `crates/yandalf-core/src/executor.rs`: Executor, ExecutorProvider and ExecutorConnection.
- `crates/yandalf-core/src/store.rs`: persistence and exclusive execution ownership.
- `crates/yandalf-runtime/src/lib.rs`: TaskRunner and executor reconnection.
- `examples/custom_harness.rs`: complete host composition.

## Minimal external plugin crate

Substitute the real checkout path. Cargo does not expand `YANDALF_CHECKOUT` as an environment variable.

```toml
[package]
name = "task-instructions"
version = "0.1.0"
edition = "2024"
rust-version = "1.93"
publish = false

[dependencies]
anyhow = "1"
yandalf-core = { path = "YANDALF_CHECKOUT/crates/yandalf-core" }
```

`src/lib.rs`:

```rust
use std::sync::Arc;

use anyhow::Result;
use yandalf_core::{
    plugin::{Plugin, PluginContext},
    task::TaskPreparing,
};

pub struct TaskInstructions(pub String);

impl Plugin for TaskInstructions {
    fn name(&self) -> &str {
        "task-instructions"
    }

    fn register(&self, ctx: &mut PluginContext) -> Result<()> {
        let instructions: Arc<str> = self.0.clone().into();
        ctx.on::<TaskPreparing, _>(move |_, task| {
            let instructions = instructions.clone();
            Box::pin(async move {
                task.task.input.text.push_str("\n\n");
                task.task.input.text.push_str(&instructions);
                Ok(())
            })
        });
        Ok(())
    }
}
```

The host adds this crate as a Cargo dependency and passes `TaskInstructions(...)` to `.plugin(...)` alongside its input, Store, executor provider and runner. Use `anyhow` directly for the Result type; core does not re-export it.

## Typed services

`require::<T>()` declares a dependency and returns `()`. `provide::<T>(Arc<T>)` and `provide_with::<T, _, _>(factory)` return `Result<()>`. An async factory receives owned Services; required providers activate first.

For a user-defined `Catalog: Send + Sync + 'static` trait, the producer registers:

```rust
ctx.provide::<dyn Catalog>(Arc::new(LocalCatalog::new()))?;
```

The consumer registers `ctx.require::<dyn Catalog>()`, then obtains `services.get::<dyn Catalog>()?` inside its factory, handler or task. Retrieving LocalCatalog is a different lookup from retrieving dyn Catalog.

`build()` rejects missing required providers, duplicate providers and dependency cycles before provider factories run. Plugin names are diagnostics; users do not need to invent configuration IDs for them.

## Event modes and ordering

A custom event declares its contract with `impl SerialEvent for MyEvent {}` or `impl Notification for MyEvent {}`.

- `ctx.on::<E, _>(handler)` accepts `for<'a> Fn(&'a Services, &'a mut E) -> BoxFuture<'a, Result<()>>`. `services.dispatch(&mut event).await` runs handlers in plugin registration order and stops on error.
- `ctx.observe::<E, _>(observer)` accepts `for<'a> Fn(&'a Services, &'a E) -> BoxFuture<'a, Result<()>>`. `services.notify(&event).await` awaits all observers, including those still running when another fails.
- `on` and `observe` return `()`; neither registration call takes `?`.

Use inferred closures returning `Box::pin(async move { ... })`. A captured Arc or String belongs to the reusable Fn; clone it before moving a copy into each per-call future.

The default runner emits:

- TaskDeciding: mutate `decision` to run, skip, or archive the subject's existing session. Archive does not prepare a workspace or start a model turn and retains the binding for later reuse; executors must implement idempotent archival.
- TaskPreparing: mutate `task.input` and `task.session`; `existing_session` describes the binding when present.
- SessionPreparing: prepare a new session specification when no binding was present during preparation.
- TaskCompleted: observe the immutable raw result after it has been saved. Register with `observe`. This includes completed, failed and interrupted turns; inspect `result.status` to distinguish them. The job is still Finalizing inside the observer, and becomes terminal only after observers return. Do not wait for a terminal job status from inside its observer.

`TaskPreparing.job.event` is a cloned observation. The runner uses the resulting `task` as its plan. Once checkpointed, recovery reuses that plan rather than repeating preparation hooks. If preparation failed before its checkpoint, a later attempt may invoke the handlers again.

## Tasks, cleanup and persistence

`ctx.task(name, callback)` receives owned Services and CancellationToken; the callback is FnOnce and returns an async Result. Use cancellation-aware waits. A failing background task stops sibling tasks and starts cleanup.

`ctx.cleanup(callback)` receives owned Services. Cleanup runs after background tasks and active dispatches settle, in reverse activation and registration order. Failed activation rolls back resources already registered. Composition is static; native shared-library loading and hot replacement are outside the current API.

In-memory services and listeners belong to this application lifetime. Saved sessions, worktrees and execution facts belong in the Store or their owning durable system. Closing a client connection does not imply cancelling a model turn or deleting its workspace.

Completion observers have at-least-once semantics. When one fails, all can be retried, including observers that already succeeded. Deduplicate each external effect using `TaskCompleted.job.id` or an application receipt. The runner's Finalizing state preserves the raw result and subject reservation until completion processing succeeds or is explicitly cancelled.

## Implement a boundary

For a custom input, implement `Channel::run(inbox, stop)` and register it through ChannelPlugin, or register the same service dependency and task directly. Only acknowledge after `Inbox::accept` succeeds. Derive stable delivery IDs and subjects from the source entity.

For Git worktrees, register `GitWorktree` and a CommandRunner provider. A task-preparation handler obtains `WorktreeService` and supplies a `WorktreeRequest` with trusted paths, ownership, validated revisions and fetch refs. The service knows no channel event schema or hosting-service conventions. The GitHub channel's optional `git-worktree` feature and `with_worktree(WorktreeConfig)` construct these requests from PR observations; other channels supply their own mapping. Keep PR numbers, GitHub refs and repository identity validation in the GitHub channel.

Register multiple ChannelPlugin instances to run independent inputs in one application. They share the durable inbox; a busy executor must not block admission. Event IDs are scoped by source, while matching subjects share a session across sources. For blocking input such as stdin, bridge a dedicated reader thread into a bounded async channel; a blocked `tokio::io::stdin()` read can prevent Tokio runtime shutdown. The custom harness example uses two application mailboxes as transport boundaries.

For an agent backend, implement Executor for session creation, configuration, turn execution, inspection and interruption. ExecutorProvider supplies a connection plus a future reporting closure. Match the selected checkout's interface and establish what the remote API can recover before claiming idempotent creation or submission.

For a database, implement every Store operation with its documented atomicity, read-after-write consistency and exclusive runner ownership. In-flight mutations must settle before a dropped caller releases execution ownership. Use SQLite's failure/recovery tests as behavioral examples; SQL and connections stay inside the storage provider.

Archiving records an idempotent lifecycle intent before the executor call. Keep it in recovery and subject reservations until its outcome is confirmed, including after cancellation; it is not a model turn or an ordinary skipped job.

The current runner does not automatically retire a binding to a deleted backend session. An explicitly requested reset needs a deliberate lifecycle policy; an unavailable or unloaded session alone is not evidence that a new one should be created.
