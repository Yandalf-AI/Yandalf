use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use anyhow::{Result, bail};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use yandalf_core::plugin::{App, Notification, Plugin, PluginContext, SerialEvent};

struct CustomPlugin<F> {
    name: &'static str,
    register: F,
}

impl<F> Plugin for CustomPlugin<F>
where
    F: Fn(&mut PluginContext) -> Result<()> + Send + Sync,
{
    fn name(&self) -> &str {
        self.name
    }
    fn register(&self, context: &mut PluginContext) -> Result<()> {
        (self.register)(context)
    }
}

fn plugin<F>(name: &'static str, register: F) -> CustomPlugin<F>
where
    F: Fn(&mut PluginContext) -> Result<()> + Send + Sync,
{
    CustomPlugin { name, register }
}

trait Counter: Send + Sync {
    fn value(&self) -> usize;
}
struct Fixed(usize);
impl Counter for Fixed {
    fn value(&self) -> usize {
        self.0
    }
}
struct Doubled(usize);
struct A;
struct B;
struct Numbers(Vec<usize>);
impl SerialEvent for Numbers {}
struct Bytes(Vec<u8>);
impl SerialEvent for Bytes {}
struct Number(usize);
impl SerialEvent for Number {}
struct Flag;
impl SerialEvent for Flag {}
struct Count(usize);
impl Notification for Count {}

#[tokio::test]
async fn custom_plugins_resolve_trait_services_before_factories_and_keep_handler_registration_order()
 {
    let activated = Arc::new(Mutex::new(Vec::new()));
    let consumer_log = activated.clone();
    let provider_log = activated.clone();
    let app = App::builder()
        .plugin(plugin("consumer", move |ctx| {
            ctx.require::<dyn Counter>();
            let log = consumer_log.clone();
            ctx.provide_with::<Doubled, _, _>(move |services| async move {
                let counter = services.get::<dyn Counter>()?;
                log.lock().unwrap().push("consumer");
                Ok(Arc::new(Doubled(counter.value() * 2)))
            })?;
            ctx.on::<Numbers, _>(|services, event| {
                Box::pin(async move {
                    event.0.push(services.get::<Doubled>()?.0);
                    Ok(())
                })
            });
            Ok(())
        }))
        .plugin(plugin("provider", move |ctx| {
            let log = provider_log.clone();
            ctx.provide_with::<dyn Counter, _, _>(move |_| async move {
                log.lock().unwrap().push("provider");
                Ok(Arc::new(Fixed(7)) as Arc<dyn Counter>)
            })?;
            ctx.on::<Numbers, _>(|services, event| {
                Box::pin(async move {
                    event.0.push(services.get::<dyn Counter>()?.value());
                    Ok(())
                })
            });
            Ok(())
        }))
        .build()
        .await
        .unwrap();
    assert_eq!(*activated.lock().unwrap(), ["provider", "consumer"]);
    let services = app.services();
    let mut event = Numbers(Vec::new());
    services.dispatch(&mut event).await.unwrap();
    assert_eq!(event.0, [14, 7]);
    assert!(services.get::<Fixed>().is_err());
    app.shutdown().await.unwrap();
    assert!(services.get::<dyn Counter>().is_err());
    assert!(services.dispatch(&mut event).await.is_err());
}

#[tokio::test]
async fn missing_cyclic_and_duplicate_dependencies_fail_before_any_factory_runs() {
    let starts = Arc::new(AtomicUsize::new(0));
    let missing_starts = starts.clone();
    let missing = App::builder()
        .plugin(plugin("missing", move |ctx| {
            ctx.require::<dyn Counter>();
            let starts = missing_starts.clone();
            ctx.provide_with::<A, _, _>(move |_| async move {
                starts.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(A))
            })
        }))
        .build()
        .await
        .err()
        .unwrap();
    assert!(missing.to_string().contains("missing service"));

    let a_starts = starts.clone();
    let b_starts = starts.clone();
    let cycle = App::builder()
        .plugin(plugin("a", move |ctx| {
            ctx.require::<B>();
            let starts = a_starts.clone();
            ctx.provide_with::<A, _, _>(move |_| async move {
                starts.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(A))
            })
        }))
        .plugin(plugin("b", move |ctx| {
            ctx.require::<A>();
            let starts = b_starts.clone();
            ctx.provide_with::<B, _, _>(move |_| async move {
                starts.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(B))
            })
        }))
        .build()
        .await
        .err()
        .unwrap();
    assert!(cycle.to_string().contains("cyclic plugin dependencies"));

    let duplicate_starts = starts.clone();
    let duplicate = App::builder()
        .plugin(plugin("first", move |ctx| {
            let starts = duplicate_starts.clone();
            ctx.provide_with::<A, _, _>(move |_| async move {
                starts.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(A))
            })
        }))
        .plugin(plugin("second", |ctx| ctx.provide(Arc::new(A))))
        .build()
        .await
        .err()
        .unwrap();
    assert!(duplicate.to_string().contains("duplicate providers"));
    assert_eq!(starts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn preparation_stops_on_failure_but_notifications_await_every_observer() {
    let observations = Arc::new(AtomicUsize::new(0));
    let observed = observations.clone();
    let app = App::builder()
        .plugin(plugin("events", move |ctx| {
            ctx.on::<Bytes, _>(|_, event| {
                Box::pin(async move {
                    event.0.push(1);
                    Ok(())
                })
            });
            ctx.on::<Bytes, _>(|_, event| {
                Box::pin(async move {
                    event.0.push(2);
                    bail!("not prepared")
                })
            });
            ctx.on::<Bytes, _>(|_, event| {
                Box::pin(async move {
                    event.0.push(3);
                    Ok(())
                })
            });
            ctx.observe::<Count, _>(|_, _| Box::pin(async move { bail!("observer failed") }));
            let observed = observed.clone();
            ctx.observe::<Count, _>(move |_, event| {
                let observed = observed.clone();
                Box::pin(async move {
                    tokio::task::yield_now().await;
                    observed.fetch_add(event.0, Ordering::SeqCst);
                    Ok(())
                })
            });
            Ok(())
        }))
        .build()
        .await
        .unwrap();
    let mut event = Bytes(Vec::new());
    assert!(app.services().dispatch(&mut event).await.is_err());
    assert_eq!(event.0, [1, 2]);
    let notification = Count(4);
    assert!(app.services().notify(&notification).await.is_err());
    assert_eq!(observations.load(Ordering::SeqCst), 4);
    assert_eq!(notification.0, 4);
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn input_tasks_continue_while_executor_waits_and_cleanup_runs_after_tasks_settle() {
    let observed_input = Arc::new(Notify::new());
    let log = Arc::new(Mutex::new(Vec::new()));
    let provider_log = log.clone();
    let consumer_log = log.clone();
    let input = observed_input.clone();
    let app = App::builder()
        .plugin(plugin("workers", move |ctx| {
            ctx.require::<A>();
            let input = input.clone();
            let log = consumer_log.clone();
            ctx.task("intake", move |_, stop| async move {
                input.notify_one();
                stop.cancelled().await;
                log.lock().unwrap().push("intake settled");
                Ok(())
            });
            let log = consumer_log.clone();
            ctx.task("offline executor", move |_, stop| async move {
                stop.cancelled().await;
                log.lock().unwrap().push("executor settled");
                Ok(())
            });
            let log = consumer_log.clone();
            ctx.cleanup(move |services| async move {
                services.get::<A>()?;
                let mut log = log.lock().unwrap();
                assert!(log.contains(&"intake settled") && log.contains(&"executor settled"));
                log.push("workers cleanup");
                Ok(())
            });
            Ok(())
        }))
        .plugin(plugin("store", move |ctx| {
            ctx.provide(Arc::new(A))?;
            let log = provider_log.clone();
            ctx.cleanup(move |_| async move {
                log.lock().unwrap().push("store cleanup");
                Ok(())
            });
            Ok(())
        }))
        .build()
        .await
        .unwrap();
    let services = app.services();
    let stop = CancellationToken::new();
    let run = tokio::spawn(app.run(stop.clone()));
    observed_input.notified().await;
    assert!(!run.is_finished());
    stop.cancel();
    run.await.unwrap().unwrap();
    let log = log.lock().unwrap();
    assert_eq!(&log[2..], ["workers cleanup", "store cleanup"]);
    assert!(services.get::<A>().is_err());
}

#[tokio::test]
async fn failed_activation_rolls_back_partial_plugin_and_its_dependencies() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let base_log = log.clone();
    let failed_log = log.clone();
    let task_started = Arc::new(AtomicBool::new(false));
    let started = task_started.clone();
    let failure = App::builder()
        .plugin(plugin("base", move |ctx| {
            ctx.provide(Arc::new(A))?;
            let log = base_log.clone();
            ctx.cleanup(move |_| async move {
                log.lock().unwrap().push("base");
                Ok(())
            });
            Ok(())
        }))
        .plugin(plugin("broken", move |ctx| {
            ctx.require::<A>();
            ctx.provide(Arc::new(B))?;
            ctx.provide_with::<Doubled, _, _>(|_| async move { bail!("activation failed") })?;
            let log = failed_log.clone();
            ctx.cleanup(move |services| async move {
                services.get::<A>()?;
                services.get::<B>()?;
                log.lock().unwrap().push("partial");
                Ok(())
            });
            let started = started.clone();
            ctx.task("must not start", move |_, _| async move {
                started.store(true, Ordering::SeqCst);
                Ok(())
            });
            Ok(())
        }))
        .build()
        .await
        .err()
        .unwrap();
    assert!(format!("{failure:#}").contains("activation failed"));
    assert_eq!(*log.lock().unwrap(), ["partial", "base"]);
    assert!(!task_started.load(Ordering::SeqCst));
}

#[tokio::test]
async fn failed_background_task_stops_siblings_before_cleanup() {
    let settled = Arc::new(AtomicBool::new(false));
    let cleanup_seen = Arc::new(AtomicBool::new(false));
    let task_settled = settled.clone();
    let cleanup = cleanup_seen.clone();
    let app = App::builder()
        .plugin(plugin("workers", move |ctx| {
            ctx.task("failed", |_, _| async move { bail!("worker failed") });
            let settled = task_settled.clone();
            ctx.task("sibling", move |_, stop| async move {
                stop.cancelled().await;
                settled.store(true, Ordering::SeqCst);
                Ok(())
            });
            let settled = task_settled.clone();
            let cleanup = cleanup.clone();
            ctx.cleanup(move |_| async move {
                assert!(settled.load(Ordering::SeqCst));
                cleanup.store(true, Ordering::SeqCst);
                Ok(())
            });
            Ok(())
        }))
        .build()
        .await
        .unwrap();
    let error = app.run(CancellationToken::new()).await.unwrap_err();
    assert!(format!("{error:#}").contains("worker failed"));
    assert!(cleanup_seen.load(Ordering::SeqCst));
}

#[tokio::test]
async fn shutdown_waits_for_dispatched_handlers_and_runs_remaining_cleanup_after_an_error() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let cleaned = Arc::new(AtomicBool::new(false));
    let started = entered.clone();
    let gate = release.clone();
    let cleanup = cleaned.clone();
    let app = App::builder()
        .plugin(plugin("owned", move |ctx| {
            let entered = started.clone();
            let release = gate.clone();
            ctx.on::<Number, _>(move |_, event| {
                let entered = entered.clone();
                let release = release.clone();
                Box::pin(async move {
                    entered.notify_one();
                    release.notified().await;
                    event.0 += 1;
                    Ok(())
                })
            });
            let cleanup = cleanup.clone();
            ctx.cleanup(move |_| async move {
                cleanup.store(true, Ordering::SeqCst);
                Ok(())
            });
            ctx.cleanup(|_| async move { bail!("cleanup failed") });
            Ok(())
        }))
        .build()
        .await
        .unwrap();
    let services = app.services();
    let dispatched = tokio::spawn(async move {
        let mut value = Number(0);
        services.dispatch(&mut value).await.unwrap();
        value.0
    });
    entered.notified().await;
    let shutdown = tokio::spawn(app.shutdown());
    tokio::task::yield_now().await;
    assert!(!cleaned.load(Ordering::SeqCst));
    release.notify_one();
    assert_eq!(dispatched.await.unwrap(), 1);
    assert!(shutdown.await.unwrap().is_err());
    assert!(cleaned.load(Ordering::SeqCst));
}

#[tokio::test]
async fn nested_dispatch_during_shutdown_fails_without_deadlocking_outer_handler() {
    let entered = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    let entered_handler = entered.clone();
    let resume_handler = resume.clone();
    let app = App::builder()
        .plugin(plugin("nested", move |ctx| {
            let entered = entered_handler.clone();
            let resume = resume_handler.clone();
            ctx.on::<Number, _>(move |services, _| {
                let entered = entered.clone();
                let resume = resume.clone();
                Box::pin(async move {
                    entered.notify_one();
                    resume.notified().await;
                    services.dispatch(&mut Flag).await
                })
            });
            Ok(())
        }))
        .build()
        .await
        .unwrap();
    let services = app.services();
    let nested = tokio::spawn(async move { services.dispatch(&mut Number(1)).await });
    entered.notified().await;
    let shutdown = app.shutdown();
    tokio::pin!(shutdown);
    tokio::select! {
        biased;
        _ = &mut shutdown => panic!("shutdown must wait for the outer handler"),
        _ = std::future::ready(()) => {},
    }
    resume.notify_one();
    assert!(nested.await.unwrap().is_err());
    shutdown.await.unwrap();
}

#[tokio::test]
async fn already_cancelled_application_cleans_up_without_starting_tasks() {
    let started = Arc::new(AtomicBool::new(false));
    let cleaned = Arc::new(AtomicBool::new(false));
    let task_started = started.clone();
    let cleanup_seen = cleaned.clone();
    let app = App::builder()
        .plugin(plugin("cancelled", move |ctx| {
            let started = task_started.clone();
            ctx.task("must not run", move |_, _| async move {
                started.store(true, Ordering::SeqCst);
                Ok(())
            });
            let cleaned = cleanup_seen.clone();
            ctx.cleanup(move |_| async move {
                cleaned.store(true, Ordering::SeqCst);
                Ok(())
            });
            Ok(())
        }))
        .build()
        .await
        .unwrap();
    let stop = CancellationToken::new();
    stop.cancel();
    app.run(stop).await.unwrap();
    assert!(!started.load(Ordering::SeqCst));
    assert!(cleaned.load(Ordering::SeqCst));
}
