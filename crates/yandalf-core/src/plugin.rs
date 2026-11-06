use std::{
    any::{Any, TypeId, type_name},
    collections::{HashMap, HashSet},
    future::Future,
    marker::PhantomData,
    sync::{Arc, Mutex, RwLock},
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use futures_util::future::BoxFuture;
use tokio::{sync::watch, task::JoinSet};
use tokio_util::sync::CancellationToken;

pub trait SerialEvent: Send + 'static {}

pub trait Notification: Send + Sync + 'static {}

pub trait Plugin: Send + Sync {
    fn name(&self) -> &str;
    fn register(&self, context: &mut PluginContext) -> Result<()>;
}

type Value = Arc<dyn Any + Send + Sync>;
type Factory = Box<dyn FnOnce(Services) -> BoxFuture<'static, Result<Value>> + Send>;
type Cleanup = Box<dyn FnOnce(Services) -> BoxFuture<'static, Result<()>> + Send>;
type Task = Box<dyn FnOnce(Services, CancellationToken) -> BoxFuture<'static, Result<()>> + Send>;

trait Handler: Send + Sync {
    fn call<'a>(
        &'a self,
        services: &'a Services,
        event: &'a mut (dyn Any + Send),
    ) -> BoxFuture<'a, Result<()>>;
}

struct EventHandler<E, F>(F, PhantomData<fn(E)>);

impl<E, F> Handler for EventHandler<E, F>
where
    E: SerialEvent,
    F: for<'a> Fn(&'a Services, &'a mut E) -> BoxFuture<'a, Result<()>> + Send + Sync,
{
    fn call<'a>(
        &'a self,
        services: &'a Services,
        event: &'a mut (dyn Any + Send),
    ) -> BoxFuture<'a, Result<()>> {
        (self.0)(
            services,
            event
                .downcast_mut::<E>()
                .expect("event type matches registry key"),
        )
    }
}

trait Observer: Send + Sync {
    fn call<'a>(
        &'a self,
        services: &'a Services,
        event: &'a (dyn Any + Send + Sync),
    ) -> BoxFuture<'a, Result<()>>;
}

struct EventObserver<E, F>(F, PhantomData<fn(E)>);

impl<E, F> Observer for EventObserver<E, F>
where
    E: Notification,
    F: for<'a> Fn(&'a Services, &'a E) -> BoxFuture<'a, Result<()>> + Send + Sync,
{
    fn call<'a>(
        &'a self,
        services: &'a Services,
        event: &'a (dyn Any + Send + Sync),
    ) -> BoxFuture<'a, Result<()>> {
        (self.0)(
            services,
            event
                .downcast_ref::<E>()
                .expect("event type matches registry key"),
        )
    }
}

#[derive(Clone, Copy)]
struct ServiceType {
    id: TypeId,
    name: &'static str,
}

impl ServiceType {
    fn of<T: ?Sized + 'static>() -> Self {
        Self {
            id: TypeId::of::<T>(),
            name: type_name::<T>(),
        }
    }
}

struct Provider {
    service: ServiceType,
    factory: Factory,
}

#[derive(Default)]
pub struct PluginContext {
    dependencies: Vec<ServiceType>,
    providers: Vec<Provider>,
    handlers: Vec<(TypeId, Arc<dyn Handler>)>,
    observers: Vec<(TypeId, Arc<dyn Observer>)>,
    tasks: Vec<(String, Task)>,
    cleanups: Vec<Cleanup>,
}

impl PluginContext {
    pub fn require<T: ?Sized + Send + Sync + 'static>(&mut self) {
        self.dependencies.push(ServiceType::of::<T>());
    }

    pub fn provide<T: ?Sized + Send + Sync + 'static>(&mut self, value: Arc<T>) -> Result<()> {
        self.provide_with(move |_| async move { Ok(value) })
    }

    pub fn provide_with<T, F, Fut>(&mut self, factory: F) -> Result<()>
    where
        T: ?Sized + Send + Sync + 'static,
        F: FnOnce(Services) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Arc<T>>> + Send + 'static,
    {
        let service = ServiceType::of::<T>();
        ensure!(
            self.providers
                .iter()
                .all(|provider| provider.service.id != service.id),
            "duplicate service provider: {}",
            service.name
        );
        self.providers.push(Provider {
            service,
            factory: Box::new(move |services| {
                Box::pin(async move { Ok(Arc::new(factory(services).await?) as Value) })
            }),
        });
        Ok(())
    }

    /// Serial preparation handlers run in plugin registration order and stop on the first error.
    pub fn on<E, F>(&mut self, handler: F)
    where
        E: SerialEvent,
        F: for<'a> Fn(&'a Services, &'a mut E) -> BoxFuture<'a, Result<()>> + Send + Sync + 'static,
    {
        self.handlers.push((
            TypeId::of::<E>(),
            Arc::new(EventHandler(handler, PhantomData)),
        ));
    }

    /// Observers receive immutable events; notification awaits every observer, including after an error.
    pub fn observe<E, F>(&mut self, observer: F)
    where
        E: Notification,
        F: for<'a> Fn(&'a Services, &'a E) -> BoxFuture<'a, Result<()>> + Send + Sync + 'static,
    {
        self.observers.push((
            TypeId::of::<E>(),
            Arc::new(EventObserver(observer, PhantomData)),
        ));
    }

    pub fn task<F, Fut>(&mut self, name: impl Into<String>, task: F)
    where
        F: FnOnce(Services, CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.tasks.push((
            name.into(),
            Box::new(move |services, stop| Box::pin(task(services, stop))),
        ));
    }

    /// Cleanup runs after background tasks settle, in reverse dependency and registration order.
    pub fn cleanup<F, Fut>(&mut self, cleanup: F)
    where
        F: FnOnce(Services) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.cleanups
            .push(Box::new(move |services| Box::pin(cleanup(services))));
    }
}

struct Owned<T> {
    owner: usize,
    value: T,
}

#[derive(Default)]
struct Registry {
    values: HashMap<TypeId, Owned<Value>>,
    handlers: HashMap<TypeId, Vec<Owned<Arc<dyn Handler>>>>,
    observers: HashMap<TypeId, Vec<Owned<Arc<dyn Observer>>>>,
}

struct DispatchGate {
    state: Mutex<(bool, usize)>,
    changes: watch::Sender<usize>,
}

struct DispatchGuard<'a>(&'a DispatchGate);

impl DispatchGate {
    fn enter(&self) -> Result<DispatchGuard<'_>> {
        let mut state = self.state.lock().expect("dispatch lifecycle poisoned");
        ensure!(state.0, "plugin application is stopped");
        state.1 += 1;
        Ok(DispatchGuard(self))
    }

    async fn stop(&self) {
        let mut changes = self.changes.subscribe();
        loop {
            {
                let mut state = self.state.lock().expect("dispatch lifecycle poisoned");
                state.0 = false;
                if state.1 == 0 {
                    return;
                }
            }
            if changes.changed().await.is_err() {
                return;
            }
        }
    }
}

impl Drop for DispatchGuard<'_> {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().expect("dispatch lifecycle poisoned");
        state.1 -= 1;
        self.0.changes.send_replace(state.1);
    }
}

#[derive(Clone)]
pub struct Services {
    registry: Arc<RwLock<Registry>>,
    dispatch_gate: Arc<DispatchGate>,
}

impl Default for Services {
    fn default() -> Self {
        Self {
            registry: Arc::new(RwLock::new(Registry::default())),
            dispatch_gate: Arc::new(DispatchGate {
                state: Mutex::new((true, 0)),
                changes: watch::channel(0).0,
            }),
        }
    }
}

impl Services {
    pub fn get<T: ?Sized + Send + Sync + 'static>(&self) -> Result<Arc<T>> {
        let registry = self.registry.read().expect("service registry poisoned");
        registry
            .values
            .get(&TypeId::of::<T>())
            .and_then(|entry| entry.value.downcast_ref::<Arc<T>>())
            .cloned()
            .with_context(|| format!("service is unavailable: {}", type_name::<T>()))
    }

    pub async fn dispatch<E: SerialEvent>(&self, event: &mut E) -> Result<()> {
        let _dispatch = self.dispatch_gate.enter()?;
        let handlers: Vec<_> = self
            .registry
            .read()
            .expect("service registry poisoned")
            .handlers
            .get(&TypeId::of::<E>())
            .into_iter()
            .flatten()
            .map(|entry| entry.value.clone())
            .collect();
        for handler in handlers {
            handler.call(self, event).await?;
        }
        Ok(())
    }

    pub async fn notify<E: Notification>(&self, event: &E) -> Result<()> {
        let _dispatch = self.dispatch_gate.enter()?;
        let observers: Vec<_> = self
            .registry
            .read()
            .expect("service registry poisoned")
            .observers
            .get(&TypeId::of::<E>())
            .into_iter()
            .flatten()
            .map(|entry| entry.value.clone())
            .collect();
        let results = futures_util::future::join_all(
            observers.iter().map(|observer| observer.call(self, event)),
        )
        .await;
        results.into_iter().collect()
    }

    fn remove_owner(&self, owner: usize) {
        let mut registry = self.registry.write().expect("service registry poisoned");
        registry.values.retain(|_, entry| entry.owner != owner);
        registry.handlers.retain(|_, entries| {
            entries.retain(|entry| entry.owner != owner);
            !entries.is_empty()
        });
        registry.observers.retain(|_, entries| {
            entries.retain(|entry| entry.owner != owner);
            !entries.is_empty()
        });
    }
}

#[derive(Default)]
pub struct AppBuilder {
    plugins: Vec<Box<dyn Plugin>>,
}

impl AppBuilder {
    pub fn plugin(mut self, plugin: impl Plugin + 'static) -> Self {
        self.plugins.push(Box::new(plugin));
        self
    }

    pub async fn build(self) -> Result<App> {
        let mut definitions = Vec::new();
        for plugin in self.plugins {
            let name = plugin.name().to_owned();
            let mut context = PluginContext::default();
            plugin
                .register(&mut context)
                .with_context(|| format!("register plugin {name}"))?;
            definitions.push((name, context));
        }
        let order = activation_order(&definitions)?;
        let mut definitions: Vec<_> = definitions.into_iter().map(Some).collect();
        let mut app = App {
            services: Services::default(),
            active: Vec::new(),
            tasks: Vec::new(),
        };
        for owner in order {
            let (name, mut context) = definitions[owner].take().unwrap();
            app.active.push(ActivePlugin {
                owner,
                name: name.clone(),
                cleanups: context.cleanups,
            });
            for provider in context.providers {
                let value = match (provider.factory)(app.services.clone()).await {
                    Ok(value) => value,
                    Err(error) => {
                        let original = error.context(format!("activate plugin {name}"));
                        return match app.shutdown().await {
                            Ok(()) => Err(original),
                            Err(cleanup) => Err(original
                                .context(format!("plugin rollback also failed: {cleanup:#}"))),
                        };
                    }
                };
                app.services
                    .registry
                    .write()
                    .expect("service registry poisoned")
                    .values
                    .insert(provider.service.id, Owned { owner, value });
            }
            {
                let mut registry = app
                    .services
                    .registry
                    .write()
                    .expect("service registry poisoned");
                for (kind, value) in context.handlers {
                    let entries = registry.handlers.entry(kind).or_default();
                    entries.push(Owned { owner, value });
                    entries.sort_by_key(|entry| entry.owner);
                }
                for (kind, value) in context.observers {
                    let entries = registry.observers.entry(kind).or_default();
                    entries.push(Owned { owner, value });
                    entries.sort_by_key(|entry| entry.owner);
                }
            }
            app.tasks.extend(
                context
                    .tasks
                    .drain(..)
                    .map(|(task_name, task)| (format!("{name}/{task_name}"), task)),
            );
        }
        Ok(app)
    }
}

fn activation_order(definitions: &[(String, PluginContext)]) -> Result<Vec<usize>> {
    let mut providers = HashMap::new();
    for (owner, (name, context)) in definitions.iter().enumerate() {
        for provider in &context.providers {
            if let Some(other) = providers.insert(provider.service.id, owner) {
                bail!(
                    "duplicate providers for {}: {} and {name}",
                    provider.service.name,
                    definitions[other].0
                );
            }
        }
    }
    for (name, context) in definitions {
        for required in &context.dependencies {
            ensure!(
                providers.contains_key(&required.id),
                "plugin {name} requires missing service {}",
                required.name
            );
        }
    }
    let mut active = HashSet::new();
    let mut order = Vec::new();
    while order.len() < definitions.len() {
        let ready = definitions
            .iter()
            .enumerate()
            .find(|(owner, (_, context))| {
                !active.contains(owner)
                    && context.dependencies.iter().all(|required| {
                        let provider = providers[&required.id];
                        provider == *owner || active.contains(&provider)
                    })
            });
        let Some((owner, _)) = ready else {
            let names: Vec<_> = definitions
                .iter()
                .enumerate()
                .filter(|(owner, _)| !active.contains(owner))
                .map(|(_, (name, _))| name.as_str())
                .collect();
            bail!("cyclic plugin dependencies: {}", names.join(", "));
        };
        active.insert(owner);
        order.push(owner);
    }
    Ok(order)
}

struct ActivePlugin {
    owner: usize,
    name: String,
    cleanups: Vec<Cleanup>,
}

pub struct App {
    services: Services,
    active: Vec<ActivePlugin>,
    tasks: Vec<(String, Task)>,
}

impl App {
    pub fn builder() -> AppBuilder {
        AppBuilder::default()
    }

    pub fn services(&self) -> Services {
        self.services.clone()
    }

    /// Cancel `stop` and await this method to let owned tasks settle before asynchronous cleanup.
    pub async fn run(mut self, stop: CancellationToken) -> Result<()> {
        let stop = stop.child_token();
        let _cancel_on_drop = stop.clone().drop_guard();
        if stop.is_cancelled() {
            return self.shutdown().await;
        }
        let mut tasks = JoinSet::new();
        for (name, task) in self.tasks.drain(..) {
            let services = self.services.clone();
            let task_stop = stop.child_token();
            tasks.spawn(async move {
                task(services, task_stop)
                    .await
                    .with_context(|| format!("plugin task {name}"))
            });
        }
        let mut failure = None;
        loop {
            tokio::select! {
                _ = stop.cancelled() => break,
                result = tasks.join_next(), if !tasks.is_empty() => {
                    match result.unwrap() {
                        Ok(Ok(())) => {},
                        Ok(Err(error)) => { failure = Some(error); break; },
                        Err(error) => { failure = Some(error.into()); break; },
                    }
                }
            }
        }
        stop.cancel();
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    failure.get_or_insert(error);
                }
                Err(error) => {
                    failure.get_or_insert(error.into());
                }
            }
        }
        if let Err(error) = self.shutdown().await {
            failure.get_or_insert(error);
        }
        failure.map_or(Ok(()), Err)
    }

    pub async fn shutdown(mut self) -> Result<()> {
        self.services.dispatch_gate.stop().await;
        let mut failures = Vec::new();
        for plugin in self.active.drain(..).rev() {
            for cleanup in plugin.cleanups.into_iter().rev() {
                if let Err(error) = cleanup(self.services.clone()).await {
                    failures.push(format!("{}: {error:#}", plugin.name));
                }
            }
            self.services.remove_owner(plugin.owner);
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(anyhow!("plugin cleanup failed: {}", failures.join("; ")))
        }
    }
}
