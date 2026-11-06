use anyhow::Result;
use async_trait::async_trait;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use yandalf_core::{
    domain::*,
    executor::Executor,
    store::{RunnerLease, Store},
};
use yandalf_runtime::{Config, Runtime};
use yandalf_sqlite::SqliteStore;

struct UnusedExecutor;
#[async_trait]
impl Executor for UnusedExecutor {
    async fn create_thread(&self, _: &Path, _: &str) -> Result<String> {
        unreachable!()
    }
    async fn inspect_thread(&self, _: &str) -> Result<ThreadInspection> {
        unreachable!()
    }
    async fn thread_cwd(&self, _: &str) -> Result<PathBuf> {
        unreachable!()
    }
    async fn configure_thread(&self, _: &str, _: &SessionSpec) -> Result<()> {
        unreachable!()
    }
    async fn start_turn(&self, _: &str, _: &str, _: &TurnInput) -> Result<String> {
        unreachable!()
    }
    async fn wait_turn(&self, _: &str, _: &str) -> Result<TurnResult> {
        unreachable!()
    }
    async fn inspect(&self, _: &str, _: &str, _: Option<&str>) -> Result<Inspection> {
        unreachable!()
    }
    async fn interrupt(&self, _: &str, _: &str) -> Result<()> {
        unreachable!()
    }
}
fn event(id: &str, subject: &str) -> Event {
    Event {
        id: id.into(),
        source: "test".into(),
        subject: subject.into(),
        title: None,
        text: "Perform task".into(),
        context: serde_json::Value::Null,
    }
}

struct FailCount {
    inner: Arc<dyn Store>,
    worker_entered: Arc<Notify>,
    calls: std::sync::atomic::AtomicUsize,
}
#[async_trait]
impl Store for FailCount {
    async fn acquire_runner(&self) -> Result<Box<dyn RunnerLease>> {
        self.inner.acquire_runner().await
    }
    async fn enqueue(&self, event: &Event) -> Result<Job> {
        self.inner.enqueue(event).await
    }
    async fn last_event(&self, source: &str, subject: &str) -> Result<Option<Event>> {
        self.inner.last_event(source, subject).await
    }
    async fn last_events(&self, source: &str) -> Result<Vec<Event>> {
        self.inner.last_events(source).await
    }
    async fn claim_next(&self) -> Result<Option<Job>> {
        self.inner.claim_next().await
    }
    async fn get_job(&self, id: &str) -> Result<Job> {
        self.inner.get_job(id).await
    }
    async fn list_jobs(&self, limit: usize) -> Result<Vec<Job>> {
        self.inner.list_jobs(limit).await
    }
    async fn job_result(&self, id: &str) -> Result<Option<TurnResult>> {
        self.inner.job_result(id).await
    }
    async fn job_preparation(&self, id: &str) -> Result<Option<PreparedTask>> {
        self.inner.job_preparation(id).await
    }
    async fn save_preparation(&self, id: &str, preparation: &PreparedTask) -> Result<()> {
        self.inner.save_preparation(id, preparation).await
    }
    async fn record_result(&self, id: &str, result: &TurnResult) -> Result<()> {
        self.inner.record_result(id, result).await
    }
    async fn finalizing_jobs(&self) -> Result<Vec<Job>> {
        self.inner.finalizing_jobs().await
    }
    async fn finalization_failed(&self, id: &str, error: &str) -> Result<()> {
        self.inner.finalization_failed(id, error).await
    }
    async fn uncertain_jobs(&self) -> Result<Vec<Job>> {
        self.inner.uncertain_jobs().await
    }
    async fn session(&self, subject: &str) -> Result<Option<Session>> {
        self.inner.session(subject).await
    }
    async fn bind_session(&self, session: &Session) -> Result<()> {
        self.inner.bind_session(session).await
    }
    async fn begin_session_creation(
        &self,
        subject: &str,
        cwd: &std::path::Path,
    ) -> Result<(SessionCreation, bool)> {
        self.inner.begin_session_creation(subject, cwd).await
    }
    async fn session_creation(&self, subject: &str) -> Result<Option<SessionCreation>> {
        self.inner.session_creation(subject).await
    }
    async fn resume_preparation(&self, id: &str) -> Result<()> {
        self.inner.resume_preparation(id).await
    }
    async fn mark_submitting(&self, id: &str, thread_id: &str) -> Result<()> {
        self.inner.mark_submitting(id, thread_id).await
    }
    async fn mark_running(&self, id: &str, turn_id: &str) -> Result<()> {
        self.inner.mark_running(id, turn_id).await
    }
    async fn mark_uncertain(&self, id: &str, error: &str) -> Result<()> {
        self.inner.mark_uncertain(id, error).await
    }
    async fn finish(
        &self,
        id: &str,
        status: JobStatus,
        result: Option<&TurnResult>,
        error: Option<&str>,
    ) -> Result<()> {
        self.inner.finish(id, status, result, error).await
    }
    async fn request_cancel(&self, id: &str) -> Result<()> {
        self.inner.request_cancel(id).await
    }
    async fn mark_archiving(&self, id: &str) -> Result<()> {
        self.inner.mark_archiving(id).await
    }
    async fn recover(&self) -> Result<Vec<Job>> {
        self.inner.recover().await
    }
    async fn source_cursor(&self, source: &str, initial: i64) -> Result<i64> {
        self.inner.source_cursor(source, initial).await
    }
    async fn advance_source_cursor(&self, source: &str, next: i64) -> Result<()> {
        self.inner.advance_source_cursor(source, next).await
    }
    async fn accept_scheduled(&self, event: &Event, next_tick: i64) -> Result<Job> {
        self.inner.accept_scheduled(event, next_tick).await
    }
    async fn inflight_count(&self) -> Result<usize> {
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1 {
            self.worker_entered.notified().await;
            anyhow::bail!("injected transient store error");
        }
        self.inner.inflight_count().await
    }
}

struct SyncHook {
    worker_entered: Arc<Notify>,
    release: Arc<std::sync::Mutex<std::sync::mpsc::Receiver<()>>>,
    dropped: Arc<std::sync::atomic::AtomicBool>,
}
struct DropFlag(Arc<std::sync::atomic::AtomicBool>);
impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}
impl yandalf_core::plugin::Plugin for SyncHook {
    fn name(&self) -> &str {
        "synchronous-preparation"
    }
    fn register(&self, ctx: &mut yandalf_core::plugin::PluginContext) -> Result<()> {
        let entered = self.worker_entered.clone();
        let release = self.release.clone();
        let dropped = self.dropped.clone();
        ctx.on::<yandalf_core::task::TaskPreparing, _>(move |_, _| {
            let entered = entered.clone();
            let release = release.clone();
            let dropped = dropped.clone();
            Box::pin(async move {
                let _guard = DropFlag(dropped);
                entered.notify_one();
                tokio::task::block_in_place(|| release.lock().unwrap().recv().unwrap());
                Ok(())
            })
        });
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn store_error_waits_for_worker_before_releasing_runner_lease() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use yandalf_core::plugin::App;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let inner = Arc::new(SqliteStore::open(&path).unwrap());
    let job = inner.enqueue(&event("first", "a")).await.unwrap();
    let worker_entered = Arc::new(Notify::new());
    let store = Arc::new(FailCount {
        inner: inner.clone(),
        worker_entered: worker_entered.clone(),
        calls: AtomicUsize::new(0),
    });
    let (release, blocked) = std::sync::mpsc::channel();
    let dropped = Arc::new(AtomicBool::new(false));
    let app = App::builder()
        .plugin(SyncHook {
            worker_entered,
            release: Arc::new(std::sync::Mutex::new(blocked)),
            dropped: dropped.clone(),
        })
        .build()
        .await
        .unwrap();
    let executor = Arc::new(UnusedExecutor);
    let stop = CancellationToken::new();
    let config = Arc::new(Config {
        cwd: dir.path().into(),
        concurrency: 2,
        timeout_secs: 1,
    });
    let run = tokio::spawn(
        Arc::new(
            Runtime::new(config, store.clone(), executor)
                .unwrap()
                .with_services(app.services()),
        )
        .run(stop, false),
    );
    let mut run = run;
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut run)
            .await
            .is_err()
    );
    assert!(!dropped.load(Ordering::SeqCst));
    let successor = SqliteStore::open(&path).unwrap();
    assert!(successor.acquire_runner().await.is_err());
    release.send(()).unwrap();
    let error = tokio::time::timeout(Duration::from_secs(1), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(format!("{error:#}").contains("injected transient store error"));
    let _successor_lease = successor.acquire_runner().await.unwrap();
    assert!(dropped.load(Ordering::SeqCst));
    successor.recover().await.unwrap();
    let reclaimed = successor.claim_next().await.unwrap().unwrap();
    assert_eq!(reclaimed.id, job.id);
    tokio::time::timeout(Duration::from_secs(1), async {
        while !dropped.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    app.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aborted_runner_keeps_lease_until_worker_is_dropped() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use yandalf_core::plugin::App;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let store = Arc::new(SqliteStore::open(&path).unwrap());
    let job = store.enqueue(&event("first", "a")).await.unwrap();
    let worker_entered = Arc::new(Notify::new());
    let (release, blocked) = std::sync::mpsc::channel();
    let dropped = Arc::new(AtomicBool::new(false));
    let app = App::builder()
        .plugin(SyncHook {
            worker_entered: worker_entered.clone(),
            release: Arc::new(std::sync::Mutex::new(blocked)),
            dropped: dropped.clone(),
        })
        .build()
        .await
        .unwrap();
    let runtime = Arc::new(
        Runtime::new(
            Arc::new(Config {
                cwd: dir.path().into(),
                concurrency: 1,
                timeout_secs: 1,
            }),
            store,
            Arc::new(UnusedExecutor),
        )
        .unwrap()
        .with_services(app.services()),
    );
    let run = tokio::spawn(runtime.run(CancellationToken::new(), false));
    tokio::time::timeout(Duration::from_secs(1), worker_entered.notified())
        .await
        .unwrap();

    run.abort();
    assert!(run.await.unwrap_err().is_cancelled());
    assert!(!dropped.load(Ordering::SeqCst));
    let successor = SqliteStore::open(&path).unwrap();
    assert!(successor.acquire_runner().await.is_err());

    release.send(()).unwrap();
    let _lease = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let Ok(lease) = successor.acquire_runner().await {
                break lease;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(dropped.load(Ordering::SeqCst));
    successor.recover().await.unwrap();
    assert_eq!(successor.claim_next().await.unwrap().unwrap().id, job.id);
    app.shutdown().await.unwrap();
}
