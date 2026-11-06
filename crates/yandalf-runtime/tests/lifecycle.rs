use anyhow::Result;
use async_trait::async_trait;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use yandalf_core::{
    decision::Decision,
    domain::{
        Event, Inspection, JobStatus, Session, SessionSpec, ThreadInspection, TurnInput,
        TurnResult, TurnStatus,
    },
    executor::Executor,
    plugin::{App, Plugin, PluginContext},
    store::Store,
    task::TaskDeciding,
};
use yandalf_runtime::{Config, Runtime};
use yandalf_sqlite::SqliteStore;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Phase {
    Create,
    Configure,
    Submit,
    ReadCwd,
    Inspect,
    Interrupt,
    Archive,
    None,
}

struct BlockingExecutor {
    phase: Phase,
    block_once: AtomicBool,
    entered: watch::Sender<bool>,
    cwd: Mutex<PathBuf>,
    creations: Mutex<HashMap<String, String>>,
    hide_creation: AtomicBool,
    starts: Mutex<Vec<String>>,
    configurations: AtomicUsize,
    archives: AtomicUsize,
}

impl BlockingExecutor {
    fn new(phase: Phase, cwd: &Path) -> Self {
        Self {
            phase,
            block_once: AtomicBool::new(true),
            entered: watch::channel(false).0,
            cwd: Mutex::new(cwd.into()),
            creations: Mutex::new(HashMap::new()),
            hide_creation: AtomicBool::new(false),
            starts: Mutex::new(Vec::new()),
            configurations: AtomicUsize::new(0),
            archives: AtomicUsize::new(0),
        }
    }

    async fn block(&self, phase: Phase) {
        if self.phase == phase && self.block_once.swap(false, Ordering::SeqCst) {
            self.entered.send_replace(true);
            std::future::pending::<()>().await;
        }
    }

    async fn wait_entered(&self) {
        let mut entered = self.entered.subscribe();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !*entered.borrow_and_update() {
                entered.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
    }
}

fn result() -> TurnResult {
    TurnResult {
        status: TurnStatus::Completed,
        text: "Result".into(),
        usage: serde_json::Value::Null,
    }
}

#[async_trait]
impl Executor for BlockingExecutor {
    async fn create_thread(&self, cwd: &Path, creation_id: &str) -> Result<String> {
        assert!(
            self.creations
                .lock()
                .unwrap()
                .insert(creation_id.into(), "thread".into())
                .is_none()
        );
        *self.cwd.lock().unwrap() = cwd.into();
        self.block(Phase::Create).await;
        Ok("thread".into())
    }

    async fn inspect_thread(&self, creation_id: &str) -> Result<ThreadInspection> {
        if self.hide_creation.load(Ordering::SeqCst) {
            return Ok(ThreadInspection::Missing);
        }
        Ok(match self.creations.lock().unwrap().get(creation_id) {
            Some(thread_id) => ThreadInspection::Found {
                thread_id: thread_id.clone(),
            },
            None => ThreadInspection::Missing,
        })
    }

    async fn thread_cwd(&self, _: &str) -> Result<PathBuf> {
        self.block(Phase::ReadCwd).await;
        Ok(self.cwd.lock().unwrap().clone())
    }

    async fn configure_thread(&self, _: &str, spec: &SessionSpec) -> Result<()> {
        self.configurations.fetch_add(1, Ordering::SeqCst);
        *self.cwd.lock().unwrap() = spec.cwd.clone();
        self.block(Phase::Configure).await;
        Ok(())
    }

    async fn archive_thread(&self, _: &str) -> Result<()> {
        self.archives.fetch_add(1, Ordering::SeqCst);
        self.block(Phase::Archive).await;
        Ok(())
    }

    async fn start_turn(&self, _: &str, client_message_id: &str, _: &TurnInput) -> Result<String> {
        self.starts.lock().unwrap().push(client_message_id.into());
        self.block(Phase::Submit).await;
        Ok(format!("turn-{client_message_id}"))
    }

    async fn wait_turn(&self, _: &str, _: &str) -> Result<TurnResult> {
        if self.phase == Phase::Interrupt {
            return std::future::pending().await;
        }
        Ok(result())
    }

    async fn inspect(
        &self,
        _: &str,
        client_message_id: &str,
        _: Option<&str>,
    ) -> Result<Inspection> {
        self.block(Phase::Inspect).await;
        Ok(Inspection::Finished {
            turn_id: format!("turn-{client_message_id}"),
            result: result(),
        })
    }

    async fn interrupt(&self, _: &str, _: &str) -> Result<()> {
        self.block(Phase::Interrupt).await;
        Ok(())
    }
}

fn event(id: &str) -> Event {
    Event {
        id: id.into(),
        source: "test".into(),
        subject: "subject".into(),
        title: None,
        text: "Perform task".into(),
        context: serde_json::Value::Null,
    }
}

fn config(cwd: &Path) -> Arc<Config> {
    Arc::new(Config {
        cwd: cwd.into(),
        concurrency: 1,
        timeout_secs: 30,
    })
}

fn runtime(
    config: Arc<Config>,
    store: Arc<dyn Store>,
    executor: Arc<BlockingExecutor>,
) -> Arc<Runtime> {
    Arc::new(Runtime::new(config, store, executor).unwrap())
}

#[tokio::test]
async fn shutdown_detaches_from_hung_lifecycle_requests() {
    for phase in [
        Phase::Create,
        Phase::Configure,
        Phase::Submit,
        Phase::ReadCwd,
        Phase::Inspect,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(SqliteStore::open(&dir.path().join("state.sqlite")).unwrap());
        let job = store.enqueue(&event("first")).await.unwrap();
        if phase == Phase::ReadCwd {
            store
                .bind_session(&Session {
                    subject: "subject".into(),
                    cwd: dir.path().into(),
                    thread_id: "thread".into(),
                })
                .await
                .unwrap();
        } else if phase == Phase::Inspect {
            store.claim_next().await.unwrap().unwrap();
            store.mark_submitting(&job.id, "thread").await.unwrap();
        }
        let executor = Arc::new(BlockingExecutor::new(phase, dir.path()));
        let stop = CancellationToken::new();
        let run = tokio::spawn(
            runtime(config(dir.path()), store.clone(), executor.clone()).run(stop.clone(), false),
        );
        executor.wait_entered().await;
        stop.cancel();
        tokio::time::timeout(Duration::from_secs(1), run)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let expected = if phase == Phase::ReadCwd {
            JobStatus::Preparing
        } else {
            JobStatus::Uncertain
        };
        assert_eq!(
            store.get_job(&job.id).await.unwrap().status,
            expected,
            "{phase:?}"
        );
        let _lease = store.acquire_runner().await.unwrap();
    }
}

#[tokio::test]
async fn cancellation_reconciles_unknown_writes_before_releasing_the_subject() {
    for phase in [Phase::Create, Phase::Configure, Phase::Submit] {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(SqliteStore::open(&dir.path().join("state.sqlite")).unwrap());
        let first = store.enqueue(&event("first")).await.unwrap();
        let next = store.enqueue(&event("next")).await.unwrap();
        let executor = Arc::new(BlockingExecutor::new(phase, dir.path()));
        let stop = CancellationToken::new();
        let run = tokio::spawn(
            runtime(config(dir.path()), store.clone(), executor.clone()).run(stop.clone(), false),
        );
        executor.wait_entered().await;
        store.request_cancel(&first.id).await.unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(
            store.get_job(&first.id).await.unwrap().status,
            JobStatus::Uncertain,
            "{phase:?}"
        );
        assert_eq!(
            store.get_job(&next.id).await.unwrap().status,
            JobStatus::Queued,
            "{phase:?}"
        );
        stop.cancel();
        tokio::time::timeout(Duration::from_secs(1), run)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        runtime(config(dir.path()), store.clone(), executor.clone())
            .run(CancellationToken::new(), true)
            .await
            .unwrap();
        assert_eq!(
            store.get_job(&first.id).await.unwrap().status,
            JobStatus::Cancelled,
            "{phase:?}"
        );
        assert_eq!(
            store.get_job(&next.id).await.unwrap().status,
            JobStatus::Succeeded,
            "{phase:?}"
        );
        assert_eq!(executor.creations.lock().unwrap().len(), 1, "{phase:?}");
        let starts = executor.starts.lock().unwrap();
        assert_eq!(
            starts.iter().filter(|id| **id == first.id).count(),
            usize::from(phase == Phase::Submit),
            "{phase:?}"
        );
        assert_eq!(
            starts.iter().filter(|id| **id == next.id).count(),
            1,
            "{phase:?}"
        );
    }
}

#[tokio::test]
async fn cancelled_creation_keeps_its_reservation_while_inspection_cannot_find_it() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SqliteStore::open(&dir.path().join("state.sqlite")).unwrap());
    let first = store.enqueue(&event("first")).await.unwrap();
    let next = store.enqueue(&event("next")).await.unwrap();
    let executor = Arc::new(BlockingExecutor::new(Phase::Create, dir.path()));
    let stop = CancellationToken::new();
    let run = tokio::spawn(
        runtime(config(dir.path()), store.clone(), executor.clone()).run(stop.clone(), false),
    );
    executor.wait_entered().await;
    store.request_cancel(&first.id).await.unwrap();
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(1), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    executor.hide_creation.store(true, Ordering::SeqCst);

    assert!(
        runtime(config(dir.path()), store.clone(), executor.clone())
            .run(CancellationToken::new(), true)
            .await
            .is_err()
    );
    assert_eq!(
        store.get_job(&first.id).await.unwrap().status,
        JobStatus::Uncertain
    );
    assert_eq!(
        store.get_job(&next.id).await.unwrap().status,
        JobStatus::Queued
    );
    assert_eq!(executor.creations.lock().unwrap().len(), 1);
    assert!(executor.starts.lock().unwrap().is_empty());

    executor.hide_creation.store(false, Ordering::SeqCst);
    runtime(config(dir.path()), store.clone(), executor.clone())
        .run(CancellationToken::new(), true)
        .await
        .unwrap();
    assert_eq!(
        store.get_job(&first.id).await.unwrap().status,
        JobStatus::Cancelled
    );
    assert_eq!(
        store.get_job(&next.id).await.unwrap().status,
        JobStatus::Succeeded
    );
}

struct Archive;
impl Plugin for Archive {
    fn name(&self) -> &str {
        "archive"
    }
    fn register(&self, ctx: &mut PluginContext) -> Result<()> {
        ctx.on::<TaskDeciding, _>(|_, task| {
            Box::pin(async move {
                task.decision = Decision::Archive;
                Ok(())
            })
        });
        Ok(())
    }
}

#[tokio::test]
async fn cancelled_archive_reconciles_its_saved_action_after_policy_changes() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SqliteStore::open(&dir.path().join("state.sqlite")).unwrap());
    store
        .bind_session(&Session {
            subject: "subject".into(),
            cwd: dir.path().into(),
            thread_id: "thread".into(),
        })
        .await
        .unwrap();
    let job = store.enqueue(&event("archive")).await.unwrap();
    let executor = Arc::new(BlockingExecutor::new(Phase::Archive, dir.path()));
    let app = App::builder().plugin(Archive).build().await.unwrap();
    let stop = CancellationToken::new();
    let runner = Arc::new(
        Runtime::new(config(dir.path()), store.clone(), executor.clone())
            .unwrap()
            .with_services(app.services()),
    );
    let run = tokio::spawn(runner.run(stop.clone(), false));
    executor.wait_entered().await;
    store.request_cancel(&job.id).await.unwrap();
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(1), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        store.get_job(&job.id).await.unwrap().status,
        JobStatus::Archiving
    );

    runtime(config(dir.path()), store.clone(), executor.clone())
        .run(CancellationToken::new(), true)
        .await
        .unwrap();
    assert_eq!(
        store.get_job(&job.id).await.unwrap().status,
        JobStatus::Cancelled
    );
    assert_eq!(executor.archives.load(Ordering::SeqCst), 2);
    assert!(executor.starts.lock().unwrap().is_empty());
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_detaches_while_interrupt_acknowledgement_is_missing() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SqliteStore::open(&dir.path().join("state.sqlite")).unwrap());
    let job = store.enqueue(&event("first")).await.unwrap();
    let executor = Arc::new(BlockingExecutor::new(Phase::Interrupt, dir.path()));
    let stop = CancellationToken::new();
    let run = tokio::spawn(
        runtime(config(dir.path()), store.clone(), executor.clone()).run(stop.clone(), false),
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while store.get_job(&job.id).await.unwrap().status != JobStatus::Running {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    store.request_cancel(&job.id).await.unwrap();
    executor.wait_entered().await;
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(1), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let job = store.get_job(&job.id).await.unwrap();
    assert_eq!(job.status, JobStatus::Running);
    assert!(job.cancel_requested);
    assert_eq!(executor.starts.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn default_and_relative_cwd_work_without_explicit_path_resolution() {
    for cwd in [PathBuf::from("."), PathBuf::from("src/..")] {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(SqliteStore::open(&dir.path().join("state.sqlite")).unwrap());
        let job = store.enqueue(&event("first")).await.unwrap();
        let executor = Arc::new(BlockingExecutor::new(Phase::None, dir.path()));
        let config = Arc::new(Config {
            cwd: cwd.clone(),
            ..Config::default()
        });
        runtime(config, store.clone(), executor.clone())
            .run(CancellationToken::new(), true)
            .await
            .unwrap();
        assert_eq!(
            store.get_job(&job.id).await.unwrap().status,
            JobStatus::Succeeded
        );
        assert_eq!(
            *executor.cwd.lock().unwrap(),
            std::fs::canonicalize(cwd).unwrap()
        );
    }
}
