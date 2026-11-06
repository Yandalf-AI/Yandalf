use anyhow::{Result, bail};
use async_trait::async_trait;
use serde_json::json;
use std::{
    collections::BTreeMap,
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
    decision::{Decision, DecisionProvider},
    domain::{
        Event, GitInfo, Inspection, JobStatus, PreparedTask, SessionSpec, ThreadInspection,
        TurnInput, TurnResult, TurnStatus,
    },
    executor::Executor,
    plugin::{App, Plugin, PluginContext},
    store::Store,
};
use yandalf_runtime::{
    config::Config,
    runtime::{Runtime, SessionPreparing, TaskCompleted, TaskDeciding, TaskPreparing},
};
use yandalf_sqlite::SqliteStore;

struct FakeExecutor {
    starts: AtomicUsize,
    titles: Mutex<Vec<(String, String)>>,
    operations: Mutex<Vec<&'static str>>,
    lose_title_ack: AtomicBool,
    cwds: Mutex<Vec<PathBuf>>,
    thread_directories: Mutex<BTreeMap<String, PathBuf>>,
    configured: Mutex<Vec<SessionSpec>>,
    archives: Mutex<Vec<String>>,
    lose_archive_ack: AtomicBool,
    inputs: Mutex<Vec<TurnInput>>,
    active: AtomicUsize,
    max_active: AtomicUsize,
    lose_ack: AtomicBool,
    lose_creation_ack: AtomicBool,
    hide_creation: AtomicBool,
    creations: Mutex<BTreeMap<String, String>>,
    recovered: AtomicBool,
    wait_for_interrupt: bool,
    result_text: String,
    interrupted: watch::Sender<bool>,
}

impl FakeExecutor {
    fn new() -> Self {
        Self {
            starts: AtomicUsize::new(0),
            titles: Mutex::new(Vec::new()),
            operations: Mutex::new(Vec::new()),
            lose_title_ack: AtomicBool::new(false),
            cwds: Mutex::new(Vec::new()),
            thread_directories: Mutex::new(BTreeMap::new()),
            configured: Mutex::new(Vec::new()),
            archives: Mutex::new(Vec::new()),
            lose_archive_ack: AtomicBool::new(false),
            inputs: Mutex::new(Vec::new()),
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            lose_ack: AtomicBool::new(false),
            lose_creation_ack: AtomicBool::new(false),
            hide_creation: AtomicBool::new(false),
            creations: Mutex::new(BTreeMap::new()),
            recovered: AtomicBool::new(false),
            wait_for_interrupt: false,
            result_text: "Task result".into(),
            interrupted: watch::channel(false).0,
        }
    }
}

fn result() -> TurnResult {
    TurnResult {
        status: TurnStatus::Completed,
        text: "Task result".into(),
        usage: json!({"tokens": 42}),
    }
}

#[async_trait]
impl Executor for FakeExecutor {
    async fn create_thread(&self, cwd: &Path, creation_id: &str) -> Result<String> {
        self.operations.lock().unwrap().push("create");
        self.cwds.lock().unwrap().push(cwd.to_owned());
        let thread_id = uuid::Uuid::new_v4().to_string();
        self.thread_directories
            .lock()
            .unwrap()
            .insert(thread_id.clone(), cwd.to_owned());
        assert!(
            self.creations
                .lock()
                .unwrap()
                .insert(creation_id.to_owned(), thread_id.clone())
                .is_none()
        );
        if self.lose_creation_ack.swap(false, Ordering::SeqCst) {
            bail!("connection closed after accepted thread creation");
        }
        Ok(thread_id)
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
    async fn thread_cwd(&self, thread_id: &str) -> Result<PathBuf> {
        self.thread_directories
            .lock()
            .unwrap()
            .get(thread_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unknown test thread"))
    }
    async fn configure_thread(&self, thread_id: &str, spec: &SessionSpec) -> Result<()> {
        self.configured.lock().unwrap().push(spec.clone());
        self.thread_directories
            .lock()
            .unwrap()
            .insert(thread_id.into(), spec.cwd.clone());
        if let Some(title) = &spec.title {
            self.operations.lock().unwrap().push("title");
            self.titles
                .lock()
                .unwrap()
                .push((thread_id.into(), title.clone()));
            if self.lose_title_ack.swap(false, Ordering::SeqCst) {
                bail!("connection closed after accepted title update");
            }
        }
        Ok(())
    }
    async fn archive_thread(&self, thread_id: &str) -> Result<()> {
        assert_eq!(self.active.load(Ordering::SeqCst), 0);
        self.archives.lock().unwrap().push(thread_id.to_owned());
        if self.lose_archive_ack.swap(false, Ordering::SeqCst) {
            bail!("archive acknowledgement lost");
        }
        Ok(())
    }
    async fn start_turn(&self, _: &str, id: &str, input: &TurnInput) -> Result<String> {
        self.operations.lock().unwrap().push("turn");
        self.inputs.lock().unwrap().push(TurnInput {
            text: input.text.clone(),
            context: input.context.clone(),
        });
        self.starts.fetch_add(1, Ordering::SeqCst);
        if self.lose_ack.swap(false, Ordering::SeqCst) {
            bail!("connection closed after accepted submission");
        }
        Ok(format!("turn-{id}"))
    }
    async fn wait_turn(&self, _: &str, _: &str) -> Result<TurnResult> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        let mut response = result();
        response.text = self.result_text.clone();
        if self.wait_for_interrupt {
            let mut interrupted = self.interrupted.subscribe();
            while !*interrupted.borrow_and_update() {
                interrupted.changed().await?;
            }
            response.status = TurnStatus::Interrupted;
        } else {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        self.active.fetch_sub(1, Ordering::SeqCst);
        Ok(response)
    }
    async fn inspect(&self, _: &str, id: &str, _: Option<&str>) -> Result<Inspection> {
        if self.recovered.load(Ordering::SeqCst) {
            Ok(Inspection::Finished {
                turn_id: format!("turn-{id}"),
                result: result(),
            })
        } else {
            Ok(Inspection::Missing)
        }
    }
    async fn interrupt(&self, _: &str, _: &str) -> Result<()> {
        self.interrupted.send_replace(true);
        Ok(())
    }
}

fn setup(dir: &Path) -> (Arc<Config>, Arc<dyn Store>, Arc<FakeExecutor>) {
    let config = Arc::new(Config {
        cwd: dir.into(),
        concurrency: 2,
        ..Config::default()
    });
    let store = Arc::new(SqliteStore::open(&dir.join("state.sqlite")).unwrap());
    (config, store, Arc::new(FakeExecutor::new()))
}

fn runtime(
    config: Arc<Config>,
    store: Arc<dyn Store>,
    executor: Arc<FakeExecutor>,
) -> Arc<Runtime> {
    Arc::new(Runtime::new(config, store, executor).unwrap())
}

fn event(id: &str, subject: &str) -> Event {
    Event {
        id: id.into(),
        source: "test".into(),
        subject: subject.into(),
        title: None,
        text: "Perform the task".into(),
        context: json!({}),
    }
}

#[tokio::test]
async fn bounds_execution_and_reuses_session_for_followup() {
    let dir = tempfile::tempdir().unwrap();
    let (config, store, executor) = setup(dir.path());
    for (id, subject) in [("1", "a"), ("2", "b"), ("3", "a"), ("4", "c")] {
        store.enqueue(&event(id, subject)).await.unwrap();
    }
    runtime(config, store.clone(), executor.clone())
        .run(CancellationToken::new(), true)
        .await
        .unwrap();
    assert_eq!(executor.max_active.load(Ordering::SeqCst), 2);
    assert_eq!(executor.starts.load(Ordering::SeqCst), 4);
    assert!(executor.titles.lock().unwrap().is_empty());
    assert_eq!(
        *executor.cwds.lock().unwrap(),
        vec![dir.path().to_owned(); 3]
    );
    let jobs = store.list_jobs(10).await.unwrap();
    assert!(jobs.iter().all(|job| job.status == JobStatus::Succeeded));
    let subjects: Vec<_> = jobs.iter().filter(|job| job.event.subject == "a").collect();
    assert_eq!(subjects[0].thread_id, subjects[1].thread_id);
    assert_eq!(
        store.job_result(&jobs[0].id).await.unwrap().unwrap().usage,
        json!({"tokens":42})
    );
}

#[tokio::test]
async fn shares_subject_session_across_sources_and_preserves_each_event_context() {
    let dir = tempfile::tempdir().unwrap();
    let (config, store, executor) = setup(dir.path());
    let first_event = Event {
        source: "github".into(),
        context: json!({"repository":"example/project", "issue":42}),
        ..event("1", "task-a")
    };
    let second_event = Event {
        source: "chat".into(),
        context: json!({"conversation":"chat-7", "reply_to":"message-8"}),
        ..event("2", "task-a")
    };
    let first = store.enqueue(&first_event).await.unwrap();
    let second = store.enqueue(&second_event).await.unwrap();

    runtime(config, store.clone(), executor.clone())
        .run(CancellationToken::new(), true)
        .await
        .unwrap();

    let first = store.get_job(&first.id).await.unwrap();
    let second = store.get_job(&second.id).await.unwrap();
    assert_eq!(first.status, JobStatus::Succeeded);
    assert_eq!(second.status, JobStatus::Succeeded);
    assert!(first.thread_id.is_some());
    assert_eq!(first.thread_id, second.thread_id);
    assert_eq!(executor.cwds.lock().unwrap().len(), 1);
    assert_eq!(executor.starts.load(Ordering::SeqCst), 2);
    assert_eq!(executor.max_active.load(Ordering::SeqCst), 1);
    let inputs = executor.inputs.lock().unwrap();
    assert_eq!(inputs.len(), 2);
    assert_eq!(inputs[0].text, first_event.text);
    assert_eq!(inputs[0].context, first_event.context);
    assert_eq!(inputs[1].text, second_event.text);
    assert_eq!(inputs[1].context, second_event.context);
}

#[tokio::test]
async fn uncertain_submission_reserves_slot_and_recovers_without_replay() {
    let dir = tempfile::tempdir().unwrap();
    let (mut config, store, executor) = setup(dir.path());
    Arc::make_mut(&mut config).concurrency = 1;
    executor.lose_ack.store(true, Ordering::SeqCst);
    let first = store.enqueue(&event("1", "a")).await.unwrap();
    let second = store.enqueue(&event("2", "b")).await.unwrap();
    assert!(
        runtime(config.clone(), store.clone(), executor.clone())
            .run(CancellationToken::new(), true)
            .await
            .is_err()
    );
    assert_eq!(
        store.get_job(&first.id).await.unwrap().status,
        JobStatus::Uncertain
    );
    assert_eq!(
        store.get_job(&second.id).await.unwrap().status,
        JobStatus::Queued
    );
    assert_eq!(executor.starts.load(Ordering::SeqCst), 1);
    executor.recovered.store(true, Ordering::SeqCst);
    runtime(config, store.clone(), executor.clone())
        .run(CancellationToken::new(), true)
        .await
        .unwrap();
    assert_eq!(executor.starts.load(Ordering::SeqCst), 2);
    assert_eq!(
        store.get_job(&first.id).await.unwrap().status,
        JobStatus::Succeeded
    );
}

#[tokio::test]
async fn cancellation_remains_available_when_execution_is_full() {
    let dir = tempfile::tempdir().unwrap();
    let (mut config, store, mut executor) = setup(dir.path());
    Arc::make_mut(&mut config).concurrency = 1;
    Arc::get_mut(&mut executor).unwrap().wait_for_interrupt = true;
    let first = store.enqueue(&event("1", "a")).await.unwrap();
    let second = store.enqueue(&event("2", "b")).await.unwrap();
    let runner = tokio::spawn(
        runtime(config, store.clone(), executor.clone()).run(CancellationToken::new(), true),
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        while store.get_job(&first.id).await.unwrap().status != JobStatus::Running {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    store.request_cancel(&second.id).await.unwrap();
    store.request_cancel(&first.id).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), runner)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        store.get_job(&first.id).await.unwrap().status,
        JobStatus::Cancelled
    );
    assert_eq!(
        store.get_job(&second.id).await.unwrap().status,
        JobStatus::Cancelled
    );
    assert_eq!(executor.starts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn saves_successful_tool_turn_without_final_text() {
    let dir = tempfile::tempdir().unwrap();
    let (config, store, mut executor) = setup(dir.path());
    Arc::get_mut(&mut executor).unwrap().result_text.clear();
    let job = store.enqueue(&event("1", "task-a")).await.unwrap();
    runtime(config, store.clone(), executor.clone())
        .run(CancellationToken::new(), true)
        .await
        .unwrap();
    assert_eq!(
        store.get_job(&job.id).await.unwrap().status,
        JobStatus::Succeeded
    );
    let saved = store.job_result(&job.id).await.unwrap().unwrap();
    assert_eq!(saved.status, TurnStatus::Completed);
    assert!(saved.text.is_empty());
    assert_eq!(executor.starts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn reuses_bound_worktree_session_when_default_cwd_is_different() {
    let dir = tempfile::tempdir().unwrap();
    let worktree = tempfile::tempdir().unwrap();
    let (config, store, executor) = setup(dir.path());
    store
        .bind_session(&yandalf_core::domain::Session {
            subject: "existing-pr".into(),
            cwd: worktree.path().into(),
            thread_id: "existing-thread".into(),
        })
        .await
        .unwrap();
    executor
        .thread_directories
        .lock()
        .unwrap()
        .insert("existing-thread".into(), worktree.path().into());
    let job = store
        .enqueue(&event("update", "existing-pr"))
        .await
        .unwrap();
    runtime(config, store.clone(), executor.clone())
        .run(CancellationToken::new(), true)
        .await
        .unwrap();
    let saved = store.get_job(&job.id).await.unwrap();
    assert_eq!(saved.status, JobStatus::Succeeded);
    assert_eq!(saved.thread_id.as_deref(), Some("existing-thread"));
    assert!(executor.cwds.lock().unwrap().is_empty());
    assert_eq!(
        store.session("existing-pr").await.unwrap().unwrap().cwd,
        worktree.path()
    );
}

struct TestDecision(Arc<dyn DecisionProvider>);

impl Plugin for TestDecision {
    fn name(&self) -> &str {
        "test-decision"
    }
    fn register(&self, ctx: &mut PluginContext) -> Result<()> {
        let provider = self.0.clone();
        ctx.on::<TaskDeciding, _>(move |_, event| {
            let provider = provider.clone();
            Box::pin(async move {
                event.decision = provider.decide(&event.job.event).await?;
                Ok(())
            })
        });
        Ok(())
    }
}

async fn decision_app(provider: impl DecisionProvider + 'static) -> App {
    App::builder()
        .plugin(TestDecision(Arc::new(provider)))
        .build()
        .await
        .unwrap()
}

struct SessionLifecycle;

#[async_trait]
impl DecisionProvider for SessionLifecycle {
    async fn decide(&self, event: &Event) -> Result<Decision> {
        Ok(if event.context["closed"] == true {
            Decision::Archive
        } else {
            Decision::Run
        })
    }
}

fn closure(id: &str, subject: &str) -> Event {
    let mut closed = event(id, subject);
    closed.context = json!({"closed":true});
    closed
}

#[tokio::test]
async fn closure_archives_after_the_review_without_creating_another_turn() {
    let dir = tempfile::tempdir().unwrap();
    let (config, store, executor) = setup(dir.path());
    let app = decision_app(SessionLifecycle).await;
    let review = store.enqueue(&event("review", "pr-42")).await.unwrap();
    let close = store.enqueue(&closure("closed", "pr-42")).await.unwrap();

    Arc::new(
        Runtime::new(config.clone(), store.clone(), executor.clone())
            .unwrap()
            .with_services(app.services()),
    )
    .run(CancellationToken::new(), true)
    .await
    .unwrap();

    let session = store.session("pr-42").await.unwrap().unwrap();
    assert_eq!(
        executor.archives.lock().unwrap().as_slice(),
        std::slice::from_ref(&session.thread_id)
    );
    assert_eq!(executor.starts.load(Ordering::SeqCst), 1);
    assert_eq!(executor.cwds.lock().unwrap().len(), 1);
    assert_eq!(
        store.get_job(&review.id).await.unwrap().status,
        JobStatus::Succeeded
    );
    assert_eq!(
        store.get_job(&close.id).await.unwrap().status,
        JobStatus::Succeeded
    );
    assert!(store.job_preparation(&close.id).await.unwrap().is_none());
    assert!(store.job_result(&close.id).await.unwrap().is_none());

    let reopened = store.enqueue(&event("reopened", "pr-42")).await.unwrap();
    Arc::new(
        Runtime::new(config, store.clone(), executor.clone())
            .unwrap()
            .with_services(app.services()),
    )
    .run(CancellationToken::new(), true)
    .await
    .unwrap();
    assert_eq!(
        store.get_job(&reopened.id).await.unwrap().thread_id,
        Some(session.thread_id)
    );
    assert_eq!(executor.starts.load(Ordering::SeqCst), 2);
    assert_eq!(executor.cwds.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn closure_without_a_session_does_not_create_one() {
    let dir = tempfile::tempdir().unwrap();
    let (config, store, executor) = setup(dir.path());
    let app = decision_app(SessionLifecycle).await;
    let close = store.enqueue(&closure("closed", "pr-42")).await.unwrap();
    Arc::new(
        Runtime::new(config, store.clone(), executor.clone())
            .unwrap()
            .with_services(app.services()),
    )
    .run(CancellationToken::new(), true)
    .await
    .unwrap();
    assert_eq!(
        store.get_job(&close.id).await.unwrap().status,
        JobStatus::Succeeded
    );
    assert!(store.session("pr-42").await.unwrap().is_none());
    assert!(store.session_creation("pr-42").await.unwrap().is_none());
    assert!(executor.archives.lock().unwrap().is_empty());
    assert_eq!(executor.starts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn unconfirmed_archive_retries_after_restart_and_preserves_the_binding() {
    let dir = tempfile::tempdir().unwrap();
    let (config, store, executor) = setup(dir.path());
    let app = decision_app(SessionLifecycle).await;
    store
        .bind_session(&yandalf_core::domain::Session {
            subject: "pr-42".into(),
            cwd: dir.path().into(),
            thread_id: "existing-thread".into(),
        })
        .await
        .unwrap();
    executor.lose_archive_ack.store(true, Ordering::SeqCst);
    let close = store.enqueue(&closure("closed", "pr-42")).await.unwrap();
    assert!(
        Arc::new(
            Runtime::new(config.clone(), store.clone(), executor.clone())
                .unwrap()
                .with_services(app.services()),
        )
        .run(CancellationToken::new(), true)
        .await
        .is_err()
    );
    let pending = store.get_job(&close.id).await.unwrap();
    assert_eq!(pending.status, JobStatus::Archiving);
    assert!(pending.thread_id.is_none());
    assert_eq!(store.inflight_count().await.unwrap(), 1);

    let reopened: Arc<dyn Store> =
        Arc::new(SqliteStore::open(&dir.path().join("state.sqlite")).unwrap());
    Arc::new(
        Runtime::new(config, reopened.clone(), executor.clone())
            .unwrap()
            .with_services(app.services()),
    )
    .run(CancellationToken::new(), true)
    .await
    .unwrap();

    assert_eq!(
        reopened.get_job(&close.id).await.unwrap().status,
        JobStatus::Succeeded
    );
    assert_eq!(
        reopened.session("pr-42").await.unwrap().unwrap().thread_id,
        "existing-thread"
    );
    assert_eq!(
        *executor.archives.lock().unwrap(),
        ["existing-thread", "existing-thread"]
    );
    assert_eq!(executor.starts.load(Ordering::SeqCst), 0);
    assert!(executor.cwds.lock().unwrap().is_empty());
    assert_eq!(reopened.inflight_count().await.unwrap(), 0);
}

#[tokio::test]
async fn closure_recovers_an_unbound_creation_before_archiving_without_creating_or_running() {
    let dir = tempfile::tempdir().unwrap();
    let (config, store, executor) = setup(dir.path());
    let (creation, _) = store
        .begin_session_creation("pr-42", dir.path())
        .await
        .unwrap();
    executor
        .creations
        .lock()
        .unwrap()
        .insert(creation.id, "unconfirmed-thread".into());
    let close = store.enqueue(&closure("closed", "pr-42")).await.unwrap();
    let app = decision_app(SessionLifecycle).await;

    Arc::new(
        Runtime::new(config, store.clone(), executor.clone())
            .unwrap()
            .with_services(app.services()),
    )
    .run(CancellationToken::new(), true)
    .await
    .unwrap();

    assert_eq!(
        store.get_job(&close.id).await.unwrap().status,
        JobStatus::Succeeded
    );
    assert_eq!(
        store.session("pr-42").await.unwrap().unwrap().thread_id,
        "unconfirmed-thread"
    );
    assert_eq!(*executor.archives.lock().unwrap(), ["unconfirmed-thread"]);
    assert_eq!(executor.starts.load(Ordering::SeqCst), 0);
    assert!(executor.cwds.lock().unwrap().is_empty());
}

#[tokio::test]
async fn closure_with_an_unresolved_creation_intent_keeps_its_execution_slot_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let (mut config, store, executor) = setup(dir.path());
    Arc::make_mut(&mut config).concurrency = 1;
    store
        .begin_session_creation("pr-42", dir.path())
        .await
        .unwrap();
    let close = store.enqueue(&closure("closed", "pr-42")).await.unwrap();
    let next = store.enqueue(&event("review", "pr-43")).await.unwrap();
    let app = decision_app(SessionLifecycle).await;

    for _ in 0..2 {
        assert!(
            Arc::new(
                Runtime::new(config.clone(), store.clone(), executor.clone())
                    .unwrap()
                    .with_services(app.services()),
            )
            .run(CancellationToken::new(), true)
            .await
            .is_err()
        );
        assert_eq!(
            store.get_job(&close.id).await.unwrap().status,
            JobStatus::Archiving
        );
        assert_eq!(
            store.get_job(&next.id).await.unwrap().status,
            JobStatus::Queued
        );
        assert_eq!(store.inflight_count().await.unwrap(), 1);
    }
    assert!(store.session("pr-42").await.unwrap().is_none());
    assert!(executor.archives.lock().unwrap().is_empty());
    assert!(executor.cwds.lock().unwrap().is_empty());
    assert_eq!(executor.starts.load(Ordering::SeqCst), 0);
}

struct FilterGithubNoise;

#[async_trait]
impl DecisionProvider for FilterGithubNoise {
    async fn decide(&self, event: &Event) -> Result<Decision> {
        if event.source == "github" && event.context["noise"] == true {
            Ok(Decision::Skip {
                reason: "Only our own review changed".into(),
            })
        } else {
            Ok(Decision::Run)
        }
    }
}

#[tokio::test]
async fn skipped_observation_never_creates_a_session_or_turn() {
    let dir = tempfile::tempdir().unwrap();
    let (config, store, executor) = setup(dir.path());
    let observation = Event {
        source: "github".into(),
        context: json!({"noise":true}),
        ..event("1", "pr-1")
    };
    let job = store.enqueue(&observation).await.unwrap();
    let app = decision_app(FilterGithubNoise).await;
    Arc::new(
        Runtime::new(config, store.clone(), executor.clone())
            .unwrap()
            .with_services(app.services()),
    )
    .run(CancellationToken::new(), true)
    .await
    .unwrap();

    let saved = store.get_job(&job.id).await.unwrap();
    assert_eq!(saved.status, JobStatus::Skipped);
    assert_eq!(saved.error.as_deref(), Some("Only our own review changed"));
    assert!(saved.thread_id.is_none());
    assert!(saved.turn_id.is_none());
    assert!(store.job_result(&job.id).await.unwrap().is_none());
    assert!(store.session("pr-1").await.unwrap().is_none());
    assert_eq!(
        store.last_event("github", "pr-1").await.unwrap(),
        Some(observation)
    );
    assert_eq!(executor.starts.load(Ordering::SeqCst), 0);
    assert!(executor.cwds.lock().unwrap().is_empty());
}

#[tokio::test]
async fn skipped_event_does_not_block_same_subject_or_unrelated_inputs() {
    let dir = tempfile::tempdir().unwrap();
    let (config, store, executor) = setup(dir.path());
    let noise = store
        .enqueue(&Event {
            source: "github".into(),
            context: json!({"noise":true}),
            ..event("1", "pr-1")
        })
        .await
        .unwrap();
    let revision = store
        .enqueue(&Event {
            source: "github".into(),
            ..event("2", "pr-1")
        })
        .await
        .unwrap();
    let unrelated = store
        .enqueue(&Event {
            context: json!({"noise":true}),
            ..event("3", "manual-task")
        })
        .await
        .unwrap();
    let app = decision_app(FilterGithubNoise).await;
    Arc::new(
        Runtime::new(config, store.clone(), executor.clone())
            .unwrap()
            .with_services(app.services()),
    )
    .run(CancellationToken::new(), true)
    .await
    .unwrap();

    assert_eq!(
        store.get_job(&noise.id).await.unwrap().status,
        JobStatus::Skipped
    );
    for id in [&revision.id, &unrelated.id] {
        assert_eq!(
            store.get_job(id).await.unwrap().status,
            JobStatus::Succeeded
        );
    }
    assert_eq!(executor.starts.load(Ordering::SeqCst), 2);
    assert_eq!(executor.cwds.lock().unwrap().len(), 2);
}

struct BrokenDecisionProvider;

#[async_trait]
impl DecisionProvider for BrokenDecisionProvider {
    async fn decide(&self, _: &Event) -> Result<Decision> {
        bail!("decision provider unavailable")
    }
}

#[tokio::test]
async fn decision_failure_does_not_fall_through_to_the_executor() {
    let dir = tempfile::tempdir().unwrap();
    let (config, store, executor) = setup(dir.path());
    let job = store.enqueue(&event("1", "pr-1")).await.unwrap();
    let app = decision_app(BrokenDecisionProvider).await;
    Arc::new(
        Runtime::new(config, store.clone(), executor.clone())
            .unwrap()
            .with_services(app.services()),
    )
    .run(CancellationToken::new(), true)
    .await
    .unwrap();

    let saved = store.get_job(&job.id).await.unwrap();
    assert_eq!(saved.status, JobStatus::Failed);
    assert_eq!(
        saved.error.as_deref(),
        Some("decision: decision provider unavailable")
    );
    assert!(store.job_result(&job.id).await.unwrap().is_none());
    assert!(store.session("pr-1").await.unwrap().is_none());
    assert_eq!(executor.starts.load(Ordering::SeqCst), 0);
    assert!(executor.cwds.lock().unwrap().is_empty());
}

#[tokio::test]
async fn lost_thread_creation_ack_recovers_same_thread_without_recreating_it() {
    let dir = tempfile::tempdir().unwrap();
    let (config, store, executor) = setup(dir.path());
    executor.lose_creation_ack.store(true, Ordering::SeqCst);
    let job = store.enqueue(&event("1", "pr-42")).await.unwrap();
    assert!(
        runtime(config.clone(), store.clone(), executor.clone())
            .run(CancellationToken::new(), true)
            .await
            .is_err()
    );
    let saved = store.get_job(&job.id).await.unwrap();
    assert_eq!(saved.status, JobStatus::Uncertain);
    assert!(saved.thread_id.is_none());
    assert!(store.session("pr-42").await.unwrap().is_none());
    let intent = store.session_creation("pr-42").await.unwrap().unwrap();
    let created_thread = executor.creations.lock().unwrap()[&intent.id].clone();
    assert_eq!(executor.starts.load(Ordering::SeqCst), 0);

    let restarted_store: Arc<dyn Store> =
        Arc::new(SqliteStore::open(&dir.path().join("state.sqlite")).unwrap());
    runtime(config, restarted_store.clone(), executor.clone())
        .run(CancellationToken::new(), true)
        .await
        .unwrap();

    assert_eq!(executor.cwds.lock().unwrap().len(), 1);
    assert_eq!(executor.starts.load(Ordering::SeqCst), 1);
    assert_eq!(
        restarted_store
            .session("pr-42")
            .await
            .unwrap()
            .unwrap()
            .thread_id,
        created_thread
    );
    assert_eq!(
        restarted_store.get_job(&job.id).await.unwrap().status,
        JobStatus::Succeeded
    );
}

#[tokio::test]
async fn missing_creation_keeps_its_slot_and_is_never_reissued_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let (mut config, store, executor) = setup(dir.path());
    Arc::make_mut(&mut config).concurrency = 1;
    executor.lose_creation_ack.store(true, Ordering::SeqCst);
    executor.hide_creation.store(true, Ordering::SeqCst);
    let first = store.enqueue(&event("1", "pr-1")).await.unwrap();
    let second = store.enqueue(&event("2", "pr-2")).await.unwrap();
    for _ in 0..3 {
        assert!(
            runtime(config.clone(), store.clone(), executor.clone())
                .run(CancellationToken::new(), true)
                .await
                .is_err()
        );
        assert_eq!(
            store.get_job(&first.id).await.unwrap().status,
            JobStatus::Uncertain
        );
        assert_eq!(
            store.get_job(&second.id).await.unwrap().status,
            JobStatus::Queued
        );
        assert_eq!(store.inflight_count().await.unwrap(), 1);
    }
    assert_eq!(executor.cwds.lock().unwrap().len(), 1);
    assert_eq!(executor.starts.load(Ordering::SeqCst), 0);
    executor.hide_creation.store(false, Ordering::SeqCst);
    runtime(config, store.clone(), executor.clone())
        .run(CancellationToken::new(), true)
        .await
        .unwrap();
    assert_eq!(executor.cwds.lock().unwrap().len(), 2);
    assert_eq!(executor.starts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn stopping_runtime_detaches_from_active_turn_and_restart_collects_its_result() {
    let dir = tempfile::tempdir().unwrap();
    let (config, store, mut executor) = setup(dir.path());
    Arc::get_mut(&mut executor).unwrap().wait_for_interrupt = true;
    let job = store.enqueue(&event("1", "pr-1")).await.unwrap();
    let stop = CancellationToken::new();
    let runner = tokio::spawn(
        runtime(config.clone(), store.clone(), executor.clone()).run(stop.clone(), false),
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        while store.get_job(&job.id).await.unwrap().status != JobStatus::Running {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let active = store.get_job(&job.id).await.unwrap();
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(1), runner)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let detached = store.get_job(&job.id).await.unwrap();
    assert_eq!(detached.status, JobStatus::Running);
    assert!(!detached.cancel_requested);
    assert!(!*executor.interrupted.borrow());
    assert_eq!(detached.thread_id, active.thread_id);
    assert_eq!(detached.turn_id, active.turn_id);

    executor.recovered.store(true, Ordering::SeqCst);
    runtime(config, store.clone(), executor.clone())
        .run(CancellationToken::new(), true)
        .await
        .unwrap();
    assert_eq!(
        store.get_job(&job.id).await.unwrap().status,
        JobStatus::Succeeded
    );
    assert_eq!(executor.cwds.lock().unwrap().len(), 1);
    assert_eq!(executor.starts.load(Ordering::SeqCst), 1);
}

struct WaitForShutdown(watch::Sender<bool>);

#[async_trait]
impl DecisionProvider for WaitForShutdown {
    async fn decide(&self, _: &Event) -> Result<Decision> {
        self.0.send_replace(true);
        std::future::pending().await
    }
}

#[tokio::test]
async fn shutdown_before_submission_leaves_preparing_work_for_restart() {
    let dir = tempfile::tempdir().unwrap();
    let (config, store, executor) = setup(dir.path());
    let job = store.enqueue(&event("1", "pr-1")).await.unwrap();
    let (entered, mut observed) = watch::channel(false);
    let stop = CancellationToken::new();
    let app = decision_app(WaitForShutdown(entered)).await;
    let runner = tokio::spawn(
        Arc::new(
            Runtime::new(config.clone(), store.clone(), executor.clone())
                .unwrap()
                .with_services(app.services()),
        )
        .run(stop.clone(), false),
    );
    tokio::time::timeout(Duration::from_secs(1), async {
        while !*observed.borrow_and_update() {
            observed.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(1), runner)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let saved = store.get_job(&job.id).await.unwrap();
    assert_eq!(saved.status, JobStatus::Preparing);
    assert!(!saved.cancel_requested);
    assert!(executor.cwds.lock().unwrap().is_empty());
    runtime(config, store.clone(), executor.clone())
        .run(CancellationToken::new(), true)
        .await
        .unwrap();
    assert_eq!(
        store.get_job(&job.id).await.unwrap().status,
        JobStatus::Succeeded
    );
    assert_eq!(executor.starts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn sets_event_title_before_turn_and_updates_the_reused_session() {
    let dir = tempfile::tempdir().unwrap();
    let (config, store, executor) = setup(dir.path());
    for (id, title) in [
        ("1", "PR #42 — New feature"),
        ("2", "PR #42 — Revised feature"),
    ] {
        let mut input = event(id, "pr-42");
        input.title = Some(title.into());
        store.enqueue(&input).await.unwrap();
    }

    runtime(config, store.clone(), executor.clone())
        .run(CancellationToken::new(), true)
        .await
        .unwrap();

    let thread = store.session("pr-42").await.unwrap().unwrap().thread_id;
    assert_eq!(
        *executor.titles.lock().unwrap(),
        [
            (thread.clone(), "PR #42 — New feature".into()),
            (thread, "PR #42 — Revised feature".into())
        ]
    );
    assert_eq!(
        *executor.operations.lock().unwrap(),
        ["create", "title", "turn", "title", "turn"]
    );
}

#[tokio::test]
async fn lost_title_ack_retries_metadata_without_recreating_session_or_replaying_turn() {
    let dir = tempfile::tempdir().unwrap();
    let (config, store, executor) = setup(dir.path());
    executor.lose_title_ack.store(true, Ordering::SeqCst);
    let mut input = event("1", "pr-42");
    input.title = Some("PR #42 — New feature".into());
    let job = store.enqueue(&input).await.unwrap();

    assert!(
        runtime(config.clone(), store.clone(), executor.clone())
            .run(CancellationToken::new(), true)
            .await
            .is_err()
    );
    let saved = store.get_job(&job.id).await.unwrap();
    assert_eq!(saved.status, JobStatus::Uncertain);
    assert!(saved.thread_id.is_none());
    assert!(saved.turn_id.is_none());
    assert_eq!(executor.starts.load(Ordering::SeqCst), 0);
    let thread = store.session("pr-42").await.unwrap().unwrap().thread_id;

    let reopened: Arc<dyn Store> =
        Arc::new(SqliteStore::open(&dir.path().join("state.sqlite")).unwrap());
    runtime(config, reopened.clone(), executor.clone())
        .run(CancellationToken::new(), true)
        .await
        .unwrap();

    assert_eq!(
        reopened.get_job(&job.id).await.unwrap().status,
        JobStatus::Succeeded
    );
    assert_eq!(executor.cwds.lock().unwrap().len(), 1);
    assert_eq!(executor.starts.load(Ordering::SeqCst), 1);
    assert_eq!(
        *executor.titles.lock().unwrap(),
        [
            (thread.clone(), input.title.clone().unwrap()),
            (thread, input.title.unwrap())
        ]
    );
    assert_eq!(
        *executor.operations.lock().unwrap(),
        ["create", "title", "title", "turn"]
    );
}

struct PrepareWorkspace {
    cwd: PathBuf,
    calls: Arc<AtomicUsize>,
    stages: Arc<Mutex<Vec<&'static str>>>,
    fail: bool,
}

impl Plugin for PrepareWorkspace {
    fn name(&self) -> &str {
        "prepare-test-workspace"
    }
    fn register(&self, context: &mut PluginContext) -> Result<()> {
        let cwd = self.cwd.clone();
        let calls = self.calls.clone();
        let stages = self.stages.clone();
        let fail = self.fail;
        context.on::<TaskPreparing, _>(move |_, event| {
            let cwd = cwd.clone();
            let calls = calls.clone();
            let stages = stages.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                stages.lock().unwrap().push("task");
                if fail {
                    bail!("workspace contains local changes");
                }
                if let Some(existing) = &event.existing_session {
                    assert_eq!(existing.cwd, cwd);
                }
                event.task.session.cwd = cwd;
                event.task.session.git = Some(GitInfo {
                    sha: "a".repeat(40),
                    branch: Some("review/pr-42".into()),
                    origin_url: None,
                });
                event.task.input.context["prepared"] = true.into();
                Ok(())
            })
        });
        let stages = self.stages.clone();
        context.on::<SessionPreparing, _>(move |_, event| {
            let stages = stages.clone();
            Box::pin(async move {
                stages.lock().unwrap().push("session");
                event.session.title = Some("Prepared review session".into());
                Ok(())
            })
        });
        Ok(())
    }
}

#[tokio::test]
async fn preparation_plugins_choose_workspace_before_session_and_run_for_each_turn() {
    let dir = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let (config, store, executor) = setup(dir.path());
    let calls = Arc::new(AtomicUsize::new(0));
    let stages = Arc::new(Mutex::new(Vec::new()));
    let app = App::builder()
        .plugin(PrepareWorkspace {
            cwd: workspace.path().into(),
            calls: calls.clone(),
            stages: stages.clone(),
            fail: false,
        })
        .build()
        .await
        .unwrap();
    for id in ["first", "next"] {
        store.enqueue(&event(id, "pr-42")).await.unwrap();
    }

    Arc::new(
        Runtime::new(config, store.clone(), executor.clone())
            .unwrap()
            .with_services(app.services()),
    )
    .run(CancellationToken::new(), true)
    .await
    .unwrap();

    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(*stages.lock().unwrap(), ["task", "session", "task"]);
    assert_eq!(*executor.cwds.lock().unwrap(), [workspace.path()]);
    assert!(
        executor
            .configured
            .lock()
            .unwrap()
            .iter()
            .all(|spec| spec.cwd == workspace.path()
                && spec.git.as_ref().unwrap().branch.as_deref() == Some("review/pr-42"))
    );
    assert!(
        executor
            .inputs
            .lock()
            .unwrap()
            .iter()
            .all(|input| input.context["prepared"] == true)
    );
    assert!(
        store
            .list_jobs(10)
            .await
            .unwrap()
            .iter()
            .all(|job| job.status == JobStatus::Succeeded)
    );
}

#[tokio::test]
async fn failed_preparation_does_not_create_a_session_or_submit_a_turn() {
    let dir = tempfile::tempdir().unwrap();
    let (config, store, executor) = setup(dir.path());
    let app = App::builder()
        .plugin(PrepareWorkspace {
            cwd: dir.path().into(),
            calls: Arc::new(AtomicUsize::new(0)),
            stages: Arc::new(Mutex::new(Vec::new())),
            fail: true,
        })
        .build()
        .await
        .unwrap();
    let job = store.enqueue(&event("first", "pr-42")).await.unwrap();
    Arc::new(
        Runtime::new(config, store.clone(), executor.clone())
            .unwrap()
            .with_services(app.services()),
    )
    .run(CancellationToken::new(), true)
    .await
    .unwrap();
    let saved = store.get_job(&job.id).await.unwrap();
    assert_eq!(saved.status, JobStatus::Failed);
    assert_eq!(
        saved.error.as_deref(),
        Some("preparation: workspace contains local changes")
    );
    assert!(store.job_preparation(&job.id).await.unwrap().is_none());
    assert!(store.session_creation("pr-42").await.unwrap().is_none());
    assert!(executor.cwds.lock().unwrap().is_empty());
    assert_eq!(executor.starts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn saved_preparation_is_rechecked_by_the_decision_policy_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let (config, store, executor) = setup(dir.path());
    let observation = Event {
        source: "github".into(),
        context: json!({"noise":true}),
        ..event("1", "pr-1")
    };
    let job = store.enqueue(&observation).await.unwrap();
    store.claim_next().await.unwrap();
    let preparation = PreparedTask {
        session: SessionSpec {
            cwd: dir.path().into(),
            title: None,
            git: None,
        },
        input: TurnInput {
            text: observation.text,
            context: observation.context,
        },
    };
    store.save_preparation(&job.id, &preparation).await.unwrap();
    let app = decision_app(FilterGithubNoise).await;

    Arc::new(
        Runtime::new(config, store.clone(), executor.clone())
            .unwrap()
            .with_services(app.services()),
    )
    .run(CancellationToken::new(), true)
    .await
    .unwrap();

    assert_eq!(
        store.get_job(&job.id).await.unwrap().status,
        JobStatus::Skipped
    );
    assert_eq!(
        store.job_preparation(&job.id).await.unwrap(),
        Some(preparation)
    );
    assert!(store.session_creation("pr-1").await.unwrap().is_none());
    assert!(executor.cwds.lock().unwrap().is_empty());
    assert_eq!(executor.starts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn prepared_workspace_checkpoint_survives_lost_creation_ack_without_repeating_hooks() {
    let dir = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let (config, store, executor) = setup(dir.path());
    let calls = Arc::new(AtomicUsize::new(0));
    let app = App::builder()
        .plugin(PrepareWorkspace {
            cwd: workspace.path().into(),
            calls: calls.clone(),
            stages: Arc::new(Mutex::new(Vec::new())),
            fail: false,
        })
        .build()
        .await
        .unwrap();
    executor.lose_creation_ack.store(true, Ordering::SeqCst);
    let job = store.enqueue(&event("first", "pr-42")).await.unwrap();
    assert!(
        Arc::new(
            Runtime::new(config.clone(), store.clone(), executor.clone())
                .unwrap()
                .with_services(app.services())
        )
        .run(CancellationToken::new(), true)
        .await
        .is_err()
    );
    let checkpoint = store.job_preparation(&job.id).await.unwrap().unwrap();
    assert_eq!(checkpoint.session.cwd, workspace.path());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    drop(app);

    let app = App::builder()
        .plugin(PrepareWorkspace {
            cwd: dir.path().into(),
            calls: calls.clone(),
            stages: Arc::new(Mutex::new(Vec::new())),
            fail: true,
        })
        .build()
        .await
        .unwrap();
    let reopened: Arc<dyn Store> =
        Arc::new(SqliteStore::open(&dir.path().join("state.sqlite")).unwrap());
    Arc::new(
        Runtime::new(config, reopened.clone(), executor.clone())
            .unwrap()
            .with_services(app.services()),
    )
    .run(CancellationToken::new(), true)
    .await
    .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(*executor.cwds.lock().unwrap(), [workspace.path()]);
    assert_eq!(executor.starts.load(Ordering::SeqCst), 1);
    assert_eq!(
        reopened.job_preparation(&job.id).await.unwrap().unwrap(),
        checkpoint
    );
}

struct ObserveCompletion {
    store: Arc<dyn Store>,
    calls: Arc<AtomicUsize>,
    fail_first: Arc<AtomicBool>,
    delay: Duration,
}

impl Plugin for ObserveCompletion {
    fn name(&self) -> &str {
        "observe-test-completion"
    }
    fn register(&self, context: &mut PluginContext) -> Result<()> {
        let store = self.store.clone();
        let calls = self.calls.clone();
        let fail_first = self.fail_first.clone();
        let delay = self.delay;
        context.observe::<TaskCompleted, _>(move |_, event| {
            let store = store.clone();
            let calls = calls.clone();
            let fail_first = fail_first.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                assert_eq!(
                    store.get_job(&event.job.id).await?.status,
                    JobStatus::Finalizing
                );
                assert_eq!(
                    store.job_result(&event.job.id).await?.unwrap().text,
                    event.result.text
                );
                tokio::time::sleep(delay).await;
                if fail_first.swap(false, Ordering::SeqCst) {
                    bail!("completion sink temporarily unavailable");
                }
                Ok(())
            })
        });
        Ok(())
    }
}

#[tokio::test]
async fn completion_failure_retries_saved_result_after_restart_without_replaying_model() {
    let dir = tempfile::tempdir().unwrap();
    let (config, store, executor) = setup(dir.path());
    let calls = Arc::new(AtomicUsize::new(0));
    let app = App::builder()
        .plugin(ObserveCompletion {
            store: store.clone(),
            calls: calls.clone(),
            fail_first: Arc::new(AtomicBool::new(true)),
            delay: Duration::ZERO,
        })
        .build()
        .await
        .unwrap();
    let job = store.enqueue(&event("first", "pr-42")).await.unwrap();
    assert!(
        Arc::new(
            Runtime::new(config.clone(), store.clone(), executor.clone())
                .unwrap()
                .with_services(app.services())
        )
        .run(CancellationToken::new(), true)
        .await
        .is_err()
    );
    let pending = store.get_job(&job.id).await.unwrap();
    assert_eq!(pending.status, JobStatus::Finalizing);
    assert_eq!(
        pending.error.as_deref(),
        Some("completion processing: completion sink temporarily unavailable")
    );
    assert_eq!(
        store.job_result(&job.id).await.unwrap().unwrap().text,
        "Task result"
    );
    assert_eq!(store.inflight_count().await.unwrap(), 1);

    let reopened: Arc<dyn Store> =
        Arc::new(SqliteStore::open(&dir.path().join("state.sqlite")).unwrap());
    Arc::new(
        Runtime::new(config, reopened.clone(), executor.clone())
            .unwrap()
            .with_services(app.services()),
    )
    .run(CancellationToken::new(), true)
    .await
    .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(executor.starts.load(Ordering::SeqCst), 1);
    let completed = reopened.get_job(&job.id).await.unwrap();
    assert_eq!(completed.status, JobStatus::Succeeded);
    assert_eq!(completed.thread_id, pending.thread_id);
    assert_eq!(completed.turn_id, pending.turn_id);
}

#[tokio::test]
async fn reconciliation_does_not_run_a_completion_observer_concurrently_twice() {
    let dir = tempfile::tempdir().unwrap();
    let (config, store, executor) = setup(dir.path());
    let job = store.enqueue(&event("first", "pr-42")).await.unwrap();
    store.claim_next().await.unwrap();
    store
        .mark_submitting(&job.id, "saved-thread")
        .await
        .unwrap();
    store.mark_running(&job.id, "saved-turn").await.unwrap();
    store.record_result(&job.id, &result()).await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let app = App::builder()
        .plugin(ObserveCompletion {
            store: store.clone(),
            calls: calls.clone(),
            fail_first: Arc::new(AtomicBool::new(false)),
            delay: Duration::from_millis(80),
        })
        .build()
        .await
        .unwrap();
    let stop = CancellationToken::new();
    let runner = tokio::spawn(
        Arc::new(
            Runtime::new(config, store.clone(), executor.clone())
                .unwrap()
                .with_services(app.services()),
        )
        .run(stop.clone(), false),
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while store.get_job(&job.id).await.unwrap().status != JobStatus::Succeeded {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    stop.cancel();
    runner.await.unwrap().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(executor.starts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn shutdown_during_completion_keeps_saved_result_for_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let (config, store, executor) = setup(dir.path());
    let calls = Arc::new(AtomicUsize::new(0));
    let app = App::builder()
        .plugin(ObserveCompletion {
            store: store.clone(),
            calls: calls.clone(),
            fail_first: Arc::new(AtomicBool::new(false)),
            delay: Duration::from_secs(60),
        })
        .build()
        .await
        .unwrap();
    let job = store.enqueue(&event("first", "pr-42")).await.unwrap();
    let stop = CancellationToken::new();
    let runner = tokio::spawn(
        Arc::new(
            Runtime::new(config.clone(), store.clone(), executor.clone())
                .unwrap()
                .with_services(app.services()),
        )
        .run(stop.clone(), false),
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(1), runner)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let pending = store.get_job(&job.id).await.unwrap();
    assert_eq!(pending.status, JobStatus::Finalizing);
    assert!(!pending.cancel_requested);
    assert!(!*executor.interrupted.borrow());
    assert!(store.job_result(&job.id).await.unwrap().is_some());

    runtime(config, store.clone(), executor.clone())
        .run(CancellationToken::new(), true)
        .await
        .unwrap();
    assert_eq!(
        store.get_job(&job.id).await.unwrap().status,
        JobStatus::Succeeded
    );
    assert_eq!(executor.starts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelling_failed_completion_skips_retry_and_releases_the_subject() {
    let dir = tempfile::tempdir().unwrap();
    let (mut config, store, executor) = setup(dir.path());
    Arc::make_mut(&mut config).concurrency = 1;
    let calls = Arc::new(AtomicUsize::new(0));
    let app = App::builder()
        .plugin(ObserveCompletion {
            store: store.clone(),
            calls: calls.clone(),
            fail_first: Arc::new(AtomicBool::new(true)),
            delay: Duration::ZERO,
        })
        .build()
        .await
        .unwrap();
    let first = store.enqueue(&event("first", "pr-42")).await.unwrap();
    let next = store.enqueue(&event("next", "pr-42")).await.unwrap();
    assert!(
        Arc::new(
            Runtime::new(config.clone(), store.clone(), executor.clone())
                .unwrap()
                .with_services(app.services())
        )
        .run(CancellationToken::new(), true)
        .await
        .is_err()
    );
    assert_eq!(
        store.get_job(&first.id).await.unwrap().status,
        JobStatus::Finalizing
    );
    assert_eq!(
        store.get_job(&next.id).await.unwrap().status,
        JobStatus::Queued
    );
    store.request_cancel(&first.id).await.unwrap();

    Arc::new(
        Runtime::new(config, store.clone(), executor.clone())
            .unwrap()
            .with_services(app.services()),
    )
    .run(CancellationToken::new(), true)
    .await
    .unwrap();

    let cancelled = store.get_job(&first.id).await.unwrap();
    assert_eq!(cancelled.status, JobStatus::Cancelled);
    assert_eq!(
        cancelled.error.as_deref(),
        Some("completion processing cancelled")
    );
    assert_eq!(
        store.job_result(&first.id).await.unwrap().unwrap().status,
        TurnStatus::Completed
    );
    assert_eq!(
        store.get_job(&next.id).await.unwrap().status,
        JobStatus::Succeeded
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(executor.starts.load(Ordering::SeqCst), 2);
    assert_eq!(store.inflight_count().await.unwrap(), 0);
    assert!(!*executor.interrupted.borrow());
}

#[tokio::test]
async fn cancelling_hung_completion_drops_observer_and_starts_next_subject_job() {
    let dir = tempfile::tempdir().unwrap();
    let (mut config, store, executor) = setup(dir.path());
    Arc::make_mut(&mut config).concurrency = 1;
    let calls = Arc::new(AtomicUsize::new(0));
    let app = App::builder()
        .plugin(ObserveCompletion {
            store: store.clone(),
            calls: calls.clone(),
            fail_first: Arc::new(AtomicBool::new(false)),
            delay: Duration::from_secs(60),
        })
        .build()
        .await
        .unwrap();
    let first = store.enqueue(&event("first", "pr-42")).await.unwrap();
    let next = store.enqueue(&event("next", "pr-42")).await.unwrap();
    let stop = CancellationToken::new();
    let runner = tokio::spawn(
        Arc::new(
            Runtime::new(config.clone(), store.clone(), executor.clone())
                .unwrap()
                .with_services(app.services()),
        )
        .run(stop.clone(), false),
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while calls.load(Ordering::SeqCst) < 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    store.request_cancel(&first.id).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while calls.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        store.get_job(&first.id).await.unwrap().status,
        JobStatus::Cancelled
    );
    assert_eq!(
        store.job_result(&first.id).await.unwrap().unwrap().status,
        TurnStatus::Completed
    );
    assert_eq!(
        store.get_job(&next.id).await.unwrap().status,
        JobStatus::Finalizing
    );
    assert!(!*executor.interrupted.borrow());
    assert_eq!(executor.starts.load(Ordering::SeqCst), 2);
    stop.cancel();
    runner.await.unwrap().unwrap();
    runtime(config, store.clone(), executor.clone())
        .run(CancellationToken::new(), true)
        .await
        .unwrap();
    assert_eq!(
        store.get_job(&next.id).await.unwrap().status,
        JobStatus::Succeeded
    );
    assert_eq!(executor.starts.load(Ordering::SeqCst), 2);
}

struct BlockPreparation {
    job_id: String,
    entered: watch::Sender<bool>,
    dropped: Arc<AtomicBool>,
}

struct PreparationDropped(Arc<AtomicBool>);
impl Drop for PreparationDropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

impl Plugin for BlockPreparation {
    fn name(&self) -> &str {
        "block-test-preparation"
    }
    fn register(&self, ctx: &mut PluginContext) -> Result<()> {
        let job_id = self.job_id.clone();
        let entered = self.entered.clone();
        let dropped = self.dropped.clone();
        ctx.on::<TaskPreparing, _>(move |_, task| {
            let job_id = job_id.clone();
            let entered = entered.clone();
            let dropped = dropped.clone();
            Box::pin(async move {
                if task.job.id == job_id {
                    let _drop = PreparationDropped(dropped);
                    entered.send_replace(true);
                    std::future::pending::<()>().await;
                }
                Ok(())
            })
        });
        Ok(())
    }
}

#[tokio::test]
async fn cancelling_hung_preparation_drops_hook_before_releasing_subject() {
    let dir = tempfile::tempdir().unwrap();
    let (mut config, store, executor) = setup(dir.path());
    Arc::make_mut(&mut config).concurrency = 1;
    let first = store.enqueue(&event("first", "pr-42")).await.unwrap();
    let next = store.enqueue(&event("next", "pr-42")).await.unwrap();
    let (entered, mut observed) = watch::channel(false);
    let dropped = Arc::new(AtomicBool::new(false));
    let app = App::builder()
        .plugin(BlockPreparation {
            job_id: first.id.clone(),
            entered,
            dropped: dropped.clone(),
        })
        .build()
        .await
        .unwrap();
    let stop = CancellationToken::new();
    let runner = tokio::spawn(
        Arc::new(
            Runtime::new(config, store.clone(), executor.clone())
                .unwrap()
                .with_services(app.services()),
        )
        .run(stop.clone(), false),
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while !*observed.borrow_and_update() {
            observed.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    store.request_cancel(&first.id).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while store.get_job(&next.id).await.unwrap().status != JobStatus::Succeeded {
            if store.get_job(&first.id).await.unwrap().status == JobStatus::Cancelled {
                assert!(dropped.load(Ordering::SeqCst));
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    stop.cancel();
    runner.await.unwrap().unwrap();
    let cancelled = store.get_job(&first.id).await.unwrap();
    assert_eq!(cancelled.status, JobStatus::Cancelled);
    assert!(cancelled.thread_id.is_none());
    assert!(cancelled.turn_id.is_none());
    assert!(store.job_preparation(&first.id).await.unwrap().is_none());
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(executor.cwds.lock().unwrap().len(), 1);
    assert_eq!(executor.starts.load(Ordering::SeqCst), 1);
    assert_eq!(store.inflight_count().await.unwrap(), 0);
}
