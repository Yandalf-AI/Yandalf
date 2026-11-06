use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, anyhow};

use yandalf_core::{
    domain::{Event, Job, JobStatus, PreparedTask, Session, SessionCreation, TurnResult},
    plugin::{Plugin, PluginContext},
    store::{RunnerLease, Store},
};

mod database;
mod files;

use database::Database;

#[derive(Clone)]
pub struct SqliteStore {
    database: Arc<Database>,
}

impl SqliteStore {
    pub fn open(path: &Path) -> Result<Self> {
        Ok(Self {
            database: Arc::new(Database::open(path)?),
        })
    }

    async fn call<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&Database) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let database = self.database.clone();
        let lease = database
            .active_lease
            .lock()
            .map_err(|_| anyhow!("SQLite lease mutex poisoned"))?
            .upgrade();
        // Cancellation of the caller must not release ownership before submitted SQL work settles.
        tokio::task::spawn_blocking(move || {
            let _lease = lease;
            operation(&database)
        })
        .await
        .context("SQLite blocking task failed")?
    }
}

struct LeaseState {
    _file: File,
}

struct SqliteRunnerLease {
    _state: Arc<LeaseState>,
}

impl RunnerLease for SqliteRunnerLease {}

#[async_trait::async_trait]
impl Store for SqliteStore {
    async fn acquire_runner(&self) -> Result<Box<dyn RunnerLease>> {
        self.call(Database::acquire_runner).await
    }

    async fn enqueue(&self, event: &Event) -> Result<Job> {
        let event = event.clone();
        self.call(move |database| database.enqueue(&event)).await
    }

    async fn last_event(&self, source: &str, subject: &str) -> Result<Option<Event>> {
        let source = source.to_owned();
        let subject = subject.to_owned();
        self.call(move |database| database.last_event(&source, &subject))
            .await
    }

    async fn last_events(&self, source: &str) -> Result<Vec<Event>> {
        let source = source.to_owned();
        self.call(move |database| database.last_events(&source))
            .await
    }

    async fn claim_next(&self) -> Result<Option<Job>> {
        self.call(Database::claim_next).await
    }

    async fn get_job(&self, id: &str) -> Result<Job> {
        let id = id.to_owned();
        self.call(move |database| database.get_job(&id)).await
    }

    async fn list_jobs(&self, limit: usize) -> Result<Vec<Job>> {
        self.call(move |database| database.list_jobs(limit)).await
    }

    async fn job_result(&self, id: &str) -> Result<Option<TurnResult>> {
        let id = id.to_owned();
        self.call(move |database| database.job_result(&id)).await
    }

    async fn job_preparation(&self, id: &str) -> Result<Option<PreparedTask>> {
        let id = id.to_owned();
        self.call(move |database| database.job_preparation(&id))
            .await
    }

    async fn save_preparation(&self, id: &str, preparation: &PreparedTask) -> Result<()> {
        let id = id.to_owned();
        let preparation = preparation.clone();
        self.call(move |database| database.save_preparation(&id, &preparation))
            .await
    }

    async fn record_result(&self, id: &str, result: &TurnResult) -> Result<()> {
        let id = id.to_owned();
        let result = result.clone();
        self.call(move |database| database.record_result(&id, &result))
            .await
    }

    async fn finalizing_jobs(&self) -> Result<Vec<Job>> {
        self.call(Database::finalizing_jobs).await
    }

    async fn finalization_failed(&self, id: &str, error: &str) -> Result<()> {
        let id = id.to_owned();
        let error = error.to_owned();
        self.call(move |database| database.finalization_failed(&id, &error))
            .await
    }

    async fn inflight_count(&self) -> Result<usize> {
        self.call(Database::inflight_count).await
    }

    async fn uncertain_jobs(&self) -> Result<Vec<Job>> {
        self.call(Database::uncertain_jobs).await
    }

    async fn session(&self, subject: &str) -> Result<Option<Session>> {
        let subject = subject.to_owned();
        self.call(move |database| database.session(&subject)).await
    }

    async fn bind_session(&self, session: &Session) -> Result<()> {
        let session = session.clone();
        self.call(move |database| database.bind_session(&session))
            .await
    }

    async fn begin_session_creation(
        &self,
        subject: &str,
        cwd: &Path,
    ) -> Result<(SessionCreation, bool)> {
        let subject = subject.to_owned();
        let cwd = cwd.to_owned();
        self.call(move |database| database.begin_session_creation(&subject, &cwd))
            .await
    }

    async fn session_creation(&self, subject: &str) -> Result<Option<SessionCreation>> {
        let subject = subject.to_owned();
        self.call(move |database| database.session_creation(&subject))
            .await
    }

    async fn resume_preparation(&self, id: &str) -> Result<()> {
        let id = id.to_owned();
        self.call(move |database| database.resume_preparation(&id))
            .await
    }

    async fn mark_submitting(&self, id: &str, thread_id: &str) -> Result<()> {
        let id = id.to_owned();
        let thread_id = thread_id.to_owned();
        self.call(move |database| database.mark_submitting(&id, &thread_id))
            .await
    }

    async fn mark_running(&self, id: &str, turn_id: &str) -> Result<()> {
        let id = id.to_owned();
        let turn_id = turn_id.to_owned();
        self.call(move |database| database.mark_running(&id, &turn_id))
            .await
    }

    async fn mark_uncertain(&self, id: &str, error: &str) -> Result<()> {
        let id = id.to_owned();
        let error = error.to_owned();
        self.call(move |database| database.mark_uncertain(&id, &error))
            .await
    }

    async fn mark_archiving(&self, id: &str) -> Result<()> {
        let id = id.to_owned();
        self.call(move |database| database.mark_archiving(&id))
            .await
    }

    async fn finish(
        &self,
        id: &str,
        status: JobStatus,
        result: Option<&TurnResult>,
        error: Option<&str>,
    ) -> Result<()> {
        let id = id.to_owned();
        let result = result.cloned();
        let error = error.map(str::to_owned);
        self.call(move |database| database.finish(&id, status, result.as_ref(), error.as_deref()))
            .await
    }

    async fn request_cancel(&self, id: &str) -> Result<()> {
        let id = id.to_owned();
        self.call(move |database| database.request_cancel(&id))
            .await
    }

    async fn recover(&self) -> Result<Vec<Job>> {
        self.call(Database::recover).await
    }

    async fn source_cursor(&self, source: &str, initial: i64) -> Result<i64> {
        let source = source.to_owned();
        self.call(move |database| database.source_cursor(&source, initial))
            .await
    }

    async fn advance_source_cursor(&self, source: &str, next: i64) -> Result<()> {
        let source = source.to_owned();
        self.call(move |database| database.advance_source_cursor(&source, next))
            .await
    }

    async fn accept_scheduled(&self, event: &Event, next_tick: i64) -> Result<Job> {
        let event = event.clone();
        self.call(move |database| database.accept_scheduled(&event, next_tick))
            .await
    }
}

pub struct Sqlite(pub PathBuf);

impl Plugin for Sqlite {
    fn name(&self) -> &str {
        "sqlite"
    }

    fn register(&self, ctx: &mut PluginContext) -> Result<()> {
        let path = self.0.clone();
        ctx.provide_with::<dyn Store, _, _>(move |_| async move {
            let store = tokio::task::spawn_blocking(move || SqliteStore::open(&path)).await??;
            Ok(Arc::new(store) as Arc<dyn Store>)
        })
    }
}
