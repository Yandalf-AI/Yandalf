use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard, Weak},
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use rusqlite::{Connection, OptionalExtension, Row, Transaction, TransactionBehavior, params};
use serde::de::DeserializeOwned;
use serde_json::Value;

use yandalf_core::{
    domain::{Event, Job, JobStatus, PreparedTask, Session, SessionCreation, TurnResult, now_ms},
    plugin::{Plugin, PluginContext},
    store::{RunnerLease, Store},
};

const APPLICATION_ID: i32 = 0x5941_4e44;
const SCHEMA_VERSION: i32 = 1;
const JOB_COLUMNS: &str =
    "id, event_json, status, thread_id, turn_id, cancel_requested, error, created_at, updated_at";

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

struct Database {
    connection: Mutex<Connection>,
    runner_path: PathBuf,
    active_lease: Mutex<Weak<LeaseState>>,
}

impl Database {
    fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent).context("create SQLite store directory")?;
        }
        let mut connection = Connection::open(path).context("open SQLite store")?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.pragma_update(None, "foreign_keys", true)?;
        initialize_schema(&mut connection)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        let runner_path = std::fs::canonicalize(path)
            .context("canonicalize SQLite database path")?
            .with_extension("runner.lock");
        Ok(Self {
            connection: Mutex::new(connection),
            runner_path,
            active_lease: Mutex::new(Weak::new()),
        })
    }

    fn acquire_runner(&self) -> Result<Box<dyn RunnerLease>> {
        let file = File::options()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&self.runner_path)
            .context("open SQLite runner lock")?;
        file.try_lock()
            .map_err(|error| anyhow!("cannot acquire exclusive SQLite runner lease: {error}"))?;
        let state = Arc::new(LeaseState { _file: file });
        *self
            .active_lease
            .lock()
            .map_err(|_| anyhow!("SQLite lease mutex poisoned"))? = Arc::downgrade(&state);
        Ok(Box::new(SqliteRunnerLease { _state: state }))
    }

    fn connection(&self) -> Result<MutexGuard<'_, Connection>> {
        self.connection
            .lock()
            .map_err(|_| anyhow!("SQLite store mutex poisoned"))
    }

    fn enqueue(&self, event: &Event) -> Result<Job> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let job = enqueue(&transaction, event)?;
        transaction.commit()?;
        Ok(job)
    }

    fn last_event(&self, source: &str, subject: &str) -> Result<Option<Event>> {
        Ok(self
            .connection()?
            .query_row(
                "SELECT event_json FROM yandalf_jobs WHERE source = ?1 AND subject = ?2 ORDER BY seq DESC LIMIT 1",
                params![source, subject],
                |row| json_column(row, 0),
            )
            .optional()?)
    }

    fn last_events(&self, source: &str) -> Result<Vec<Event>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT event_json FROM yandalf_jobs
             WHERE seq IN (SELECT max(seq) FROM yandalf_jobs WHERE source = ?1 GROUP BY subject)
             ORDER BY subject",
        )?;
        let events = statement.query_map([source], |row| json_column(row, 0))?;
        Ok(events.collect::<rusqlite::Result<Vec<Event>>>()?)
    }

    fn claim_next(&self) -> Result<Option<Job>> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let id: Option<String> = transaction
            .query_row(
                "SELECT queued.id FROM yandalf_jobs AS queued
             WHERE queued.status = 'queued' AND NOT EXISTS (
                 SELECT 1 FROM yandalf_jobs AS active
                 WHERE active.subject = queued.subject
                   AND active.status IN ('preparing', 'submitting', 'running', 'uncertain', 'finalizing')
             ) ORDER BY queued.seq LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()?;
        let job = if let Some(id) = id {
            transaction.execute(
                "UPDATE yandalf_jobs SET status = 'preparing', updated_at = ?2 WHERE id = ?1",
                params![id, now_ms()],
            )?;
            Some(read_job(&transaction, &id)?)
        } else {
            None
        };
        transaction.commit()?;
        Ok(job)
    }

    fn get_job(&self, id: &str) -> Result<Job> {
        read_job(&*self.connection()?, id)
    }

    fn list_jobs(&self, limit: usize) -> Result<Vec<Job>> {
        query_jobs(
            &*self.connection()?,
            &format!("SELECT {JOB_COLUMNS} FROM yandalf_jobs ORDER BY seq DESC LIMIT ?1"),
            [sql_limit(limit)?],
        )
    }

    fn job_result(&self, id: &str) -> Result<Option<TurnResult>> {
        let connection = self.connection()?;
        let result: Option<String> = connection
            .query_row(
                "SELECT result_json FROM yandalf_jobs WHERE id = ?1",
                [id],
                |row| row.get(0),
            )
            .optional()?
            .context("job not found")?;
        result
            .map(|value| serde_json::from_str(&value).context("decode stored turn result"))
            .transpose()
    }

    fn job_preparation(&self, id: &str) -> Result<Option<PreparedTask>> {
        Ok(self
            .connection()?
            .query_row(
                "SELECT preparation_json FROM yandalf_job_preparations WHERE job_id = ?1",
                [id],
                |row| json_column(row, 0),
            )
            .optional()?)
    }

    fn save_preparation(&self, id: &str, preparation: &PreparedTask) -> Result<()> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let job = read_job(&transaction, id)?;
        ensure!(
            job.status == JobStatus::Preparing,
            "job cannot prepare from {}",
            job.status.as_str()
        );
        let existing: Option<PreparedTask> = transaction
            .query_row(
                "SELECT preparation_json FROM yandalf_job_preparations WHERE job_id = ?1",
                [id],
                |row| json_column(row, 0),
            )
            .optional()?;
        if let Some(existing) = existing {
            ensure!(
                existing == *preparation,
                "job already has a different preparation"
            );
        } else {
            transaction.execute(
                "INSERT INTO yandalf_job_preparations (job_id, preparation_json) VALUES (?1, ?2)",
                params![id, serde_json::to_string(preparation)?],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn record_result(&self, id: &str, result: &TurnResult) -> Result<()> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let job = read_job(&transaction, id)?;
        ensure!(
            matches!(job.status, JobStatus::Running | JobStatus::Finalizing),
            "job cannot record a result from {}",
            job.status.as_str()
        );
        let serialized = serde_json::to_string(result)?;
        let existing: Option<String> = transaction.query_row(
            "SELECT result_json FROM yandalf_jobs WHERE id = ?1",
            [id],
            |row| row.get(0),
        )?;
        if let Some(existing) = existing {
            ensure!(
                serde_json::from_str::<Value>(&existing)?
                    == serde_json::from_str::<Value>(&serialized)?,
                "job already has a different result"
            );
        }
        if job.status != JobStatus::Finalizing {
            transaction.execute("UPDATE yandalf_jobs SET status = 'finalizing', result_json = ?2, error = NULL, updated_at = ?3 WHERE id = ?1", params![id, serialized, now_ms()])?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn finalizing_jobs(&self) -> Result<Vec<Job>> {
        query_jobs(
            &*self.connection()?,
            &format!(
                "SELECT {JOB_COLUMNS} FROM yandalf_jobs WHERE status = 'finalizing' ORDER BY seq"
            ),
            [],
        )
    }

    fn finalization_failed(&self, id: &str, error: &str) -> Result<()> {
        let changed = self.connection()?.execute(
            "UPDATE yandalf_jobs SET error = ?2, updated_at = ?3 WHERE id = ?1 AND status = 'finalizing'",
            params![id, error, now_ms()],
        )?;
        ensure!(changed == 1, "job is not finalizing");
        Ok(())
    }

    fn inflight_count(&self) -> Result<usize> {
        let count: i64 = self.connection()?.query_row(
            "SELECT count(*) FROM yandalf_jobs WHERE status IN ('preparing', 'submitting', 'running', 'uncertain', 'finalizing')", [], |row| row.get(0),
        )?;
        Ok(usize::try_from(count)?)
    }

    fn uncertain_jobs(&self) -> Result<Vec<Job>> {
        query_jobs(
            &*self.connection()?,
            &format!(
                "SELECT {JOB_COLUMNS} FROM yandalf_jobs WHERE status = 'uncertain' ORDER BY seq"
            ),
            [],
        )
    }

    fn session(&self, subject: &str) -> Result<Option<Session>> {
        read_session(&*self.connection()?, subject)
    }

    fn bind_session(&self, session: &Session) -> Result<()> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) = read_session(&transaction, &session.subject)? {
            ensure!(
                existing.cwd == session.cwd && existing.thread_id == session.thread_id,
                "subject is already bound to a different cwd or thread"
            );
        } else {
            transaction.execute(
                "INSERT INTO yandalf_sessions (subject, workspace_json, thread_id) VALUES (?1, ?2, ?3)",
                params![session.subject, serde_json::to_string(&session.cwd)?, session.thread_id],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn begin_session_creation(&self, subject: &str, cwd: &Path) -> Result<(SessionCreation, bool)> {
        ensure!(
            !subject.is_empty(),
            "session creation subject must not be empty"
        );
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (creation, fresh) = if let Some(existing) =
            read_session_creation(&transaction, subject)?
        {
            (existing, false)
        } else {
            ensure!(
                read_session(&transaction, subject)?.is_none(),
                "session is already bound"
            );
            let creation = SessionCreation {
                id: uuid::Uuid::new_v4().to_string(),
                subject: subject.to_owned(),
                cwd: cwd.to_owned(),
            };
            transaction.execute(
                "INSERT INTO yandalf_session_creations (subject, creation_id, workspace_json) VALUES (?1, ?2, ?3)",
                params![creation.subject, creation.id, serde_json::to_string(&creation.cwd)?],
            )?;
            (creation, true)
        };
        transaction.commit()?;
        Ok((creation, fresh))
    }

    fn session_creation(&self, subject: &str) -> Result<Option<SessionCreation>> {
        read_session_creation(&*self.connection()?, subject)
    }

    fn resume_preparation(&self, id: &str) -> Result<()> {
        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE yandalf_jobs SET status = 'preparing', error = NULL, updated_at = ?2 WHERE id = ?1 AND status = 'uncertain' AND thread_id IS NULL AND turn_id IS NULL",
            params![id, now_ms()],
        )?;
        ensure!(changed == 1, "job cannot resume preparation");
        Ok(())
    }

    fn mark_submitting(&self, id: &str, thread_id: &str) -> Result<()> {
        ensure!(!thread_id.is_empty(), "thread id must not be empty");
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let job = read_job(&transaction, id)?;
        ensure!(
            job.thread_id
                .as_deref()
                .is_none_or(|stored| stored == thread_id),
            "job is already bound to a different thread"
        );
        match job.status {
            JobStatus::Preparing => {
                transaction.execute(
                    "UPDATE yandalf_jobs SET status = 'submitting', thread_id = ?2, error = NULL, updated_at = ?3 WHERE id = ?1",
                    params![id, thread_id, now_ms()],
                )?;
            }
            JobStatus::Submitting if job.thread_id.as_deref() == Some(thread_id) => {}
            _ => bail!("job cannot submit from {}", job.status.as_str()),
        }
        transaction.commit()?;
        Ok(())
    }

    fn mark_running(&self, id: &str, turn_id: &str) -> Result<()> {
        ensure!(!turn_id.is_empty(), "turn id must not be empty");
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let job = read_job(&transaction, id)?;
        ensure!(
            matches!(
                job.status,
                JobStatus::Submitting | JobStatus::Running | JobStatus::Uncertain
            ),
            "job cannot run from {}",
            job.status.as_str()
        );
        ensure!(job.thread_id.is_some(), "job has no persisted thread id");
        ensure!(
            job.turn_id
                .as_deref()
                .is_none_or(|stored| stored == turn_id),
            "job is already bound to a different turn"
        );
        if job.status != JobStatus::Running || job.turn_id.as_deref() != Some(turn_id) {
            transaction.execute(
                "UPDATE yandalf_jobs SET status = 'running', turn_id = ?2, error = NULL, updated_at = ?3 WHERE id = ?1",
                params![id, turn_id, now_ms()],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn mark_uncertain(&self, id: &str, error: &str) -> Result<()> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let job = read_job(&transaction, id)?;
        ensure!(
            matches!(
                job.status,
                JobStatus::Preparing
                    | JobStatus::Submitting
                    | JobStatus::Running
                    | JobStatus::Uncertain
            ),
            "job cannot become uncertain from {}",
            job.status.as_str()
        );
        transaction.execute("UPDATE yandalf_jobs SET status = 'uncertain', error = ?2, updated_at = ?3 WHERE id = ?1", params![id, error, now_ms()])?;
        transaction.commit()?;
        Ok(())
    }

    fn finish(
        &self,
        id: &str,
        status: JobStatus,
        result: Option<&TurnResult>,
        error: Option<&str>,
    ) -> Result<()> {
        ensure!(status.is_terminal(), "finish requires a terminal status");
        let result_json = result.map(serde_json::to_string).transpose()?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let job = read_job(&transaction, id)?;
        if job.status.is_terminal() {
            let stored_result: Option<String> = transaction.query_row(
                "SELECT result_json FROM yandalf_jobs WHERE id = ?1",
                [id],
                |row| row.get(0),
            )?;
            ensure!(
                job.status == status
                    && job.error.as_deref() == error
                    && json_option(&stored_result)? == json_option(&result_json)?,
                "job already finished with a different outcome"
            );
        } else {
            if job.status == JobStatus::Finalizing {
                let saved_result: Option<String> = transaction.query_row(
                    "SELECT result_json FROM yandalf_jobs WHERE id = ?1",
                    [id],
                    |row| row.get(0),
                )?;
                ensure!(
                    result_json.is_some()
                        && json_option(&saved_result)? == json_option(&result_json)?,
                    "completion must preserve the saved executor result"
                );
                let expected = match result.expect("validated saved result").status {
                    yandalf_core::domain::TurnStatus::Completed => JobStatus::Succeeded,
                    yandalf_core::domain::TurnStatus::Failed => JobStatus::Failed,
                    yandalf_core::domain::TurnStatus::Interrupted => JobStatus::Cancelled,
                };
                ensure!(
                    status == expected || (status == JobStatus::Cancelled && job.cancel_requested),
                    "completion status contradicts the saved executor result"
                );
            }
            transaction.execute(
                "UPDATE yandalf_jobs SET status = ?2, result_json = ?3, error = ?4, updated_at = ?5 WHERE id = ?1",
                params![id, status.as_str(), result_json, error, now_ms()],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn request_cancel(&self, id: &str) -> Result<()> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let job = read_job(&transaction, id)?;
        if !job.status.is_terminal() {
            let status = if job.status == JobStatus::Queued {
                JobStatus::Cancelled
            } else {
                job.status
            };
            transaction.execute("UPDATE yandalf_jobs SET cancel_requested = 1, status = ?2, updated_at = ?3 WHERE id = ?1", params![id, status.as_str(), now_ms()])?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn recover(&self) -> Result<Vec<Job>> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "UPDATE yandalf_jobs SET status = 'queued', updated_at = ?1 WHERE status = 'preparing'",
            [now_ms()],
        )?;
        let jobs = query_jobs(
            &transaction,
            &format!(
                "SELECT {JOB_COLUMNS} FROM yandalf_jobs WHERE status IN ('submitting', 'running', 'uncertain', 'finalizing') ORDER BY seq"
            ),
            [],
        )?;
        transaction.commit()?;
        Ok(jobs)
    }

    fn source_cursor(&self, source: &str, initial: i64) -> Result<i64> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute("INSERT INTO yandalf_schedule_cursors (source, next_tick) VALUES (?1, ?2) ON CONFLICT(source) DO NOTHING", params![source, initial])?;
        let cursor = transaction.query_row(
            "SELECT next_tick FROM yandalf_schedule_cursors WHERE source = ?1",
            [source],
            |row| row.get(0),
        )?;
        transaction.commit()?;
        Ok(cursor)
    }

    fn advance_source_cursor(&self, source: &str, next: i64) -> Result<()> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let exists: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM yandalf_schedule_cursors WHERE source = ?1)",
            [source],
            |row| row.get(0),
        )?;
        ensure!(exists, "source cursor must be initialized before advancing");
        transaction.execute(
            "UPDATE yandalf_schedule_cursors SET next_tick = ?2 WHERE source = ?1 AND next_tick < ?2",
            params![source, next],
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn accept_scheduled(&self, event: &Event, next_tick: i64) -> Result<Job> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let job = enqueue(&transaction, event)?;
        transaction.execute(
            "INSERT INTO yandalf_schedule_cursors (source, next_tick) VALUES (?1, ?2)
             ON CONFLICT(source) DO UPDATE SET next_tick = max(yandalf_schedule_cursors.next_tick, excluded.next_tick)",
            params![event.source, next_tick],
        )?;
        transaction.commit()?;
        Ok(job)
    }
}

fn initialize_schema(connection: &mut Connection) -> Result<()> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let application_id: i32 =
        transaction.pragma_query_value(None, "application_id", |row| row.get(0))?;
    let version: i32 = transaction.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if application_id == 0 && version == 0 {
        let occupied: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%')",
            [],
            |row| row.get(0),
        )?;
        ensure!(
            !occupied,
            "refusing to use an existing legacy or unrelated SQLite database"
        );
        transaction.execute_batch(
            "CREATE TABLE yandalf_jobs (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                id TEXT NOT NULL UNIQUE,
                source TEXT NOT NULL,
                event_id TEXT NOT NULL,
                subject TEXT NOT NULL,
                event_json TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('queued','preparing','submitting','running','uncertain','finalizing','succeeded','skipped','failed','cancelled')),
                thread_id TEXT,
                turn_id TEXT,
                cancel_requested INTEGER NOT NULL DEFAULT 0 CHECK(cancel_requested IN (0,1)),
                result_json TEXT,
                error TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                UNIQUE(source, event_id)
            );
            CREATE INDEX yandalf_jobs_queue ON yandalf_jobs(status, seq);
            CREATE INDEX yandalf_jobs_source_subject ON yandalf_jobs(source, subject, seq);
            CREATE UNIQUE INDEX yandalf_jobs_active_subject ON yandalf_jobs(subject)
                WHERE status IN ('preparing','submitting','running','uncertain','finalizing');
            CREATE TABLE yandalf_sessions (
                subject TEXT PRIMARY KEY NOT NULL, workspace_json TEXT NOT NULL, thread_id TEXT NOT NULL
            );
            CREATE TABLE yandalf_session_creations (
                subject TEXT PRIMARY KEY NOT NULL, creation_id TEXT NOT NULL UNIQUE, workspace_json TEXT NOT NULL
            );
            CREATE TABLE yandalf_job_preparations (
                job_id TEXT PRIMARY KEY NOT NULL REFERENCES yandalf_jobs(id), preparation_json TEXT NOT NULL
            );
            CREATE TABLE yandalf_schedule_cursors (source TEXT PRIMARY KEY NOT NULL, next_tick INTEGER NOT NULL);",
        )?;
        transaction.pragma_update(None, "application_id", APPLICATION_ID)?;
        transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    } else {
        ensure!(
            application_id == APPLICATION_ID,
            "SQLite database does not belong to this Yandalf store"
        );
        ensure!(
            version == SCHEMA_VERSION,
            "unsupported Yandalf store schema version {version}"
        );
        let tables: i64 = transaction.query_row("SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name IN ('yandalf_jobs','yandalf_sessions','yandalf_session_creations','yandalf_schedule_cursors','yandalf_job_preparations')", [], |row| row.get(0))?;
        ensure!(tables == 5, "Yandalf store schema is incomplete");
    }
    transaction.commit()?;
    Ok(())
}

fn enqueue(transaction: &Transaction<'_>, event: &Event) -> Result<Job> {
    let existing = transaction
        .query_row(
            &format!("SELECT {JOB_COLUMNS} FROM yandalf_jobs WHERE source = ?1 AND event_id = ?2"),
            params![event.source, event.id],
            job_from_row,
        )
        .optional()?;
    if let Some(job) = existing {
        ensure!(
            job.event == *event,
            "event id already exists with a different payload"
        );
        return Ok(job);
    }
    let id = uuid::Uuid::new_v4().to_string();
    let now = now_ms();
    transaction.execute(
        "INSERT INTO yandalf_jobs (id, source, event_id, subject, event_json, status, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, 'queued', ?6, ?6)",
        params![id, event.source, event.id, event.subject, serde_json::to_string(event)?, now],
    )?;
    read_job(transaction, &id)
}

fn read_job(connection: &Connection, id: &str) -> Result<Job> {
    connection
        .query_row(
            &format!("SELECT {JOB_COLUMNS} FROM yandalf_jobs WHERE id = ?1"),
            [id],
            job_from_row,
        )
        .optional()?
        .context("job not found")
}

fn query_jobs(
    connection: &Connection,
    sql: &str,
    parameters: impl rusqlite::Params,
) -> Result<Vec<Job>> {
    let mut statement = connection.prepare(sql)?;
    Ok(statement
        .query_map(parameters, job_from_row)?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

fn job_from_row(row: &Row<'_>) -> rusqlite::Result<Job> {
    let status: String = row.get(2)?;
    let status = serde_json::from_value(Value::String(status)).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(Job {
        id: row.get(0)?,
        event: json_column(row, 1)?,
        status,
        thread_id: row.get(3)?,
        turn_id: row.get(4)?,
        cancel_requested: row.get(5)?,
        error: row.get(6)?,
        created_at: row.get(7)?,
        updated_at: row.get(8)?,
    })
}

fn json_column<T: DeserializeOwned>(row: &Row<'_>, index: usize) -> rusqlite::Result<T> {
    let text: String = row.get(index)?;
    serde_json::from_str(&text).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}

fn read_session_creation(
    connection: &Connection,
    subject: &str,
) -> Result<Option<SessionCreation>> {
    Ok(connection.query_row(
        "SELECT creation_id, subject, workspace_json FROM yandalf_session_creations WHERE subject = ?1",
        [subject],
        |row| Ok(SessionCreation { id: row.get(0)?, subject: row.get(1)?, cwd: json_column(row, 2)? }),
    ).optional()?)
}

fn read_session(connection: &Connection, subject: &str) -> Result<Option<Session>> {
    Ok(connection
        .query_row(
            "SELECT subject, workspace_json, thread_id FROM yandalf_sessions WHERE subject = ?1",
            [subject],
            |row| {
                Ok(Session {
                    subject: row.get(0)?,
                    cwd: json_column(row, 1)?,
                    thread_id: row.get(2)?,
                })
            },
        )
        .optional()?)
}

fn json_option(value: &Option<String>) -> Result<Option<Value>> {
    Ok(value.as_deref().map(serde_json::from_str).transpose()?)
}

fn sql_limit(limit: usize) -> Result<i64> {
    i64::try_from(limit).context("SQLite limit exceeds i64")
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc, sync::Barrier, thread};

    use serde_json::json;
    use tempfile::TempDir;

    use super::*;
    use yandalf_core::domain::TurnStatus;

    fn event(id: &str, subject: &str) -> Event {
        Event {
            id: id.into(),
            source: "files".into(),
            subject: subject.into(),
            title: None,
            text: format!("request {id}"),
            context: json!({"type": "fixture"}),
        }
    }

    fn fixture() -> (TempDir, Database) {
        let directory = TempDir::new().unwrap();
        let store = Database::open(&directory.path().join("state.sqlite")).unwrap();
        (directory, store)
    }

    fn result() -> TurnResult {
        TurnResult {
            status: TurnStatus::Completed,
            text: "complete".into(),
            usage: json!({"input": 12}),
        }
    }

    #[test]
    fn preparation_is_immutable_and_survives_pre_submission_recovery() {
        let (directory, store) = fixture();
        let job = store.enqueue(&event("prepare", "pr-1")).unwrap();
        let preparation = PreparedTask {
            session: yandalf_core::domain::SessionSpec {
                cwd: directory.path().join("worktree"),
                title: Some("Prepared task".into()),
                git: None,
            },
            input: yandalf_core::domain::TurnInput {
                text: "Pinned review".into(),
                context: json!({"sha":"abc"}),
            },
        };
        assert!(store.save_preparation(&job.id, &preparation).is_err());
        store.claim_next().unwrap().unwrap();
        store.save_preparation(&job.id, &preparation).unwrap();
        store.save_preparation(&job.id, &preparation).unwrap();
        let mut changed = preparation.clone();
        changed.session.cwd = directory.path().join("different");
        assert!(store.save_preparation(&job.id, &changed).is_err());
        drop(store);
        let reopened = Database::open(&directory.path().join("state.sqlite")).unwrap();
        assert!(reopened.recover().unwrap().is_empty());
        assert_eq!(reopened.get_job(&job.id).unwrap().status, JobStatus::Queued);
        assert_eq!(
            reopened.job_preparation(&job.id).unwrap(),
            Some(preparation)
        );
    }

    #[test]
    fn finalizing_preserves_result_and_subject_ownership_across_recovery() {
        let (directory, store) = fixture();
        let first = store.enqueue(&event("first", "pr-1")).unwrap();
        store.claim_next().unwrap().unwrap();
        store.mark_submitting(&first.id, "thread").unwrap();
        store.mark_running(&first.id, "turn").unwrap();
        let result = result();
        store.record_result(&first.id, &result).unwrap();
        store.record_result(&first.id, &result).unwrap();
        let mut different = result.clone();
        different.text = "Changed".into();
        assert!(store.record_result(&first.id, &different).is_err());
        store
            .finalization_failed(&first.id, "observer unavailable")
            .unwrap();
        let next = store.enqueue(&event("next", "pr-1")).unwrap();
        assert!(store.claim_next().unwrap().is_none());
        assert_eq!(store.inflight_count().unwrap(), 1);
        drop(store);
        let reopened = Database::open(&directory.path().join("state.sqlite")).unwrap();
        let recovered = reopened.recover().unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].status, JobStatus::Finalizing);
        assert_eq!(recovered[0].turn_id.as_deref(), Some("turn"));
        assert_eq!(
            reopened.job_result(&first.id).unwrap().unwrap().text,
            result.text
        );
        assert!(
            reopened
                .finish(&first.id, JobStatus::Succeeded, None, None)
                .is_err()
        );
        assert!(
            reopened
                .finish(&first.id, JobStatus::Succeeded, Some(&different), None)
                .is_err()
        );
        reopened
            .finish(&first.id, JobStatus::Succeeded, Some(&result), None)
            .unwrap();
        assert_eq!(reopened.claim_next().unwrap().unwrap().id, next.id);
    }

    #[test]
    fn only_requested_cancellation_can_override_a_completed_result_status() {
        let (_directory, store) = fixture();
        let job = store.enqueue(&event("completed", "pr-1")).unwrap();
        store.claim_next().unwrap();
        store.mark_submitting(&job.id, "thread").unwrap();
        store.mark_running(&job.id, "turn").unwrap();
        let result = result();
        store.record_result(&job.id, &result).unwrap();
        assert!(
            store
                .finish(
                    &job.id,
                    JobStatus::Cancelled,
                    Some(&result),
                    Some("completion processing cancelled")
                )
                .is_err()
        );
        assert!(
            store
                .finish(&job.id, JobStatus::Failed, Some(&result), None)
                .is_err()
        );
        store.request_cancel(&job.id).unwrap();
        store
            .finish(
                &job.id,
                JobStatus::Cancelled,
                Some(&result),
                Some("completion processing cancelled"),
            )
            .unwrap();
        assert_eq!(store.get_job(&job.id).unwrap().status, JobStatus::Cancelled);
        assert_eq!(
            store.job_result(&job.id).unwrap().unwrap().status,
            TurnStatus::Completed
        );
        assert_eq!(store.inflight_count().unwrap(), 0);
    }

    #[test]
    fn failed_result_checkpoint_does_not_advance_execution_state() {
        let (_directory, store) = fixture();
        let job = store.enqueue(&event("result", "pr-1")).unwrap();
        store.claim_next().unwrap().unwrap();
        store.mark_submitting(&job.id, "thread").unwrap();
        store.mark_running(&job.id, "turn").unwrap();
        store.connection().unwrap().execute_batch(
            "CREATE TRIGGER reject_checkpoint BEFORE UPDATE OF result_json ON yandalf_jobs WHEN NEW.result_json IS NOT NULL BEGIN SELECT RAISE(ABORT, 'checkpoint unavailable'); END;"
        ).unwrap();
        assert!(store.record_result(&job.id, &result()).is_err());
        assert_eq!(store.get_job(&job.id).unwrap().status, JobStatus::Running);
        assert!(store.job_result(&job.id).unwrap().is_none());
    }

    #[test]
    fn session_creation_intent_is_unique_and_survives_restart_before_binding() {
        let (directory, store) = fixture();
        let cwd = directory.path().join("initial-cwd");
        let (creation, fresh) = store.begin_session_creation("pr-1", &cwd).unwrap();
        assert!(fresh);
        let other = Database::open(&directory.path().join("state.sqlite")).unwrap();
        let (existing, fresh) = other
            .begin_session_creation("pr-1", Path::new("/different-cwd"))
            .unwrap();
        assert!(!fresh);
        assert_eq!(existing, creation);
        drop(other);
        drop(store);
        let restarted = Database::open(&directory.path().join("state.sqlite")).unwrap();
        assert_eq!(
            restarted.session_creation("pr-1").unwrap(),
            Some(creation.clone())
        );
        assert!(restarted.session("pr-1").unwrap().is_none());
        restarted
            .bind_session(&Session {
                subject: "pr-1".into(),
                cwd: creation.cwd,
                thread_id: "native-thread".into(),
            })
            .unwrap();
        assert_eq!(
            restarted.session("pr-1").unwrap().unwrap().thread_id,
            "native-thread"
        );
        assert_eq!(
            restarted.session_creation("pr-1").unwrap().unwrap().id,
            creation.id
        );
    }

    #[test]
    fn session_recovery_is_exclusive_and_cannot_reopen_a_submitted_turn() {
        let (_directory, store) = fixture();
        let first = store.enqueue(&event("one", "subject")).unwrap();
        store.claim_next().unwrap().unwrap();
        store
            .mark_uncertain(&first.id, "creation ack lost")
            .unwrap();
        store.resume_preparation(&first.id).unwrap();
        assert!(store.resume_preparation(&first.id).is_err());
        assert_eq!(
            store.get_job(&first.id).unwrap().status,
            JobStatus::Preparing
        );
        store.mark_submitting(&first.id, "native-thread").unwrap();
        store.mark_uncertain(&first.id, "turn ack lost").unwrap();
        assert!(store.resume_preparation(&first.id).is_err());
        let retained = store.get_job(&first.id).unwrap();
        assert_eq!(retained.status, JobStatus::Uncertain);
        assert_eq!(retained.thread_id.as_deref(), Some("native-thread"));
    }

    #[test]
    fn persists_events_and_deduplicates_by_source_and_id() {
        fn send_sync<T: Send + Sync>() {}
        send_sync::<Database>();
        let (directory, store) = fixture();
        let original = event("one", "subject");
        let first = store.enqueue(&original).unwrap();
        assert_eq!(store.enqueue(&original).unwrap().id, first.id);
        let mut changed = original.clone();
        changed.context = json!({"type": "different"});
        assert!(store.enqueue(&changed).is_err());
        changed = original.clone();
        changed.source = "another-source".into();
        assert_ne!(store.enqueue(&changed).unwrap().id, first.id);
        drop(store);

        let store = Database::open(&directory.path().join("state.sqlite")).unwrap();
        let persisted = store.enqueue(&original).unwrap();
        assert_eq!(persisted.id, first.id);
        assert_eq!(persisted.event, original);
        assert_eq!(persisted.created_at, first.created_at);
        assert_eq!(persisted.status, JobStatus::Queued);
        assert_eq!(store.list_jobs(10).unwrap().len(), 2);
        let connection = store.connection().unwrap();
        let journal: String = connection
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .unwrap();
        let version: i32 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(journal, "wal");
        assert_eq!(version, SCHEMA_VERSION);
    }

    #[tokio::test]
    async fn last_event_tracks_admission_per_source_and_subject_across_reopen() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("state.sqlite");
        let store = SqliteStore::open(&path).unwrap();
        assert!(store.last_event("files", "a").await.unwrap().is_none());

        let first = event("first", "a");
        let first_job = store.enqueue(&first).await.unwrap();
        store
            .finish(&first_job.id, JobStatus::Succeeded, Some(&result()), None)
            .await
            .unwrap();
        assert_eq!(
            store.last_event("files", "a").await.unwrap(),
            Some(first.clone())
        );

        let latest = event("latest", "a");
        let latest_job = store.enqueue(&latest).await.unwrap();
        let other_subject = event("other-subject", "b");
        store.enqueue(&other_subject).await.unwrap();
        let mut other_source = event("other-source", "a");
        other_source.source = "another-source".into();
        store.enqueue(&other_source).await.unwrap();
        store.enqueue(&first).await.unwrap();
        assert_eq!(
            store.last_event("files", "a").await.unwrap(),
            Some(latest.clone())
        );
        assert_eq!(
            store.last_event("files", "b").await.unwrap(),
            Some(other_subject)
        );
        assert_eq!(
            store.last_event("another-source", "a").await.unwrap(),
            Some(other_source)
        );
        assert!(
            store
                .last_event("another-source", "b")
                .await
                .unwrap()
                .is_none()
        );
        drop(store);

        let store = SqliteStore::open(&path).unwrap();
        assert_eq!(
            store.get_job(&latest_job.id).await.unwrap().status,
            JobStatus::Queued
        );
        assert_eq!(
            store.last_event("files", "a").await.unwrap(),
            Some(latest.clone())
        );
        store
            .finish(&latest_job.id, JobStatus::Succeeded, Some(&result()), None)
            .await
            .unwrap();
        assert_eq!(store.last_event("files", "a").await.unwrap(), Some(latest));
    }

    #[test]
    fn last_events_returns_each_subjects_latest_observation_including_skipped() {
        let (_directory, store) = fixture();
        let earlier = store.enqueue(&event("earlier", "a")).unwrap();
        store
            .finish(&earlier.id, JobStatus::Succeeded, Some(&result()), None)
            .unwrap();
        let skipped_event = event("ignored", "a");
        let skipped = store.enqueue(&skipped_event).unwrap();
        store
            .finish(&skipped.id, JobStatus::Skipped, None, Some("own activity"))
            .unwrap();
        let queued_event = event("queued", "b");
        store.enqueue(&queued_event).unwrap();
        let mut foreign_event = event("foreign", "a");
        foreign_event.source = "another-source".into();
        store.enqueue(&foreign_event).unwrap();

        assert_eq!(
            store.last_events("files").unwrap(),
            vec![skipped_event, queued_event]
        );
        assert_eq!(
            store.last_events("another-source").unwrap(),
            vec![foreign_event]
        );
        assert!(store.last_events("missing-source").unwrap().is_empty());
    }

    #[test]
    fn skipped_observation_survives_reopen_and_does_not_block_next_subject_event() {
        let (directory, store) = fixture();
        let observation = event("own-comment", "pull-request");
        let skipped = store.enqueue(&observation).unwrap();
        assert_eq!(store.claim_next().unwrap().unwrap().id, skipped.id);
        store
            .finish(
                &skipped.id,
                JobStatus::Skipped,
                None,
                Some("own comment does not require a turn"),
            )
            .unwrap();
        drop(store);

        let store = Database::open(&directory.path().join("state.sqlite")).unwrap();
        let persisted = store.get_job(&skipped.id).unwrap();
        assert_eq!(persisted.status, JobStatus::Skipped);
        assert!(persisted.status.is_terminal());
        assert_eq!(
            persisted.error.as_deref(),
            Some("own comment does not require a turn")
        );
        assert!(persisted.thread_id.is_none());
        assert!(store.job_result(&skipped.id).unwrap().is_none());
        assert_eq!(
            store.last_event("files", "pull-request").unwrap(),
            Some(observation)
        );
        assert_eq!(store.inflight_count().unwrap(), 0);
        assert!(store.recover().unwrap().is_empty());
        assert!(store.claim_next().unwrap().is_none());

        let next = store.enqueue(&event("new-push", "pull-request")).unwrap();
        assert_eq!(store.claim_next().unwrap().unwrap().id, next.id);
    }

    #[test]
    fn serializes_same_subject_across_sources_and_allows_other_subjects() {
        let (_directory, store) = fixture();
        let first = store.enqueue(&event("a1", "a")).unwrap();
        let second = store.enqueue(&event("a2", "a")).unwrap();
        let other_subject = store.enqueue(&event("b1", "b")).unwrap();
        let mut other = event("a3", "a");
        other.source = "another-source".into();
        let other_source = store.enqueue(&other).unwrap();

        assert_eq!(store.claim_next().unwrap().unwrap().id, first.id);
        assert_eq!(store.claim_next().unwrap().unwrap().id, other_subject.id);
        assert!(store.claim_next().unwrap().is_none());
        assert_eq!(store.inflight_count().unwrap(), 2);
        store.mark_submitting(&first.id, "thread-a").unwrap();
        store.mark_submitting(&first.id, "thread-a").unwrap();
        assert!(
            store
                .mark_submitting(&first.id, "different-thread")
                .is_err()
        );
        store.mark_running(&first.id, "turn-a").unwrap();
        store.mark_running(&first.id, "turn-a").unwrap();
        assert!(store.mark_running(&first.id, "different-turn").is_err());
        store
            .mark_uncertain(&first.id, "connection closed")
            .unwrap();
        assert_eq!(store.uncertain_jobs().unwrap()[0].id, first.id);
        assert_eq!(store.inflight_count().unwrap(), 2);
        assert!(store.claim_next().unwrap().is_none());

        store
            .finish(&first.id, JobStatus::Succeeded, Some(&result()), None)
            .unwrap();
        assert_eq!(store.claim_next().unwrap().unwrap().id, second.id);
        assert!(store.claim_next().unwrap().is_none());
        store
            .finish(&second.id, JobStatus::Succeeded, Some(&result()), None)
            .unwrap();
        assert_eq!(store.claim_next().unwrap().unwrap().id, other_source.id);
    }

    #[test]
    fn independent_connections_cannot_claim_a_subject_twice() {
        let (directory, store) = fixture();
        for subject in 0..12 {
            for ordinal in 0..2 {
                store
                    .enqueue(&event(
                        &format!("{subject}-{ordinal}"),
                        &format!("subject-{subject}"),
                    ))
                    .unwrap();
            }
        }
        let barrier = Arc::new(Barrier::new(3));
        let workers = (0..2)
            .map(|_| {
                let connection = Database::open(&directory.path().join("state.sqlite")).unwrap();
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier.wait();
                    let mut jobs = Vec::new();
                    while let Some(job) = connection.claim_next().unwrap() {
                        jobs.push(job);
                    }
                    jobs
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let claimed = workers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(claimed.len(), 12);
        assert_eq!(
            claimed
                .iter()
                .map(|job| &job.event.subject)
                .collect::<HashSet<_>>()
                .len(),
            12
        );
        assert_eq!(store.inflight_count().unwrap(), 12);
        assert_eq!(
            store
                .list_jobs(100)
                .unwrap()
                .iter()
                .filter(|job| job.status == JobStatus::Queued)
                .count(),
            12
        );
    }

    #[test]
    fn binds_a_session_once_and_preserves_it_across_reopen() {
        let (directory, store) = fixture();
        let binding = Session {
            subject: "a".into(),
            cwd: "/srv/yandalf".into(),
            thread_id: "thread-a".into(),
        };
        assert!(store.session(&binding.subject).unwrap().is_none());
        store.bind_session(&binding).unwrap();
        store.bind_session(&binding).unwrap();
        let mut different = binding.clone();
        different.cwd = "/other-cwd".into();
        assert!(store.bind_session(&different).is_err());
        different = binding.clone();
        different.thread_id = "thread-b".into();
        assert!(store.bind_session(&different).is_err());
        drop(store);

        let store = Database::open(&directory.path().join("state.sqlite")).unwrap();
        let saved = store.session("a").unwrap().unwrap();
        assert_eq!(saved.cwd, binding.cwd);
        assert_eq!(saved.thread_id, binding.thread_id);
    }

    #[test]
    fn terminal_result_is_idempotent_and_survives_reopen() {
        let (directory, store) = fixture();
        let job = store.enqueue(&event("done", "a")).unwrap();
        let result = result();
        store
            .finish(&job.id, JobStatus::Succeeded, Some(&result), None)
            .unwrap();
        drop(store);

        let store = Database::open(&directory.path().join("state.sqlite")).unwrap();
        store
            .finish(&job.id, JobStatus::Succeeded, Some(&result), None)
            .unwrap();
        let saved_result = store.job_result(&job.id).unwrap().unwrap();
        assert_eq!(saved_result.text, result.text);
        assert_eq!(saved_result.usage, result.usage);
        let mut different = result.clone();
        different.text = "different result".into();
        assert!(
            store
                .finish(&job.id, JobStatus::Succeeded, Some(&different), None)
                .is_err()
        );
        assert!(
            store
                .finish(&job.id, JobStatus::Succeeded, None, None)
                .is_err()
        );
        assert!(
            store
                .finish(&job.id, JobStatus::Failed, Some(&result), None)
                .is_err()
        );
        assert!(
            store
                .finish(
                    &job.id,
                    JobStatus::Succeeded,
                    Some(&result),
                    Some("different error")
                )
                .is_err()
        );
        assert_eq!(store.get_job(&job.id).unwrap().status, JobStatus::Succeeded);
        assert_eq!(
            store.job_result(&job.id).unwrap().unwrap().text,
            result.text
        );
    }

    #[test]
    fn failed_result_write_rolls_back_terminal_status() {
        let (_directory, store) = fixture();
        let job = store.enqueue(&event("atomic", "a")).unwrap();
        store.connection().unwrap().execute_batch(
            "CREATE TRIGGER yandalf_test_reject BEFORE UPDATE OF result_json ON yandalf_jobs WHEN NEW.result_json IS NOT NULL
             BEGIN SELECT RAISE(ABORT, 'fixture result failure'); END;",
        ).unwrap();
        assert!(
            store
                .finish(&job.id, JobStatus::Succeeded, Some(&result()), None)
                .is_err()
        );
        assert_eq!(store.get_job(&job.id).unwrap().status, JobStatus::Queued);
        assert!(store.job_result(&job.id).unwrap().is_none());
        store
            .connection()
            .unwrap()
            .execute_batch("DROP TRIGGER yandalf_test_reject")
            .unwrap();
        store
            .finish(&job.id, JobStatus::Succeeded, Some(&result()), None)
            .unwrap();
        assert_eq!(store.get_job(&job.id).unwrap().status, JobStatus::Succeeded);
        assert!(store.job_result(&job.id).unwrap().is_some());
    }

    #[test]
    fn cancellation_only_finishes_jobs_that_have_not_been_claimed() {
        let (_directory, store) = fixture();
        let queued = store.enqueue(&event("queued", "a")).unwrap();
        store.request_cancel(&queued.id).unwrap();
        store.request_cancel(&queued.id).unwrap();
        let cancelled = store.get_job(&queued.id).unwrap();
        assert_eq!(cancelled.status, JobStatus::Cancelled);
        assert!(cancelled.cancel_requested);
        assert!(store.claim_next().unwrap().is_none());

        let active = store.enqueue(&event("active", "a")).unwrap();
        store.claim_next().unwrap().unwrap();
        store.mark_submitting(&active.id, "thread-a").unwrap();
        store.mark_running(&active.id, "turn-a").unwrap();
        store.request_cancel(&active.id).unwrap();
        let cancelling = store.get_job(&active.id).unwrap();
        assert_eq!(cancelling.status, JobStatus::Running);
        assert!(cancelling.cancel_requested);
        assert_eq!(store.inflight_count().unwrap(), 1);
        store
            .finish(&active.id, JobStatus::Cancelled, None, None)
            .unwrap();
        store.request_cancel(&active.id).unwrap();
        assert_eq!(store.inflight_count().unwrap(), 0);
    }

    #[test]
    fn recovery_only_requeues_preparing_and_preserves_ambiguous_admissions() {
        let (directory, store) = fixture();
        let jobs = (0..4)
            .map(|index| {
                store
                    .enqueue(&event(&format!("job-{index}"), &format!("subject-{index}")))
                    .unwrap()
            })
            .collect::<Vec<_>>();
        for _ in &jobs {
            store.claim_next().unwrap().unwrap();
        }
        store.request_cancel(&jobs[0].id).unwrap();
        store.mark_submitting(&jobs[1].id, "thread-1").unwrap();
        store.mark_submitting(&jobs[2].id, "thread-2").unwrap();
        store.mark_running(&jobs[2].id, "turn-2").unwrap();
        store.request_cancel(&jobs[2].id).unwrap();
        store.mark_submitting(&jobs[3].id, "thread-3").unwrap();
        store
            .mark_uncertain(&jobs[3].id, "lost acknowledgement")
            .unwrap();
        drop(store);

        let store = Database::open(&directory.path().join("state.sqlite")).unwrap();
        let recovered = store.recover().unwrap();
        assert_eq!(
            recovered.iter().map(|job| job.status).collect::<Vec<_>>(),
            vec![
                JobStatus::Submitting,
                JobStatus::Running,
                JobStatus::Uncertain
            ]
        );
        assert_eq!(recovered[1].turn_id.as_deref(), Some("turn-2"));
        assert!(recovered[1].cancel_requested);
        assert_eq!(recovered[2].thread_id.as_deref(), Some("thread-3"));
        assert_eq!(store.inflight_count().unwrap(), 3);
        let preparing = store.get_job(&jobs[0].id).unwrap();
        assert_eq!(preparing.status, JobStatus::Queued);
        assert!(preparing.cancel_requested);
        assert_eq!(store.claim_next().unwrap().unwrap().id, jobs[0].id);
        store.mark_running(&jobs[2].id, "turn-2").unwrap();
        store.mark_running(&jobs[3].id, "turn-3").unwrap();
        assert!(store.uncertain_jobs().unwrap().is_empty());
    }

    #[test]
    fn source_cursor_advances_monotonically_and_survives_reopen() {
        let (directory, store) = fixture();
        assert!(store.advance_source_cursor("github:poll", 200).is_err());
        assert_eq!(store.source_cursor("github:poll", 100).unwrap(), 100);
        assert_eq!(store.source_cursor("github:start", 90).unwrap(), 90);
        store.advance_source_cursor("github:poll", 200).unwrap();
        store
            .connection()
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER reject_cursor_rewind BEFORE UPDATE ON yandalf_schedule_cursors
                 WHEN NEW.next_tick <= OLD.next_tick BEGIN SELECT RAISE(ABORT, 'cursor did not advance'); END;",
            )
            .unwrap();
        store.advance_source_cursor("github:poll", 200).unwrap();
        store.advance_source_cursor("github:poll", 150).unwrap();
        assert_eq!(store.source_cursor("github:poll", 999).unwrap(), 200);
        assert_eq!(store.source_cursor("github:start", 999).unwrap(), 90);
        drop(store);

        let store = Database::open(&directory.path().join("state.sqlite")).unwrap();
        assert_eq!(store.source_cursor("github:poll", 999).unwrap(), 200);
        store.advance_source_cursor("github:poll", 300).unwrap();
        assert_eq!(store.source_cursor("github:poll", 0).unwrap(), 300);
        assert_eq!(store.source_cursor("github:start", 0).unwrap(), 90);
    }

    #[test]
    fn scheduled_acceptance_is_atomic_and_replays_cannot_rewind_the_cursor() {
        let (directory, store) = fixture();
        assert_eq!(store.source_cursor("timer", 100).unwrap(), 100);
        assert_eq!(store.source_cursor("timer", 999).unwrap(), 100);
        let mut first = event("100", "a");
        first.source = "timer".into();
        let accepted = store.accept_scheduled(&first, 200).unwrap();
        assert_eq!(store.accept_scheduled(&first, 200).unwrap().id, accepted.id);
        let mut second = first.clone();
        second.id = "200".into();
        store.accept_scheduled(&second, 300).unwrap();
        store.accept_scheduled(&first, 200).unwrap();
        let mut conflict = first.clone();
        conflict.text = "different event".into();
        assert!(store.accept_scheduled(&conflict, 400).is_err());
        assert_eq!(store.source_cursor("timer", 0).unwrap(), 300);
        store
            .connection()
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER yandalf_test_cursor BEFORE UPDATE ON yandalf_schedule_cursors
             BEGIN SELECT RAISE(ABORT, 'fixture cursor failure'); END;",
            )
            .unwrap();
        second.id = "300".into();
        assert!(store.accept_scheduled(&second, 400).is_err());
        assert_eq!(store.list_jobs(10).unwrap().len(), 2);
        assert_eq!(store.source_cursor("timer", 0).unwrap(), 300);
        drop(store);

        let store = Database::open(&directory.path().join("state.sqlite")).unwrap();
        assert_eq!(store.source_cursor("timer", 0).unwrap(), 300);
        assert_eq!(store.get_job(&accepted.id).unwrap().event, first);
    }

    #[test]
    fn refuses_legacy_databases_without_changing_their_journal_or_data() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("legacy.sqlite");
        let legacy = Connection::open(&path).unwrap();
        legacy
            .execute_batch(
                "CREATE TABLE events (payload TEXT); INSERT INTO events VALUES ('retained');",
            )
            .unwrap();
        drop(legacy);

        assert!(Database::open(&path).is_err());
        let legacy = Connection::open(&path).unwrap();
        assert_eq!(
            legacy
                .query_row("SELECT payload FROM events", [], |row| row
                    .get::<_, String>(0))
                .unwrap(),
            "retained"
        );
        assert_eq!(
            legacy
                .pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))
                .unwrap(),
            "delete"
        );
        assert_eq!(
            legacy
                .pragma_query_value(None, "user_version", |row| row.get::<_, i32>(0))
                .unwrap(),
            0
        );
    }

    #[test]
    fn refuses_newer_schema_versions() {
        let (directory, store) = fixture();
        store
            .connection()
            .unwrap()
            .pragma_update(None, "user_version", SCHEMA_VERSION + 1)
            .unwrap();
        drop(store);
        assert!(Database::open(&directory.path().join("state.sqlite")).is_err());
    }

    #[tokio::test]
    async fn runner_lease_is_exclusive_for_aliases_and_releases_on_drop() {
        fn send_sync<T: Send + Sync>() {}
        send_sync::<SqliteStore>();
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("state.sqlite");
        let store = SqliteStore::open(&path).unwrap();
        std::fs::create_dir(directory.path().join("nested")).unwrap();
        let alias = SqliteStore::open(&directory.path().join("nested/../state.sqlite")).unwrap();
        let lease = store.acquire_runner().await.unwrap();
        assert!(store.acquire_runner().await.is_err());
        assert!(alias.acquire_runner().await.is_err());
        #[cfg(unix)]
        {
            let symlink = directory.path().join("alias.sqlite");
            std::os::unix::fs::symlink(&path, &symlink).unwrap();
            let linked = SqliteStore::open(&symlink).unwrap();
            assert!(linked.acquire_runner().await.is_err());
        }
        drop(lease);

        let next = alias.acquire_runner().await.unwrap();
        assert!(store.acquire_runner().await.is_err());
        drop(next);
        assert!(store.acquire_runner().await.is_ok());
    }

    #[tokio::test]
    async fn cancelled_database_future_keeps_runner_lease_until_commit() {
        use std::task::Poll;

        let directory = TempDir::new().unwrap();
        let path = directory.path().join("state.sqlite");
        let store = SqliteStore::open(&path).unwrap();
        let successor = SqliteStore::open(&path).unwrap();
        let lease = store.acquire_runner().await.unwrap();
        let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let database = store.database.clone();
        let holder = thread::spawn(move || {
            let _connection = database.connection().unwrap();
            locked_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        locked_rx.await.unwrap();
        let input = event("cancelled-await", "a");
        let mut pending = store.enqueue(&input);
        std::future::poll_fn(|context| {
            assert!(pending.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(pending);
        drop(lease);
        let blocked = successor.acquire_runner().await.is_err();
        release_tx.send(()).unwrap();
        tokio::task::spawn_blocking(move || holder.join().unwrap())
            .await
            .unwrap();
        assert!(
            blocked,
            "runner ownership was released before the SQLite operation settled"
        );

        let _next = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(lease) = successor.acquire_runner().await {
                    break lease;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let persisted = successor.list_jobs(10).await.unwrap();
        assert_eq!(persisted.len(), 1);
        assert_eq!(persisted[0].event, input);
        assert_eq!(successor.enqueue(&input).await.unwrap().id, persisted[0].id);
    }
}
