use std::{
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
    store::RunnerLease,
};

use crate::{LeaseState, SqliteRunnerLease, files};

const APPLICATION_ID: i32 = 0x5941_4e44;
const SCHEMA_VERSION: i32 = 1;
const JOB_COLUMNS: &str =
    "id, event_json, status, thread_id, turn_id, cancel_requested, error, created_at, updated_at";

pub(super) struct Database {
    connection: Mutex<Connection>,
    runner_path: PathBuf,
    pub(super) active_lease: Mutex<Weak<LeaseState>>,
}

impl Database {
    pub(super) fn open(path: &Path) -> Result<Self> {
        files::prepare_database(path)?;
        let mut connection = Connection::open(path).context("open SQLite store")?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.pragma_update(None, "foreign_keys", true)?;
        initialize_schema(&mut connection)?;
        let path = std::fs::canonicalize(path).context("canonicalize SQLite database path")?;
        files::restrict_database(&path)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        let runner_path = files::with_suffix(&path, ".runner.lock");
        Ok(Self {
            connection: Mutex::new(connection),
            runner_path,
            active_lease: Mutex::new(Weak::new()),
        })
    }

    pub(super) fn acquire_runner(&self) -> Result<Box<dyn RunnerLease>> {
        let file = files::open_private(&self.runner_path).context("open SQLite runner lock")?;
        file.try_lock()
            .map_err(|error| anyhow!("cannot acquire exclusive SQLite runner lease: {error}"))?;
        let state = Arc::new(LeaseState { _file: file });
        *self
            .active_lease
            .lock()
            .map_err(|_| anyhow!("SQLite lease mutex poisoned"))? = Arc::downgrade(&state);
        Ok(Box::new(SqliteRunnerLease { _state: state }))
    }

    pub(super) fn connection(&self) -> Result<MutexGuard<'_, Connection>> {
        self.connection
            .lock()
            .map_err(|_| anyhow!("SQLite store mutex poisoned"))
    }

    pub(super) fn enqueue(&self, event: &Event) -> Result<Job> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let job = enqueue(&transaction, event)?;
        transaction.commit()?;
        Ok(job)
    }

    pub(super) fn last_event(&self, source: &str, subject: &str) -> Result<Option<Event>> {
        Ok(self
            .connection()?
            .query_row(
                "SELECT event_json FROM yandalf_jobs WHERE source = ?1 AND subject = ?2 ORDER BY seq DESC LIMIT 1",
                params![source, subject],
                |row| json_column(row, 0),
            )
            .optional()?)
    }

    pub(super) fn last_events(&self, source: &str) -> Result<Vec<Event>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT event_json FROM yandalf_jobs
             WHERE seq IN (SELECT max(seq) FROM yandalf_jobs WHERE source = ?1 GROUP BY subject)
             ORDER BY subject",
        )?;
        let events = statement.query_map([source], |row| json_column(row, 0))?;
        Ok(events.collect::<rusqlite::Result<Vec<Event>>>()?)
    }

    pub(super) fn claim_next(&self) -> Result<Option<Job>> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let id: Option<String> = transaction
            .query_row(
                "SELECT queued.id FROM yandalf_jobs AS queued
             WHERE queued.status = 'queued' AND NOT EXISTS (
                 SELECT 1 FROM yandalf_jobs AS active
                 WHERE active.subject = queued.subject
                   AND active.status IN ('preparing', 'submitting', 'running', 'uncertain', 'archiving', 'finalizing')
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

    pub(super) fn get_job(&self, id: &str) -> Result<Job> {
        read_job(&*self.connection()?, id)
    }

    pub(super) fn list_jobs(&self, limit: usize) -> Result<Vec<Job>> {
        query_jobs(
            &*self.connection()?,
            &format!("SELECT {JOB_COLUMNS} FROM yandalf_jobs ORDER BY seq DESC LIMIT ?1"),
            [sql_limit(limit)?],
        )
    }

    pub(super) fn job_result(&self, id: &str) -> Result<Option<TurnResult>> {
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

    pub(super) fn job_preparation(&self, id: &str) -> Result<Option<PreparedTask>> {
        Ok(self
            .connection()?
            .query_row(
                "SELECT preparation_json FROM yandalf_job_preparations WHERE job_id = ?1",
                [id],
                |row| json_column(row, 0),
            )
            .optional()?)
    }

    pub(super) fn save_preparation(&self, id: &str, preparation: &PreparedTask) -> Result<()> {
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

    pub(super) fn record_result(&self, id: &str, result: &TurnResult) -> Result<()> {
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
            transaction.execute(
                "UPDATE yandalf_jobs
                 SET status = 'finalizing', result_json = ?2, error = NULL, updated_at = ?3
                 WHERE id = ?1",
                params![id, serialized, now_ms()],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub(super) fn finalizing_jobs(&self) -> Result<Vec<Job>> {
        query_jobs(
            &*self.connection()?,
            &format!(
                "SELECT {JOB_COLUMNS} FROM yandalf_jobs WHERE status = 'finalizing' ORDER BY seq"
            ),
            [],
        )
    }

    pub(super) fn finalization_failed(&self, id: &str, error: &str) -> Result<()> {
        let changed = self.connection()?.execute(
            "UPDATE yandalf_jobs SET error = ?2, updated_at = ?3 WHERE id = ?1 AND status = 'finalizing'",
            params![id, error, now_ms()],
        )?;
        ensure!(changed == 1, "job is not finalizing");
        Ok(())
    }

    pub(super) fn inflight_count(&self) -> Result<usize> {
        let count: i64 = self.connection()?.query_row(
            "SELECT count(*) FROM yandalf_jobs
             WHERE status IN ('preparing', 'submitting', 'running', 'uncertain', 'archiving', 'finalizing')",
            [],
            |row| row.get(0),
        )?;
        Ok(usize::try_from(count)?)
    }

    pub(super) fn uncertain_jobs(&self) -> Result<Vec<Job>> {
        query_jobs(
            &*self.connection()?,
            &format!(
                "SELECT {JOB_COLUMNS} FROM yandalf_jobs WHERE status IN ('uncertain', 'archiving') ORDER BY seq"
            ),
            [],
        )
    }

    pub(super) fn session(&self, subject: &str) -> Result<Option<Session>> {
        read_session(&*self.connection()?, subject)
    }

    pub(super) fn bind_session(&self, session: &Session) -> Result<()> {
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

    pub(super) fn begin_session_creation(
        &self,
        subject: &str,
        cwd: &Path,
    ) -> Result<(SessionCreation, bool)> {
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

    pub(super) fn session_creation(&self, subject: &str) -> Result<Option<SessionCreation>> {
        read_session_creation(&*self.connection()?, subject)
    }

    pub(super) fn resume_preparation(&self, id: &str) -> Result<()> {
        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE yandalf_jobs SET status = 'preparing', error = NULL, updated_at = ?2
             WHERE id = ?1 AND status = 'uncertain' AND thread_id IS NULL AND turn_id IS NULL",
            params![id, now_ms()],
        )?;
        ensure!(changed == 1, "job cannot resume preparation");
        Ok(())
    }

    pub(super) fn mark_submitting(&self, id: &str, thread_id: &str) -> Result<()> {
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

    pub(super) fn mark_running(&self, id: &str, turn_id: &str) -> Result<()> {
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

    pub(super) fn mark_uncertain(&self, id: &str, error: &str) -> Result<()> {
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
                    | JobStatus::Archiving
            ),
            "job cannot become uncertain from {}",
            job.status.as_str()
        );
        transaction.execute(
            "UPDATE yandalf_jobs
             SET status = CASE WHEN status = 'archiving' THEN 'archiving' ELSE 'uncertain' END,
                 error = ?2, updated_at = ?3 WHERE id = ?1",
            params![id, error, now_ms()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(super) fn mark_archiving(&self, id: &str) -> Result<()> {
        let changed = self.connection()?.execute(
            "UPDATE yandalf_jobs SET status = 'archiving', error = NULL, updated_at = ?2
             WHERE id = ?1 AND status IN ('preparing', 'uncertain', 'archiving')
                 AND thread_id IS NULL AND turn_id IS NULL",
            params![id, now_ms()],
        )?;
        ensure!(
            changed == 1,
            "job cannot archive a session from its current state"
        );
        Ok(())
    }

    pub(super) fn finish(
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

    pub(super) fn request_cancel(&self, id: &str) -> Result<()> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let job = read_job(&transaction, id)?;
        if !job.status.is_terminal() {
            let status = if job.status == JobStatus::Queued {
                JobStatus::Cancelled
            } else {
                job.status
            };
            transaction.execute(
                "UPDATE yandalf_jobs SET cancel_requested = 1, status = ?2, updated_at = ?3
                 WHERE id = ?1",
                params![id, status.as_str(), now_ms()],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub(super) fn recover(&self) -> Result<Vec<Job>> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "UPDATE yandalf_jobs SET status = 'queued', updated_at = ?1 WHERE status = 'preparing'",
            [now_ms()],
        )?;
        let jobs = query_jobs(
            &transaction,
            &format!(
                "SELECT {JOB_COLUMNS} FROM yandalf_jobs
                 WHERE status IN ('submitting', 'running', 'uncertain', 'archiving', 'finalizing')
                 ORDER BY seq"
            ),
            [],
        )?;
        transaction.commit()?;
        Ok(jobs)
    }

    pub(super) fn source_cursor(&self, source: &str, initial: i64) -> Result<i64> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "INSERT INTO yandalf_schedule_cursors (source, next_tick) VALUES (?1, ?2)
             ON CONFLICT(source) DO NOTHING",
            params![source, initial],
        )?;
        let cursor = transaction.query_row(
            "SELECT next_tick FROM yandalf_schedule_cursors WHERE source = ?1",
            [source],
            |row| row.get(0),
        )?;
        transaction.commit()?;
        Ok(cursor)
    }

    pub(super) fn advance_source_cursor(&self, source: &str, next: i64) -> Result<()> {
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

    pub(super) fn accept_scheduled(&self, event: &Event, next_tick: i64) -> Result<Job> {
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
                status TEXT NOT NULL CHECK(status IN (
                    'queued','preparing','submitting','running','uncertain','archiving',
                    'finalizing','succeeded','skipped','failed','cancelled'
                )),
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
                WHERE status IN ('preparing','submitting','running','uncertain','archiving','finalizing');
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
        let tables: i64 = transaction.query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name IN (
                'yandalf_jobs','yandalf_sessions','yandalf_session_creations',
                'yandalf_schedule_cursors','yandalf_job_preparations'
             )",
            [],
            |row| row.get(0),
        )?;
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
mod tests;
