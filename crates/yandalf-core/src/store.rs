use anyhow::Result;
use async_trait::async_trait;

use crate::domain::{Event, Job, JobStatus, PreparedTask, Session, SessionCreation, TurnResult};

/// Holds exclusive execution ownership for one logical store until dropped.
/// Backends must prevent another owner from recovering or executing the same queue.
/// Ownership must not silently expire, and in-flight mutations must settle before it is released.
pub trait RunnerLease: Send + Sync {}

/// Persistent runtime state with read-after-write consistency.
/// Mutation errors or dropped futures do not imply rollback; callers must reconcile persisted state.
/// Execution and recovery require a runner lease; ingestion, inspection and cancellation may run concurrently.
#[async_trait]
pub trait Store: Send + Sync {
    async fn acquire_runner(&self) -> Result<Box<dyn RunnerLease>>;

    /// Commits before returning; a repeated (source, event ID) must have identical content.
    async fn enqueue(&self, event: &Event) -> Result<Job>;

    /// Returns the last admitted event for the exact source and subject, regardless of job status.
    async fn last_event(&self, source: &str, subject: &str) -> Result<Option<Event>>;

    /// Returns the last admitted event for each subject from the source, regardless of job status.
    async fn last_events(&self, source: &str) -> Result<Vec<Event>>;

    /// Atomically claims the oldest eligible job, excluding subjects with active or uncertain work.
    async fn claim_next(&self) -> Result<Option<Job>>;
    async fn get_job(&self, id: &str) -> Result<Job>;
    async fn list_jobs(&self, limit: usize) -> Result<Vec<Job>>;
    async fn job_result(&self, id: &str) -> Result<Option<TurnResult>>;
    async fn job_preparation(&self, id: &str) -> Result<Option<PreparedTask>>;
    /// Commits an immutable preparation before any session or turn request.
    async fn save_preparation(&self, id: &str, preparation: &PreparedTask) -> Result<()>;
    /// Saves the executor result and reserves the subject until completion observers finish.
    async fn record_result(&self, id: &str, result: &TurnResult) -> Result<()>;
    async fn finalizing_jobs(&self) -> Result<Vec<Job>>;
    async fn finalization_failed(&self, id: &str, error: &str) -> Result<()>;
    /// Includes preparing, submitting, running, uncertain and finalizing jobs.
    async fn inflight_count(&self) -> Result<usize>;
    async fn uncertain_jobs(&self) -> Result<Vec<Job>>;
    async fn session(&self, subject: &str) -> Result<Option<Session>>;
    async fn bind_session(&self, session: &Session) -> Result<()>;

    /// Commits a unique subject creation intent before the executor is called.
    /// The boolean is true only for the call that inserted the intent; existing intents must be inspected, never resubmitted.
    async fn begin_session_creation(
        &self,
        subject: &str,
        cwd: &std::path::Path,
    ) -> Result<(SessionCreation, bool)>;
    async fn session_creation(&self, subject: &str) -> Result<Option<SessionCreation>>;

    /// Reserves uncertain preparation or session lifecycle work before any turn submission has been recorded.
    async fn resume_preparation(&self, id: &str) -> Result<()>;
    async fn mark_submitting(&self, id: &str, thread_id: &str) -> Result<()>;
    async fn mark_running(&self, id: &str, turn_id: &str) -> Result<()>;
    async fn mark_uncertain(&self, id: &str, error: &str) -> Result<()>;

    /// Commits the terminal result atomically; identical retries are no-ops.
    async fn finish(
        &self,
        id: &str,
        status: JobStatus,
        result: Option<&TurnResult>,
        error: Option<&str>,
    ) -> Result<()>;
    async fn request_cancel(&self, id: &str) -> Result<()>;

    /// Requires execution ownership. Only pre-submission work may return to the queue.
    async fn recover(&self) -> Result<Vec<Job>>;
    async fn source_cursor(&self, source: &str, initial: i64) -> Result<i64>;

    /// Advances an initialized cursor monotonically after its input batch has been persisted.
    async fn advance_source_cursor(&self, source: &str, next: i64) -> Result<()>;

    /// Persists the event and advances the source cursor in one atomic operation.
    async fn accept_scheduled(&self, event: &Event, next_tick: i64) -> Result<Job>;
}
