use crate::config::Config;
use anyhow::{Context, Result, bail, ensure};
use std::{collections::HashSet, future::Future, sync::Arc, time::Duration};
use tokio::{
    task::JoinSet,
    time::{interval, timeout},
};
use tokio_util::sync::CancellationToken;
use yandalf_core::{
    decision::Decision,
    domain::{
        Inspection, Job, JobStatus, PreparedTask, Session, SessionCreation, SessionSpec,
        ThreadInspection, TurnInput, TurnResult, TurnStatus,
    },
    executor::Executor,
    plugin::Services,
    store::{RunnerLease, Store},
};

pub use yandalf_core::task::{SessionPreparing, TaskCompleted, TaskDeciding, TaskPreparing};

type WorkerResult = std::result::Result<(String, Result<()>), tokio::task::JoinError>;

struct Workers {
    tasks: JoinSet<(String, Result<()>)>,
    lease: Arc<dyn RunnerLease>,
}

impl Workers {
    fn new(lease: Box<dyn RunnerLease>) -> Self {
        Self {
            tasks: JoinSet::new(),
            lease: Arc::from(lease),
        }
    }

    fn spawn(&mut self, worker: impl Future<Output = (String, Result<()>)> + Send + 'static) {
        let lease = self.lease.clone();
        self.tasks.spawn(async move {
            // Aborting the runner cannot release ownership while a worker is still being polled.
            let _lease = lease;
            worker.await
        });
    }

    fn len(&self) -> usize {
        self.tasks.len()
    }

    fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    async fn join_next(&mut self) -> Option<WorkerResult> {
        self.tasks.join_next().await
    }
}

enum OperationOutcome<T> {
    Ready(T),
    Stopped,
    Cancelled,
}

enum PreparationRecovery {
    Pending,
    Ready { configured_thread: Option<String> },
}

pub struct Runtime {
    config: Arc<Config>,
    store: Arc<dyn Store>,
    executor: Arc<dyn Executor>,
    services: Services,
}

impl Runtime {
    pub fn new(
        config: Arc<Config>,
        store: Arc<dyn Store>,
        executor: Arc<dyn Executor>,
    ) -> Result<Self> {
        let mut config = (*config).clone();
        if config.cwd.is_relative() {
            config.resolve_paths(&std::env::current_dir().context("resolve harness cwd")?)?;
        } else {
            config.validate()?;
        }
        Ok(Self {
            config: Arc::new(config),
            store,
            executor,
            services: Services::default(),
        })
    }

    pub fn with_services(mut self, services: Services) -> Self {
        self.services = services;
        self
    }

    pub async fn run(self: Arc<Self>, stop: CancellationToken, once: bool) -> Result<()> {
        let _shutdown = stop.clone().drop_guard();
        let mut workers = Workers::new(self.store.acquire_runner().await?);
        let result = self.drive(&stop, once, &mut workers).await;
        stop.cancel();
        let mut failure = result.err();
        while let Some(result) = workers.join_next().await {
            if let Err(error) = worker_result(result) {
                failure.get_or_insert(error);
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
        if once {
            ensure!(
                self.store.uncertain_jobs().await?.is_empty(),
                "unconfirmed executor tasks remain; inspect jobs and run again to reconcile"
            );
            ensure!(
                self.store.finalizing_jobs().await?.is_empty(),
                "saved results still require completion processing"
            );
        }
        Ok(())
    }

    async fn drive(
        self: &Arc<Self>,
        stop: &CancellationToken,
        once: bool,
        workers: &mut Workers,
    ) -> Result<()> {
        let mut active = HashSet::new();
        for job in self.store.recover().await? {
            self.spawn_recovery(job, workers, &mut active, stop.clone());
        }
        let mut tick = interval(Duration::from_millis(200));
        let mut reconcile = interval(Duration::from_secs(5));
        loop {
            if stop.is_cancelled() {
                return Ok(());
            }
            self.claim_available(workers, &mut active, stop).await?;
            if once && workers.is_empty() {
                return Ok(());
            }
            tokio::select! {
                _ = stop.cancelled() => return Ok(()),
                _ = tick.tick() => {},
                _ = reconcile.tick(), if !once => {
                    let mut recoverable = self.store.uncertain_jobs().await?;
                    recoverable.extend(self.store.finalizing_jobs().await?);
                    for job in recoverable {
                        self.spawn_recovery(job, workers, &mut active, stop.clone());
                    }
                },
                result = workers.join_next(), if !workers.is_empty() => {
                    let id = worker_result(result.context("worker set became empty")?)?;
                    active.remove(&id);
                },
            }
        }
    }

    async fn claim_available(
        self: &Arc<Self>,
        workers: &mut Workers,
        active: &mut HashSet<String>,
        stop: &CancellationToken,
    ) -> Result<()> {
        while !stop.is_cancelled() && workers.len() < self.config.concurrency {
            if self.store.inflight_count().await? >= self.config.concurrency {
                break;
            }
            let Some(job) = self.store.claim_next().await? else {
                break;
            };
            active.insert(job.id.clone());
            let runtime = self.clone();
            let stop = stop.clone();
            workers.spawn(async move {
                let id = job.id.clone();
                (id, runtime.execute(job, stop, None).await)
            });
        }
        Ok(())
    }

    fn spawn_recovery(
        self: &Arc<Self>,
        job: Job,
        workers: &mut Workers,
        active: &mut HashSet<String>,
        stop: CancellationToken,
    ) {
        if stop.is_cancelled() || !active.insert(job.id.clone()) {
            return;
        }
        let runtime = self.clone();
        workers.spawn(async move {
            let id = job.id.clone();
            (id, runtime.reconcile(job, stop).await)
        });
    }

    async fn reconcile(&self, job: Job, stop: CancellationToken) -> Result<()> {
        if job.status == JobStatus::Finalizing {
            return self.finalize(&job, stop).await;
        }
        if job.status == JobStatus::Archiving {
            return self.archive(&job, &stop, true).await;
        }
        let Some(thread_id) = &job.thread_id else {
            if let PreparationRecovery::Ready { configured_thread } =
                self.reconcile_preparation(&job, &stop).await?
            {
                self.store.resume_preparation(&job.id).await?;
                self.execute(job, stop, configured_thread).await?;
            }
            return Ok(());
        };
        let Some(inspection) = self
            .reconciliation_request(
                &job,
                &stop,
                "executor turn inspection",
                self.executor
                    .inspect(thread_id, &job.id, job.turn_id.as_deref()),
            )
            .await?
        else {
            return Ok(());
        };
        match inspection {
            Inspection::Finished { turn_id, result } => {
                self.store.mark_running(&job.id, &turn_id).await?;
                self.complete(&job, &result, stop).await?;
            }
            Inspection::Active { turn_id } => {
                self.store.mark_running(&job.id, &turn_id).await?;
                self.monitor(&job, &turn_id, stop).await?;
            }
            Inspection::Missing => {
                self.store
                    .mark_uncertain(
                        &job.id,
                        "no confirmed executor turn; automatic resubmission is unsafe",
                    )
                    .await?;
            }
            Inspection::Ambiguous { reason } => {
                self.store.mark_uncertain(&job.id, &reason).await?;
            }
        }
        Ok(())
    }

    async fn reconcile_preparation(
        &self,
        job: &Job,
        stop: &CancellationToken,
    ) -> Result<PreparationRecovery> {
        let existing = self.store.session(&job.event.subject).await?;
        let mut configured_thread = None;
        if existing.is_none() {
            let Some(creation) = self.store.session_creation(&job.event.subject).await? else {
                self.store
                    .mark_uncertain(
                        &job.id,
                        "session creation has no persisted intent or binding",
                    )
                    .await?;
                return Ok(PreparationRecovery::Pending);
            };
            if self.identify_creation(job, stop, creation).await?.is_none() {
                return Ok(PreparationRecovery::Pending);
            }
        } else if let Some(prepared) = self.store.job_preparation(&job.id).await? {
            let session = existing.context("session disappeared")?;
            if self
                .reconciliation_request(
                    job,
                    stop,
                    "session configuration",
                    self.executor
                        .configure_thread(&session.thread_id, &prepared.session),
                )
                .await?
                .is_none()
            {
                return Ok(PreparationRecovery::Pending);
            }
            configured_thread = Some(session.thread_id);
        }
        if stop.is_cancelled() {
            return Ok(PreparationRecovery::Pending);
        }
        Ok(PreparationRecovery::Ready { configured_thread })
    }

    async fn execute(
        &self,
        job: Job,
        stop: CancellationToken,
        configured_thread: Option<String>,
    ) -> Result<()> {
        tracing::info!(job_id = %job.id, subject = %job.event.subject, "job started");
        if self.stop_before_submission(&job, &stop).await? {
            return Ok(());
        }
        let Some(decision) = self.decide(&job, &stop).await? else {
            return Ok(());
        };
        match decision {
            Decision::Run => {}
            Decision::Archive => return self.archive(&job, &stop, false).await,
            Decision::Skip { reason } => {
                self.store
                    .finish(&job.id, JobStatus::Skipped, None, Some(&reason))
                    .await?;
                tracing::info!(job_id = %job.id, reason, "event skipped");
                return Ok(());
            }
        }
        let Some(prepared) = self.prepare_task(&job, &stop).await? else {
            return Ok(());
        };
        let thread_id = if let Some(thread_id) = configured_thread {
            thread_id
        } else {
            let Some(thread_id) = self.prepare_session(&job, &prepared.session, &stop).await?
            else {
                return Ok(());
            };
            thread_id
        };
        self.submit(job, thread_id, &prepared.input, stop).await
    }

    async fn decide(&self, job: &Job, stop: &CancellationToken) -> Result<Option<Decision>> {
        let mut decision = TaskDeciding {
            job: job.clone(),
            decision: Decision::Run,
        };
        let Some(result) = self
            .before_submission(job, stop, self.services.dispatch(&mut decision))
            .await?
        else {
            return Ok(None);
        };
        if let Err(error) = result {
            self.fail_before_submission(job, "decision", &error).await?;
            return Ok(None);
        }
        Ok(Some(decision.decision))
    }

    async fn prepare_task(
        &self,
        job: &Job,
        stop: &CancellationToken,
    ) -> Result<Option<PreparedTask>> {
        if let Some(prepared) = self.store.job_preparation(&job.id).await? {
            return Ok(Some(prepared));
        }
        if self.stop_before_submission(job, stop).await? {
            return Ok(None);
        }
        let mut existing = self.store.session(&job.event.subject).await?;
        if let Some(session) = &mut existing {
            let Some(result) = self
                .before_submission(job, stop, self.executor.thread_cwd(&session.thread_id))
                .await?
            else {
                return Ok(None);
            };
            match result {
                Ok(cwd) => session.cwd = cwd,
                Err(error) => {
                    self.store
                        .mark_uncertain(&job.id, &format!("cannot inspect session cwd: {error:#}"))
                        .await?;
                    return Ok(None);
                }
            }
        }
        let mut preparation = TaskPreparing {
            job: job.clone(),
            task: PreparedTask {
                session: SessionSpec {
                    cwd: existing
                        .as_ref()
                        .map_or_else(|| self.config.cwd.clone(), |session| session.cwd.clone()),
                    title: job.event.title.clone(),
                    git: None,
                },
                input: TurnInput {
                    text: job.event.text.clone(),
                    context: job.event.context.clone(),
                },
            },
            existing_session: existing,
        };
        let Some(result) = self
            .before_submission(job, stop, self.prepare(&mut preparation))
            .await?
        else {
            return Ok(None);
        };
        if let Err(error) = result {
            self.fail_before_submission(job, "preparation", &error)
                .await?;
            return Ok(None);
        }
        self.store
            .save_preparation(&job.id, &preparation.task)
            .await?;
        Ok(Some(preparation.task))
    }

    async fn fail_before_submission(
        &self,
        job: &Job,
        phase: &str,
        error: &anyhow::Error,
    ) -> Result<()> {
        self.store
            .finish(
                &job.id,
                JobStatus::Failed,
                None,
                Some(&format!("{phase}: {error:#}")),
            )
            .await
    }

    async fn prepare_session(
        &self,
        job: &Job,
        spec: &SessionSpec,
        stop: &CancellationToken,
    ) -> Result<Option<String>> {
        if self.stop_before_submission(job, stop).await? {
            return Ok(None);
        }
        let thread_id = match self.session(job, spec, stop).await {
            Ok(Some(thread_id)) => thread_id,
            Ok(None) => return Ok(None),
            Err(error) => {
                self.store
                    .mark_uncertain(
                        &job.id,
                        &format!("session creation requires reconciliation: {error:#}"),
                    )
                    .await?;
                return Ok(None);
            }
        };
        if self.stop_before_submission(job, stop).await? {
            return Ok(None);
        }
        if self
            .lifecycle_request(
                job,
                stop,
                "session configuration",
                self.executor.configure_thread(&thread_id, spec),
            )
            .await?
            .is_none()
        {
            return Ok(None);
        }
        self.store.resume_preparation(&job.id).await?;
        Ok(Some(thread_id))
    }

    async fn submit(
        &self,
        mut job: Job,
        thread_id: String,
        input: &TurnInput,
        stop: CancellationToken,
    ) -> Result<()> {
        if self.stop_before_submission(&job, &stop).await? {
            return Ok(());
        }
        self.store.mark_submitting(&job.id, &thread_id).await?;
        let Some(turn_id) = self
            .lifecycle_request(
                &job,
                &stop,
                "turn submission",
                self.executor.start_turn(&thread_id, &job.id, input),
            )
            .await?
        else {
            return Ok(());
        };
        self.store.mark_running(&job.id, &turn_id).await?;
        job.thread_id = Some(thread_id);
        self.monitor(&job, &turn_id, stop).await
    }

    async fn archive(&self, job: &Job, stop: &CancellationToken, reconciling: bool) -> Result<()> {
        if stop.is_cancelled() || (!reconciling && self.stop_before_submission(job, stop).await?) {
            return Ok(());
        }
        self.store.mark_archiving(&job.id).await?;
        let session = match self.store.session(&job.event.subject).await? {
            Some(session) => Some(session),
            None => {
                if let Some(creation) = self.store.session_creation(&job.event.subject).await? {
                    self.identify_creation(job, stop, creation).await?
                } else {
                    None
                }
            }
        };
        if let Some(session) = session {
            let archived = if reconciling {
                self.reconciliation_request(
                    job,
                    stop,
                    "session archival",
                    self.executor.archive_thread(&session.thread_id),
                )
                .await?
            } else {
                self.lifecycle_request(
                    job,
                    stop,
                    "session archival",
                    self.executor.archive_thread(&session.thread_id),
                )
                .await?
            };
            if archived.is_none() {
                return Ok(());
            }
            tracing::info!(job_id = %job.id, thread_id = %session.thread_id, "session archived");
        } else if self
            .store
            .session_creation(&job.event.subject)
            .await?
            .is_some()
        {
            return Ok(());
        }
        let status = if self.store.get_job(&job.id).await?.cancel_requested {
            JobStatus::Cancelled
        } else {
            JobStatus::Succeeded
        };
        self.store.finish(&job.id, status, None, None).await
    }

    async fn identify_creation(
        &self,
        job: &Job,
        stop: &CancellationToken,
        creation: SessionCreation,
    ) -> Result<Option<Session>> {
        let Some(inspection) = self
            .reconciliation_request(
                job,
                stop,
                "session creation inspection",
                self.executor.inspect_thread(&creation.id),
            )
            .await?
        else {
            return Ok(None);
        };
        let thread_id = match inspection {
            ThreadInspection::Found { thread_id } => thread_id,
            ThreadInspection::Missing => {
                self.store
                    .mark_uncertain(&job.id, "no confirmed thread for persisted creation intent")
                    .await?;
                return Ok(None);
            }
            ThreadInspection::Ambiguous { reason } => {
                self.store.mark_uncertain(&job.id, &reason).await?;
                return Ok(None);
            }
        };
        let session = Session {
            subject: creation.subject,
            cwd: creation.cwd,
            thread_id,
        };
        self.store.bind_session(&session).await?;
        Ok(Some(session))
    }

    async fn wait_operation<T: Send>(
        &self,
        job: &Job,
        stop: &CancellationToken,
        operation: impl Future<Output = T> + Send,
    ) -> Result<OperationOutcome<T>> {
        tokio::pin!(operation);
        let mut cancel_poll = interval(Duration::from_millis(200));
        loop {
            tokio::select! {
                biased;
                _ = stop.cancelled() => return Ok(OperationOutcome::Stopped),
                _ = cancel_poll.tick() => {
                    if self.store.get_job(&job.id).await?.cancel_requested {
                        return Ok(OperationOutcome::Cancelled);
                    }
                }
                result = &mut operation => return Ok(OperationOutcome::Ready(result)),
            }
        }
    }

    async fn before_submission<T: Send>(
        &self,
        job: &Job,
        stop: &CancellationToken,
        operation: impl Future<Output = T> + Send,
    ) -> Result<Option<T>> {
        match self.wait_operation(job, stop, operation).await? {
            OperationOutcome::Ready(result) => Ok(Some(result)),
            OperationOutcome::Stopped => Ok(None),
            OperationOutcome::Cancelled => {
                self.store
                    .finish(&job.id, JobStatus::Cancelled, None, None)
                    .await?;
                Ok(None)
            }
        }
    }

    async fn lifecycle_request<T: Send>(
        &self,
        job: &Job,
        stop: &CancellationToken,
        phase: &str,
        operation: impl Future<Output = Result<T>> + Send,
    ) -> Result<Option<T>> {
        // A dropped executor request may still complete after its caller stops waiting.
        self.store
            .mark_uncertain(&job.id, &format!("{phase} request in progress"))
            .await?;
        let error = match self.wait_operation(job, stop, operation).await? {
            OperationOutcome::Ready(Ok(result)) => return Ok(Some(result)),
            OperationOutcome::Ready(Err(error)) => format!("{phase}: {error:#}"),
            OperationOutcome::Stopped => format!("{phase}: shutdown before outcome was confirmed"),
            OperationOutcome::Cancelled => {
                format!("{phase}: cancellation before outcome was confirmed")
            }
        };
        self.store.mark_uncertain(&job.id, &error).await?;
        tracing::warn!(job_id = %job.id, error, "executor request needs reconciliation");
        Ok(None)
    }

    async fn reconciliation_request<T: Send>(
        &self,
        job: &Job,
        stop: &CancellationToken,
        phase: &str,
        operation: impl Future<Output = Result<T>> + Send,
    ) -> Result<Option<T>> {
        let error = tokio::select! {
            biased;
            _ = stop.cancelled() => format!("{phase}: shutdown before outcome was confirmed"),
            result = operation => match result {
                Ok(result) => return Ok(Some(result)),
                Err(error) => format!("{phase}: {error:#}"),
            },
        };
        self.store.mark_uncertain(&job.id, &error).await?;
        tracing::warn!(job_id = %job.id, error, "executor reconciliation remains incomplete");
        Ok(None)
    }

    async fn stop_before_submission(&self, job: &Job, stop: &CancellationToken) -> Result<bool> {
        if stop.is_cancelled() {
            return Ok(true);
        }
        if self.store.get_job(&job.id).await?.cancel_requested {
            self.store
                .finish(&job.id, JobStatus::Cancelled, None, None)
                .await?;
            return Ok(true);
        }
        Ok(false)
    }

    async fn prepare(&self, preparation: &mut TaskPreparing) -> Result<()> {
        self.services.dispatch(preparation).await?;
        if preparation.existing_session.is_none() {
            let mut session = SessionPreparing {
                job: preparation.job.clone(),
                session: preparation.task.session.clone(),
            };
            self.services.dispatch(&mut session).await?;
            preparation.task.session = session.session;
        }
        ensure!(
            preparation.task.session.cwd.is_absolute(),
            "prepared cwd must be absolute"
        );
        ensure!(
            !preparation.task.input.text.trim().is_empty(),
            "prepared turn text must not be empty"
        );
        ensure!(
            preparation
                .task
                .session
                .title
                .as_ref()
                .is_none_or(|title| !title.trim().is_empty()),
            "prepared session title must not be empty"
        );
        Ok(())
    }

    async fn session(
        &self,
        job: &Job,
        spec: &SessionSpec,
        stop: &CancellationToken,
    ) -> Result<Option<String>> {
        if let Some(session) = self.store.session(&job.event.subject).await? {
            return Ok(Some(session.thread_id));
        }
        let (creation, fresh) = self
            .store
            .begin_session_creation(&job.event.subject, &spec.cwd)
            .await?;
        ensure!(
            creation.cwd == spec.cwd,
            "creation intent differs from prepared cwd"
        );
        let id = if fresh {
            let Some(id) = self
                .lifecycle_request(
                    job,
                    stop,
                    "session creation",
                    self.executor.create_thread(&creation.cwd, &creation.id),
                )
                .await?
            else {
                return Ok(None);
            };
            id
        } else {
            let Some(inspection) = self
                .reconciliation_request(
                    job,
                    stop,
                    "session creation inspection",
                    self.executor.inspect_thread(&creation.id),
                )
                .await?
            else {
                return Ok(None);
            };
            match inspection {
                ThreadInspection::Found { thread_id } => thread_id,
                ThreadInspection::Missing => bail!(
                    "no confirmed thread for persisted creation intent; automatic creation retry is unsafe"
                ),
                ThreadInspection::Ambiguous { reason } => bail!("{reason}"),
            }
        };
        self.store
            .bind_session(&Session {
                subject: creation.subject,
                cwd: creation.cwd,
                thread_id: id.clone(),
            })
            .await?;
        if fresh {
            self.store.resume_preparation(&job.id).await?;
        }
        Ok(Some(id))
    }

    async fn monitor(&self, job: &Job, turn_id: &str, stop: CancellationToken) -> Result<()> {
        let thread = job.thread_id.as_deref().context("missing thread id")?;
        let wait = self.executor.wait_turn(thread, turn_id);
        tokio::pin!(wait);
        let deadline = tokio::time::sleep(Duration::from_secs(self.config.timeout_secs));
        tokio::pin!(deadline);
        let mut cancel_poll = interval(Duration::from_millis(200));
        let reason = loop {
            tokio::select! {
                result = &mut wait => return self.receive_result(job, result, stop).await,
                _ = stop.cancelled() => return Ok(()),
                _ = &mut deadline => break "execution timeout",
                _ = cancel_poll.tick() => {
                    if self.store.get_job(&job.id).await?.cancel_requested {
                        break "cancellation requested";
                    }
                }
            }
        };
        self.interrupt_turn(job, turn_id, &mut wait, stop, reason)
            .await
    }

    async fn receive_result(
        &self,
        job: &Job,
        result: Result<TurnResult>,
        stop: CancellationToken,
    ) -> Result<()> {
        match result {
            Ok(result) => self.complete(job, &result, stop).await,
            Err(error) => {
                self.store
                    .mark_uncertain(&job.id, &format!("lost executor result: {error:#}"))
                    .await
            }
        }
    }

    async fn interrupt_turn(
        &self,
        job: &Job,
        turn_id: &str,
        wait: &mut (impl Future<Output = Result<TurnResult>> + Unpin),
        stop: CancellationToken,
        reason: &str,
    ) -> Result<()> {
        let thread = job.thread_id.as_deref().context("missing thread id")?;
        self.store.request_cancel(&job.id).await?;
        let interrupted = tokio::select! {
            biased;
            _ = stop.cancelled() => return Ok(()),
            result = &mut *wait => return self.receive_result(job, result, stop).await,
            result = timeout(Duration::from_secs(10), self.executor.interrupt(thread, turn_id)) => {
                result.context("interrupt acknowledgement timed out").and_then(|result| result)
            },
        };
        if let Err(error) = interrupted {
            self.store
                .mark_uncertain(&job.id, &format!("{reason}; interrupt failed: {error:#}"))
                .await?;
            return Ok(());
        }
        let result = tokio::select! {
            biased;
            _ = stop.cancelled() => return Ok(()),
            result = timeout(Duration::from_secs(10), wait) => {
                result.context("terminal state acknowledgement timed out").and_then(|result| result)
            },
        };
        match result {
            Ok(result) => self.complete(job, &result, stop).await,
            Err(error) => {
                self.store
                    .mark_uncertain(
                        &job.id,
                        &format!("{reason}; terminal state not confirmed: {error:#}"),
                    )
                    .await
            }
        }
    }

    async fn complete(
        &self,
        job: &Job,
        result: &TurnResult,
        stop: CancellationToken,
    ) -> Result<()> {
        self.store.record_result(&job.id, result).await?;
        self.finalize(job, stop).await
    }

    async fn cancel_completion(&self, job: &Job, result: &TurnResult) -> Result<()> {
        self.store
            .finish(
                &job.id,
                JobStatus::Cancelled,
                Some(result),
                Some("completion processing cancelled"),
            )
            .await?;
        tracing::info!(job_id = %job.id, "completion processing cancelled");
        Ok(())
    }

    async fn finalize(&self, job: &Job, stop: CancellationToken) -> Result<()> {
        let result = self
            .store
            .job_result(&job.id)
            .await?
            .context("finalizing job has no saved result")?;
        let event = TaskCompleted {
            job: self.store.get_job(&job.id).await?,
            result: result.clone(),
        };
        if event.job.cancel_requested {
            return self.cancel_completion(job, &result).await;
        }
        let observed = {
            let observers = self.services.notify(&event);
            tokio::pin!(observers);
            let mut cancel_poll = interval(Duration::from_millis(200));
            loop {
                tokio::select! {
                    _ = stop.cancelled() => return Ok(()),
                    result = &mut observers => break Some(result),
                    _ = cancel_poll.tick() => {
                        if self.store.get_job(&job.id).await?.cancel_requested {
                            break None;
                        }
                    }
                }
            }
        };
        let Some(observed) = observed else {
            return self.cancel_completion(job, &result).await;
        };
        if let Err(error) = observed {
            self.store
                .finalization_failed(&job.id, &format!("completion processing: {error:#}"))
                .await?;
            tracing::warn!(job_id = %job.id, error = %format!("{error:#}"), "saved result awaits completion processing");
            return Ok(());
        }
        let (status, error) = match result.status {
            TurnStatus::Completed => (JobStatus::Succeeded, None),
            TurnStatus::Failed => (JobStatus::Failed, Some("executor turn failed")),
            TurnStatus::Interrupted => (JobStatus::Cancelled, Some("executor turn interrupted")),
        };
        self.store
            .finish(&job.id, status, Some(&result), error)
            .await?;
        tracing::info!(job_id = %job.id, "job finished");
        Ok(())
    }
}

fn worker_result(result: WorkerResult) -> Result<String> {
    let (id, result) = result.context("task worker failed")?;
    result.with_context(|| format!("job {id}"))?;
    Ok(id)
}
