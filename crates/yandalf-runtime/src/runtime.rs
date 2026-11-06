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
        Inspection, Job, JobStatus, PreparedTask, Session, SessionSpec, ThreadInspection,
        TurnInput, TurnResult, TurnStatus,
    },
    executor::Executor,
    plugin::Services,
    store::Store,
};

pub use yandalf_core::task::{SessionPreparing, TaskCompleted, TaskDeciding, TaskPreparing};

type Workers = JoinSet<(String, Result<()>)>;

enum PreparationOutcome<T> {
    Ready(T),
    Stopped,
    Cancelled,
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
        config.validate()?;
        Ok(Self {
            config,
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
        let _lease = self.store.acquire_runner().await?;
        let mut workers = Workers::new();
        let mut active = HashSet::new();
        for job in self.store.recover().await? {
            self.recover_job(job, &mut workers, &mut active, stop.clone())
                .await?;
        }
        let mut tick = interval(Duration::from_millis(200));
        let mut reconcile = interval(Duration::from_secs(5));
        let mut failure = None;
        loop {
            if stop.is_cancelled() {
                break;
            }
            while !stop.is_cancelled()
                && workers.len() < self.config.concurrency
                && self.store.inflight_count().await? < self.config.concurrency
            {
                match self.store.claim_next().await {
                    Ok(Some(job)) => {
                        active.insert(job.id.clone());
                        let runtime = self.clone();
                        let stop = stop.clone();
                        workers.spawn(async move {
                            let id = job.id.clone();
                            (id, runtime.execute(job, stop).await)
                        });
                    }
                    Ok(None) => break,
                    Err(error) => {
                        failure = Some(error);
                        break;
                    }
                }
            }
            if failure.is_some() || (once && workers.is_empty()) {
                break;
            }
            tokio::select! {
                _ = stop.cancelled() => break,
                _ = tick.tick() => {},
                _ = reconcile.tick(), if !once => {
                    let mut recoverable = self.store.uncertain_jobs().await?;
                    recoverable.extend(self.store.finalizing_jobs().await?);
                    for job in recoverable {
                        self.recover_job(job, &mut workers, &mut active, stop.clone()).await?;
                    }
                },
                result = workers.join_next(), if !workers.is_empty() => {
                    match result.unwrap() {
                        Ok((id, result)) => {
                            active.remove(&id);
                            if let Err(error) = result { failure = Some(error); break; }
                        }
                        Err(error) => { failure = Some(error.into()); break; }
                    }
                },
            }
        }
        stop.cancel();
        while let Some(result) = workers.join_next().await {
            match result {
                Ok((_, Ok(()))) => {}
                Ok((_, Err(error))) => {
                    failure.get_or_insert(error);
                }
                Err(error) => {
                    failure.get_or_insert(error.into());
                }
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

    async fn recover_job(
        self: &Arc<Self>,
        job: Job,
        workers: &mut Workers,
        active: &mut HashSet<String>,
        stop: CancellationToken,
    ) -> Result<()> {
        if stop.is_cancelled() || active.contains(&job.id) {
            return Ok(());
        }
        if job.status == JobStatus::Finalizing {
            active.insert(job.id.clone());
            let runtime = self.clone();
            workers.spawn(async move {
                let result = runtime.finalize(&job, stop).await;
                (job.id, result)
            });
            return Ok(());
        }
        let Some(thread_id) = &job.thread_id else {
            if self
                .store
                .session_creation(&job.event.subject)
                .await?
                .is_some()
                || self.store.session(&job.event.subject).await?.is_some()
            {
                self.store.resume_preparation(&job.id).await?;
                active.insert(job.id.clone());
                let runtime = self.clone();
                workers.spawn(async move {
                    let id = job.id.clone();
                    (id, runtime.execute(job, stop).await)
                });
            } else {
                self.store
                    .mark_uncertain(
                        &job.id,
                        "session creation has no persisted intent or binding",
                    )
                    .await?;
            }
            return Ok(());
        };
        match self
            .executor
            .inspect(thread_id, &job.id, job.turn_id.as_deref())
            .await
        {
            Ok(Inspection::Finished { turn_id, result }) => {
                self.store.mark_running(&job.id, &turn_id).await?;
                self.store.record_result(&job.id, &result).await?;
                active.insert(job.id.clone());
                let runtime = self.clone();
                workers.spawn(async move {
                    let result = runtime.finalize(&job, stop).await;
                    (job.id, result)
                });
            }
            Ok(Inspection::Active { turn_id }) => {
                self.store.mark_running(&job.id, &turn_id).await?;
                active.insert(job.id.clone());
                let runtime = self.clone();
                workers.spawn(async move {
                    let result = runtime.monitor(&job, &turn_id, stop).await;
                    (job.id, result)
                });
            }
            Ok(Inspection::Missing) => {
                self.store
                    .mark_uncertain(
                        &job.id,
                        "no confirmed executor turn; automatic resubmission is unsafe",
                    )
                    .await?
            }
            Ok(Inspection::Ambiguous { reason }) => {
                self.store.mark_uncertain(&job.id, &reason).await?
            }
            Err(error) => {
                self.store
                    .mark_uncertain(&job.id, &format!("cannot inspect executor turn: {error:#}"))
                    .await?
            }
        }
        Ok(())
    }

    async fn execute(&self, job: Job, stop: CancellationToken) -> Result<()> {
        tracing::info!(job_id = %job.id, subject = %job.event.subject, "job started");
        if self.stop_before_submission(&job, &stop).await? {
            return Ok(());
        }
        let mut decision = TaskDeciding {
            job: job.clone(),
            decision: Decision::Run,
        };
        let Some(result) = self
            .before_submission(&job, &stop, self.services.dispatch(&mut decision))
            .await?
        else {
            return Ok(());
        };
        if let Err(error) = result {
            self.store
                .finish(
                    &job.id,
                    JobStatus::Failed,
                    None,
                    Some(&format!("decision: {error:#}")),
                )
                .await?;
            return Ok(());
        }
        match decision.decision {
            Decision::Run => {}
            Decision::Archive => return self.archive(&job, &stop).await,
            Decision::Skip { reason } => {
                self.store
                    .finish(&job.id, JobStatus::Skipped, None, Some(&reason))
                    .await?;
                tracing::info!(job_id = %job.id, reason, "event skipped");
                return Ok(());
            }
        }
        let prepared = if let Some(prepared) = self.store.job_preparation(&job.id).await? {
            prepared
        } else {
            if self.stop_before_submission(&job, &stop).await? {
                return Ok(());
            }
            let mut existing = self.store.session(&job.event.subject).await?;
            if let Some(session) = &mut existing {
                match self.executor.thread_cwd(&session.thread_id).await {
                    Ok(cwd) => session.cwd = cwd,
                    Err(error) => {
                        self.store
                            .mark_uncertain(
                                &job.id,
                                &format!("cannot inspect session cwd: {error:#}"),
                            )
                            .await?;
                        return Ok(());
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
                .before_submission(&job, &stop, self.prepare(&mut preparation))
                .await?
            else {
                return Ok(());
            };
            if let Err(error) = result {
                self.store
                    .finish(
                        &job.id,
                        JobStatus::Failed,
                        None,
                        Some(&format!("preparation: {error:#}")),
                    )
                    .await?;
                return Ok(());
            }
            self.store
                .save_preparation(&job.id, &preparation.task)
                .await?;
            preparation.task
        };
        if self.stop_before_submission(&job, &stop).await? {
            return Ok(());
        }
        let thread_id = match self.session(&job, &prepared.session).await {
            Ok(id) => id,
            Err(error) => {
                self.store
                    .mark_uncertain(
                        &job.id,
                        &format!("session creation requires reconciliation: {error:#}"),
                    )
                    .await?;
                return Ok(());
            }
        };
        if self.stop_before_submission(&job, &stop).await? {
            return Ok(());
        }
        if let Err(error) = self
            .executor
            .configure_thread(&thread_id, &prepared.session)
            .await
        {
            self.store
                .mark_uncertain(&job.id, &format!("session configuration: {error:#}"))
                .await?;
            return Ok(());
        }
        if self.stop_before_submission(&job, &stop).await? {
            return Ok(());
        }
        self.store.mark_submitting(&job.id, &thread_id).await?;
        match self
            .executor
            .start_turn(&thread_id, &job.id, &prepared.input)
            .await
        {
            Ok(turn_id) => {
                self.store.mark_running(&job.id, &turn_id).await?;
                let mut job = job;
                job.thread_id = Some(thread_id);
                self.monitor(&job, &turn_id, stop).await
            }
            Err(error) => {
                self.store
                    .mark_uncertain(
                        &job.id,
                        &format!("submission acknowledgement lost or rejected: {error:#}"),
                    )
                    .await?;
                tracing::warn!(job_id = %job.id, "job needs reconciliation");
                Ok(())
            }
        }
    }

    async fn archive(&self, job: &Job, stop: &CancellationToken) -> Result<()> {
        if self.stop_before_submission(job, stop).await? {
            return Ok(());
        }
        let session = match self.store.session(&job.event.subject).await? {
            Some(session) => Some(session),
            None => {
                if let Some(creation) = self.store.session_creation(&job.event.subject).await? {
                    let Some(outcome) = self
                        .before_submission(job, stop, self.executor.inspect_thread(&creation.id))
                        .await?
                    else {
                        return Ok(());
                    };
                    let identified = outcome.and_then(|inspection| match inspection {
                        ThreadInspection::Found { thread_id } => Ok(thread_id),
                        ThreadInspection::Missing => {
                            bail!("no confirmed thread for persisted creation intent")
                        }
                        ThreadInspection::Ambiguous { reason } => bail!("{reason}"),
                    });
                    let thread_id = match identified {
                        Ok(thread_id) => thread_id,
                        Err(error) => {
                            self.store
                                .mark_uncertain(&job.id, &format!("session archival: {error:#}"))
                                .await?;
                            return Ok(());
                        }
                    };
                    let session = Session {
                        subject: creation.subject,
                        cwd: creation.cwd,
                        thread_id,
                    };
                    self.store.bind_session(&session).await?;
                    Some(session)
                } else {
                    None
                }
            }
        };
        if let Some(session) = session {
            let Some(outcome) = self
                .before_submission(job, stop, self.executor.archive_thread(&session.thread_id))
                .await?
            else {
                return Ok(());
            };
            if let Err(error) = outcome {
                self.store
                    .mark_uncertain(&job.id, &format!("session archival: {error:#}"))
                    .await?;
                return Ok(());
            }
            tracing::info!(job_id = %job.id, thread_id = %session.thread_id, "session archived");
        }
        self.store
            .finish(&job.id, JobStatus::Succeeded, None, None)
            .await
    }

    async fn before_submission<T: Send>(
        &self,
        job: &Job,
        stop: &CancellationToken,
        operation: impl Future<Output = T> + Send,
    ) -> Result<Option<T>> {
        let outcome = {
            let operation = operation;
            tokio::pin!(operation);
            let mut cancel_poll = interval(Duration::from_millis(200));
            loop {
                tokio::select! {
                    _ = stop.cancelled() => break PreparationOutcome::Stopped,
                    result = &mut operation => break PreparationOutcome::Ready(result),
                    _ = cancel_poll.tick() => {
                        if self.store.get_job(&job.id).await?.cancel_requested {
                            break PreparationOutcome::Cancelled;
                        }
                    }
                }
            }
        };
        match outcome {
            PreparationOutcome::Ready(result) => Ok(Some(result)),
            PreparationOutcome::Stopped => Ok(None),
            PreparationOutcome::Cancelled => {
                self.store
                    .finish(&job.id, JobStatus::Cancelled, None, None)
                    .await?;
                Ok(None)
            }
        }
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

    async fn session(&self, job: &Job, spec: &SessionSpec) -> Result<String> {
        if let Some(session) = self.store.session(&job.event.subject).await? {
            return Ok(session.thread_id);
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
            self.executor
                .create_thread(&creation.cwd, &creation.id)
                .await?
        } else {
            match self.executor.inspect_thread(&creation.id).await? {
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
        Ok(id)
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
                result = &mut wait => {
                    match result {
                        Ok(result) => self.complete(job, &result, stop).await?,
                        Err(error) => self.store.mark_uncertain(&job.id, &format!("lost executor result: {error:#}")).await?,
                    }
                    return Ok(());
                },
                _ = stop.cancelled() => return Ok(()),
                _ = &mut deadline => break "execution timeout",
                _ = cancel_poll.tick() => {
                    if self.store.get_job(&job.id).await?.cancel_requested { break "cancellation requested"; }
                }
            }
        };
        self.store.request_cancel(&job.id).await?;
        if let Err(error) = self.executor.interrupt(thread, turn_id).await {
            self.store
                .mark_uncertain(&job.id, &format!("{reason}; interrupt failed: {error:#}"))
                .await?;
            return Ok(());
        }
        match timeout(Duration::from_secs(10), &mut wait).await {
            Ok(Ok(result)) => self.complete(job, &result, stop).await?,
            _ => {
                self.store
                    .mark_uncertain(&job.id, &format!("{reason}; terminal state not confirmed"))
                    .await?
            }
        }
        Ok(())
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
                        if self.store.get_job(&job.id).await?.cancel_requested { break None; }
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
            tracing::warn!(job_id = %job.id, error = %error, "saved result awaits completion processing");
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
