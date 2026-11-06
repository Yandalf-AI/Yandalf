use std::{collections::HashSet, sync::Arc, sync::Barrier, thread};

use serde_json::json;
use tempfile::TempDir;

use super::*;
use crate::SqliteStore;
use yandalf_core::domain::TurnStatus;
use yandalf_core::store::Store;

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

#[cfg(unix)]
#[test]
fn database_sidecars_and_runner_lock_are_private() {
    use std::os::unix::fs::PermissionsExt;

    let directory = TempDir::new().unwrap();
    let parent = directory.path().join("private/data");
    let path = parent.join("state.sqlite");
    let database = Database::open(&path).unwrap();
    database.enqueue(&event("private", "project")).unwrap();
    let _lease = database.acquire_runner().unwrap();

    for path in [&parent, parent.parent().unwrap()] {
        assert_eq!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o700
        );
    }
    for path in [
        &path,
        &files::with_suffix(&path, "-wal"),
        &files::with_suffix(&path, "-shm"),
        &database.runner_path,
    ] {
        assert_eq!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    for path in [
        &path,
        &files::with_suffix(&path, "-wal"),
        &files::with_suffix(&path, "-shm"),
    ] {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o644)).unwrap();
    }
    let reopened = Database::open(&path).unwrap();
    assert_eq!(reopened.list_jobs(10).unwrap().len(), 1);
    for path in [
        &path,
        &files::with_suffix(&path, "-wal"),
        &files::with_suffix(&path, "-shm"),
    ] {
        assert_eq!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}

#[test]
fn distinct_database_extensions_have_independent_runner_locks() {
    let directory = TempDir::new().unwrap();
    let first = Database::open(&directory.path().join("state.db")).unwrap();
    let second = Database::open(&directory.path().join("state.sqlite")).unwrap();
    let _first_lease = first.acquire_runner().unwrap();
    let _second_lease = second.acquire_runner().unwrap();
    assert_ne!(first.runner_path, second.runner_path);
}

#[test]
fn archival_retains_its_kind_and_subject_until_reconciled() {
    let (directory, database) = fixture();
    let archive = database.enqueue(&event("close", "project")).unwrap();
    let next = database.enqueue(&event("reopen", "project")).unwrap();
    database.claim_next().unwrap().unwrap();
    database.mark_archiving(&archive.id).unwrap();
    database.request_cancel(&archive.id).unwrap();
    database
        .mark_uncertain(&archive.id, "archive acknowledgement lost")
        .unwrap();
    assert_eq!(
        database.get_job(&archive.id).unwrap().status,
        JobStatus::Archiving
    );
    assert_eq!(database.inflight_count().unwrap(), 1);
    assert!(database.claim_next().unwrap().is_none());
    drop(database);

    let reopened = Database::open(&directory.path().join("state.sqlite")).unwrap();
    assert_eq!(reopened.recover().unwrap()[0].status, JobStatus::Archiving);
    assert_eq!(reopened.uncertain_jobs().unwrap()[0].id, archive.id);
    reopened
        .finish(&archive.id, JobStatus::Succeeded, None, None)
        .unwrap();
    assert_eq!(reopened.claim_next().unwrap().unwrap().id, next.id);
    reopened.mark_submitting(&next.id, "thread").unwrap();
    assert!(reopened.mark_archiving(&next.id).is_err());
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
