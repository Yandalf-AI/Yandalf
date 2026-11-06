#![cfg(all(unix, feature = "sqlite"))]

use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant},
};

mod support;

use serde_json::{Value, json};
use support::FakeCodex;
use yandalf::{
    domain::{Event, Job, JobStatus, Session, TurnResult, TurnStatus},
    sqlite::SqliteStore,
    store::Store,
};

struct Service(Child);

impl Service {
    async fn stop(&mut self) -> ExitStatus {
        if let Some(status) = self.0.try_wait().unwrap() {
            return status;
        }
        assert!(
            Command::new("/bin/kill")
                .args(["-INT", &self.0.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "service did not stop after SIGINT"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn install_fake_github(bin: &Path) {
    let gh_path = bin.join("gh");
    fs::write(
        &gh_path,
        r#"#!/bin/sh
[ "$#" = 5 ] && [ "$1" = api ] && [ "$2" = --method ] && [ "$3" = GET ] && [ "$4" = --paginate ] || exit 1
printf '%s\n' "$5" >> "$FAKE_GH_CALLS"
if [ -n "$FAKE_GH_FAIL_ACTIVITY" ] && [ -f "$FAKE_GH_FAIL_ACTIVITY" ]; then
    case "$5" in
        */comments*|*/timeline*) exit 1 ;;
    esac
fi
case "$5" in
    user) printf '%s\n' '{"login":"review-bot"}' ;;
    'repos/ydb-platform/ydb-js-sdk/pulls?state=open&sort=created&direction=asc&per_page=100')
        /bin/cat "$FAKE_GH_SNAPSHOT" ;;
    'repos/ydb-platform/ydb-js-sdk/pulls/722')
        /bin/cat "$FAKE_GH_PULL" ;;
    'repos/ydb-platform/ydb-js-sdk/issues/comments?sort=updated&direction=asc&since='*)
        printf '%s\n' '[{"issue_url":"https://api.github.com/repos/ydb-platform/ydb-js-sdk/issues/722"}]' ;;
    'repos/ydb-platform/ydb-js-sdk/pulls/comments?sort=updated&direction=asc&since='*)
        printf '%s\n' '[{"pull_request_url":"https://api.github.com/repos/ydb-platform/ydb-js-sdk/pulls/722"}]' ;;
    'repos/ydb-platform/ydb-js-sdk/issues/722/timeline?per_page=100')
        /bin/cat "$FAKE_GH_TIMELINE" ;;
    'repos/ydb-platform/ydb-js-sdk/pulls/722/comments?per_page=100')
        printf '%s\n' '[]' ;;
    *) printf 'Unexpected endpoint: %s\n' "$5" >&2; exit 1 ;;
esac
"#,
    )
    .unwrap();
    fs::set_permissions(&gh_path, fs::Permissions::from_mode(0o755)).unwrap();
}

fn write_json(path: &Path, value: &Value) {
    let temporary = path.with_extension("next");
    let mut bytes = Vec::new();
    for page in value.as_array().unwrap() {
        serde_json::to_writer(&mut bytes, page).unwrap();
        bytes.push(b'\n');
    }
    fs::write(&temporary, bytes).unwrap();
    fs::rename(temporary, path).unwrap();
}

fn pull(base: &str, head: &str, updated_at: &str) -> Value {
    json!({
        "number":722, "html_url":"https://github.com/ydb-platform/ydb-js-sdk/pull/722",
        "title":"Standalone change", "body":null, "state":"open", "draft":false,
        "created_at":"2026-11-01T00:00:00Z", "updated_at":updated_at,
        "base":{"sha":base,"ref":"main","repo":{"url":"https://api.github.com/repos/ydb-platform/ydb-js-sdk"}},
        "head":{"sha":head,"ref":"standalone-topic","repo":null}
    })
}

fn comment(id: u64, login: &str, body: &str, timestamp: &str) -> Value {
    json!({
        "event":"commented", "id":id, "node_id":format!("comment-{id}"),
        "user":{"login":login,"type":"User"},
        "actor":{"login":login,"type":"User"},
        "body":body, "created_at":timestamp, "updated_at":timestamp,
        "html_url":format!("https://github.com/ydb-platform/ydb-js-sdk/pull/722#issuecomment-{id}")
    })
}

async fn await_job(
    service: &mut Service,
    store: &SqliteStore,
    log_path: &Path,
    status: JobStatus,
    excluded: &[&str],
) -> Job {
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        assert!(
            service.0.try_wait().unwrap().is_none(),
            "service exited: {}",
            fs::read_to_string(log_path).unwrap()
        );
        if let Some(job) = store
            .list_jobs(20)
            .await
            .unwrap()
            .into_iter()
            .find(|job| job.status == status && !excluded.contains(&job.id.as_str()))
        {
            return job;
        }
        assert!(
            Instant::now() < deadline,
            "expected {status:?} job: {}",
            fs::read_to_string(log_path).unwrap()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn repository_changes_trigger_turns_but_own_comments_and_unchanged_polls_do_not() {
    let directory = tempfile::tempdir().unwrap();
    let cwd = directory.path().join("service-cwd");
    let existing_cwd = directory.path().join("session-cwd");
    let bin = directory.path().join("bin");
    for path in [&cwd, &existing_cwd, &bin] {
        fs::create_dir(path).unwrap();
    }
    let database = directory.path().join("state.sqlite");
    let config_path = directory.path().join("yandalf.toml");
    let snapshot_path = directory.path().join("snapshot.json");
    let timeline_path = directory.path().join("timeline.json");
    let pull_path = directory.path().join("pull.json");
    let activity_failure = directory.path().join("activity-failed");
    let github_calls_path = directory.path().join("github-calls.log");
    let log_path = directory.path().join("service.log");
    let old_base = "a".repeat(40);
    let old_head = "b".repeat(40);
    let new_base = "c".repeat(40);
    let new_head = "d".repeat(40);
    let source = "github:ydb-platform/ydb-js-sdk";
    let subject = "github:ydb-platform/ydb-js-sdk/pr/722";
    let thread_id = "existing-thread";
    write_json(
        &snapshot_path,
        &json!([[pull(&new_base, &new_head, "2026-11-04T10:00:00Z")]]),
    );
    write_json(&timeline_path, &json!([[]]));
    install_fake_github(&bin);
    let server = FakeCodex::start_with_cwd(
        &directory.path().join("codex.sock"),
        thread_id,
        &existing_cwd,
        true,
    )
    .await;
    fs::write(&config_path, toml::to_string(&json!({"plugins":[
        {"type":"github","repository":"ydb-platform/ydb-js-sdk","text":"Use the configured review instructions.","interval":"50ms"},
        {"type":"runtime","cwd":cwd,"timeout_secs":3},
        {"type":"codex","endpoint":server.endpoint,"request_timeout_secs":2},
        {"type":"sqlite","path":database}
    ]})).unwrap()).unwrap();
    let store = SqliteStore::open(&database).unwrap();
    store
        .bind_session(&Session {
            subject: subject.into(),
            cwd: cwd.clone(),
            thread_id: thread_id.into(),
        })
        .await
        .unwrap();
    let seed = store
        .enqueue(&Event {
            id: "previous-revision".into(),
            source: source.into(),
            subject: subject.into(),
            title: None,
            text: "Previous event".into(),
            context: json!({"base_sha":old_base,"head_sha":old_head}),
        })
        .await
        .unwrap();
    store
        .finish(
            &seed.id,
            JobStatus::Succeeded,
            Some(&TurnResult {
                status: TurnStatus::Completed,
                text: "Previous result.".into(),
                usage: Value::Null,
            }),
            None,
        )
        .await
        .unwrap();
    assert_ne!(cwd, existing_cwd);

    let start_service = || {
        Service(
            Command::new(env!("CARGO_BIN_EXE_yandalf"))
                .env("YANDALF_CONFIG", &config_path)
                .env("PATH", &bin)
                .env("FAKE_GH_CALLS", &github_calls_path)
                .env("FAKE_GH_SNAPSHOT", &snapshot_path)
                .env("FAKE_GH_TIMELINE", &timeline_path)
                .env("FAKE_GH_PULL", &pull_path)
                .env("FAKE_GH_FAIL_ACTIVITY", &activity_failure)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(
                    fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&log_path)
                        .unwrap(),
                )
                .spawn()
                .unwrap(),
        )
    };
    let mut service = start_service();
    let updated = await_job(
        &mut service,
        &store,
        &log_path,
        JobStatus::Succeeded,
        &[&seed.id],
    )
    .await;
    assert_eq!(updated.thread_id.as_deref(), Some(thread_id));
    assert_eq!(updated.event.context["base_sha"], new_base);
    assert_eq!(updated.event.context["head_sha"], new_head);
    assert_eq!(
        updated.event.context["previous_revision"],
        json!({"base_sha":old_base,"head_sha":old_head})
    );

    let own_comment = comment(1, "review-bot", "Замечаний нет.", "2026-11-04T10:01:00Z");
    write_json(&timeline_path, &json!([[own_comment]]));
    let skipped = await_job(&mut service, &store, &log_path, JobStatus::Skipped, &[]).await;
    assert!(skipped.thread_id.is_none());
    assert!(skipped.turn_id.is_none());
    assert!(store.job_result(&skipped.id).await.unwrap().is_none());
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|request| request["method"] == "turn/start")
            .count(),
        1
    );

    let human_comment = comment(
        2,
        "developer",
        "Проверь обработку отмены.",
        "2026-11-04T10:02:00Z",
    );
    write_json(&timeline_path, &json!([[own_comment, human_comment]]));
    let response = await_job(
        &mut service,
        &store,
        &log_path,
        JobStatus::Succeeded,
        &[&seed.id, &updated.id],
    )
    .await;
    assert_eq!(response.thread_id.as_deref(), Some(thread_id));
    assert!(
        response
            .event
            .context
            .to_string()
            .contains("Проверь обработку отмены.")
    );

    let poll_count = || {
        fs::read_to_string(&github_calls_path)
            .unwrap_or_default()
            .lines()
            .filter(|endpoint| endpoint.contains("pulls?state=open"))
            .count()
    };
    for restart in [false, true] {
        if restart {
            service = start_service();
        }
        let target_polls = poll_count() + 3;
        let deadline = Instant::now() + Duration::from_secs(3);
        while poll_count() < target_polls {
            assert!(service.0.try_wait().unwrap().is_none());
            assert!(Instant::now() < deadline, "GitHub polling stopped");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            service.stop().await.success(),
            "service failed: {}",
            fs::read_to_string(&log_path).unwrap()
        );
    }

    assert_eq!(store.list_jobs(10).await.unwrap().len(), 4);
    let binding = store.session(subject).await.unwrap().unwrap();
    assert_eq!(binding.thread_id, thread_id);
    assert_eq!(binding.cwd, cwd);
    assert_eq!(server.cwd(), existing_cwd.to_string_lossy());
    for job in [&updated, &response] {
        assert_eq!(
            store
                .job_preparation(&job.id)
                .await
                .unwrap()
                .unwrap()
                .session
                .cwd,
            existing_cwd
        );
    }
    let last_event = store.last_event(source, subject).await.unwrap().unwrap();
    assert_eq!(last_event.id, response.event.id);
    assert!(server.is_running());
    let requests = server.requests();
    assert!(
        !requests
            .iter()
            .any(|request| request["method"] == "thread/start")
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request["method"] == "thread/resume")
            .count(),
        1
    );
    let titles: Vec<_> = requests
        .iter()
        .enumerate()
        .filter(|(_, request)| request["method"] == "thread/name/set")
        .collect();
    assert_eq!(titles.len(), 2);
    for ((title_index, title), expected) in titles.iter().zip([&updated.event, &response.event]) {
        assert_eq!(title["params"]["threadId"], thread_id);
        assert_eq!(title["params"]["name"], expected.title.as_deref().unwrap());
        assert!(expected.title.as_deref().unwrap().starts_with("PR #722 — "));
        let following_turn = requests
            .iter()
            .enumerate()
            .skip(title_index + 1)
            .find(|(_, request)| request["method"] == "turn/start")
            .unwrap();
        assert_eq!(
            following_turn.1["params"]["clientUserMessageId"],
            store
                .list_jobs(10)
                .await
                .unwrap()
                .into_iter()
                .find(|job| job.event.id == expected.id)
                .unwrap()
                .id
        );
        assert!(
            following_turn.1["params"]["input"][0]["text"]
                .as_str()
                .unwrap()
                .starts_with(expected.title.as_deref().unwrap())
        );
    }
    let turns: Vec<_> = requests
        .iter()
        .filter(|request| request["method"] == "turn/start")
        .collect();
    assert_eq!(turns.len(), 2);
    for (turn, expected) in turns.iter().zip([&updated.event, &response.event]) {
        assert_eq!(turn["params"]["threadId"], thread_id);
        let context_text = turn["params"]["input"][1]["text"]
            .as_str()
            .unwrap()
            .strip_prefix("Event context (data):\n")
            .unwrap();
        let context: Value = serde_json::from_str(context_text).unwrap();
        assert_eq!(context, expected.context);
    }
    assert!(
        !fs::read_to_string(&github_calls_path)
            .unwrap()
            .contains("/stacks/")
    );

    let mut merged = pull(&new_base, &new_head, "2026-11-04T11:00:00Z");
    merged["state"] = "closed".into();
    merged["merged"] = true.into();
    fs::write(&pull_path, serde_json::to_vec(&merged).unwrap()).unwrap();
    write_json(&snapshot_path, &json!([[]]));
    fs::write(&activity_failure, "").unwrap();
    service = start_service();
    let closed = await_job(
        &mut service,
        &store,
        &log_path,
        JobStatus::Succeeded,
        &[&seed.id, &updated.id, &response.id],
    )
    .await;
    assert_eq!(closed.event.context["pull"]["state"], "closed");
    assert!(closed.thread_id.is_none());
    assert!(server.is_archived());
    assert_eq!(
        store.session(subject).await.unwrap().unwrap().thread_id,
        thread_id
    );
    assert!(service.stop().await.success());

    service = start_service();
    let target_polls = poll_count() + 3;
    let deadline = Instant::now() + Duration::from_secs(3);
    while poll_count() < target_polls {
        assert!(service.0.try_wait().unwrap().is_none());
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(store.list_jobs(10).await.unwrap().len(), 5);
    let methods: Vec<_> = server
        .requests()
        .into_iter()
        .map(|request| request["method"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(
        methods
            .iter()
            .filter(|method| *method == "thread/archive")
            .count(),
        1
    );
    assert_eq!(
        methods
            .iter()
            .filter(|method| *method == "turn/start")
            .count(),
        2
    );
    assert!(server.is_archived());

    fs::remove_file(&activity_failure).unwrap();
    write_json(
        &snapshot_path,
        &json!([[pull(&new_base, &new_head, "2026-11-04T12:00:00Z")]]),
    );
    let reopened = await_job(
        &mut service,
        &store,
        &log_path,
        JobStatus::Succeeded,
        &[&seed.id, &updated.id, &response.id, &closed.id],
    )
    .await;
    assert_eq!(reopened.thread_id.as_deref(), Some(thread_id));
    assert!(!server.is_archived());
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|request| request["method"] == "thread/unarchive")
            .count(),
        1
    );
    assert!(service.stop().await.success());
}

#[tokio::test]
async fn github_intake_persists_while_codex_is_offline_then_executes_the_same_job() {
    let directory = tempfile::tempdir().unwrap();
    let cwd = directory.path().join("service-cwd");
    let native_cwd = directory.path().join("native-worktree");
    let bin = directory.path().join("bin");
    for path in [&cwd, &native_cwd, &bin] {
        fs::create_dir(path).unwrap();
    }
    install_fake_github(&bin);
    let database = directory.path().join("state.sqlite");
    let config_path = directory.path().join("yandalf.toml");
    let snapshot_path = directory.path().join("snapshot.json");
    let timeline_path = directory.path().join("timeline.json");
    let calls_path = directory.path().join("github-calls.log");
    let log_path = directory.path().join("service.log");
    let socket = directory.path().join("offline-codex.sock");
    let endpoint = format!("unix://{}", socket.display());
    let base = "a".repeat(40);
    let old_head = "b".repeat(40);
    let new_head = "c".repeat(40);
    write_json(
        &snapshot_path,
        &json!([[pull(&base, &new_head, "2026-11-04T10:00:00Z")]]),
    );
    write_json(&timeline_path, &json!([[]]));
    fs::write(
        &config_path,
        toml::to_string(&json!({"plugins":[
            {"type":"github","repository":"ydb-platform/ydb-js-sdk","interval":"50ms"},
            {"type":"runtime","cwd":cwd,"timeout_secs":3},
            {"type":"codex","endpoint":endpoint,"request_timeout_secs":1},
            {"type":"sqlite","path":database}
        ]}))
        .unwrap(),
    )
    .unwrap();
    let store = SqliteStore::open(&database).unwrap();
    let source = "github:ydb-platform/ydb-js-sdk";
    let subject = "github:ydb-platform/ydb-js-sdk/pr/722";
    let thread_id = "existing-offline-thread";
    store
        .bind_session(&Session {
            subject: subject.into(),
            cwd: cwd.clone(),
            thread_id: thread_id.into(),
        })
        .await
        .unwrap();
    let baseline = store
        .enqueue(&Event {
            id: "baseline".into(),
            source: source.into(),
            subject: subject.into(),
            title: None,
            text: "Prior revision".into(),
            context: json!({"base_sha":base,"head_sha":old_head}),
        })
        .await
        .unwrap();
    store
        .finish(
            &baseline.id,
            JobStatus::Succeeded,
            Some(&TurnResult {
                status: TurnStatus::Completed,
                text: "Prior result".into(),
                usage: Value::Null,
            }),
            None,
        )
        .await
        .unwrap();
    assert!(!socket.exists());
    let mut service = Service(
        Command::new(env!("CARGO_BIN_EXE_yandalf"))
            .env("YANDALF_CONFIG", &config_path)
            .env("PATH", &bin)
            .env("FAKE_GH_CALLS", &calls_path)
            .env("FAKE_GH_SNAPSHOT", &snapshot_path)
            .env("FAKE_GH_TIMELINE", &timeline_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(fs::File::create(&log_path).unwrap())
            .spawn()
            .unwrap(),
    );
    let queued = await_job(
        &mut service,
        &store,
        &log_path,
        JobStatus::Queued,
        &[&baseline.id],
    )
    .await;
    let poll_count = || {
        fs::read_to_string(&calls_path)
            .unwrap_or_default()
            .lines()
            .filter(|line| line.contains("pulls?state=open"))
            .count()
    };
    let target = poll_count() + 3;
    let deadline = Instant::now() + Duration::from_secs(3);
    while poll_count() < target {
        assert!(
            service.0.try_wait().unwrap().is_none(),
            "service stopped while executor was offline"
        );
        assert!(
            Instant::now() < deadline,
            "input stopped while executor was offline"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        store.get_job(&queued.id).await.unwrap().status,
        JobStatus::Queued
    );
    assert!(store.job_preparation(&queued.id).await.unwrap().is_none());
    assert_eq!(store.list_jobs(10).await.unwrap().len(), 2);
    assert!(!socket.exists());

    let server = FakeCodex::start_with_cwd(&socket, thread_id, &native_cwd, true).await;
    let completed = await_job(
        &mut service,
        &store,
        &log_path,
        JobStatus::Succeeded,
        &[&baseline.id],
    )
    .await;
    assert_eq!(completed.id, queued.id);
    assert_eq!(completed.thread_id.as_deref(), Some(thread_id));
    assert_eq!(
        store
            .job_preparation(&queued.id)
            .await
            .unwrap()
            .unwrap()
            .session
            .cwd,
        native_cwd
    );
    assert_eq!(server.cwd(), native_cwd.to_string_lossy());
    assert!(service.stop().await.success());
    assert!(server.is_running());
    let requests = server.requests();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request["method"] == "turn/start")
            .count(),
        1
    );
    assert!(!requests.iter().any(
        |request| request["method"] == "thread/start" || request["method"] == "turn/interrupt"
    ));
    assert_eq!(store.list_jobs(10).await.unwrap().len(), 2);
}
