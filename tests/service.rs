#![cfg(all(unix, feature = "sqlite"))]

mod support;

use std::{
    fs,
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant},
};

use serde_json::json;
use support::FakeCodex;
use yandalf::{
    domain::{Event, JobStatus},
    sqlite::SqliteStore,
    store::Store,
};

struct Service(Child);

impl Service {
    async fn stop(&mut self) -> ExitStatus {
        if let Some(status) = self.0.try_wait().unwrap() {
            return status;
        }
        let status = Command::new("/bin/kill")
            .args(["-TERM", &self.0.id().to_string()])
            .status()
            .unwrap();
        assert!(status.success());
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "service did not stop after SIGTERM"
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

#[tokio::test]
async fn service_restart_reattaches_to_active_turn_without_stopping_the_server() {
    let directory = tempfile::tempdir().unwrap();
    let cwd = directory.path().join("cwd");
    let config_path = directory.path().join("yandalf.toml");
    let log = directory.path().join("service.log");
    fs::create_dir(&cwd).unwrap();
    let server = FakeCodex::start(&directory.path().join("codex.sock"), "task-thread", false).await;
    let database = directory.path().join("state.sqlite");
    let settings = json!({"plugins":[
        {"type":"runtime","cwd":cwd,"timeout_secs":20},
        {"type":"codex","endpoint":server.endpoint,"request_timeout_secs":2},
        {"type":"sqlite","path":database}
    ]});
    fs::write(&config_path, toml::to_string(&settings).unwrap()).unwrap();
    let store = SqliteStore::open(&database).unwrap();
    let event = Event {
        id: "event-1".into(),
        source: "test".into(),
        subject: "task-1".into(),
        title: None,
        text: "Perform the task.".into(),
        context: json!({"reply_to": "thread-42"}),
    };
    let job = store.enqueue(&event).await.unwrap();
    let start_service = || {
        Service(
            Command::new(env!("CARGO_BIN_EXE_yandalf"))
                .env("YANDALF_CONFIG", &config_path)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(
                    fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&log)
                        .unwrap(),
                )
                .spawn()
                .unwrap(),
        )
    };
    let mut service = start_service();
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        assert!(
            service.0.try_wait().unwrap().is_none(),
            "service exited: {}",
            fs::read_to_string(&log).unwrap()
        );
        if store.get_job(&job.id).await.unwrap().status == JobStatus::Running {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "event did not start: {}",
            fs::read_to_string(&log).unwrap()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let original = store.get_job(&job.id).await.unwrap();
    assert!(
        service.stop().await.success(),
        "service failed: {}",
        fs::read_to_string(&log).unwrap()
    );
    assert!(server.is_running());
    let stopped = store.get_job(&job.id).await.unwrap();
    assert!(matches!(
        stopped.status,
        JobStatus::Running | JobStatus::Uncertain
    ));
    assert!(!stopped.cancel_requested);
    assert_eq!(stopped.thread_id, original.thread_id);
    assert_eq!(stopped.turn_id, original.turn_id);

    service = start_service();
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        assert!(
            service.0.try_wait().unwrap().is_none(),
            "restarted service exited: {}",
            fs::read_to_string(&log).unwrap()
        );
        if server
            .requests()
            .iter()
            .any(|request| request["method"] == "thread/turns/list")
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "active turn was not inspected after restart: {}",
            fs::read_to_string(&log).unwrap()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    server.finish();
    loop {
        assert!(
            service.0.try_wait().unwrap().is_none(),
            "restarted service exited: {}",
            fs::read_to_string(&log).unwrap()
        );
        if store.get_job(&job.id).await.unwrap().status == JobStatus::Succeeded {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "event did not complete after reconnect: {}",
            fs::read_to_string(&log).unwrap()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(service.stop().await.success());
    assert!(server.is_running());
    assert_eq!(store.enqueue(&event).await.unwrap().id, job.id);
    assert_eq!(
        store.job_result(&job.id).await.unwrap().unwrap().text,
        "Task result."
    );
    assert_eq!(store.list_jobs(10).await.unwrap().len(), 1);
    let completed = store.get_job(&job.id).await.unwrap();
    assert_eq!(completed.thread_id, original.thread_id);
    assert_eq!(completed.turn_id, original.turn_id);
    let requests = server.requests();
    for (method, expected) in [
        ("thread/start", 1),
        ("turn/start", 1),
        ("turn/interrupt", 0),
        ("initialize", 2),
    ] {
        assert_eq!(
            requests
                .iter()
                .filter(|request| request["method"] == method)
                .count(),
            expected,
            "{method}"
        );
    }
}
