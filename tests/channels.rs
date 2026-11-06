#![cfg(all(unix, feature = "sqlite"))]

mod support;

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::Result;
use serde_json::json;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use yandalf::{
    channel::{Channel, ChannelPlugin, Inbox},
    codex::CodexConfig,
    config::Config,
    domain::{Event, Job, JobStatus},
    plugin::App,
    plugins::{CodexServer, Sqlite, TaskRunner},
    store::Store,
};

use support::FakeCodex;

struct Delivery {
    event: Event,
    accepted: oneshot::Sender<Job>,
}

struct Mailbox {
    source: &'static str,
    receiver: Mutex<mpsc::Receiver<Delivery>>,
    stopped: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Channel for Mailbox {
    async fn run(&self, inbox: Inbox, stop: CancellationToken) -> Result<()> {
        let mut receiver = self.receiver.lock().await;
        loop {
            let mut delivery = tokio::select! {
                _ = stop.cancelled() => break,
                delivery = receiver.recv() => match delivery {
                    Some(delivery) => delivery,
                    None => break,
                },
            };
            delivery.event.source = self.source.into();
            let job = inbox.accept(&delivery.event).await?;
            let _ = delivery.accepted.send(job);
        }
        self.stopped.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

async fn deliver(sender: &mpsc::Sender<Delivery>, number: usize) -> Job {
    let (accepted, admission) = oneshot::channel();
    sender
        .send(Delivery {
            event: Event {
                id: number.to_string(),
                source: String::new(),
                subject: "project:shared".into(),
                title: None,
                text: format!("Process message {number}"),
                context: json!({"number":number}),
            },
            accepted,
        })
        .await
        .unwrap();
    admission.await.unwrap()
}

async fn await_status(store: &dyn Store, id: &str, status: JobStatus) {
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if store.get_job(id).await.unwrap().status == status {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn independent_channels_keep_admitting_while_execution_is_busy_and_share_subject_sessions() {
    let directory = tempfile::tempdir().unwrap();
    let server =
        FakeCodex::start(&directory.path().join("codex.sock"), "shared-thread", false).await;
    let stopped = Arc::new(AtomicUsize::new(0));
    let (messages, message_input) = mpsc::channel(1);
    let (automation, automation_input) = mpsc::channel(1);
    let app = App::builder()
        .plugin(ChannelPlugin::new(
            "messages",
            Mailbox {
                source: "messages",
                receiver: Mutex::new(message_input),
                stopped: stopped.clone(),
            },
        ))
        .plugin(ChannelPlugin::new(
            "automation",
            Mailbox {
                source: "automation",
                receiver: Mutex::new(automation_input),
                stopped: stopped.clone(),
            },
        ))
        .plugin(TaskRunner(Config {
            cwd: directory.path().to_owned(),
            concurrency: 1,
            timeout_secs: 30,
        }))
        .plugin(CodexServer(CodexConfig {
            endpoint: server.endpoint.clone(),
            request_timeout_secs: 2,
            ..CodexConfig::default()
        }))
        .plugin(Sqlite(directory.path().join("state.sqlite")))
        .build()
        .await
        .unwrap();
    let store = app.services().get::<dyn Store>().unwrap();
    let stop = CancellationToken::new();
    let service = tokio::spawn(app.run(stop.clone()));

    let first = deliver(&messages, 0).await;
    await_status(store.as_ref(), &first.id, JobStatus::Running).await;
    let (mut message_jobs, automation_jobs) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(
            async {
                let mut jobs = Vec::new();
                for number in 1..5 {
                    jobs.push(deliver(&messages, number).await);
                }
                jobs
            },
            async {
                let mut jobs = Vec::new();
                for number in 0..5 {
                    jobs.push(deliver(&automation, number).await);
                }
                jobs
            },
        )
    })
    .await
    .unwrap();
    message_jobs.insert(0, first.clone());
    assert_ne!(message_jobs[0].id, automation_jobs[0].id);
    assert_eq!(deliver(&messages, 0).await.id, message_jobs[0].id);
    assert_eq!(deliver(&automation, 0).await.id, automation_jobs[0].id);
    assert_eq!(store.list_jobs(100).await.unwrap().len(), 10);
    assert_eq!(
        store.get_job(&first.id).await.unwrap().status,
        JobStatus::Running
    );
    for job in message_jobs.iter().skip(1).chain(&automation_jobs) {
        assert_eq!(
            store.get_job(&job.id).await.unwrap().status,
            JobStatus::Queued
        );
    }
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|request| request["method"] == "turn/start")
            .count(),
        1
    );

    server.finish();
    for job in message_jobs.iter().chain(&automation_jobs) {
        await_status(store.as_ref(), &job.id, JobStatus::Succeeded).await;
        assert_eq!(
            store.get_job(&job.id).await.unwrap().thread_id.as_deref(),
            Some("shared-thread")
        );
    }
    let session = store.session("project:shared").await.unwrap().unwrap();
    assert_eq!(session.thread_id, "shared-thread");
    let requests = server.requests();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request["method"] == "thread/start")
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request["method"] == "turn/start")
            .count(),
        10
    );
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(3), service)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(stopped.load(Ordering::SeqCst), 2);
    assert!(server.is_running());
}
