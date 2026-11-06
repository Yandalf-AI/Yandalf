#![cfg(unix)]

use std::{future::Future, path::Path, time::Duration};

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::{
    net::{UnixListener, UnixStream},
    task::JoinHandle,
};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};
use yandalf_codex::{Codex, CodexConfig};
use yandalf_core::{
    domain::{GitInfo, Inspection, SessionSpec, ThreadInspection, TurnInput, TurnStatus},
    executor::Executor,
};

type Socket = WebSocketStream<UnixStream>;

struct Server {
    config: CodexConfig,
    _directory: tempfile::TempDir,
    task: JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn receive(socket: &mut Socket) -> Value {
    let message = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}

async fn send(socket: &mut Socket, value: Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}

async fn reply(socket: &mut Socket, request: &Value, result: Value) {
    send(socket, json!({"id":request["id"], "result":result})).await;
}

async fn initialize(socket: &mut Socket) {
    let request = receive(socket).await;
    assert_eq!(request["method"], "initialize");
    reply(socket, &request, json!({})).await;
    assert_eq!(receive(socket).await["method"], "initialized");
}

async fn no_archived_threads(socket: &mut Socket) {
    let request = receive(socket).await;
    assert_eq!(request["method"], "thread/list");
    assert_eq!(request["params"]["archived"], true);
    reply(socket, &request, json!({"data":[]})).await;
}

async fn server<F, Fut>(script: F) -> Server
where
    F: FnOnce(Socket) -> Fut + Send + 'static,
    Fut: Future<Output = Socket> + Send,
{
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("codex.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let config = CodexConfig {
        endpoint: format!("unix://{}", path.display()),
        request_timeout_secs: 1,
        ..CodexConfig::default()
    };
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        initialize(&mut socket).await;
        let mut socket = script(socket).await;
        while socket.next().await.is_some() {}
    });
    Server {
        config,
        _directory: directory,
        task,
    }
}

#[tokio::test]
async fn creates_thread_with_cwd_and_durable_creation_marker() {
    let server = server(|mut socket| async move {
        let request = receive(&mut socket).await;
        assert_eq!(request["method"], "thread/start");
        assert_eq!(
            request["params"],
            json!({"cwd":"/tmp","threadSource":"yandalf:creation-1"})
        );
        reply(&mut socket, &request, json!({"thread":{"id":"thread-1"}})).await;
        socket
    })
    .await;
    let client = Codex::connect(&server.config).await.unwrap();
    assert_eq!(
        client
            .create_thread(Path::new("/tmp"), "creation-1")
            .await
            .unwrap(),
        "thread-1"
    );
}

#[tokio::test]
async fn sends_event_context_as_data_without_output_schema() {
    let server = server(|mut socket| async move {
        let request = receive(&mut socket).await;
        reply(&mut socket, &request, json!({"thread":{"id":"thread-1"}})).await;
        let request = receive(&mut socket).await;
        assert_eq!(request["params"], json!({
            "threadId":"thread-1", "clientUserMessageId":"job-1", "input":[
                {"type":"text","text":"Handle this event","text_elements":[]},
                {"type":"text","text":"Event context (data):\n{\"reply_to\":\"conversation-42\"}","text_elements":[]}
            ]
        }));
        reply(&mut socket, &request, json!({"turn":{"id":"turn-1"}})).await;
        socket
    }).await;
    let client = Codex::connect(&server.config).await.unwrap();
    let thread = client
        .create_thread(Path::new("/tmp"), "creation-1")
        .await
        .unwrap();
    client
        .start_turn(
            &thread,
            "job-1",
            &TurnInput {
                text: "Handle this event".into(),
                context: json!({"reply_to":"conversation-42"}),
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn preserves_completion_received_before_start_acknowledgment() {
    let server = server(|mut socket| async move {
        let request = receive(&mut socket).await;
        reply(&mut socket, &request, json!({"thread":{"id":"thread-1"}})).await;
        let request = receive(&mut socket).await;
        send(&mut socket, json!({"method":"item/completed","params":{"threadId":"thread-1","turnId":"turn-1","item":{"type":"agentMessage","id":"message","phase":"final_answer","text":"task result"}}})).await;
        send(&mut socket, json!({"method":"turn/completed","params":{"threadId":"thread-1","turn":{"id":"turn-1","status":"completed","items":[]}}})).await;
        reply(&mut socket, &request, json!({"turn":{"id":"turn-1"}})).await;
        socket
    }).await;
    let client = Codex::connect(&server.config).await.unwrap();
    let thread = client
        .create_thread(Path::new("/tmp"), "creation-1")
        .await
        .unwrap();
    let turn = client
        .start_turn(
            &thread,
            "job-1",
            &TurnInput {
                text: "task".into(),
                context: Value::Null,
            },
        )
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), client.wait_turn(&thread, &turn))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.status, TurnStatus::Completed);
    assert_eq!(result.text, "task result");
}

#[tokio::test]
async fn reconciles_using_client_id_across_history_pages() {
    let server = server(|mut socket| async move {
        no_archived_threads(&mut socket).await;
        let request = receive(&mut socket).await;
        assert_eq!(request["method"], "thread/resume");
        assert_eq!(request["params"], json!({"threadId":"thread-1","excludeTurns":true}));
        reply(&mut socket, &request, json!({"thread":{"id":"thread-1"}})).await;
        let request = receive(&mut socket).await;
        reply(&mut socket, &request, json!({"data":[{"id":"other","status":"completed","items":[]}],"nextCursor":"page-2"})).await;
        let request = receive(&mut socket).await;
        assert_eq!(request["params"]["cursor"], "page-2");
        reply(&mut socket, &request, json!({"data":[{"id":"turn-1","status":"completed","items":[{"type":"userMessage","clientId":"job-1"},{"type":"agentMessage","phase":"commentary","text":"looking"},{"type":"agentMessage","phase":"final_answer","text":"found"}]}]})).await;
        socket
    }).await;
    let client = Codex::connect(&server.config).await.unwrap();
    match client.inspect("thread-1", "job-1", None).await.unwrap() {
        Inspection::Finished { turn_id, result } => {
            assert_eq!(turn_id, "turn-1");
            assert_eq!(result.text, "found");
        }
        result => panic!("unexpected inspection: {result:?}"),
    }
}

#[tokio::test]
async fn duplicate_client_ids_are_ambiguous_instead_of_assumed_idempotent() {
    let server = server(|mut socket| async move {
        no_archived_threads(&mut socket).await;
        let request = receive(&mut socket).await;
        reply(&mut socket, &request, json!({})).await;
        let request = receive(&mut socket).await;
        reply(&mut socket, &request, json!({"data":[{"id":"turn-1","status":"completed","items":[{"type":"userMessage","clientId":"job-1"}]},{"id":"turn-2","status":"inProgress","items":[{"type":"userMessage","clientId":"job-1"}]}]})).await;
        socket
    }).await;
    let client = Codex::connect(&server.config).await.unwrap();
    assert!(matches!(
        client.inspect("thread-1", "job-1", None).await.unwrap(),
        Inspection::Ambiguous { .. }
    ));
}

#[tokio::test]
async fn connection_failure_releases_pending_requests() {
    let mut server = server(|mut socket| async move {
        receive(&mut socket).await;
        socket.close(None).await.unwrap();
        socket
    })
    .await;
    server.config.request_timeout_secs = 10;
    let client = Codex::connect(&server.config).await.unwrap();
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        client.create_thread(Path::new("/tmp"), "creation-1"),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(error.to_string().contains("closed"), "{error:#}");
    assert!(client.wait_closed().await.contains("closed"));
}

#[tokio::test]
async fn timeout_reports_unknown_request_outcome() {
    let server = server(|mut socket| async move {
        receive(&mut socket).await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        socket
    })
    .await;
    let client = Codex::connect(&server.config).await.unwrap();
    let error = client
        .create_thread(Path::new("/tmp"), "creation-1")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("outcome may be unknown"));
}

#[tokio::test]
async fn declines_approval_and_keeps_transport_available() {
    let server = server(|mut socket| async move {
        let request = receive(&mut socket).await;
        send(
            &mut socket,
            json!({"method":"item/commandExecution/requestApproval","id":"approval","params":{}}),
        )
        .await;
        let response = receive(&mut socket).await;
        assert_eq!(response["result"]["decision"], "decline");
        reply(&mut socket, &request, json!({"thread":{"id":"thread-1"}})).await;
        socket
    })
    .await;
    let client = Codex::connect(&server.config).await.unwrap();
    assert_eq!(
        client
            .create_thread(Path::new("/tmp"), "creation-1")
            .await
            .unwrap(),
        "thread-1"
    );
}

#[tokio::test]
async fn matches_out_of_order_responses_to_concurrent_requests() {
    let server = server(|mut socket| async move {
        let first = receive(&mut socket).await;
        let second = receive(&mut socket).await;
        reply(&mut socket, &second, json!({"thread":{"id":"second"}})).await;
        reply(&mut socket, &first, json!({"thread":{"id":"first"}})).await;
        socket
    })
    .await;
    let client = Codex::connect(&server.config).await.unwrap();
    let (first, second) = tokio::join!(
        client.create_thread(Path::new("/tmp"), "first"),
        client.create_thread(Path::new("/tmp"), "second")
    );
    assert_eq!(first.unwrap(), "first");
    assert_eq!(second.unwrap(), "second");
}

#[tokio::test]
async fn finds_loaded_empty_thread_and_deduplicates_listed_match() {
    let server = server(|mut socket| async move {
        let request = receive(&mut socket).await;
        assert_eq!(request["method"], "thread/loaded/list");
        reply(&mut socket, &request, json!({"data":["empty"]})).await;
        let request = receive(&mut socket).await;
        assert_eq!(request["method"], "thread/read");
        reply(
            &mut socket,
            &request,
            json!({"thread":{"id":"empty","threadSource":"yandalf:create-1"}}),
        )
        .await;
        let request = receive(&mut socket).await;
        assert_eq!(request["method"], "thread/list");
        assert_eq!(request["params"]["archived"], false);
        reply(
            &mut socket,
            &request,
            json!({"data":[{"id":"empty","threadSource":"yandalf:create-1"}],"nextCursor":"next"}),
        )
        .await;
        let request = receive(&mut socket).await;
        assert_eq!(request["params"]["cursor"], "next");
        reply(&mut socket, &request, json!({"data":[]})).await;
        let request = receive(&mut socket).await;
        assert_eq!(request["params"]["archived"], true);
        reply(&mut socket, &request, json!({"data":[]})).await;
        socket
    })
    .await;
    let client = Codex::connect(&server.config).await.unwrap();
    assert!(
        matches!(client.inspect_thread("create-1").await.unwrap(),ThreadInspection::Found { thread_id } if thread_id=="empty")
    );
}

#[tokio::test]
async fn ambiguous_creation_marker_never_creates_another_thread() {
    let server = server(|mut socket| async move {
        let request = receive(&mut socket).await;
        reply(&mut socket, &request, json!({"data":[]})).await;
        let request = receive(&mut socket).await;
        reply(&mut socket, &request, json!({"data":[{"id":"one","threadSource":"yandalf:create-1"},{"id":"two","threadSource":"yandalf:create-1"}]})).await;
        let request = receive(&mut socket).await;
        reply(&mut socket, &request, json!({"data":[]})).await;
        socket
    }).await;
    let client = Codex::connect(&server.config).await.unwrap();
    assert!(matches!(
        client.inspect_thread("create-1").await.unwrap(),
        ThreadInspection::Ambiguous { .. }
    ));
}

#[tokio::test]
async fn dropping_client_preserves_server_and_reconnects_to_active_turn() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("codex.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let config = CodexConfig {
        endpoint: format!("unix://{}", path.display()),
        request_timeout_secs: 2,
        ..CodexConfig::default()
    };
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        initialize(&mut socket).await;
        let request = receive(&mut socket).await;
        reply(&mut socket, &request, json!({"thread":{"id":"thread-1"}})).await;
        let request = receive(&mut socket).await;
        reply(&mut socket, &request, json!({"turn":{"id":"turn-1"}})).await;
        while let Some(message) = socket.next().await {
            if message.is_err() {
                break;
            }
        }
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        initialize(&mut socket).await;
        no_archived_threads(&mut socket).await;
        let request = receive(&mut socket).await;
        assert_eq!(request["method"], "thread/resume");
        reply(&mut socket, &request, json!({"thread":{"id":"thread-1"}})).await;
        let request = receive(&mut socket).await;
        assert_eq!(request["method"], "thread/turns/list");
        reply(&mut socket, &request, json!({"data":[{"id":"turn-1","status":"inProgress","items":[{"type":"userMessage","clientId":"job-1"}]}]})).await;
        send(&mut socket, json!({"method":"turn/completed","params":{"threadId":"thread-1","turn":{"id":"turn-1","status":"completed","items":[{"type":"agentMessage","phase":"final_answer","text":"continued"}]}}})).await;
        while socket.next().await.is_some() {}
    });
    let client = Codex::connect(&config).await.unwrap();
    let thread = client
        .create_thread(Path::new("/tmp"), "create-1")
        .await
        .unwrap();
    client
        .start_turn(
            &thread,
            "job-1",
            &TurnInput {
                text: "task".into(),
                context: Value::Null,
            },
        )
        .await
        .unwrap();
    drop(client);
    let client = Codex::connect(&config).await.unwrap();
    assert!(
        matches!(client.inspect(&thread,"job-1",Some("turn-1")).await.unwrap(),Inspection::Active { turn_id } if turn_id=="turn-1")
    );
    assert_eq!(
        client.wait_turn(&thread, "turn-1").await.unwrap().text,
        "continued"
    );
    drop(client);
    task.await.unwrap();
}

#[tokio::test]
async fn connects_to_an_external_tcp_websocket_server() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = CodexConfig {
        endpoint: format!("ws://{}/", listener.local_addr().unwrap()),
        request_timeout_secs: 2,
        ..CodexConfig::default()
    };
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let initialize: Value =
            serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        socket
            .send(Message::Text(
                json!({"id":initialize["id"],"result":{}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let initialized: Value =
            serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        assert_eq!(initialized["method"], "initialized");
        while socket.next().await.is_some() {}
    });
    let client = Codex::connect(&config).await.unwrap();
    drop(client);
    task.await.unwrap();
}

#[test]
fn rejects_launch_configuration_and_credentials_in_endpoint() {
    for endpoint in [
        "",
        "stdio://",
        "https://example.com",
        "unix://relative",
        "ws://user:secret@example.com",
    ] {
        assert!(
            CodexConfig {
                endpoint: endpoint.into(),
                ..CodexConfig::default()
            }
            .validate()
            .is_err()
        );
    }
    assert!(toml::from_str::<CodexConfig>("command = 'codex'\nargs = ['app-server']").is_err());
    assert!(
        CodexConfig {
            endpoint: "wss://example.com/codex".into(),
            ..CodexConfig::default()
        }
        .validate()
        .is_ok()
    );
}

#[tokio::test]
async fn confirms_prepared_metadata_and_reapplies_stale_git_without_starting_a_turn() {
    let server=server(|mut socket|async move {
        let old=json!({"thread":{"id":"existing-thread","cwd":"/old/worktree","status":{"type":"idle"}}});
        let new=json!({"thread":{"id":"existing-thread","cwd":"/prepared/worktree","status":{"type":"idle"},"name":"PR #42 — Correct title","gitInfo":{"sha":"a".repeat(40),"branch":"review/pr-42","originUrl":"https://github.com/owner/repo.git"}}});
        for (method,result) in [
            ("thread/read",old.clone()),
            ("thread/read",old.clone()),
            ("thread/list",json!({"data":[]})),
            ("thread/resume",old.clone()),
            ("thread/read",old),
            ("thread/settings/update",json!({})),
            ("thread/read",json!({"thread":{"id":"existing-thread","cwd":"/prepared/worktree","status":{"type":"idle"}}})),
            ("thread/name/set",json!({})),
            ("thread/metadata/update",json!({})),
            ("thread/read",json!({"thread":{"id":"existing-thread","cwd":"/prepared/worktree","status":{"type":"idle"},"name":"PR #42 — Correct title","gitInfo":{"sha":"b".repeat(40),"branch":"main"}}})),
            ("thread/metadata/update",json!({})),
            ("thread/read",new),
        ] {
            let request=receive(&mut socket).await;
            assert_eq!(request["method"],method);
            if method != "thread/list" {assert_eq!(request["params"]["threadId"],"existing-thread");}
            if method=="thread/resume" {assert_eq!(request["params"]["cwd"],"/prepared/worktree");}
            if method=="thread/metadata/update" {assert_eq!(request["params"]["gitInfo"]["branch"],"review/pr-42");}
            reply(&mut socket,&request,result).await;
        }
        socket
    }).await;
    let client = Codex::connect(&server.config).await.unwrap();
    assert_eq!(
        client.thread_cwd("existing-thread").await.unwrap(),
        Path::new("/old/worktree")
    );
    client
        .configure_thread(
            "existing-thread",
            &SessionSpec {
                cwd: "/prepared/worktree".into(),
                title: Some("PR #42 — Correct title".into()),
                git: Some(GitInfo {
                    sha: "a".repeat(40),
                    branch: Some("review/pr-42".into()),
                    origin_url: Some("https://github.com/owner/repo.git".into()),
                }),
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn external_active_turn_blocks_workspace_lookup_and_reconfiguration() {
    let server=server(|mut socket|async move {
        for method in ["thread/read","thread/read"] {
            let request=receive(&mut socket).await;
            assert_eq!(request["method"],method);
            reply(&mut socket,&request,json!({"thread":{"id":"external-thread","cwd":"/worktree","status":{"type":"active","activeFlags":[]}}})).await;
        }
        socket
    }).await;
    let client = Codex::connect(&server.config).await.unwrap();
    assert!(
        client
            .thread_cwd("external-thread")
            .await
            .unwrap_err()
            .to_string()
            .contains("active")
    );
    assert!(
        client
            .configure_thread(
                "external-thread",
                &SessionSpec {
                    cwd: "/other".into(),
                    title: None,
                    git: None
                }
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("active")
    );
}

#[tokio::test]
async fn resumes_a_missing_old_workspace_at_the_prepared_cwd_before_configuration() {
    let server = server(|mut socket| async move {
        let old = json!({"thread":{"id":"existing-thread","cwd":"/deleted/old-worktree","status":{"type":"notLoaded"}}});
        let ready = json!({"thread":{"id":"existing-thread","cwd":"/prepared/worktree","status":{"type":"idle"}}});
        let request = receive(&mut socket).await;
        assert_eq!(request["method"], "thread/read");
        reply(&mut socket, &request, old).await;
        no_archived_threads(&mut socket).await;
        let request = receive(&mut socket).await;
        assert_eq!(request["method"], "thread/resume");
        assert_eq!(request["params"], json!({"threadId":"existing-thread","excludeTurns":true,"cwd":"/prepared/worktree"}));
        reply(&mut socket, &request, ready.clone()).await;
        for _ in 0..2 {
            let request = receive(&mut socket).await;
            assert_eq!(request["method"], "thread/read");
            reply(&mut socket, &request, ready.clone()).await;
        }
        socket
    }).await;
    let client = Codex::connect(&server.config).await.unwrap();
    client
        .configure_thread(
            "existing-thread",
            &SessionSpec {
                cwd: "/prepared/worktree".into(),
                title: None,
                git: None,
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn newly_created_loaded_thread_is_configured_without_resuming() {
    let server = server(|mut socket| async move {
        let ready = json!({"thread":{"id":"new-thread","cwd":"/prepared/worktree","status":{"type":"idle"}}});
        let request = receive(&mut socket).await;
        assert_eq!(request["method"], "thread/start");
        reply(&mut socket, &request, ready.clone()).await;
        for _ in 0..2 {
            let request = receive(&mut socket).await;
            assert_eq!(request["method"], "thread/read");
            reply(&mut socket, &request, ready.clone()).await;
        }
        socket
    }).await;
    let client = Codex::connect(&server.config).await.unwrap();
    let thread = client
        .create_thread(Path::new("/prepared/worktree"), "creation-1")
        .await
        .unwrap();
    client
        .configure_thread(
            &thread,
            &SessionSpec {
                cwd: "/prepared/worktree".into(),
                title: None,
                git: None,
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn archives_an_idle_thread_and_recognizes_it_on_a_later_archive_attempt() {
    let server = server(|mut socket| async move {
        no_archived_threads(&mut socket).await;
        let request = receive(&mut socket).await;
        assert_eq!(request["method"], "thread/read");
        reply(
            &mut socket,
            &request,
            json!({"thread":{"id":"thread-1","status":{"type":"notLoaded"}}}),
        )
        .await;
        let request = receive(&mut socket).await;
        assert_eq!(request["method"], "thread/archive");
        assert_eq!(request["params"], json!({"threadId":"thread-1"}));
        reply(&mut socket, &request, json!({})).await;
        for (cursor, result) in [
            (
                Value::Null,
                json!({"data":[{"id":"other"}],"nextCursor":"next"}),
            ),
            (json!("next"), json!({"data":[{"id":"thread-1"}]})),
        ] {
            let request = receive(&mut socket).await;
            assert_eq!(request["method"], "thread/list");
            assert_eq!(request["params"]["archived"], true);
            assert_eq!(request["params"]["cursor"], cursor);
            reply(&mut socket, &request, result).await;
        }
        socket
    })
    .await;
    let client = Codex::connect(&server.config).await.unwrap();
    client.archive_thread("thread-1").await.unwrap();
    client.archive_thread("thread-1").await.unwrap();
}

#[tokio::test]
async fn does_not_archive_an_active_thread() {
    let server = server(|mut socket| async move {
        no_archived_threads(&mut socket).await;
        let request = receive(&mut socket).await;
        assert_eq!(request["method"], "thread/read");
        reply(
            &mut socket,
            &request,
            json!({"thread":{"id":"thread-1","status":{"type":"active","activeFlags":[]}}}),
        )
        .await;
        socket
    })
    .await;
    let client = Codex::connect(&server.config).await.unwrap();
    assert!(
        client
            .archive_thread("thread-1")
            .await
            .unwrap_err()
            .to_string()
            .contains("active")
    );
}

#[tokio::test]
async fn archive_retry_reconciles_a_lost_acknowledgment_without_repeating_the_mutation() {
    let server = server(|mut socket| async move {
        no_archived_threads(&mut socket).await;
        let request = receive(&mut socket).await;
        assert_eq!(request["method"], "thread/read");
        reply(
            &mut socket,
            &request,
            json!({"thread":{"id":"thread-1","status":{"type":"idle"}}}),
        )
        .await;
        let request = receive(&mut socket).await;
        assert_eq!(request["method"], "thread/archive");
        let request = receive(&mut socket).await;
        assert_eq!(request["method"], "thread/list");
        assert_eq!(request["params"]["archived"], true);
        reply(&mut socket, &request, json!({"data":[{"id":"thread-1"}]})).await;
        socket
    })
    .await;
    let client = Codex::connect(&server.config).await.unwrap();
    assert!(client.archive_thread("thread-1").await.is_err());
    client.archive_thread("thread-1").await.unwrap();
}

#[tokio::test]
async fn preparing_an_archived_session_restores_the_same_thread_even_after_archive_ack_loss() {
    for acknowledge_archive in [true, false] {
        let server = server(move |mut socket| async move {
        let ready = json!({"thread":{"id":"thread-1","cwd":"/worktree","status":{"type":"idle"}}});
        for (method, result) in [
            ("thread/start", ready.clone()),
            ("thread/list", json!({"data":[]})),
            ("thread/read", ready.clone()),
            ("thread/archive", json!({})),
            (
                "thread/read",
                json!({"thread":{"id":"thread-1","cwd":"/worktree","status":{"type":"notLoaded"}}}),
            ),
            ("thread/list", json!({"data":[{"id":"thread-1"}]})),
            ("thread/unarchive", ready.clone()),
            ("thread/resume", ready.clone()),
            ("thread/read", ready.clone()),
            ("thread/read", ready),
        ] {
            let request = receive(&mut socket).await;
            assert_eq!(request["method"], method);
            if method == "thread/list" {
                assert_eq!(request["params"]["archived"], true);
            } else if method != "thread/start" {
                assert_eq!(request["params"]["threadId"], "thread-1");
            }
            if method != "thread/archive" || acknowledge_archive {
                reply(&mut socket, &request, result).await;
            }
        }
        socket
    })
    .await;
        let client = Codex::connect(&server.config).await.unwrap();
        client
            .create_thread(Path::new("/worktree"), "creation-1")
            .await
            .unwrap();
        let archived = client.archive_thread("thread-1").await;
        assert_eq!(archived.is_ok(), acknowledge_archive);
        client
            .configure_thread(
                "thread-1",
                &SessionSpec {
                    cwd: "/worktree".into(),
                    title: None,
                    git: None,
                },
            )
            .await
            .unwrap();
    }
}
