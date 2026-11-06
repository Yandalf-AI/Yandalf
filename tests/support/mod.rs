#![allow(dead_code)]

use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::{
    net::{UnixListener, UnixStream},
    sync::watch,
    task::{JoinHandle, JoinSet},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

pub struct FakeCodex {
    pub endpoint: String,
    pub requests: Arc<Mutex<Vec<Value>>>,
    complete: watch::Sender<bool>,
    state: Arc<Mutex<State>>,
    task: JoinHandle<()>,
}

struct State {
    thread_id: String,
    thread_source: Option<String>,
    cwd: String,
    name: Option<String>,
    git: Value,
    archived: bool,
    turns: Vec<Value>,
}

impl FakeCodex {
    pub async fn start(path: &Path, thread_id: &str, complete_immediately: bool) -> Self {
        Self::start_with_cwd(path, thread_id, Path::new("/"), complete_immediately).await
    }

    pub async fn start_with_cwd(
        path: &Path,
        thread_id: &str,
        cwd: &Path,
        complete_immediately: bool,
    ) -> Self {
        let listener = UnixListener::bind(path).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let state = Arc::new(Mutex::new(State {
            thread_id: thread_id.into(),
            thread_source: None,
            cwd: cwd.to_string_lossy().into_owned(),
            name: None,
            git: Value::Null,
            archived: false,
            turns: Vec::new(),
        }));
        let (complete, finished) = watch::channel(complete_immediately);
        let connection_requests = requests.clone();
        let connection_state = state.clone();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.unwrap();
                        connections.spawn(connection(stream, connection_requests.clone(), connection_state.clone(), finished.clone()));
                    }
                    result = connections.join_next(), if !connections.is_empty() => { result.unwrap().unwrap(); }
                }
            }
        });
        Self {
            endpoint: format!("unix://{}", path.display()),
            requests,
            complete,
            state,
            task,
        }
    }

    pub fn requests(&self) -> Vec<Value> {
        self.requests.lock().unwrap().clone()
    }

    pub fn cwd(&self) -> String {
        self.state.lock().unwrap().cwd.clone()
    }

    pub fn is_archived(&self) -> bool {
        self.state.lock().unwrap().archived
    }

    pub fn finish(&self) {
        self.complete.send_replace(true);
    }

    pub fn is_running(&self) -> bool {
        !self.task.is_finished()
    }
}

impl Drop for FakeCodex {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn thread_metadata(state: &State) -> Value {
    json!({"id":state.thread_id,"threadSource":state.thread_source,"cwd":state.cwd,
        "name":state.name,"gitInfo":state.git,"status":{"type":if state.turns.iter().any(|turn|turn["status"]=="inProgress") {"active"} else {"idle"}}})
}

fn finish_turn(turn: &mut Value) {
    turn["status"] = "completed".into();
    if turn["items"].as_array().unwrap().len() == 1 {
        turn["items"].as_array_mut().unwrap().push(json!({
            "type":"agentMessage", "id":"answer", "phase":"final_answer", "text":"Task result."
        }));
    }
}

async fn connection(
    stream: UnixStream,
    requests: Arc<Mutex<Vec<Value>>>,
    state: Arc<Mutex<State>>,
    mut finished: watch::Receiver<bool>,
) {
    let mut socket = accept_async(stream).await.unwrap();
    let mut subscribed = false;
    loop {
        let request = tokio::select! {
            item = socket.next() => {
                let Some(Ok(Message::Text(text))) = item else { return; };
                serde_json::from_str::<Value>(&text).unwrap()
            }
            result = finished.changed() => {
                if result.is_err() { return; }
                if subscribed && *finished.borrow() {
                    let notifications = {
                        let mut state = state.lock().unwrap();
                        let thread_id = state.thread_id.clone();
                        state.turns.iter_mut().filter_map(|turn| {
                            if turn["status"] != "inProgress" { return None; }
                            finish_turn(turn);
                            Some(json!({"method":"turn/completed", "params":{"threadId":thread_id, "turn":turn}}))
                        }).collect::<Vec<_>>()
                    };
                    for notification in notifications {
                        if socket.send(Message::Text(notification.to_string().into())).await.is_err() { return; }
                    }
                }
                continue;
            }
        };
        requests.lock().unwrap().push(request.clone());
        let method = request["method"].as_str().unwrap();
        let mut completion = None;
        let result = {
            let mut state = state.lock().unwrap();
            match method {
                "initialize" => json!({}),
                "initialized" => continue,
                "thread/start" => {
                    state.thread_source = request["params"]["threadSource"]
                        .as_str()
                        .map(str::to_owned);
                    state.cwd = request["params"]["cwd"].as_str().unwrap().into();
                    subscribed = true;
                    json!({"thread":thread_metadata(&state)})
                }
                "thread/resume" => {
                    assert_eq!(request["params"]["threadId"], state.thread_id);
                    assert!(
                        !state.archived,
                        "archived thread must be restored before resuming"
                    );
                    subscribed = true;
                    json!({"thread":thread_metadata(&state)})
                }
                "thread/archive" => {
                    assert_eq!(request["params"]["threadId"], state.thread_id);
                    assert!(
                        !state
                            .turns
                            .iter()
                            .any(|turn| turn["status"] == "inProgress")
                    );
                    state.archived = true;
                    json!({})
                }
                "thread/unarchive" => {
                    assert_eq!(request["params"]["threadId"], state.thread_id);
                    state.archived = false;
                    json!({"thread":thread_metadata(&state)})
                }
                "thread/read" => {
                    json!({"thread":thread_metadata(&state)})
                }
                "thread/name/set" => {
                    assert_eq!(request["params"]["threadId"], state.thread_id);
                    assert!(
                        request["params"]["name"]
                            .as_str()
                            .is_some_and(|name| !name.is_empty())
                    );
                    state.name = request["params"]["name"].as_str().map(str::to_owned);
                    json!({})
                }
                "thread/settings/update" => {
                    assert_eq!(request["params"]["threadId"], state.thread_id);
                    let cwd = request["params"]["cwd"].as_str().unwrap();
                    assert!(Path::new(cwd).is_absolute());
                    state.cwd = cwd.into();
                    json!({})
                }
                "thread/metadata/update" => {
                    assert_eq!(request["params"]["threadId"], state.thread_id);
                    state.git = request["params"]["gitInfo"].clone();
                    json!({"thread":thread_metadata(&state)})
                }
                "thread/loaded/list" => json!({"data":[state.thread_id],"nextCursor":null}),
                "thread/list" => {
                    let data = if request["params"]["archived"].as_bool().unwrap_or(false)
                        == state.archived
                    {
                        vec![json!({"id":state.thread_id,"threadSource":state.thread_source})]
                    } else {
                        Vec::new()
                    };
                    json!({"data":data,"nextCursor":null})
                }
                "turn/start" => {
                    assert_eq!(request["params"]["threadId"], state.thread_id);
                    let turn_id = format!("turn-{}", state.turns.len() + 1);
                    let mut turn = json!({"id":turn_id,"status":"inProgress","itemsView":"full","items":[{
                        "type":"userMessage","id":"input","clientId":request["params"]["clientUserMessageId"],
                        "content":request["params"]["input"]
                    }]});
                    if *finished.borrow() {
                        finish_turn(&mut turn);
                        completion = Some(
                            json!({"method":"turn/completed","params":{"threadId":state.thread_id,"turn":turn}}),
                        );
                    }
                    state.turns.push(turn);
                    json!({"turn":{"id":turn_id}})
                }
                "thread/turns/list" => {
                    if *finished.borrow() {
                        for turn in &mut state.turns {
                            finish_turn(turn);
                        }
                    }
                    json!({"data":state.turns,"nextCursor":null})
                }
                "turn/interrupt" => {
                    panic!("service shutdown must not interrupt the independently running turn")
                }
                _ => panic!("unexpected fake Codex method {method}"),
            }
        };
        let response = json!({"id":request["id"],"result":result});
        if socket
            .send(Message::Text(response.to_string().into()))
            .await
            .is_err()
        {
            return;
        }
        if let Some(completion) = completion
            && socket
                .send(Message::Text(completion.to_string().into()))
                .await
                .is_err()
        {
            return;
        }
    }
}
