use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{Mutex as AsyncMutex, mpsc, oneshot, watch},
    task::JoinHandle,
};

use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Message, client::IntoClientRequest, http::HeaderValue},
};

use yandalf_core::{
    domain::{Inspection, SessionSpec, ThreadInspection, TurnInput, TurnResult, TurnStatus},
    executor::Executor,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CodexConfig {
    pub endpoint: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bearer_token_env: Option<String>,
    pub request_timeout_secs: u64,
}

impl Default for CodexConfig {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            bearer_token_env: None,
            request_timeout_secs: 30,
        }
    }
}

impl CodexConfig {
    pub fn validate(&self) -> Result<()> {
        if self.request_timeout_secs == 0 {
            bail!("Codex request timeout must be positive");
        }
        if let Some(path) = self.endpoint.strip_prefix("unix://") {
            if !Path::new(path).is_absolute() {
                bail!("Codex Unix socket path must be absolute");
            }
        } else {
            if !self.endpoint.starts_with("ws://") && !self.endpoint.starts_with("wss://") {
                bail!("Codex endpoint must be an explicit unix://, ws:// or wss:// address");
            }
            let request = self
                .endpoint
                .as_str()
                .into_client_request()
                .context("invalid Codex endpoint")?;
            if request
                .uri()
                .authority()
                .is_some_and(|authority| authority.as_str().contains('@'))
            {
                bail!("Codex endpoint must not contain credentials; use bearer_token_env");
            }
        }
        if self
            .bearer_token_env
            .as_ref()
            .is_some_and(|name| name.is_empty() || name.contains('=') || name.contains('\0'))
        {
            bail!("invalid Codex bearer token environment variable name");
        }
        Ok(())
    }
}

type RpcResult = std::result::Result<Value, String>;
type TurnKey = (String, String);

struct Frame {
    message: Message,
    sent: oneshot::Sender<std::result::Result<(), String>>,
}

#[derive(Default)]
struct TurnState {
    messages: Vec<Value>,
    usage: Value,
    result: Option<TurnResult>,
}

#[derive(Default)]
struct ProtocolState {
    pending: HashMap<u64, oneshot::Sender<RpcResult>>,
    turns: HashMap<TurnKey, TurnState>,
    threads: HashSet<String>,
    settled: HashMap<String, String>,
    closed: Option<String>,
}

struct Shared {
    state: Mutex<ProtocolState>,
    changes: watch::Sender<u64>,
}

impl Shared {
    fn changed(&self) {
        self.changes
            .send_modify(|version| *version = version.wrapping_add(1));
    }

    fn close(&self, reason: String) {
        let mut state = self.state.lock().expect("protocol state poisoned");
        state.closed.get_or_insert_with(|| reason.clone());
        for (_, sender) in state.pending.drain() {
            let _ = sender.send(Err(reason.clone()));
        }
        drop(state);
        self.changed();
    }

    fn notification(&self, method: &str, params: &Value) -> Result<()> {
        if !matches!(
            method,
            "item/completed" | "thread/tokenUsage/updated" | "turn/completed"
        ) {
            return Ok(());
        }
        let thread_id = params["threadId"].as_str();
        let turn_id = params["turnId"]
            .as_str()
            .or_else(|| params["turn"]["id"].as_str());
        let (Some(thread_id), Some(turn_id)) = (thread_id, turn_id) else {
            return Ok(());
        };
        let mut state = self.state.lock().expect("protocol state poisoned");
        if !state.threads.contains(thread_id)
            || state.settled.get(thread_id).is_some_and(|id| id == turn_id)
        {
            return Ok(());
        }
        let turn = state
            .turns
            .entry((thread_id.into(), turn_id.into()))
            .or_default();
        match method {
            "item/completed" if params["item"]["type"] == "agentMessage" => {
                let item = &params["item"];
                if item["phase"] == "final_answer"
                    || (item["phase"].is_null()
                        && !turn
                            .messages
                            .iter()
                            .any(|message| message["phase"] == "final_answer"))
                {
                    turn.messages.clear();
                    turn.messages.push(item.clone());
                }
            }
            "thread/tokenUsage/updated" => turn.usage = params["tokenUsage"].clone(),
            "turn/completed" => {
                turn.result = Some(turn_result(
                    &params["turn"],
                    &turn.messages,
                    turn.usage.clone(),
                )?);
            }
            _ => return Ok(()),
        }
        drop(state);
        self.changed();
        Ok(())
    }
}

struct PendingRequest {
    id: u64,
    shared: Arc<Shared>,
}

impl Drop for PendingRequest {
    fn drop(&mut self) {
        self.shared
            .state
            .lock()
            .expect("protocol state poisoned")
            .pending
            .remove(&self.id);
    }
}

pub struct Codex {
    config: CodexConfig,
    outbound: mpsc::UnboundedSender<Frame>,
    shared: Arc<Shared>,
    next_id: AtomicU64,
    loaded_threads: AsyncMutex<HashSet<String>>,
    reader: JoinHandle<()>,
    writer: JoinHandle<()>,
}

impl Codex {
    pub async fn connect(config: &CodexConfig) -> Result<Self> {
        config.validate()?;
        let endpoint = &config.endpoint;
        let unix_path = endpoint.strip_prefix("unix://");
        let handshake_url = if unix_path.is_some() {
            "ws://localhost/"
        } else {
            endpoint
        };
        if unix_path.is_none() && !endpoint.starts_with("ws://") && !endpoint.starts_with("wss://")
        {
            bail!("Codex endpoint must be an explicit unix://, ws:// or wss:// address");
        }
        let mut request = handshake_url
            .into_client_request()
            .context("invalid Codex endpoint")?;
        if request
            .uri()
            .authority()
            .is_some_and(|authority| authority.as_str().contains('@'))
        {
            bail!("Codex endpoint must not contain credentials; use bearer_token_env");
        }
        if let Some(variable) = &config.bearer_token_env {
            let token = std::env::var(variable)
                .context("Codex bearer token environment variable is missing")?;
            if token.is_empty() {
                bail!("Codex bearer token must not be empty");
            }
            let mut value = HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|_| anyhow!("invalid Codex bearer token header"))?;
            value.set_sensitive(true);
            request.headers_mut().insert("authorization", value);
        }
        let connect = async {
            if let Some(path) = unix_path {
                #[cfg(unix)]
                {
                    if !Path::new(path).is_absolute() {
                        bail!("Codex Unix socket path must be absolute");
                    }
                    let stream = tokio::net::UnixStream::connect(path)
                        .await
                        .context("connect to Codex Unix socket")?;
                    let (socket, _) = tokio_tungstenite::client_async(request, stream)
                        .await
                        .context("Codex Unix socket WebSocket handshake")?;
                    Self::from_socket(config, socket).await
                }
                #[cfg(not(unix))]
                {
                    let _ = path;
                    bail!("Codex Unix sockets are unavailable on this platform")
                }
            } else {
                let (socket, _) = tokio_tungstenite::connect_async(request)
                    .await
                    .context("connect to Codex WebSocket")?;
                Self::from_socket(config, socket).await
            }
        };
        tokio::time::timeout(Duration::from_secs(config.request_timeout_secs), connect)
            .await
            .context("Codex connection timed out")?
    }

    async fn from_socket<S>(config: &CodexConfig, socket: WebSocketStream<S>) -> Result<Self>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (mut socket_writer, mut socket_reader) = socket.split();
        let shared = Arc::new(Shared {
            state: Mutex::new(ProtocolState::default()),
            changes: watch::channel(0).0,
        });
        let (outbound, mut frames) = mpsc::unbounded_channel::<Frame>();
        let writer_shared = shared.clone();
        let writer = tokio::spawn(async move {
            // Once queued, a frame must finish even if its caller stops waiting.
            while let Some(frame) = frames.recv().await {
                let result = socket_writer.send(frame.message).await;
                if let Err(error) = result {
                    let reason = format!("write Codex request: {error}");
                    let _ = frame.sent.send(Err(reason.clone()));
                    writer_shared.close(reason);
                    break;
                }
                let _ = frame.sent.send(Ok(()));
            }
        });
        let reader_shared = shared.clone();
        let reader_outbound = outbound.clone();
        let reader = tokio::spawn(async move {
            let result: Result<()> = async {
                while let Some(frame) = socket_reader.next().await {
                    let text = match frame.context("read Codex WebSocket frame")? {
                        Message::Text(text) => text,
                        Message::Ping(bytes) => {
                            write_message(&reader_outbound, Message::Pong(bytes)).await?;
                            continue;
                        }
                        Message::Pong(_) => continue,
                        Message::Close(_) => bail!("Codex connection closed"),
                        _ => bail!("unexpected non-text Codex WebSocket frame"),
                    };
                    let message: Value =
                        serde_json::from_str(&text).context("invalid Codex JSON frame")?;
                    if let Some(method) = message["method"].as_str() {
                        if let Some(id) = message.get("id") {
                            tracing::warn!(
                                method,
                                "headless Codex request requires an operator; declining"
                            );
                            let response = server_request_response(id.clone(), method);
                            write_frame(&reader_outbound, &response).await?;
                        } else {
                            reader_shared.notification(method, &message["params"])?;
                        }
                    } else if let Some(id) = message["id"].as_u64() {
                        let pending = reader_shared
                            .state
                            .lock()
                            .expect("protocol state poisoned")
                            .pending
                            .remove(&id);
                        if let Some(pending) = pending {
                            let response = if let Some(error) = message.get("error") {
                                Err(format!("Codex RPC error: {error}"))
                            } else if let Some(result) = message.get("result") {
                                Ok(result.clone())
                            } else {
                                Err("Codex response has neither result nor error".into())
                            };
                            let _ = pending.send(response);
                        }
                    }
                }
                bail!("Codex connection closed")
            }
            .await;
            reader_shared.close(result.err().map_or_else(
                || "Codex connection closed".into(),
                |error| format!("{error:#}"),
            ));
        });
        let client = Self {
            config: config.clone(),
            outbound,
            shared,
            next_id: AtomicU64::new(1),
            loaded_threads: AsyncMutex::new(HashSet::new()),
            reader,
            writer,
        };
        client
            .rpc(
                "initialize",
                json!({
                    "clientInfo": {"name":"yandalf", "version":env!("CARGO_PKG_VERSION")},
                    "capabilities":{"experimentalApi":true}
                }),
            )
            .await?;
        write_frame(
            &client.outbound,
            &json!({"method":"initialized","params":{}}),
        )
        .await?;
        Ok(client)
    }

    pub async fn wait_closed(&self) -> String {
        let mut changes = self.shared.changes.subscribe();
        loop {
            if let Some(reason) = self
                .shared
                .state
                .lock()
                .expect("protocol state poisoned")
                .closed
                .clone()
            {
                return reason;
            }
            if changes.changed().await.is_err() {
                return "Codex event stream closed".into();
            }
        }
    }

    async fn rpc(&self, method: &str, params: Value) -> Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        {
            let mut state = self.shared.state.lock().expect("protocol state poisoned");
            if let Some(reason) = &state.closed {
                bail!("{reason}");
            }
            state.pending.insert(id, sender);
        }
        let _pending = PendingRequest {
            id,
            shared: self.shared.clone(),
        };
        tokio::time::timeout(
            Duration::from_secs(self.config.request_timeout_secs),
            async {
                write_frame(
                    &self.outbound,
                    &json!({"id":id,"method":method,"params":params}),
                )
                .await?;
                receiver
                    .await
                    .context("Codex response channel closed")?
                    .map_err(|error| anyhow!(error))
            },
        )
        .await
        .with_context(|| format!("Codex {method} timed out; request outcome may be unknown"))?
    }

    async fn ensure_loaded(&self, thread_id: &str) -> Result<()> {
        self.ensure_loaded_at(thread_id, None).await?;
        Ok(())
    }

    async fn ensure_loaded_at(&self, thread_id: &str, cwd: Option<&Path>) -> Result<bool> {
        let mut loaded = self.loaded_threads.lock().await;
        if loaded.contains(thread_id) {
            return Ok(false);
        }
        if self.thread_is_archived(thread_id).await? {
            self.rpc("thread/unarchive", json!({"threadId": thread_id}))
                .await?;
        }
        self.shared
            .state
            .lock()
            .expect("protocol state poisoned")
            .threads
            .insert(thread_id.into());
        let mut params = json!({"threadId": thread_id, "excludeTurns": true});
        if let Some(cwd) = cwd {
            params["cwd"] = json!(cwd);
        }
        self.rpc("thread/resume", params).await?;
        loaded.insert(thread_id.into());
        Ok(true)
    }

    async fn thread_is_archived(&self, thread_id: &str) -> Result<bool> {
        let mut cursor = None::<String>;
        let mut cursors = HashSet::new();
        loop {
            let page = self
                .rpc(
                    "thread/list",
                    json!({"cursor":cursor,"limit":100,"archived":true,"modelProviders":[],"sourceKinds":["cli","vscode","exec","appServer","unknown"]}),
                )
                .await?;
            let threads = page["data"]
                .as_array()
                .context("thread/list omitted data")?;
            if threads.iter().any(|thread| thread["id"] == thread_id) {
                return Ok(true);
            }
            cursor = page["nextCursor"].as_str().map(str::to_owned);
            let Some(next) = &cursor else {
                return Ok(false);
            };
            if !cursors.insert(next.clone()) {
                bail!("Codex returned a repeated archived-thread cursor");
            }
        }
    }
}

impl Drop for Codex {
    fn drop(&mut self) {
        self.reader.abort();
        self.writer.abort();
        self.shared.close("Codex client stopped".into());
    }
}

#[async_trait]
impl Executor for Codex {
    async fn create_thread(&self, cwd: &Path, creation_id: &str) -> Result<String> {
        let params = json!({"cwd": cwd.to_str().context("cwd must be UTF-8")?, "threadSource":format!("yandalf:{creation_id}")});
        let response = self.rpc("thread/start", params).await?;
        let id = response["thread"]["id"]
            .as_str()
            .context("thread/start omitted thread ID")?
            .to_owned();
        self.loaded_threads.lock().await.insert(id.clone());
        self.shared
            .state
            .lock()
            .expect("protocol state poisoned")
            .threads
            .insert(id.clone());
        Ok(id)
    }

    async fn inspect_thread(&self, creation_id: &str) -> Result<ThreadInspection> {
        let marker = format!("yandalf:{creation_id}");
        let mut matches = HashSet::new();
        let mut cursor = None::<String>;
        let mut cursors = HashSet::new();
        loop {
            let page = self
                .rpc("thread/loaded/list", json!({"cursor":cursor,"limit":100}))
                .await?;
            for thread_id in page["data"]
                .as_array()
                .context("thread/loaded/list omitted data")?
            {
                let thread_id = thread_id
                    .as_str()
                    .context("loaded thread id must be a string")?;
                let response = self
                    .rpc(
                        "thread/read",
                        json!({"threadId":thread_id,"includeTurns":false}),
                    )
                    .await?;
                if response["thread"]["threadSource"] == marker {
                    matches.insert(thread_id.to_owned());
                }
            }
            cursor = page["nextCursor"].as_str().map(str::to_owned);
            let Some(next) = &cursor else {
                break;
            };
            if !cursors.insert(next.clone()) {
                bail!("Codex returned a repeated loaded-thread cursor");
            }
        }
        for archived in [false, true] {
            let mut cursor = None::<String>;
            let mut cursors = HashSet::new();
            loop {
                let page = self.rpc("thread/list", json!({"cursor":cursor,"limit":100,"archived":archived,"modelProviders":[],"sourceKinds":["cli","vscode","exec","appServer","unknown"]})).await?;
                for thread in page["data"]
                    .as_array()
                    .context("thread/list omitted data")?
                {
                    if thread["threadSource"] == marker {
                        matches.insert(
                            thread["id"]
                                .as_str()
                                .context("thread/list omitted ID")?
                                .to_owned(),
                        );
                    }
                }
                cursor = page["nextCursor"].as_str().map(str::to_owned);
                let Some(next) = &cursor else {
                    break;
                };
                if !cursors.insert(next.clone()) {
                    bail!("Codex returned a repeated thread cursor");
                }
            }
        }
        match matches.len() {
            0 => Ok(ThreadInspection::Missing),
            1 => Ok(ThreadInspection::Found {
                thread_id: matches.into_iter().next().unwrap(),
            }),
            _ => Ok(ThreadInspection::Ambiguous {
                reason: "multiple Codex threads match the persisted creation intent".into(),
            }),
        }
    }

    async fn thread_cwd(&self, thread_id: &str) -> Result<std::path::PathBuf> {
        let result = self
            .rpc(
                "thread/read",
                json!({"threadId":thread_id,"includeTurns":false}),
            )
            .await?;
        ensure_idle(&result["thread"])?;
        let cwd = result["thread"]["cwd"]
            .as_str()
            .context("thread/read omitted cwd")?;
        let cwd = std::path::PathBuf::from(cwd);
        anyhow::ensure!(
            cwd.is_absolute(),
            "executor returned a relative working directory"
        );
        Ok(cwd)
    }

    async fn archive_thread(&self, thread_id: &str) -> Result<()> {
        if !self.thread_is_archived(thread_id).await? {
            let current = self
                .rpc(
                    "thread/read",
                    json!({"threadId":thread_id,"includeTurns":false}),
                )
                .await?;
            ensure_idle(&current["thread"])?;
            self.loaded_threads.lock().await.remove(thread_id);
            self.rpc("thread/archive", json!({"threadId":thread_id}))
                .await?;
        }
        self.loaded_threads.lock().await.remove(thread_id);
        Ok(())
    }

    async fn configure_thread(&self, thread_id: &str, spec: &SessionSpec) -> Result<()> {
        let mut current = self
            .rpc(
                "thread/read",
                json!({"threadId":thread_id,"includeTurns":false}),
            )
            .await?["thread"]
            .clone();
        ensure_idle(&current)?;
        if self.ensure_loaded_at(thread_id, Some(&spec.cwd)).await? {
            current = self
                .rpc(
                    "thread/read",
                    json!({"threadId": thread_id, "includeTurns": false}),
                )
                .await?["thread"]
                .clone();
            ensure_idle(&current)?;
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        if current["cwd"] != spec.cwd.to_string_lossy().as_ref() {
            self.rpc(
                "thread/settings/update",
                json!({"threadId":thread_id,"cwd":spec.cwd}),
            )
            .await?;
            loop {
                current = self
                    .rpc(
                        "thread/read",
                        json!({"threadId":thread_id,"includeTurns":false}),
                    )
                    .await?["thread"]
                    .clone();
                ensure_idle(&current)?;
                if current["cwd"] == spec.cwd.to_string_lossy().as_ref() {
                    break;
                }
                anyhow::ensure!(
                    tokio::time::Instant::now() < deadline,
                    "Codex did not confirm prepared cwd"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        if let Some(title) = &spec.title {
            self.rpc(
                "thread/name/set",
                json!({"threadId":thread_id,"name":title}),
            )
            .await?;
        }
        let git = spec
            .git
            .as_ref()
            .map(|git| json!({"sha":git.sha,"branch":git.branch,"originUrl":git.origin_url}));
        if let Some(git) = &git {
            self.rpc(
                "thread/metadata/update",
                json!({"threadId":thread_id,"gitInfo":git}),
            )
            .await?;
        }
        let mut reapplied = false;
        loop {
            let current = self
                .rpc(
                    "thread/read",
                    json!({"threadId":thread_id,"includeTurns":false}),
                )
                .await?["thread"]
                .clone();
            ensure_idle(&current)?;
            let cwd_matches = current["cwd"] == spec.cwd.to_string_lossy().as_ref();
            let title_matches = spec
                .title
                .as_ref()
                .is_none_or(|title| current["name"] == *title);
            let git_matches = git.as_ref().is_none_or(|git| {
                ["sha", "branch", "originUrl"]
                    .iter()
                    .all(|key| current["gitInfo"][*key] == git[*key])
            });
            if cwd_matches && title_matches && git_matches {
                return Ok(());
            }
            anyhow::ensure!(
                tokio::time::Instant::now() < deadline,
                "Codex did not confirm prepared session metadata"
            );
            if cwd_matches && !git_matches && !reapplied {
                self.rpc(
                    "thread/metadata/update",
                    json!({"threadId":thread_id,"gitInfo":git}),
                )
                .await?;
                reapplied = true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn start_turn(
        &self,
        thread_id: &str,
        client_message_id: &str,
        input: &TurnInput,
    ) -> Result<String> {
        self.ensure_loaded(thread_id).await?;
        let mut content = vec![json!({"type":"text","text":input.text,"text_elements":[]})];
        let has_context = match &input.context {
            Value::Null => false,
            Value::Array(value) => !value.is_empty(),
            Value::Object(value) => !value.is_empty(),
            Value::String(value) => !value.is_empty(),
            _ => true,
        };
        if has_context {
            content.push(json!({
                "type":"text",
                "text":format!("Event context (data):\n{}", serde_json::to_string(&input.context)?),
                "text_elements":[]
            }));
        }
        let params = json!({
            "threadId":thread_id,
            "clientUserMessageId":client_message_id,
            "input":content
        });
        let response = self.rpc("turn/start", params).await?;
        Ok(response["turn"]["id"]
            .as_str()
            .context("turn/start omitted turn ID")?
            .into())
    }

    async fn wait_turn(&self, thread_id: &str, turn_id: &str) -> Result<TurnResult> {
        let key = (thread_id.to_owned(), turn_id.to_owned());
        let mut changes = self.shared.changes.subscribe();
        loop {
            {
                let mut state = self.shared.state.lock().expect("protocol state poisoned");
                if let Some(result) = state.turns.get(&key).and_then(|turn| turn.result.clone()) {
                    state.turns.remove(&key);
                    state.settled.insert(thread_id.into(), turn_id.into());
                    return Ok(result);
                }
                if let Some(reason) = &state.closed {
                    bail!("{reason}");
                }
            }
            changes
                .changed()
                .await
                .context("Codex event stream closed")?;
        }
    }

    async fn inspect(
        &self,
        thread_id: &str,
        client_message_id: &str,
        turn_id: Option<&str>,
    ) -> Result<Inspection> {
        self.ensure_loaded(thread_id).await?;
        let mut cursor = None::<String>;
        let mut cursors = HashSet::new();
        let mut matches = Vec::new();
        loop {
            let page = self
                .rpc(
                    "thread/turns/list",
                    json!({
                        "threadId":thread_id,"limit":100,"itemsView":"full","cursor":cursor
                    }),
                )
                .await?;
            for turn in page["data"]
                .as_array()
                .context("thread/turns/list omitted data")?
            {
                if turn["itemsView"]
                    .as_str()
                    .is_some_and(|view| view != "full")
                {
                    bail!("Codex returned incomplete items while reconciling a turn");
                }
                let matches_id = turn_id.is_some_and(|id| turn["id"] == id);
                let matches_client = turn["items"].as_array().is_some_and(|items| {
                    items.iter().any(|item| {
                        item["type"] == "userMessage" && item["clientId"] == client_message_id
                    })
                });
                if matches_id || matches_client {
                    matches.push(turn.clone());
                }
            }
            cursor = page["nextCursor"].as_str().map(str::to_owned);
            let Some(next) = &cursor else {
                break;
            };
            if !cursors.insert(next.clone()) {
                bail!("Codex returned a repeated history cursor");
            }
        }
        match matches.as_slice() {
            [] => Ok(Inspection::Missing),
            [turn] => {
                let id = turn["id"]
                    .as_str()
                    .context("history turn omitted ID")?
                    .to_owned();
                if turn["status"] == "inProgress" {
                    Ok(Inspection::Active { turn_id: id })
                } else {
                    let result = turn_result(turn, &[], Value::Null)?;
                    let mut state = self.shared.state.lock().expect("protocol state poisoned");
                    state.turns.remove(&(thread_id.into(), id.clone()));
                    state.settled.insert(thread_id.into(), id.clone());
                    Ok(Inspection::Finished {
                        turn_id: id,
                        result,
                    })
                }
            }
            _ => Ok(Inspection::Ambiguous {
                reason:
                    "multiple Codex turns match the persisted request; refusing automatic replay"
                        .into(),
            }),
        }
    }

    async fn interrupt(&self, thread_id: &str, turn_id: &str) -> Result<()> {
        self.ensure_loaded(thread_id).await?;
        self.rpc(
            "turn/interrupt",
            json!({"threadId":thread_id,"turnId":turn_id}),
        )
        .await?;
        Ok(())
    }
}

fn ensure_idle(thread: &Value) -> Result<()> {
    let status = thread["status"]["type"]
        .as_str()
        .context("thread/read omitted runtime status")?;
    anyhow::ensure!(
        matches!(status, "idle" | "notLoaded"),
        "Codex thread is {status}; workspace preparation must wait"
    );
    Ok(())
}

async fn write_frame(outbound: &mpsc::UnboundedSender<Frame>, value: &Value) -> Result<()> {
    write_message(
        outbound,
        Message::Text(serde_json::to_string(value)?.into()),
    )
    .await
}

async fn write_message(outbound: &mpsc::UnboundedSender<Frame>, message: Message) -> Result<()> {
    let (sent, delivered) = oneshot::channel();
    outbound
        .send(Frame { message, sent })
        .map_err(|_| anyhow!("Codex writer stopped"))?;
    delivered
        .await
        .context("Codex writer stopped")?
        .map_err(|error| anyhow!(error))
}

fn server_request_response(id: Value, method: &str) -> Value {
    let result = match method {
        "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => {
            json!({"decision":"decline"})
        }
        "item/permissions/requestApproval" => json!({"permissions":{},"scope":"turn"}),
        _ => {
            return json!({"id":id,"error":{"code":-32601,"message":"Yandalf has no interactive handler for this server request"}});
        }
    };
    json!({"id":id,"result":result})
}

fn turn_result(turn: &Value, observed: &[Value], usage: Value) -> Result<TurnResult> {
    let status = match turn["status"].as_str() {
        Some("completed") => TurnStatus::Completed,
        Some("failed") => TurnStatus::Failed,
        Some("interrupted") => TurnStatus::Interrupted,
        other => bail!("unexpected terminal Codex status: {other:?}"),
    };
    let items = turn["items"].as_array().map_or(observed, Vec::as_slice);
    let items = if items.iter().any(|item| item["type"] == "agentMessage") {
        items
    } else {
        observed
    };
    let final_message = items
        .iter()
        .rev()
        .find(|item| item["type"] == "agentMessage" && item["phase"] == "final_answer")
        .or_else(|| {
            items
                .iter()
                .rev()
                .find(|item| item["type"] == "agentMessage" && item["phase"].is_null())
        });
    let text = final_message
        .and_then(|item| item["text"].as_str())
        .or_else(|| turn["error"]["message"].as_str())
        .unwrap_or_default()
        .to_owned();
    Ok(TurnResult {
        status,
        text,
        usage,
    })
}

pub struct CodexServer(pub CodexConfig);

#[async_trait]
impl yandalf_core::executor::ExecutorProvider for CodexServer {
    async fn connect(&self) -> Result<yandalf_core::executor::ExecutorConnection> {
        let client = Arc::new(Codex::connect(&self.0).await?);
        let observer = client.clone();
        Ok(yandalf_core::executor::ExecutorConnection {
            executor: client,
            closed: Box::pin(async move { observer.wait_closed().await }),
        })
    }
}
impl yandalf_core::plugin::Plugin for CodexServer {
    fn name(&self) -> &str {
        "codex"
    }
    fn register(&self, ctx: &mut yandalf_core::plugin::PluginContext) -> Result<()> {
        self.0.validate()?;
        ctx.provide::<dyn yandalf_core::executor::ExecutorProvider>(Arc::new(Self(self.0.clone())))
    }
}
