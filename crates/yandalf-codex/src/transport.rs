use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
    time::{Instant, timeout_at},
};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Message, client::IntoClientRequest, http::HeaderValue},
};
use tokio_util::sync::CancellationToken;
use yandalf_core::domain::TurnResult;

use crate::{
    CodexConfig,
    wire::{Events, server_request_response},
};

const OUTBOUND_CAPACITY: usize = 32;

type RpcResult = std::result::Result<Value, String>;

struct Frame {
    message: Message,
    deadline: Instant,
    sent: oneshot::Sender<std::result::Result<(), String>>,
}

#[derive(Default)]
struct State {
    pending: HashMap<u64, oneshot::Sender<RpcResult>>,
    events: Events,
    closed: Option<String>,
}

struct Shared {
    state: Mutex<State>,
    changes: watch::Sender<u64>,
    stop: CancellationToken,
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
        self.stop.cancel();
        self.changed();
    }

    fn response(&self, message: &Value) {
        let Some(id) = message["id"].as_u64() else {
            return;
        };
        let pending = self
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

    fn notification(&self, method: &str, params: &Value) -> Result<()> {
        let changed = self
            .state
            .lock()
            .expect("protocol state poisoned")
            .events
            .notification(method, params)?;
        if changed {
            self.changed();
        }
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

pub(super) struct Transport {
    request_timeout: Duration,
    outbound: mpsc::Sender<Frame>,
    shared: Arc<Shared>,
    next_id: AtomicU64,
    reader: JoinHandle<()>,
    writer: JoinHandle<()>,
}

impl Transport {
    pub async fn connect(config: &CodexConfig) -> Result<Self> {
        config.validate()?;
        let endpoint = &config.endpoint;
        let unix_path = endpoint.strip_prefix("unix://");
        let handshake_url = if unix_path.is_some() {
            "ws://localhost/"
        } else {
            endpoint
        };
        let mut request = handshake_url
            .into_client_request()
            .context("invalid Codex endpoint")?;
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
        let (socket_writer, socket_reader) = socket.split();
        let shared = Arc::new(Shared {
            state: Mutex::new(State::default()),
            changes: watch::channel(0).0,
            stop: CancellationToken::new(),
        });
        let request_timeout = Duration::from_secs(config.request_timeout_secs);
        let (outbound, frames) = mpsc::channel(OUTBOUND_CAPACITY);
        let writer_shared = shared.clone();
        let writer = tokio::spawn(write_loop(socket_writer, frames, writer_shared));
        let reader_shared = shared.clone();
        let reader_outbound = outbound.clone();
        let reader = tokio::spawn(async move {
            let result = read_loop(
                socket_reader,
                &reader_outbound,
                &reader_shared,
                request_timeout,
            )
            .await;
            if let Err(error) = result {
                reader_shared.close(format!("{error:#}"));
            }
        });
        let client = Self {
            request_timeout,
            outbound,
            shared,
            next_id: AtomicU64::new(1),
            reader,
            writer,
        };
        client
            .request(
                "initialize",
                json!({
                    "clientInfo": {"name":"yandalf", "version":env!("CARGO_PKG_VERSION")},
                    "capabilities":{"experimentalApi":true}
                }),
            )
            .await?;
        write_message(
            &client.outbound,
            Message::Text(
                json!({"method":"initialized","params":{}})
                    .to_string()
                    .into(),
            ),
            Instant::now() + request_timeout,
        )
        .await?;
        Ok(client)
    }

    pub(super) async fn wait_closed(&self) -> String {
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

    pub(super) async fn request(&self, method: &str, params: Value) -> Result<Value> {
        let deadline = Instant::now() + self.request_timeout;
        let permit = timeout_at(deadline, self.outbound.reserve())
            .await
            .with_context(|| format!("Codex {method} timed out before queue admission"))?
            .context("Codex writer stopped before queue admission")?;
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
        let (sent, delivered) = oneshot::channel();
        permit.send(Frame {
            message: Message::Text(
                json!({"id":id,"method":method,"params":params})
                    .to_string()
                    .into(),
            ),
            deadline,
            sent,
        });
        timeout_at(deadline, async {
            delivered
                .await
                .context("Codex writer stopped")?
                .map_err(|error| anyhow!(error))?;
            receiver
                .await
                .context("Codex response channel closed")?
                .map_err(|error| anyhow!(error))
        })
        .await
        .with_context(|| format!("Codex {method} timed out; request outcome may be unknown"))?
    }

    pub(super) fn track_thread(&self, thread_id: &str) {
        self.shared
            .state
            .lock()
            .expect("protocol state poisoned")
            .events
            .track_thread(thread_id);
    }

    pub(super) fn forget_thread(&self, thread_id: &str) {
        self.shared
            .state
            .lock()
            .expect("protocol state poisoned")
            .events
            .forget_thread(thread_id);
    }

    pub(super) fn settle_turn(&self, thread_id: &str, turn_id: &str) {
        self.shared
            .state
            .lock()
            .expect("protocol state poisoned")
            .events
            .settle_turn(thread_id, turn_id);
    }

    pub(super) async fn wait_turn(&self, thread_id: &str, turn_id: &str) -> Result<TurnResult> {
        let mut changes = self.shared.changes.subscribe();
        loop {
            {
                let mut state = self.shared.state.lock().expect("protocol state poisoned");
                if let Some(result) = state.events.take_result(thread_id, turn_id) {
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
}

impl Drop for Transport {
    fn drop(&mut self) {
        self.reader.abort();
        self.writer.abort();
        self.shared.close("Codex client stopped".into());
    }
}

async fn write_loop<S>(
    mut socket: futures_util::stream::SplitSink<WebSocketStream<S>, Message>,
    mut frames: mpsc::Receiver<Frame>,
    shared: Arc<Shared>,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let frame = tokio::select! {
            _ = shared.stop.cancelled() => break,
            frame = frames.recv() => match frame { Some(frame) => frame, None => break },
        };
        if frame.sent.is_closed() || Instant::now() >= frame.deadline {
            continue;
        }
        // A partially written request cannot be abandoned while reusing the connection.
        let result = tokio::select! {
            _ = shared.stop.cancelled() => break,
            result = timeout_at(frame.deadline, socket.send(frame.message)) => result,
        };
        let result = match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(format!("write Codex request: {error}")),
            Err(_) => Err("write Codex request timed out; request outcome may be unknown".into()),
        };
        if let Err(reason) = &result {
            shared.close(reason.clone());
            let _ = frame.sent.send(result);
            break;
        }
        let _ = frame.sent.send(result);
    }
}

async fn read_loop<S>(
    mut socket: futures_util::stream::SplitStream<WebSocketStream<S>>,
    outbound: &mpsc::Sender<Frame>,
    shared: &Shared,
    request_timeout: Duration,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let frame = tokio::select! {
            _ = shared.stop.cancelled() => return Ok(()),
            frame = socket.next() => frame.context("Codex connection closed")?,
        };
        let deadline = Instant::now() + request_timeout;
        let text = match frame.context("read Codex WebSocket frame")? {
            Message::Text(text) => text,
            Message::Ping(bytes) => {
                write_message(outbound, Message::Pong(bytes), deadline).await?;
                continue;
            }
            Message::Pong(_) => continue,
            Message::Close(_) => bail!("Codex connection closed"),
            _ => bail!("unexpected non-text Codex WebSocket frame"),
        };
        let message: Value = serde_json::from_str(&text).context("invalid Codex JSON frame")?;
        if let Some(method) = message["method"].as_str() {
            if let Some(id) = message.get("id") {
                tracing::warn!(
                    method,
                    "headless Codex request requires an operator; declining"
                );
                let response = server_request_response(id.clone(), method);
                write_message(
                    outbound,
                    Message::Text(response.to_string().into()),
                    deadline,
                )
                .await?;
            } else {
                shared.notification(method, &message["params"])?;
            }
        } else {
            shared.response(&message);
        }
    }
}

async fn write_message(
    outbound: &mpsc::Sender<Frame>,
    message: Message,
    deadline: Instant,
) -> Result<()> {
    timeout_at(deadline, async {
        let permit = outbound.reserve().await.context("Codex writer stopped")?;
        let (sent, delivered) = oneshot::channel();
        permit.send(Frame {
            message,
            deadline,
            sent,
        });
        delivered
            .await
            .context("Codex writer stopped")?
            .map_err(|error| anyhow!(error))
    })
    .await
    .context("write Codex frame timed out; request outcome may be unknown")?
}

#[cfg(test)]
mod tests;
