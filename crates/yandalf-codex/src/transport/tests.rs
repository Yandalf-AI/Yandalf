use std::{future::Future, sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::{io::DuplexStream, sync::oneshot, task::JoinSet};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Message, protocol::Role},
};

use super::{OUTBOUND_CAPACITY, Transport};
use crate::CodexConfig;

type Socket = WebSocketStream<DuplexStream>;

async fn receive(socket: &mut Socket) -> Value {
    let message = socket.next().await.unwrap().unwrap();
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}

async fn reply(socket: &mut Socket, request: &Value) {
    socket
        .send(Message::Text(
            json!({"id":request["id"],"result":{}}).to_string().into(),
        ))
        .await
        .unwrap();
}

async fn wait_until(condition: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_millis(500), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

fn pending_count(transport: &Transport) -> usize {
    transport.shared.state.lock().unwrap().pending.len()
}

async fn connected<F, Fut>(script: F) -> (Arc<Transport>, tokio::task::JoinHandle<()>)
where
    F: FnOnce(Socket) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send,
{
    let (client, peer) = tokio::io::duplex(256);
    let server = tokio::spawn(async move {
        let mut socket = WebSocketStream::from_raw_socket(peer, Role::Server, None).await;
        let request = receive(&mut socket).await;
        assert_eq!(request["method"], "initialize");
        reply(&mut socket, &request).await;
        assert_eq!(receive(&mut socket).await["method"], "initialized");
        script(socket).await;
    });
    let socket = WebSocketStream::from_raw_socket(client, Role::Client, None).await;
    let transport = Transport::from_socket(
        &CodexConfig {
            request_timeout_secs: 1,
            ..CodexConfig::default()
        },
        socket,
    )
    .await
    .unwrap();
    (Arc::new(transport), server)
}

#[tokio::test]
async fn stalled_live_peer_bounds_admission_and_closes_the_transport() {
    let (release, wait) = oneshot::channel::<()>();
    let (transport, peer) = connected(|socket| async move {
        let _ = wait.await;
        drop(socket);
    })
    .await;
    let mut requests = JoinSet::new();
    for _ in 0..OUTBOUND_CAPACITY * 3 {
        let transport = transport.clone();
        requests.spawn(async move {
            transport
                .request("turn/start", json!({"input":"x".repeat(8192)}))
                .await
        });
    }
    wait_until(|| {
        transport.outbound.capacity() == 0 && pending_count(&transport) == OUTBOUND_CAPACITY + 1
    })
    .await;
    let pending = pending_count(&transport);
    assert!(
        pending <= OUTBOUND_CAPACITY + 1,
        "{pending} pending requests"
    );

    let waiting = {
        let transport = transport.clone();
        tokio::spawn(async move { transport.request("waiting", json!({})).await })
    };
    tokio::task::yield_now().await;
    waiting.abort();
    assert!(waiting.await.unwrap_err().is_cancelled());
    assert_eq!(pending_count(&transport), pending);

    let reason = tokio::time::timeout(Duration::from_secs(3), transport.wait_closed())
        .await
        .unwrap();
    assert!(reason.contains("write Codex request timed out"), "{reason}");
    while let Some(result) = requests.join_next().await {
        assert!(result.unwrap().is_err());
    }
    assert_eq!(pending_count(&transport), 0);
    assert!(transport.outbound.is_closed());
    drop(release);
    peer.await.unwrap();
}

#[tokio::test]
async fn a_cancelled_queued_request_is_not_sent_when_the_peer_resumes_reading() {
    let (release, wait) = oneshot::channel::<()>();
    let (transport, peer) = connected(|mut socket| async move {
        wait.await.unwrap();
        let request = receive(&mut socket).await;
        assert_eq!(request["method"], "first");
        reply(&mut socket, &request).await;
        let request = receive(&mut socket).await;
        assert_eq!(request["method"], "after-cancellation");
        reply(&mut socket, &request).await;
    })
    .await;
    let first = {
        let transport = transport.clone();
        tokio::spawn(async move {
            transport
                .request("first", json!({"input":"x".repeat(8192)}))
                .await
        })
    };
    wait_until(|| {
        pending_count(&transport) == 1 && transport.outbound.capacity() == OUTBOUND_CAPACITY
    })
    .await;
    let cancelled = {
        let transport = transport.clone();
        tokio::spawn(async move { transport.request("cancelled", json!({})).await })
    };
    wait_until(|| pending_count(&transport) == 2).await;
    cancelled.abort();
    assert!(cancelled.await.unwrap_err().is_cancelled());
    assert_eq!(pending_count(&transport), 1);
    release.send(()).unwrap();
    first.await.unwrap().unwrap();
    transport
        .request("after-cancellation", json!({}))
        .await
        .unwrap();
    peer.await.unwrap();
}
