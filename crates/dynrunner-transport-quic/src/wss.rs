use std::net::SocketAddr;

use dynrunner_core::{Identifier, MessageReceiver, MessageSender};
use dynrunner_protocol_primary_secondary::{DistributedMessage, codec};
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

/// A WSS (WebSocket Secure) connection that can send/receive distributed messages.
///
/// Used as a TCP-based fallback when QUIC (UDP) is blocked by network policy.
/// Implements the same `MessageSender` / `MessageReceiver` traits as `QuicConnection`.
pub struct WssConnection {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

/// Bound on one inbound TCP connection's WebSocket upgrade. Applied
/// inside [`WssConnection::accept_handshake`] so a dialer that
/// connects at TCP but never sends (or never completes) the HTTP
/// upgrade releases the per-connection handler task. Generous: a
/// conformant dialer upgrades immediately after connect.
const INBOUND_HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Pause between retries when the LISTENER-level `accept(2)` itself
/// errors (e.g. `ECONNABORTED` under a connection-reset storm, or
/// `EMFILE`). Keeps an accept loop alive across transient listener
/// errors without letting a persistent error (fd exhaustion) turn the
/// loop into a busy-spin. The success path never sleeps.
pub(crate) const ACCEPT_ERROR_BACKOFF: std::time::Duration = std::time::Duration::from_millis(100);

impl WssConnection {
    pub fn new(ws: WebSocketStream<MaybeTlsStream<TcpStream>>) -> Self {
        Self { ws }
    }

    /// Drive ONE accepted TCP connection through the server-side
    /// WebSocket upgrade. PER-CONNECTION fallible — a connection that
    /// resets mid-handshake (the run_20260611_202345 simultaneous
    /// connection reset aborted exactly such in-flight upgrades) errors
    /// HERE, in whatever task drove this attempt, and says nothing
    /// about the listener. Accept loops must call this from the
    /// spawned per-connection handler, never inline in the loop:
    /// pre-fix the loops folded this failure into `accept()`'s `Err`
    /// and treated it as loop-fatal, permanently killing the node's
    /// ability to accept re-dialed sessions.
    ///
    /// Runs with the EXPLICIT wire limits from
    /// [`crate::framing::wire_ws_config`] (#366).
    pub async fn accept_handshake(tcp_stream: TcpStream) -> Result<Self, String> {
        let ws_stream = tokio::time::timeout(
            INBOUND_HANDSHAKE_TIMEOUT,
            tokio_tungstenite::accept_async_with_config(
                MaybeTlsStream::Plain(tcp_stream),
                Some(crate::framing::wire_ws_config()),
            ),
        )
        .await
        .map_err(|_| {
            format!(
                "inbound WSS upgrade did not complete within {}s",
                INBOUND_HANDSHAKE_TIMEOUT.as_secs()
            )
        })?
        .map_err(|e| e.to_string())?;
        Ok(WssConnection::new(ws_stream))
    }

    /// Consume the connection and return the underlying WebSocket stream.
    pub fn into_inner(self) -> WebSocketStream<MaybeTlsStream<TcpStream>> {
        self.ws
    }
}

impl<I: Identifier> MessageSender<DistributedMessage<I>> for WssConnection {
    async fn send(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        let frame = codec::serialize_message(&msg)?;
        // Sender-side wire-limit gate (#366): tungstenite does NOT
        // size-check on write, so an oversize frame would sail out and
        // kill the RECEIVER's reader. Reject it here instead — loud,
        // connection kept.
        crate::framing::check_outbound_len(frame.len())?;
        self.ws
            .send(Message::Binary(frame.into()))
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

impl<I: Identifier> MessageReceiver<DistributedMessage<I>> for WssConnection {
    async fn recv(&mut self) -> Option<DistributedMessage<I>> {
        loop {
            match self.ws.next().await {
                Some(Ok(Message::Binary(data))) => match codec::decode_frame(&data) {
                    Ok(Some((msg, _))) => return Some(msg),
                    Ok(None) => continue,
                    Err(error) => {
                        tracing::error!(
                            error,
                            "WSS frame failed to decode (corrupt frame); \
                             dropping the connection"
                        );
                        return None;
                    }
                },
                Some(Ok(Message::Close(_))) | None => return None,
                Some(Ok(_)) => continue, // skip ping/pong/text
                Some(Err(error)) => {
                    // This is where tungstenite surfaces an over-limit
                    // frame (`CapacityError::MessageTooLong`, naming the
                    // size and the configured limit) — the pre-#366
                    // silent drop point. ERROR for that class, quieter
                    // for ordinary disconnect churn (see the classifier).
                    crate::framing::log_wss_read_error(&error, "wss-conn", "unidentified");
                    return None;
                }
            }
        }
    }
}

/// A WSS listener that accepts incoming WebSocket connections.
pub struct WssListener {
    tcp_listener: TcpListener,
    local_addr: SocketAddr,
}

impl WssListener {
    /// Bind a WSS server on the given address.
    pub async fn bind(addr: SocketAddr) -> Result<Self, String> {
        Self::try_bind(addr).await.map_err(|e| e.to_string())
    }

    /// Bind on the given address, surfacing the raw [`std::io::Error`]
    /// so a caller can classify the failure (e.g. the listener-pair
    /// retry distinguishing the TCP-twin `AddrInUse` race from a fatal
    /// bind error).
    pub(crate) async fn try_bind(addr: SocketAddr) -> std::io::Result<Self> {
        let tcp_listener = TcpListener::bind(addr).await?;
        let local_addr = tcp_listener.local_addr()?;
        tracing::info!(%local_addr, "WSS listener bound");
        Ok(Self {
            tcp_listener,
            local_addr,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn port(&self) -> u16 {
        self.local_addr.port()
    }

    /// Listener-level accept: the next raw TCP connection, BEFORE the
    /// WebSocket upgrade. An `Err` here is a listener-level `accept(2)`
    /// fault (transient `ECONNABORTED` under a reset storm, `EMFILE`,
    /// …) — an accept loop logs it and keeps accepting (paced by
    /// [`ACCEPT_ERROR_BACKOFF`]); it is NEVER loop-fatal, because the
    /// listener socket itself is owned and stays bound. Drive the
    /// returned stream through [`WssConnection::accept_handshake`]
    /// inside the per-connection handler task.
    pub async fn accept_raw(&self) -> std::io::Result<(TcpStream, SocketAddr)> {
        self.tcp_listener.accept().await
    }

    /// Accept the next incoming WebSocket connection (plain WS, no TLS)
    /// — `accept_raw` + [`WssConnection::accept_handshake`] in one call,
    /// for single-connection callers (tests, fixtures). An accept LOOP
    /// must NOT use this: a per-connection upgrade failure is
    /// indistinguishable from a listener failure in the flattened
    /// `Err`, and awaiting the upgrade inline serializes (and, treated
    /// as fatal, kills) the loop — use `accept_raw` + `accept_handshake`
    /// in the spawned handler instead.
    ///
    /// For production use behind SSH tunnels / internal networks, TLS is
    /// typically handled at the tunnel level. To add native TLS, wrap the
    /// `TcpStream` with `tokio_native_tls` before the WebSocket handshake.
    pub async fn accept(&self) -> Result<WssConnection, String> {
        let (tcp_stream, peer_addr) = self.accept_raw().await.map_err(|e| e.to_string())?;
        tracing::debug!(%peer_addr, "WSS TCP connection accepted");
        WssConnection::accept_handshake(tcp_stream).await
    }
}

/// Connect to a WSS server (plain WS, no TLS — see `WssListener::accept`
/// note). Runs with the EXPLICIT wire limits from
/// [`crate::framing::wire_ws_config`] (#366), matching the accept side.
pub async fn connect_wss(addr: SocketAddr) -> Result<WssConnection, String> {
    let url = format!("ws://{addr}");
    let (ws_stream, _) = tokio_tungstenite::connect_async_with_config(
        &url,
        Some(crate::framing::wire_ws_config()),
        false,
    )
    .await
    .map_err(|e| e.to_string())?;

    Ok(WssConnection::new(ws_stream))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dynrunner_protocol_primary_secondary::KeepaliveRole;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct TestId(String);

    #[tokio::test]
    async fn wss_message_roundtrip() {
        let listener = WssListener::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let port = listener.port();

        let outgoing: DistributedMessage<TestId> = DistributedMessage::Keepalive {
            target: None,
            sender_id: "wss-test".into(),
            timestamp: 99.0,
            secondary_id: "wss-test".into(),
            active_workers: 4,
            emitter_role: KeepaliveRole::Secondary,
        };

        let server_task = async {
            let mut conn = listener.accept().await.expect("accept failed");
            let msg: DistributedMessage<TestId> =
                MessageReceiver::recv(&mut conn).await.expect("no message");
            MessageSender::send(&mut conn, msg.clone())
                .await
                .expect("send failed");
            msg
        };

        let client_task = async {
            let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
            let mut client = connect_wss(addr).await.expect("connect failed");
            MessageSender::send(&mut client, outgoing)
                .await
                .expect("client send failed");
            let echoed: DistributedMessage<TestId> =
                MessageReceiver::recv(&mut client).await.expect("no echo");
            echoed
        };

        let (server_msg, echoed) = tokio::join!(server_task, client_task);

        match &echoed {
            DistributedMessage::Keepalive { active_workers, .. } => {
                assert_eq!(*active_workers, 4);
            }
            _ => panic!("expected Keepalive"),
        }

        assert_eq!(server_msg.sender_id(), "wss-test");
    }

    fn task_complete(payload_bytes: usize) -> DistributedMessage<TestId> {
        DistributedMessage::TaskComplete {
            target: None,
            sender_id: "wss-test".into(),
            timestamp: 0.0,
            secondary_id: "wss-test".into(),
            worker_id: 0,
            task_hash: "deadbeef".into(),
            result_data: Some(vec![b'x'; payload_bytes]),
            delivery_seq: Some(1),
            // Stamped at the send_to_primary chokepoint (ordering gate).
            msgs_posted_through: None,
        }
    }

    /// The #366 production replay: a ~55 MB `TaskComplete` — well over
    /// tungstenite's DEFAULT 16 MiB `max_frame_size`, which used to
    /// error the receiving reader and silently vanish the message —
    /// must flow through a real WSS pair intact under the explicit
    /// [`crate::framing::MAX_WIRE_FRAME_BYTES`] config.
    #[tokio::test]
    async fn wss_production_scale_frame_flows() {
        // `result_data` serializes as a JSON number array (~4 wire
        // bytes per element), so ~13.75M raw bytes ≈ a 55 MB frame.
        let payload_bytes = 13_750_000;
        let outgoing = task_complete(payload_bytes);
        let frame = codec::serialize_message(&outgoing).unwrap();
        assert!(
            frame.len() > 16 * 1024 * 1024,
            "frame ({} bytes) must exceed the old tungstenite frame \
             default for this test to pin the #366 regression",
            frame.len()
        );
        assert!(
            frame.len() < crate::framing::MAX_WIRE_FRAME_BYTES,
            "frame ({} bytes) must be a LEGITIMATE message under the \
             explicit wire limit",
            frame.len()
        );

        let listener = WssListener::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let port = listener.port();

        let server_task = async {
            let mut conn = listener.accept().await.expect("accept failed");
            MessageReceiver::<DistributedMessage<TestId>>::recv(&mut conn)
                .await
                .expect("55 MB TaskComplete must ARRIVE — a None here is \
                         the #366 silent drop")
        };

        let client_task = async {
            let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
            let mut client = connect_wss(addr).await.expect("connect failed");
            MessageSender::send(&mut client, outgoing)
                .await
                .expect("client send failed");
            client // keep the connection alive until the server read it
        };

        let (received, _client) = tokio::join!(server_task, client_task);
        match received {
            DistributedMessage::TaskComplete {
                task_hash,
                result_data,
                ..
            } => {
                assert_eq!(task_hash, "deadbeef");
                assert_eq!(result_data.unwrap().len(), payload_bytes);
            }
            other => panic!("expected TaskComplete, got {:?}", other.msg_type()),
        }
    }

    /// An over-limit frame must be (a) refused by the sender-side gate
    /// in `WssConnection::send`, and (b) — defense-in-depth, when a
    /// non-conformant sender bypasses the gate and writes raw — rejected
    /// by the receiver, whose `recv` returns `None` so the connection
    /// tears down through the NORMAL disconnect path (no silent wedge,
    /// no partial message surfaced).
    #[tokio::test]
    async fn wss_oversize_frame_rejected_both_sides() {
        use futures_util::SinkExt;
        use tokio_tungstenite::tungstenite::Message;

        // Raw oversize wire frame: 4-byte prefix + payload, one byte
        // over the limit in total.
        let oversize_total = crate::framing::MAX_WIRE_FRAME_BYTES + 1;
        let mut raw = Vec::with_capacity(oversize_total);
        raw.extend_from_slice(&((oversize_total - 4) as u32).to_be_bytes());
        raw.resize(oversize_total, b'x');

        let listener = WssListener::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let port = listener.port();

        let server_task = async {
            let mut conn = listener.accept().await.expect("accept failed");
            MessageReceiver::<DistributedMessage<TestId>>::recv(&mut conn).await
        };

        let client_task = async {
            let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
            let client = connect_wss(addr).await.expect("connect failed");

            // (b) bypass the egress gate: write the raw oversize frame
            // directly to the WebSocket. The send may itself error once
            // the server kills the connection mid-write — either way
            // the salient assertion is on the server side.
            let mut ws = client.into_inner();
            let _ = ws.send(Message::Binary(raw.into())).await;
        };

        let (received, ()) = tokio::join!(server_task, client_task);
        assert!(
            received.is_none(),
            "an over-limit frame must terminate recv with None (the \
             loud-reject + normal-disconnect path), not surface a message"
        );

        // (a) the conformant path: the sender-side gate refuses before
        // anything touches the wire.
        let listener2 = WssListener::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let port2 = listener2.port();
        let accept_task = async {
            // Accept AND read: if the gate ever regressed to letting
            // the frame through, the reading peer turns the failure
            // into a loud assert below instead of a zero-window hang.
            let mut conn = listener2.accept().await.expect("accept failed");
            MessageReceiver::<DistributedMessage<TestId>>::recv(&mut conn).await
        };
        let send_task = async {
            let addr: SocketAddr = format!("127.0.0.1:{port2}").parse().unwrap();
            let mut client = connect_wss(addr).await.expect("connect failed");
            // 26M raw bytes serialize as a JSON number array (~4 wire
            // bytes per element) ≈ 104 MB on the wire: over the 96 MiB
            // (100_663_296 bytes) limit.
            let oversize_msg = task_complete(26_000_000);
            MessageSender::send(&mut client, oversize_msg).await
        };
        let (_server_recv, send_result) = tokio::join!(accept_task, send_task);
        let err = send_result.expect_err("oversize send must be refused");
        assert!(
            err.contains("exceeds the wire limit"),
            "gate error must name the violation, got: {err}"
        );
    }
}
