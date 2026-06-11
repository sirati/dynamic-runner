//! Secondary-side network client: connects to a peer via QUIC, falling
//! back to WSS if QUIC fails.
//!
//! Carries the submitter-bound `MessageSender<DistributedMessage<I>>` +
//! `MessageReceiver<DistributedMessage<I>>` shape (formerly satisfied
//! the `PrimaryTransport<I>` marker trait via blanket impl; that trait
//! retired in Step 11 of the transport-unification refactor — the
//! underlying bidirectional contract is unchanged).
//!
//! ## Cancel-safety
//!
//! Both `send` and `recv` are cancel-safe by construction: the
//! underlying QUIC/WSS connection is owned by per-direction reader and
//! writer tasks spawned at connect time, and the public methods on
//! `NetworkClient` are thin wrappers around `tokio::sync::mpsc`
//! channels (which `tokio::select!` documents as cancellation safe).
//! Dropping a recv future does NOT discard partially-consumed bytes
//! from the underlying stream — those live inside the spawned reader
//! task, which keeps reading regardless of whether the application
//! is currently awaiting `recv`.
//!
//! This mirrors the bridge pattern already used by the accept side
//! (`network/accept.rs`) and the peer-mesh (`peer/handler.rs`):
//! reader/writer tasks shuttle bytes between the wire transport and
//! mpsc channels, and the application interacts only with channels.

use std::net::SocketAddr;
use std::time::Duration;

use dynrunner_core::{Identifier, MessageReceiver, MessageSender};
use dynrunner_protocol_primary_secondary::DistributedMessage;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

use crate::framing;
use crate::transport::QuicConnection;
use crate::wss::{WssConnection, connect_wss};

/// Outgoing-channel payload for the bridged writer task.
///
/// `Msg` carries an application message that the writer serializes
/// and writes to the wire. `Flush` carries a oneshot that the writer
/// signals after every preceding `Msg` has been written — this is the
/// rendezvous primitive that backs [`MessageSender::flush`].
///
/// Because the channel is strictly FIFO, sending a `Flush(tx)` after
/// N `Msg(...)` enqueues guarantees the oneshot fires only after all
/// N messages have been serialized and pushed to the underlying
/// transport. The writer signals the oneshot even if its own
/// outbound write fails (the caller wants to unblock; the error path
/// is captured elsewhere via the next `send` returning
/// "transport writer task exited").
enum Outgoing<I: Identifier> {
    // `Msg` is boxed so the enum stack size matches `Flush`'s 8 bytes
    // rather than carrying a ~332-byte `DistributedMessage` inline
    // through every mpsc slot (clippy::large_enum_variant).
    Msg(Box<DistributedMessage<I>>),
    Flush(oneshot::Sender<()>),
}

/// A bidirectional, mpsc-bridged connection. Reader and writer tasks
/// own the underlying transport streams and stay alive for the
/// lifetime of this struct; aborted on `Drop`.
pub struct BridgedConnection<I: Identifier> {
    outgoing_tx: mpsc::UnboundedSender<Outgoing<I>>,
    incoming_rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
    reader: JoinHandle<()>,
    writer: JoinHandle<()>,
}

impl<I: Identifier> Drop for BridgedConnection<I> {
    fn drop(&mut self) {
        // Best-effort cleanup. mpsc senders dropping naturally signals
        // the writer task to exit; aborting is belt-and-suspenders.
        self.reader.abort();
        self.writer.abort();
    }
}

pub enum NetworkClient<I: Identifier> {
    Quic(BridgedConnection<I>),
    Wss(BridgedConnection<I>),
}

impl<I: Identifier> NetworkClient<I> {
    /// Connect to `addr` using QUIC (with `peer_cert` for TLS verification),
    /// falling back to WSS if QUIC fails within `timeout`.
    pub async fn connect(
        addr: SocketAddr,
        server_name: &str,
        peer_cert: &rustls::pki_types::CertificateDer<'_>,
        timeout: Duration,
    ) -> Result<Self, String> {
        // Try QUIC first
        match tokio::time::timeout(
            timeout,
            crate::transport::connect(addr, server_name, peer_cert),
        )
        .await
        {
            Ok(Ok(conn)) => {
                tracing::info!(%addr, "connected via QUIC (UDP)");
                return Ok(NetworkClient::Quic(spawn_quic_bridge(conn)));
            }
            Ok(Err(e)) => {
                tracing::warn!(%addr, error = %e, "QUIC failed, falling back to WSS");
            }
            Err(_) => {
                tracing::warn!(%addr, "QUIC timed out, falling back to WSS");
            }
        }

        // Fallback to WSS
        match tokio::time::timeout(timeout, connect_wss(addr)).await {
            Ok(Ok(conn)) => {
                tracing::info!(%addr, "connected via WSS (TCP)");
                Ok(NetworkClient::Wss(spawn_wss_bridge(conn)))
            }
            Ok(Err(e)) => Err(format!(
                "both QUIC and WSS failed for {addr}: WSS error: {e}"
            )),
            Err(_) => Err(format!("both QUIC and WSS timed out for {addr}")),
        }
    }

    /// Connect using WSS only (no QUIC attempt).
    pub async fn connect_wss_only(addr: SocketAddr) -> Result<Self, String> {
        let conn = connect_wss(addr).await?;
        tracing::info!(%addr, "connected via WSS (TCP)");
        Ok(NetworkClient::Wss(spawn_wss_bridge(conn)))
    }

    /// True iff this client is using QUIC. (WSS is the fallback.)
    pub fn is_quic(&self) -> bool {
        matches!(self, NetworkClient::Quic(_))
    }

    fn bridge(&self) -> &BridgedConnection<I> {
        match self {
            NetworkClient::Quic(b) | NetworkClient::Wss(b) => b,
        }
    }

    fn bridge_mut(&mut self) -> &mut BridgedConnection<I> {
        match self {
            NetworkClient::Quic(b) | NetworkClient::Wss(b) => b,
        }
    }

    /// Mint a cloneable, `DistributedMessage`-typed send handle that
    /// writes to THIS client's wire — without consuming the client.
    ///
    /// The client keeps its own [`MessageSender::send`] path
    /// (`self.bridge_mut().outgoing_tx`); this handle is a second
    /// sender into the SAME writer task's FIFO outgoing channel, so
    /// both feed the one underlying QUIC/WSS connection (a fan-in, not
    /// a second wire). The returned sender accepts a bare
    /// `DistributedMessage<I>`; a small forwarder task wraps each frame
    /// in the internal [`Outgoing::Msg`] envelope and pushes it onto
    /// the client's outgoing channel, preserving send-order with the
    /// client's own sends.
    ///
    /// Used by the secondary mesh to register its dialed primary
    /// connection as a directed-routable mesh member keyed by the
    /// primary's peer-id (so `send_to_peer(primary)` resolves over the
    /// existing bootstrap link) while the bootstrap uplink keeps
    /// owning the wire. The forwarder lives on the same `LocalSet` as
    /// the client's reader/writer tasks; it exits when either end of
    /// its channel closes (the handle is dropped or the client's
    /// writer task is gone).
    pub fn mesh_writer(&self) -> mpsc::UnboundedSender<DistributedMessage<I>> {
        let outgoing_tx = self.bridge().outgoing_tx.clone();
        let (writer_tx, mut writer_rx) = mpsc::unbounded_channel::<DistributedMessage<I>>();
        tokio::task::spawn_local(async move {
            while let Some(msg) = writer_rx.recv().await {
                if outgoing_tx.send(Outgoing::Msg(Box::new(msg))).is_err() {
                    // The client's writer task exited (wire closed).
                    break;
                }
            }
        });
        writer_tx
    }
}

impl<I: Identifier> MessageSender<DistributedMessage<I>> for NetworkClient<I> {
    async fn send(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        // The mpsc send is non-blocking and cancel-safe: it either
        // queues immediately or returns an error if the writer task
        // has exited (transport closed).
        self.bridge_mut()
            .outgoing_tx
            .send(Outgoing::Msg(Box::new(msg)))
            .map_err(|_| "transport writer task exited".to_string())
    }

    /// Rendezvous with the writer task: enqueue a `Flush` marker
    /// into the outgoing channel and await its acknowledgement.
    /// Because the channel is FIFO, the writer only fires the
    /// oneshot AFTER every preceding `Msg` has been serialized and
    /// pushed to the underlying `SendStream` / `WebSocketStream`
    /// (i.e. handed off to the OS socket buffer). This is the
    /// rendezvous a clean-shutdown caller needs to ensure a final
    /// message lands on the wire before the runtime tears down and
    /// `Drop` aborts the writer task — see `MessageSender::flush`
    /// trait doc and the natural-quiesce branch in
    /// `secondary/processing.rs`.
    async fn flush(&mut self) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.bridge_mut()
            .outgoing_tx
            .send(Outgoing::Flush(tx))
            .map_err(|_| "transport writer task exited".to_string())?;
        rx.await
            .map_err(|_| "transport writer task exited before flush ack".to_string())
    }
}

impl<I: Identifier> MessageReceiver<DistributedMessage<I>> for NetworkClient<I> {
    async fn recv(&mut self) -> Option<DistributedMessage<I>> {
        // tokio::sync::mpsc::UnboundedReceiver::recv is documented as
        // cancel-safe — dropping the future leaves the channel state
        // intact and the next call resumes from the same position.
        self.bridge_mut().incoming_rx.recv().await
    }
}

/// Handler-provenance tag carried by the framed-IO pump logs. The
/// client dials exactly one server, so there is no per-peer id yet —
/// the pump's `peer` slot carries the same fixed label.
const CTX: &str = "network-client";

/// Spawn reader + writer tasks for a fresh QuicConnection and return
/// the application-side channel pair wrapped in a `BridgedConnection`.
///
/// The reader is `framing::run_quic_reader` (the shared wire-frame
/// policy pump, #366); the writer stays local because it owns the
/// [`Outgoing`] envelope (`Flush` rendezvous), but each `Msg` is
/// encoded through the same `framing::encode_outbound_frames` gate — an
/// unsendable frame is dropped loudly there and the connection kept.
fn spawn_quic_bridge<I: Identifier>(conn: QuicConnection) -> BridgedConnection<I> {
    let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel::<Outgoing<I>>();
    let (incoming_tx, incoming_rx) = mpsc::unbounded_channel::<DistributedMessage<I>>();

    let (send_stream, recv_stream, existing_buf) = conn.into_parts();

    let reader = tokio::task::spawn_local(framing::run_quic_reader(
        recv_stream,
        existing_buf,
        incoming_tx,
        CTX,
        CTX.to_string(),
        framing::new_reassembler(),
    ));

    let writer = tokio::task::spawn_local(async move {
        let mut send = send_stream;
        'pump: while let Some(item) = outgoing_rx.recv().await {
            match item {
                Outgoing::Msg(msg) => {
                    for frame in framing::encode_outbound_frames(&msg, CTX, CTX) {
                        if send.write_all(&frame).await.is_err() {
                            break 'pump;
                        }
                    }
                }
                Outgoing::Flush(ack) => {
                    // FIFO order on the mpsc means every preceding
                    // Msg's write_all has already returned by the
                    // time we get here — i.e. the OS socket buffer
                    // has accepted the bytes. Signal the waiter
                    // regardless of receiver liveness.
                    let _ = ack.send(());
                }
            }
        }
        tracing::debug!("NetworkClient QUIC writer done");
    });

    BridgedConnection {
        outgoing_tx,
        incoming_rx,
        reader,
        writer,
    }
}

/// Spawn reader + writer tasks for a fresh WssConnection. Same shape
/// as [`spawn_quic_bridge`] — shared reader pump, local Flush-aware
/// writer with the shared encode gate.
fn spawn_wss_bridge<I: Identifier>(conn: WssConnection) -> BridgedConnection<I> {
    let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel::<Outgoing<I>>();
    let (incoming_tx, incoming_rx) = mpsc::unbounded_channel::<DistributedMessage<I>>();

    let (mut ws_write, ws_read) = conn.into_inner().split();

    let reader = tokio::task::spawn_local(framing::run_wss_reader(
        ws_read,
        incoming_tx,
        CTX,
        CTX.to_string(),
        framing::new_reassembler(),
    ));

    let writer = tokio::task::spawn_local(async move {
        'pump: while let Some(item) = outgoing_rx.recv().await {
            match item {
                Outgoing::Msg(msg) => {
                    for frame in framing::encode_outbound_frames(&msg, CTX, CTX) {
                        if ws_write.send(Message::Binary(frame.into())).await.is_err() {
                            break 'pump;
                        }
                    }
                }
                Outgoing::Flush(ack) => {
                    // See `spawn_quic_bridge` for the FIFO rationale —
                    // every preceding Msg's `ws_write.send.await` has
                    // returned (i.e. the WebSocket sink has accepted
                    // and flushed the frame to the TCP socket) by the
                    // time we observe this marker.
                    let _ = ack.send(());
                }
            }
        }
        tracing::debug!("NetworkClient WSS writer done");
    });

    BridgedConnection {
        outgoing_tx,
        incoming_rx,
        reader,
        writer,
    }
}
