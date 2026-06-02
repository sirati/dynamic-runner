//! Server accept loops + per-connection handlers.
//!
//! A QUIC and a WSS listener each spawn an accept-loop that hands every
//! new connection to a per-connection handler. The handler reads the
//! first message to learn the secondary's id, registers the new
//! connection with the owning `NetworkServer` via `new_conn_tx`, and
//! then runs reader + writer tasks over the underlying transport until
//! the secondary disconnects.

use std::time::Duration;

use dynrunner_core::{Identifier, MessageReceiver};
use dynrunner_protocol_primary_secondary::{codec, DistributedMessage};
use dynrunner_transport_tunnel::{InboundTap, PeerRegistration, RegistrationSink};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::transport::{QuicConnection, QuicListener};
use crate::wss::{WssConnection, WssListener};

/// How long the per-connection handler waits for the peer's first
/// frame (`SecondaryWelcome`) before dropping the connection as
/// non-conformant. The transport can't surface the connection to
/// the coordinator until that first frame arrives — `secondary_id`
/// is read from `first_msg.sender_id()` — so without a deadline,
/// a peer that completes the WSS/QUIC handshake but never speaks
/// the runner protocol leaves the coordinator's
/// `wait_for_connections` blocked at "0/N" until its much coarser
/// `connect_timeout` (default 600s) fires with a misleading
/// "no secondaries connected" message.
///
/// 60 seconds is well above any reasonable Welcome-emit cost
/// (the secondary builds a small JSON message after worker init
/// completes); the cause of timeouts at this point is structural,
/// not slow.
const WELCOME_TIMEOUT: Duration = Duration::from_secs(60);

/// QUIC accept loop.
pub(super) async fn quic_accept_loop<I: Identifier>(
    listener: QuicListener,
    incoming_tx: InboundTap<I>,
    new_conn_tx: RegistrationSink<I>,
) {
    loop {
        match listener.accept().await {
            Ok(conn) => {
                let incoming_tx = incoming_tx.clone();
                let new_conn_tx = new_conn_tx.clone();
                tokio::task::spawn_local(async move {
                    handle_new_quic_connection(conn, incoming_tx, new_conn_tx).await;
                });
            }
            Err(e) => {
                tracing::error!(error = %e, "QUIC accept error");
                break;
            }
        }
    }
}

/// WSS accept loop.
pub(super) async fn wss_accept_loop<I: Identifier>(
    listener: WssListener,
    incoming_tx: InboundTap<I>,
    new_conn_tx: RegistrationSink<I>,
) {
    loop {
        match listener.accept().await {
            Ok(conn) => {
                let incoming_tx = incoming_tx.clone();
                let new_conn_tx = new_conn_tx.clone();
                tokio::task::spawn_local(async move {
                    handle_new_wss_connection(conn, incoming_tx, new_conn_tx).await;
                });
            }
            Err(e) => {
                tracing::error!(error = %e, "WSS accept error");
                break;
            }
        }
    }
}

/// Handle a new QUIC connection: read first message to identify secondary,
/// then split into separate reader/writer tasks.
async fn handle_new_quic_connection<I: Identifier>(
    mut conn: QuicConnection,
    incoming_tx: InboundTap<I>,
    new_conn_tx: RegistrationSink<I>,
) {
    // Read first message to identify the secondary. Bounded by
    // WELCOME_TIMEOUT so a non-conformant peer (handshake-completes
    // but never sends Welcome — usually because its worker_module
    // doesn't complete the runner-protocol Ready handshake and the
    // secondary hangs in `WorkerPool::wait_for_all_ready`) drops
    // here instead of pinning the coordinator at "0/N".
    let first_msg = match tokio::time::timeout(
        WELCOME_TIMEOUT,
        MessageReceiver::<DistributedMessage<I>>::recv(&mut conn),
    )
    .await
    {
        Ok(Some(msg)) => msg,
        Ok(None) => return,
        Err(_) => {
            tracing::error!(
                timeout_s = WELCOME_TIMEOUT.as_secs(),
                "QUIC peer connected but did not send SecondaryWelcome \
                 within {}s; closing as non-conformant. Most likely cause: \
                 the consumer worker_module on the secondary side does not \
                 complete the runner protocol's initial Ready handshake on \
                 stdin/stdout, so the secondary hangs in \
                 `WorkerPool::wait_for_all_ready` and never reaches \
                 `send_welcome`. Make sure the worker_module imports the \
                 framework's worker-protocol library and emits Ready \
                 before doing any long-running work.",
                WELCOME_TIMEOUT.as_secs()
            );
            return;
        }
    };
    let secondary_id = first_msg.sender_id().to_string();

    // Forward first message
    if incoming_tx.send(first_msg).is_err() {
        return;
    }

    // Create per-connection write channel
    let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel::<DistributedMessage<I>>();

    // Register: hand the per-connection writer to the unified
    // transport's `recv_peer` demux (it inserts into the shared
    // writer table). Emitted immediately after the first frame
    // (`SecondaryWelcome`) and before any further frame, so the
    // demux registers the writer before the secondary's
    // CertExchange / TaskRequest traffic needs a reply path.
    if new_conn_tx
        .send(PeerRegistration {
            peer_id: secondary_id.clone(),
            outgoing_tx,
        })
        .is_err()
    {
        return;
    }

    // QUIC has separate send/recv streams, so we can split safely.
    // Extract the inner streams along with any already-buffered data.
    let (send_stream, recv_stream, existing_buf) = conn.into_parts();

    // Reader task: read from QUIC recv stream, forward to incoming
    let reader_tx = incoming_tx;
    let reader_id = secondary_id.clone();
    let mut reader = tokio::task::spawn_local(async move {
        let mut recv_buf = existing_buf;
        let mut recv = recv_stream;
        loop {
            // Try to decode a complete frame from the buffer
            match codec::decode_frame::<I>(&recv_buf) {
                Ok(Some((msg, consumed))) => {
                    recv_buf.drain(..consumed);
                    if reader_tx.send(msg).is_err() {
                        break;
                    }
                }
                Ok(None) => {
                    // Need more data
                    let mut tmp = [0u8; 8192];
                    match recv.read(&mut tmp).await {
                        Ok(Some(n)) => recv_buf.extend_from_slice(&tmp[..n]),
                        _ => break,
                    }
                }
                Err(_) => break,
            }
        }
        tracing::debug!(secondary = %reader_id, "QUIC reader done");
    });

    // Writer task: drain outgoing channel, write to QUIC send stream
    let writer_id = secondary_id;
    let mut writer = tokio::task::spawn_local(async move {
        let mut send = send_stream;
        while let Some(msg) = outgoing_rx.recv().await {
            match codec::serialize_message(&msg) {
                Ok(frame) => {
                    if send.write_all(&frame).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        tracing::debug!(secondary = %writer_id, "QUIC writer done");
    });

    // Wait for either task to finish, then abort the other
    tokio::select! {
        _ = &mut reader => { writer.abort(); }
        _ = &mut writer => { reader.abort(); }
    }
}

/// Handle a new WSS connection: read first message to identify secondary,
/// then split the WebSocket stream into reader/writer halves.
async fn handle_new_wss_connection<I: Identifier>(
    mut conn: WssConnection,
    incoming_tx: InboundTap<I>,
    new_conn_tx: RegistrationSink<I>,
) {
    // Read first message to identify the secondary. Bounded by
    // WELCOME_TIMEOUT — see `handle_new_quic_connection`'s matching
    // block for the rationale (non-conformant worker_module
    // diagnosis).
    let first_msg = match tokio::time::timeout(
        WELCOME_TIMEOUT,
        MessageReceiver::<DistributedMessage<I>>::recv(&mut conn),
    )
    .await
    {
        Ok(Some(msg)) => msg,
        Ok(None) => return,
        Err(_) => {
            tracing::error!(
                timeout_s = WELCOME_TIMEOUT.as_secs(),
                "WSS peer connected but did not send SecondaryWelcome \
                 within {}s; closing as non-conformant. Most likely cause: \
                 the consumer worker_module on the secondary side does not \
                 complete the runner protocol's initial Ready handshake on \
                 stdin/stdout, so the secondary hangs in \
                 `WorkerPool::wait_for_all_ready` and never reaches \
                 `send_welcome`. Make sure the worker_module imports the \
                 framework's worker-protocol library and emits Ready \
                 before doing any long-running work.",
                WELCOME_TIMEOUT.as_secs()
            );
            return;
        }
    };
    let secondary_id = first_msg.sender_id().to_string();

    // Forward first message
    if incoming_tx.send(first_msg).is_err() {
        return;
    }

    // Create per-connection write channel
    let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel::<DistributedMessage<I>>();

    // Register: hand the per-connection writer to the unified
    // transport's `recv_peer` demux (it inserts into the shared
    // writer table). Emitted immediately after the first frame
    // (`SecondaryWelcome`) and before any further frame, so the
    // demux registers the writer before the secondary's
    // CertExchange / TaskRequest traffic needs a reply path.
    if new_conn_tx
        .send(PeerRegistration {
            peer_id: secondary_id.clone(),
            outgoing_tx,
        })
        .is_err()
    {
        return;
    }

    // Split the WebSocket stream into independent read/write halves
    let (mut ws_write, mut ws_read) = conn.into_inner().split();

    // Reader task: read from WebSocket, decode, forward to incoming
    let reader_tx = incoming_tx;
    let reader_id = secondary_id.clone();
    let mut reader = tokio::task::spawn_local(async move {
        loop {
            match ws_read.next().await {
                Some(Ok(Message::Binary(data))) => match codec::decode_frame::<I>(&data) {
                    Ok(Some((msg, _))) => {
                        if reader_tx.send(msg).is_err() {
                            break;
                        }
                    }
                    Ok(None) => continue,
                    Err(_) => break,
                },
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(_)) => continue, // skip ping/pong/text
                Some(Err(_)) => break,
            }
        }
        tracing::debug!(secondary = %reader_id, "WSS reader done");
    });

    // Writer task: drain outgoing channel, write to WebSocket
    let writer_id = secondary_id;
    let mut writer = tokio::task::spawn_local(async move {
        while let Some(msg) = outgoing_rx.recv().await {
            match codec::serialize_message(&msg) {
                Ok(frame) => {
                    if ws_write.send(Message::Binary(frame.into())).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        tracing::debug!(secondary = %writer_id, "WSS writer done");
    });

    // Wait for either task to finish, then abort the other
    tokio::select! {
        _ = &mut reader => { writer.abort(); }
        _ = &mut writer => { reader.abort(); }
    }
}
