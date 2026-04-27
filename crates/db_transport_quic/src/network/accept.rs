//! Server accept loops + per-connection handlers.
//!
//! A QUIC and a WSS listener each spawn an accept-loop that hands every
//! new connection to a per-connection handler. The handler reads the
//! first message to learn the secondary's id, registers the new
//! connection with the owning `NetworkServer` via `new_conn_tx`, and
//! then runs reader + writer tasks over the underlying transport until
//! the secondary disconnects.

use db_comm_api_base::{Identifier, MessageReceiver};
use db_primary_secondary_comm::{codec, DistributedMessage};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::transport::{QuicConnection, QuicListener};
use crate::wss::{WssConnection, WssListener};

use super::AcceptedConnection;

/// QUIC accept loop.
pub(super) async fn quic_accept_loop<I: Identifier>(
    listener: QuicListener,
    incoming_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
    new_conn_tx: mpsc::UnboundedSender<AcceptedConnection<I>>,
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
    incoming_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
    new_conn_tx: mpsc::UnboundedSender<AcceptedConnection<I>>,
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
    incoming_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
    new_conn_tx: mpsc::UnboundedSender<AcceptedConnection<I>>,
) {
    // Read first message to identify the secondary
    let first_msg = match MessageReceiver::<DistributedMessage<I>>::recv(&mut conn).await {
        Some(msg) => msg,
        None => return,
    };
    let secondary_id = first_msg.sender_id().to_string();

    // Forward first message
    if incoming_tx.send(first_msg).is_err() {
        return;
    }

    // Create per-connection write channel
    let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel::<DistributedMessage<I>>();

    // Register
    if new_conn_tx
        .send(AcceptedConnection {
            secondary_id: secondary_id.clone(),
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
    incoming_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
    new_conn_tx: mpsc::UnboundedSender<AcceptedConnection<I>>,
) {
    // Read first message to identify the secondary
    let first_msg = match MessageReceiver::<DistributedMessage<I>>::recv(&mut conn).await {
        Some(msg) => msg,
        None => return,
    };
    let secondary_id = first_msg.sender_id().to_string();

    // Forward first message
    if incoming_tx.send(first_msg).is_err() {
        return;
    }

    // Create per-connection write channel
    let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel::<DistributedMessage<I>>();

    // Register
    if new_conn_tx
        .send(AcceptedConnection {
            secondary_id: secondary_id.clone(),
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
