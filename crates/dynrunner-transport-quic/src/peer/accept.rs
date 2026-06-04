//! Peer-server accept loops + per-connection handlers.
//!
//! These are the inbound side of the peer mesh: a QUIC and a WSS listener
//! each spawn an accept-loop that hands every new connection to a
//! per-connection handler. The handler reads the first message to learn
//! the peer's id, registers the new connection with the owning
//! `PeerNetwork` via `new_conn_tx`, and then runs reader + writer tasks
//! over the underlying transport until the peer disconnects.

use dynrunner_core::{Identifier, MessageReceiver};
use dynrunner_protocol_primary_secondary::{DistributedMessage, codec};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::transport::{QuicConnection, QuicListener};
use crate::wss::{WssConnection, WssListener};

use super::{AcceptedPeer, DisconnectedPeer};

pub(super) async fn quic_accept_loop<I: Identifier>(
    listener: QuicListener,
    incoming_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
    new_conn_tx: mpsc::UnboundedSender<AcceptedPeer<I>>,
    disconnect_tx: mpsc::UnboundedSender<DisconnectedPeer<I>>,
) {
    loop {
        match listener.accept().await {
            Ok(conn) => {
                let incoming_tx = incoming_tx.clone();
                let new_conn_tx = new_conn_tx.clone();
                let disconnect_tx = disconnect_tx.clone();
                tokio::task::spawn_local(async move {
                    handle_accepted_quic(conn, incoming_tx, new_conn_tx, disconnect_tx).await;
                });
            }
            Err(e) => {
                tracing::debug!(error = %e, "peer QUIC accept loop ended");
                break;
            }
        }
    }
}

pub(super) async fn wss_accept_loop<I: Identifier>(
    listener: WssListener,
    incoming_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
    new_conn_tx: mpsc::UnboundedSender<AcceptedPeer<I>>,
    disconnect_tx: mpsc::UnboundedSender<DisconnectedPeer<I>>,
) {
    loop {
        match listener.accept().await {
            Ok(conn) => {
                let incoming_tx = incoming_tx.clone();
                let new_conn_tx = new_conn_tx.clone();
                let disconnect_tx = disconnect_tx.clone();
                tokio::task::spawn_local(async move {
                    handle_accepted_wss(conn, incoming_tx, new_conn_tx, disconnect_tx).await;
                });
            }
            Err(e) => {
                tracing::debug!(error = %e, "peer WSS accept loop ended");
                break;
            }
        }
    }
}

async fn handle_accepted_quic<I: Identifier>(
    mut conn: QuicConnection,
    incoming_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
    new_conn_tx: mpsc::UnboundedSender<AcceptedPeer<I>>,
    disconnect_tx: mpsc::UnboundedSender<DisconnectedPeer<I>>,
) {
    // Read first message to identify peer
    let first_msg = match MessageReceiver::<DistributedMessage<I>>::recv(&mut conn).await {
        Some(msg) => msg,
        None => return,
    };
    let peer_id = first_msg.sender_id().to_string();

    if incoming_tx.send(first_msg).is_err() {
        return;
    }

    let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel::<DistributedMessage<I>>();
    // Clone the writer's channel for the disconnect-signal generation
    // check (see `DisconnectedPeer`): the owner prunes only if its live
    // `connections[peer_id]` is STILL this exact channel.
    let supervisor_tx = outgoing_tx.clone();

    if new_conn_tx
        .send(AcceptedPeer {
            peer_id: peer_id.clone(),
            outgoing_tx,
        })
        .is_err()
    {
        return;
    }

    let (send_stream, recv_stream, existing_buf) = conn.into_parts();

    let reader_tx = incoming_tx;
    let reader_id = peer_id.clone();
    let mut reader = tokio::task::spawn_local(async move {
        let mut recv_buf = existing_buf;
        let mut recv = recv_stream;
        loop {
            match codec::decode_frame::<I>(&recv_buf) {
                Ok(Some((msg, consumed))) => {
                    recv_buf.drain(..consumed);
                    if reader_tx.send(msg).is_err() {
                        break;
                    }
                }
                Ok(None) => {
                    let mut tmp = [0u8; 8192];
                    match recv.read(&mut tmp).await {
                        Ok(Some(n)) => recv_buf.extend_from_slice(&tmp[..n]),
                        _ => break,
                    }
                }
                Err(_) => break,
            }
        }
        tracing::debug!(peer = %reader_id, "accepted peer QUIC reader done");
    });

    let writer_id = peer_id.clone();
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
        tracing::debug!(peer = %writer_id, "accepted peer QUIC writer done");
    });

    tokio::select! {
        _ = &mut reader => { writer.abort(); }
        _ = &mut writer => { reader.abort(); }
    }
    // Reader/writer exited (peer gone, or QUIC IDLE_TIMEOUT on a
    // blackholed link): signal the owner to prune+redial.
    let _ = disconnect_tx.send(DisconnectedPeer {
        peer_id,
        outgoing_tx: supervisor_tx,
    });
}

async fn handle_accepted_wss<I: Identifier>(
    mut conn: WssConnection,
    incoming_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
    new_conn_tx: mpsc::UnboundedSender<AcceptedPeer<I>>,
    disconnect_tx: mpsc::UnboundedSender<DisconnectedPeer<I>>,
) {
    // Read first message to identify peer
    let first_msg = match MessageReceiver::<DistributedMessage<I>>::recv(&mut conn).await {
        Some(msg) => msg,
        None => return,
    };
    let peer_id = first_msg.sender_id().to_string();

    if incoming_tx.send(first_msg).is_err() {
        return;
    }

    let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel::<DistributedMessage<I>>();
    // Clone the writer's channel for the disconnect-signal generation
    // check (see `DisconnectedPeer`).
    let supervisor_tx = outgoing_tx.clone();

    if new_conn_tx
        .send(AcceptedPeer {
            peer_id: peer_id.clone(),
            outgoing_tx,
        })
        .is_err()
    {
        return;
    }

    let (mut ws_write, mut ws_read) = conn.into_inner().split();

    let reader_tx = incoming_tx;
    let reader_id = peer_id.clone();
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
                Some(Ok(_)) => continue,
                Some(Err(_)) => break,
            }
        }
        tracing::debug!(peer = %reader_id, "accepted peer WSS reader done");
    });

    let writer_id = peer_id.clone();
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
        tracing::debug!(peer = %writer_id, "accepted peer WSS writer done");
    });

    tokio::select! {
        _ = &mut reader => { writer.abort(); }
        _ = &mut writer => { reader.abort(); }
    }
    // Reader/writer exited: signal the owner to prune+redial.
    let _ = disconnect_tx.send(DisconnectedPeer {
        peer_id,
        outgoing_tx: supervisor_tx,
    });
}
