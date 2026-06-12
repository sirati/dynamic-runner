//! Peer-server accept loops + per-connection handlers.
//!
//! These are the inbound side of the peer mesh: a QUIC and a WSS listener
//! each spawn an accept-loop that hands every new connection to a
//! per-connection handler. The handler reads the first message to learn
//! the peer's id, registers the new connection with the owning
//! `PeerNetwork` via `new_conn_tx`, and then runs reader + writer tasks
//! over the underlying transport until the peer disconnects.

use dynrunner_core::{Identifier, MessageReceiver};
use dynrunner_protocol_primary_secondary::{DistributedMessage, InboundTap};
use futures_util::StreamExt;
use tokio::sync::mpsc;

use crate::framing;
use crate::transport::{QuicConnection, QuicListener};
use crate::wss::{WssConnection, WssListener};

use super::{AcceptedPeer, DisconnectedPeer};

/// Handler-provenance tag carried by the framed-IO pump logs.
const CTX: &str = "peer-accepted";

pub(super) async fn quic_accept_loop<I: Identifier>(
    listener: QuicListener,
    incoming_tx: InboundTap<I>,
    new_conn_tx: mpsc::UnboundedSender<AcceptedPeer<I>>,
    disconnect_tx: mpsc::UnboundedSender<DisconnectedPeer<I>>,
) {
    // Resilient loop (see `crate::accept_loop`): the per-connection
    // handshake runs on the spawned task, so one aborted/stalled inbound
    // can never kill or wedge the listener the whole mesh's reconnect
    // machinery depends on.
    crate::accept_loop::quic_accept_loop_resilient(listener, CTX, move |conn| {
        let incoming_tx = incoming_tx.clone();
        let new_conn_tx = new_conn_tx.clone();
        let disconnect_tx = disconnect_tx.clone();
        async move {
            handle_accepted_quic(conn, incoming_tx, new_conn_tx, disconnect_tx).await;
        }
    })
    .await;
}

pub(super) async fn wss_accept_loop<I: Identifier>(
    listener: WssListener,
    incoming_tx: InboundTap<I>,
    new_conn_tx: mpsc::UnboundedSender<AcceptedPeer<I>>,
    disconnect_tx: mpsc::UnboundedSender<DisconnectedPeer<I>>,
) {
    // Resilient loop — see the QUIC twin above.
    crate::accept_loop::wss_accept_loop_resilient(listener, CTX, move |conn| {
        let incoming_tx = incoming_tx.clone();
        let new_conn_tx = new_conn_tx.clone();
        let disconnect_tx = disconnect_tx.clone();
        async move {
            handle_accepted_wss(conn, incoming_tx, new_conn_tx, disconnect_tx).await;
        }
    })
    .await;
}

async fn handle_accepted_quic<I: Identifier>(
    mut conn: QuicConnection,
    incoming_tx: InboundTap<I>,
    new_conn_tx: mpsc::UnboundedSender<AcceptedPeer<I>>,
    disconnect_tx: mpsc::UnboundedSender<DisconnectedPeer<I>>,
) {
    // Read first message to identify peer
    let first_msg = match MessageReceiver::<DistributedMessage<I>>::recv(&mut conn).await {
        Some(msg) => msg,
        None => return,
    };
    let peer_id = first_msg.sender_id().to_string();

    // Per-connection chunk reassembly, created BEFORE the first frame
    // is resolved: if the dialer's first frame is chunk 0 of an
    // oversized transfer (e.g. an immediate snapshot reply), the chunk
    // is buffered here and the SAME reassembler continues the transfer
    // inside the reader pump — the identify→pump boundary is seamless.
    let mut reassembler = framing::new_reassembler();
    match framing::resolve_inbound(first_msg, &mut reassembler, CTX, &peer_id) {
        framing::InboundStep::Deliver(msg) => {
            if incoming_tx.send(msg).is_err() {
                return;
            }
        }
        framing::InboundStep::Consumed => {}
        framing::InboundStep::Fatal => return,
    }

    let (outgoing_tx, outgoing_rx) = mpsc::unbounded_channel::<DistributedMessage<I>>();
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

    let mut reader = tokio::task::spawn_local(framing::run_quic_reader(
        recv_stream,
        existing_buf,
        incoming_tx,
        CTX,
        peer_id.clone(),
        reassembler,
    ));

    let mut writer = tokio::task::spawn_local(framing::run_quic_writer(
        send_stream,
        outgoing_rx,
        CTX,
        peer_id.clone(),
    ));

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
    incoming_tx: InboundTap<I>,
    new_conn_tx: mpsc::UnboundedSender<AcceptedPeer<I>>,
    disconnect_tx: mpsc::UnboundedSender<DisconnectedPeer<I>>,
) {
    // Read first message to identify peer
    let first_msg = match MessageReceiver::<DistributedMessage<I>>::recv(&mut conn).await {
        Some(msg) => msg,
        None => return,
    };
    let peer_id = first_msg.sender_id().to_string();

    // Per-connection chunk reassembly across the identify→pump
    // boundary — see `handle_accepted_quic`.
    let mut reassembler = framing::new_reassembler();
    match framing::resolve_inbound(first_msg, &mut reassembler, CTX, &peer_id) {
        framing::InboundStep::Deliver(msg) => {
            if incoming_tx.send(msg).is_err() {
                return;
            }
        }
        framing::InboundStep::Consumed => {}
        framing::InboundStep::Fatal => return,
    }

    let (outgoing_tx, outgoing_rx) = mpsc::unbounded_channel::<DistributedMessage<I>>();
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

    let (ws_write, ws_read) = conn.into_inner().split();

    let mut reader = tokio::task::spawn_local(framing::run_wss_reader(
        ws_read,
        incoming_tx,
        CTX,
        peer_id.clone(),
        reassembler,
    ));

    let mut writer = tokio::task::spawn_local(framing::run_wss_writer(
        ws_write,
        outgoing_rx,
        CTX,
        peer_id.clone(),
    ));

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
