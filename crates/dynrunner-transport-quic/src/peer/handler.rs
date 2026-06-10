//! Per-connection reader/writer task pair for outgoing peer connections.
//!
//! The accept-side equivalent lives in `accept.rs` (it has slightly
//! different setup because the handshake also has to register the new
//! connection with the owning `PeerNetwork`); this module only handles
//! connections we initiated via `connect_to_peers`.
//!
//! The reader/writer pump loops themselves live in `crate::framing`
//! (the wire-frame size policy + loud-failure module, #366); this
//! module owns only the task supervision + disconnect signalling.

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::DistributedMessage;
use tokio::sync::mpsc;

use crate::framing;

use super::DisconnectedPeer;
use super::util::PeerConnection;

/// Spawn reader/writer tasks for an outgoing connection. Returns the
/// `outgoing_tx` channel the caller pushes outbound messages into.
///
/// When the reader OR writer task exits — including on the QUIC
/// `IDLE_TIMEOUT` that fires when a blackholed link stops acking
/// keep-alive PINGs (see `certs.rs`) — the supervisor fires a
/// [`DisconnectedPeer`] through `disconnect_tx` so the owning
/// `PeerNetwork::recv_peer` runs prune+redial. The supervisor carries a
/// clone of `outgoing_tx` for the owner's generation check.
pub(super) fn spawn_outgoing_handler<I: Identifier>(
    peer_id: String,
    connection: PeerConnection,
    incoming_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
    disconnect_tx: mpsc::UnboundedSender<DisconnectedPeer<I>>,
) -> mpsc::UnboundedSender<DistributedMessage<I>> {
    const CTX: &str = "peer-outgoing";
    let (outgoing_tx, outgoing_rx) = mpsc::unbounded_channel::<DistributedMessage<I>>();

    let (mut reader, mut writer) = match connection {
        PeerConnection::Quic(conn) => {
            let (send_stream, recv_stream, existing_buf) = conn.into_parts();
            let reader = tokio::task::spawn_local(framing::run_quic_reader(
                recv_stream,
                existing_buf,
                incoming_tx,
                CTX,
                peer_id.clone(),
            ));
            let writer = tokio::task::spawn_local(framing::run_quic_writer(
                send_stream,
                outgoing_rx,
                CTX,
                peer_id.clone(),
            ));
            (reader, writer)
        }
        PeerConnection::Wss(conn) => {
            use futures_util::StreamExt;
            let (ws_write, ws_read) = conn.into_inner().split();
            let reader = tokio::task::spawn_local(framing::run_wss_reader(
                ws_read,
                incoming_tx,
                CTX,
                peer_id.clone(),
            ));
            let writer = tokio::task::spawn_local(framing::run_wss_writer(
                ws_write,
                outgoing_rx,
                CTX,
                peer_id.clone(),
            ));
            (reader, writer)
        }
    };

    let supervisor_tx = outgoing_tx.clone();
    tokio::task::spawn_local(async move {
        tokio::select! {
            _ = &mut reader => { writer.abort(); }
            _ = &mut writer => { reader.abort(); }
        }
        let _ = disconnect_tx.send(DisconnectedPeer {
            peer_id,
            outgoing_tx: supervisor_tx,
        });
    });

    outgoing_tx
}
