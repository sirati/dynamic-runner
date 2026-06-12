//! Server accept loops + per-connection handlers.
//!
//! A QUIC and a WSS listener each spawn an accept-loop that hands every
//! new connection to a per-connection handler. The handler reads the
//! first message to learn the connecting peer's id, registers the new
//! connection with the owning `NetworkServer` via `new_conn_tx`, and
//! then runs reader + writer tasks over the underlying transport until
//! the peer disconnects.

use std::time::Duration;

use dynrunner_core::{Identifier, MessageReceiver};
use dynrunner_protocol_primary_secondary::DistributedMessage;
use dynrunner_transport_tunnel::{InboundTap, PeerRegistration, RegistrationSink};
use futures_util::StreamExt;
use tokio::sync::mpsc;

use crate::framing;
use crate::transport::{QuicConnection, QuicListener};
use crate::wss::{WssConnection, WssListener};

/// Handler-provenance tag carried by the framed-IO pump logs.
const CTX: &str = "network-accepted";

/// How long the per-connection handler waits for the peer's first
/// frame (`SecondaryWelcome`) before dropping the connection as
/// non-conformant. The transport can't surface the connection to
/// the coordinator until that first frame arrives — `peer_id`
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
///
/// Resilient (see `crate::accept_loop`): the per-connection handshake
/// runs on the spawned task, so one aborted/stalled inbound can never
/// kill or wedge the listener the secondaries' redials depend on.
pub(super) async fn quic_accept_loop<I: Identifier>(
    listener: QuicListener,
    incoming_tx: InboundTap<I>,
    new_conn_tx: RegistrationSink<I>,
) {
    crate::accept_loop::quic_accept_loop_resilient(listener, CTX, move |conn| {
        let incoming_tx = incoming_tx.clone();
        let new_conn_tx = new_conn_tx.clone();
        async move {
            handle_new_quic_connection(conn, incoming_tx, new_conn_tx).await;
        }
    })
    .await;
}

/// WSS accept loop.
///
/// Resilient — see the QUIC twin above.
pub(super) async fn wss_accept_loop<I: Identifier>(
    listener: WssListener,
    incoming_tx: InboundTap<I>,
    new_conn_tx: RegistrationSink<I>,
) {
    crate::accept_loop::wss_accept_loop_resilient(listener, CTX, move |conn| {
        let incoming_tx = incoming_tx.clone();
        let new_conn_tx = new_conn_tx.clone();
        async move {
            handle_new_wss_connection(conn, incoming_tx, new_conn_tx).await;
        }
    })
    .await;
}

/// Handle a new QUIC connection: read first message to identify the peer,
/// then split into separate reader/writer tasks.
async fn handle_new_quic_connection<I: Identifier>(
    mut conn: QuicConnection,
    incoming_tx: InboundTap<I>,
    new_conn_tx: RegistrationSink<I>,
) {
    // Read first message to identify the peer. Bounded by
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
    let peer_id = first_msg.sender_id().to_string();

    // Per-connection chunk reassembly across the identify→pump
    // boundary (see `peer::accept::handle_accepted_quic`): resolve the
    // first frame through the same seam the pump uses, so a transfer
    // whose chunk 0 IS the first frame reassembles seamlessly.
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

    // Create per-connection write channel
    let (outgoing_tx, outgoing_rx) = mpsc::unbounded_channel::<DistributedMessage<I>>();

    // Register: hand the per-connection writer to the unified
    // transport's `recv_peer` demux (it inserts into the shared
    // writer table). Emitted immediately after the first frame
    // (`SecondaryWelcome`) and before any further frame, so the
    // demux registers the writer before the peer's
    // CertExchange / TaskRequest traffic needs a reply path.
    if new_conn_tx
        .send(PeerRegistration {
            peer_id: peer_id.clone(),
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
    let mut reader = tokio::task::spawn_local(framing::run_quic_reader(
        recv_stream,
        existing_buf,
        incoming_tx,
        CTX,
        peer_id.clone(),
        reassembler,
    ));

    // Writer task: drain outgoing channel, write to QUIC send stream
    let mut writer = tokio::task::spawn_local(framing::run_quic_writer(
        send_stream,
        outgoing_rx,
        CTX,
        peer_id,
    ));

    // Wait for either task to finish, then abort the other
    tokio::select! {
        _ = &mut reader => { writer.abort(); }
        _ = &mut writer => { reader.abort(); }
    }
}

/// Handle a new WSS connection: read first message to identify the peer,
/// then split the WebSocket stream into reader/writer halves.
async fn handle_new_wss_connection<I: Identifier>(
    mut conn: WssConnection,
    incoming_tx: InboundTap<I>,
    new_conn_tx: RegistrationSink<I>,
) {
    // Read first message to identify the peer. Bounded by
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
    let peer_id = first_msg.sender_id().to_string();

    // Per-connection chunk reassembly across the identify→pump
    // boundary (see `peer::accept::handle_accepted_quic`): resolve the
    // first frame through the same seam the pump uses, so a transfer
    // whose chunk 0 IS the first frame reassembles seamlessly.
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

    // Create per-connection write channel
    let (outgoing_tx, outgoing_rx) = mpsc::unbounded_channel::<DistributedMessage<I>>();

    // Register: hand the per-connection writer to the unified
    // transport's `recv_peer` demux (it inserts into the shared
    // writer table). Emitted immediately after the first frame
    // (`SecondaryWelcome`) and before any further frame, so the
    // demux registers the writer before the peer's
    // CertExchange / TaskRequest traffic needs a reply path.
    if new_conn_tx
        .send(PeerRegistration {
            peer_id: peer_id.clone(),
            outgoing_tx,
        })
        .is_err()
    {
        return;
    }

    // Split the WebSocket stream into independent read/write halves
    let (ws_write, ws_read) = conn.into_inner().split();

    // Reader task: read from WebSocket, decode, forward to incoming
    let mut reader = tokio::task::spawn_local(framing::run_wss_reader(
        ws_read,
        incoming_tx,
        CTX,
        peer_id.clone(),
        reassembler,
    ));

    // Writer task: drain outgoing channel, write to WebSocket
    let mut writer = tokio::task::spawn_local(framing::run_wss_writer(
        ws_write,
        outgoing_rx,
        CTX,
        peer_id,
    ));

    // Wait for either task to finish, then abort the other
    tokio::select! {
        _ = &mut reader => { writer.abort(); }
        _ = &mut writer => { reader.abort(); }
    }
}
