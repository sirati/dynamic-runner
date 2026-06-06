//! Honest-liveness: reader/writer-exit is the AUTHORITATIVE disconnect
//! detector, and the prune disposition is generation-checked so a stale
//! signal can't delete a freshly-reconnected entry.
//!
//! - `reader_exit_prunes_connection`: end-to-end — once a peer's
//!   connection dies, the surviving peer's reader-exit supervisor fires
//!   the disconnect signal and `recv_peer` prunes the dead entry WITHOUT
//!   any outbound send having to fail first (the pre-fix prune path).
//! - `stale_disconnect_does_not_prune_reconnected_entry`: the
//!   `same_channel` generation check — a disconnect signal carrying the
//!   OLD channel is a no-op against a re-inserted fresh channel.

use std::net::SocketAddr;
use std::time::Duration;

use super::super::PeerNetwork;
use super::TestId;
use dynrunner_core::MessageSender;
use dynrunner_protocol_primary_secondary::{DistributedMessage, KeepaliveRole, PeerTransport};
use tokio::sync::mpsc;

/// End-to-end reader-exit prune. A test-owned raw QUIC client dials
/// peer-b's accept loop and sends a first frame so peer-b registers it
/// as `peer-x`. We then DROP the raw client connection — its QUIC
/// streams close, peer-b's accept-side reader hits EOF, the supervisor
/// fires a `DisconnectedPeer`, and peer-b's next `recv_peer` poll runs
/// `handle_peer_disconnect` and prunes `peer-x`.
///
/// This proves the AUTHORITATIVE close-side detector works WITHOUT any
/// outbound send from peer-b failing first (the pre-fix path). The raw
/// client is owned by the test (not a `spawn_local` task that would
/// outlive a dropped `PeerNetwork`), so dropping it is a real,
/// deterministic connection close.
#[tokio::test(flavor = "current_thread")]
async fn reader_exit_prunes_connection() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut peer_b: PeerNetwork<TestId> = PeerNetwork::start("peer-b").await.unwrap();
            let port_b = peer_b.port();
            let cert_b = peer_b.cert_der().clone();

            // Raw client dial into peer-b's QUIC accept loop.
            let addr: SocketAddr = format!("127.0.0.1:{port_b}").parse().unwrap();
            let mut client = crate::transport::connect(addr, "peer-b", &cert_b)
                .await
                .expect("raw QUIC dial to peer-b should connect");

            // First frame identifies the connecting peer as "peer-x";
            // peer-b's accept handler reads it, forwards it, and emits the
            // AcceptedPeer registration.
            let first: DistributedMessage<TestId> = DistributedMessage::Keepalive {
                target: None,
                sender_id: "peer-x".into(),
                timestamp: 1.0,
                secondary_id: "peer-x".into(),
                active_workers: 0,
                emitter_role: KeepaliveRole::Secondary,
            };
            MessageSender::send(&mut client, first)
                .await
                .expect("first frame send should succeed");

            // Surface the registration via recv_peer (yields the frame
            // and drains the AcceptedPeer).
            let _ = tokio::time::timeout(Duration::from_secs(5), peer_b.recv_peer())
                .await
                .expect("timeout waiting for first frame")
                .expect("no first frame");
            assert_eq!(
                PeerTransport::<TestId>::peer_count(&peer_b),
                1,
                "peer-b should have peer-x connected before the drop"
            );

            // Close the wire: dropping the raw client tears down its QUIC
            // streams + endpoint, so peer-b's accept-side reader sees EOF.
            drop(client);

            // Drive peer-b's recv_peer until the disconnect signal is
            // drained and peer-x is pruned. A bounded poll lets the
            // select! service the disconnect_rx arm; the timeout returns
            // control so we can re-check peer_count.
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                let _ = tokio::time::timeout(Duration::from_millis(100), peer_b.recv_peer()).await;
                if PeerTransport::<TestId>::peer_count(&peer_b) == 0 {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "peer-b should have pruned peer-x after its reader exited; \
                     peer_count={}",
                    PeerTransport::<TestId>::peer_count(&peer_b)
                );
            }
        })
        .await;
}

/// Generation check: `handle_peer_disconnect` prunes only when the live
/// `connections` entry is STILL the dead channel. A disconnect signal
/// carrying a torn-down connection's channel must NOT delete a
/// freshly-reconnected entry for the same peer.
#[tokio::test(flavor = "current_thread")]
async fn stale_disconnect_does_not_prune_reconnected_entry() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut net: PeerNetwork<TestId> = PeerNetwork::start("self").await.unwrap();

            // The "old" (dead) connection's writer channel.
            let (old_tx, _old_rx) = mpsc::unbounded_channel::<DistributedMessage<TestId>>();
            // A "fresh" reconnect installed a different channel under the
            // same peer id.
            let (fresh_tx, _fresh_rx) = mpsc::unbounded_channel::<DistributedMessage<TestId>>();

            net.connections
                .insert("peer-x".to_string(), fresh_tx.clone());

            // Stale signal for the OLD channel: must be a no-op.
            net.handle_peer_disconnect("peer-x", &old_tx);
            assert_eq!(
                PeerTransport::<TestId>::peer_count(&net),
                1,
                "stale disconnect for the old channel must not prune the fresh entry"
            );

            // Signal carrying the CURRENT channel: must prune.
            net.handle_peer_disconnect("peer-x", &fresh_tx);
            assert_eq!(
                PeerTransport::<TestId>::peer_count(&net),
                0,
                "disconnect for the live channel must prune the entry"
            );
        })
        .await;
}
