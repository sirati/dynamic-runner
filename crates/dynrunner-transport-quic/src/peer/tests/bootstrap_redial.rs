//! Tests for the bootstrap-wire re-dial (defect (b)): the secondary
//! re-dials its dropped submitter→primary bootstrap wire and re-folds it
//! into the mesh, so the observer link is restored after the `-R` tunnel
//! is rebuilt.
//!
//! Both tests drive a real WSS wire end-to-end against a test-owned
//! `WssListener` that stays bound across the drop — so the re-dial hits
//! the SAME fixed address (mirroring the fixed `localhost:<tunnel_port>`
//! the tunnel terminates at) without any port-reuse flakiness. The
//! listener owns the server side of the wire, so DROPPING the accepted
//! `WssConnection` is a real, deterministic wire close that surfaces to
//! the folded client's inbound forwarder as `recv() == None`.

use std::net::SocketAddr;
use std::time::Duration;

use super::super::PeerNetwork;
use super::super::bootstrap_redial::{BootstrapDialTarget, BootstrapRedial, redial_bootstrap_wire};
use super::TestId;
use crate::NetworkClient;
use crate::wss::{WssConnection, WssListener};
use dynrunner_core::{MessageReceiver, MessageSender};
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, KeepaliveRole, PeerId, PeerTransport,
};
use tokio::sync::mpsc;

fn keepalive(active_workers: u32) -> DistributedMessage<TestId> {
    DistributedMessage::Keepalive {
        target: None,
        sender_id: "primary".into(),
        timestamp: 1.0,
        secondary_id: "primary".into(),
        active_workers,
        emitter_role: KeepaliveRole::Primary,
    }
}

/// Pump `recv_peer` for a bounded window, returning the next delivered
/// frame if one arrives (or `None` on timeout). Lets the test service the
/// network's `select!` arms — including the bootstrap-redial re-fold arm —
/// without blocking forever.
async fn pump_once(net: &mut PeerNetwork<TestId>) -> Option<DistributedMessage<TestId>> {
    tokio::time::timeout(Duration::from_millis(200), net.recv_peer())
        .await
        .ok()
        .flatten()
}

/// Drive `recv_peer` until `cond` holds or the deadline elapses. Used to
/// wait for the asynchronous re-dial → re-fold cycle to complete.
async fn pump_until(
    net: &mut PeerNetwork<TestId>,
    deadline: Duration,
    mut cond: impl FnMut(&PeerNetwork<TestId>) -> bool,
) -> bool {
    let end = std::time::Instant::now() + deadline;
    while std::time::Instant::now() < end {
        if cond(net) {
            return true;
        }
        let _ = pump_once(net).await;
    }
    cond(net)
}

/// The re-dial supervisor dials the FIXED bootstrap address via
/// `connect_wss_only` and hands back a LIVE client. This is the unit pin
/// of "the bootstrap id becomes redial-eligible with the fixed addr; the
/// supervisor attempts it and yields a usable wire" — independent of the
/// re-fold.
#[tokio::test(flavor = "current_thread")]
async fn redial_supervisor_dials_fixed_addr_and_hands_back_live_client() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let listener = WssListener::bind("127.0.0.1:0".parse().unwrap())
                .await
                .expect("bind test WSS listener");
            let addr: SocketAddr = listener.local_addr();

            let (tx, mut rx) = mpsc::unbounded_channel::<BootstrapRedial<TestId>>();
            let target = BootstrapDialTarget {
                addr,
                primary_id: "primary".to_string(),
            };

            // Run the supervisor; it must dial `addr` and send a redial.
            tokio::task::spawn_local(redial_bootstrap_wire::<TestId>(target, tx));

            // Server side accepts the supervisor's dial.
            let mut server_conn: WssConnection =
                tokio::time::timeout(Duration::from_secs(5), listener.accept())
                    .await
                    .expect("supervisor must dial the fixed addr")
                    .expect("accept the re-dial");

            // The supervisor handed back a live client.
            let redial = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("supervisor must hand back a redial")
                .expect("redial channel open");
            assert_eq!(redial.target.primary_id, "primary");
            assert_eq!(redial.target.addr, addr, "the re-dial used the FIXED addr");

            // The handed-back client is usable: a frame the server sends
            // reaches it.
            MessageSender::send(&mut server_conn, keepalive(3))
                .await
                .expect("server send over the re-dialed wire");
            let mut client = redial.client;
            let got = tokio::time::timeout(Duration::from_secs(5), client.recv())
                .await
                .expect("client recv over the re-dialed wire")
                .expect("a frame must arrive");
            assert!(matches!(
                got,
                DistributedMessage::Keepalive {
                    active_workers: 3,
                    ..
                }
            ));
        })
        .await;
}

/// FULL cycle (defect (b) end-to-end): fold a bootstrap wire, observe the
/// primary as a mesh peer, CLOSE the wire (the `-R` tunnel "drops"), and
/// confirm the secondary RE-DIALS the fixed address and RE-FOLDS the fresh
/// wire — `has_peer("primary")` recovers and inbound frames flow again.
/// The re-fold also re-arms the redial, so the link stays restorable.
///
/// Pre-fix, the inbound forwarder exited silently on wire close and the
/// dial addr was discarded — nobody re-dialed, the primary stayed gone
/// from the mesh forever.
#[tokio::test(flavor = "current_thread")]
async fn dropped_bootstrap_wire_is_redialed_and_refolded() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let listener = WssListener::bind("127.0.0.1:0".parse().unwrap())
                .await
                .expect("bind test WSS listener");
            let addr: SocketAddr = listener.local_addr();

            let mut net: PeerNetwork<TestId> = PeerNetwork::start("sec-0").await.unwrap();

            // ---- Initial fold ----
            // Dial and accept CONCURRENTLY: the WSS handshake the client
            // drives only completes once the server `accept()`s it, so the
            // two must run together (a sequential dial-then-accept would
            // deadlock on the handshake).
            let (client_res, server_res) = tokio::join!(
                NetworkClient::<TestId>::connect_wss_only(addr),
                listener.accept()
            );
            let client = client_res.expect("initial bootstrap dial");
            let mut server_conn: WssConnection = server_res.expect("accept initial bootstrap wire");
            net.register_primary_link("primary".to_string(), addr, client);

            assert!(
                PeerTransport::<TestId>::has_peer(&net, &PeerId::from("primary")),
                "the folded primary must be a mesh member"
            );

            // Inbound forwarding works on the folded wire.
            MessageSender::send(&mut server_conn, keepalive(1))
                .await
                .expect("server send pre-drop");
            let got = pump_once(&mut net)
                .await
                .expect("pre-drop frame must arrive");
            assert!(matches!(
                got,
                DistributedMessage::Keepalive {
                    active_workers: 1,
                    ..
                }
            ));

            // ---- Wire drop ----
            // Dropping the server side closes the wire; the folded client's
            // inbound forwarder sees recv()==None and arms the re-dial. The
            // LISTENER stays bound, so the re-dial hits the same addr.
            drop(server_conn);

            // ---- Re-dial → re-fold ----
            // Accept the re-dial the supervisor makes against the listener.
            let mut server_conn2: WssConnection =
                tokio::time::timeout(Duration::from_secs(5), listener.accept())
                    .await
                    .expect("the secondary must RE-DIAL the dropped bootstrap wire")
                    .expect("accept the re-dial");

            // Drive recv_peer so the re-dial handoff arm re-folds the fresh
            // client. `has_peer("primary")` must recover.
            let refolded = pump_until(&mut net, Duration::from_secs(5), |n| {
                PeerTransport::<TestId>::has_peer(n, &PeerId::from("primary"))
            })
            .await;
            assert!(
                refolded,
                "the re-dialed bootstrap wire must be re-folded into the mesh"
            );

            // Inbound forwarding works again over the re-folded wire — the
            // observer link is genuinely restored, not just present in the
            // table.
            MessageSender::send(&mut server_conn2, keepalive(9))
                .await
                .expect("server send post-refold");
            let mut delivered = None;
            for _ in 0..25 {
                if let Some(DistributedMessage::Keepalive {
                    active_workers: 9, ..
                }) = pump_once(&mut net).await
                {
                    delivered = Some(());
                    break;
                }
            }
            assert!(
                delivered.is_some(),
                "a frame over the re-folded wire must reach recv_peer — inbound forwarding restored"
            );
        })
        .await;
}
