//! Full-mesh dialing + symmetric duplicate-connection tiebreak.
//!
//! Full-mesh: EVERY node dials EVERY peer regardless of id order, so the
//! old lower-id-dials asymmetry (and the rosterless-joiner `dial_all_seeds`
//! override that escaped it) is gone — `start_joining` now equals `start`.
//! Both ends of a pair dial, so a pair can momentarily hold TWO connections
//! at each end (our outbound dial + our inbound accept of the peer's dial).
//! `register_accepted`'s SYMMETRIC tiebreak deterministically keeps the
//! connection INITIATED BY THE LOWER-ID NODE on BOTH ends, so the two
//! endpoints converge on the SAME physical wire (never each-keeps-its-own,
//! which would disconnect them).
//!
//! These tests pin:
//!   1. SWEEP — a node whose id sorts ABOVE a peer DIALS it (the case the
//!      old lower-id rule left parked awaiting-inbound).
//!   2. REAL WIRE — a higher-id node dialing a lower-id peer forms the leg
//!      in both directions (case the old rule could never form on its own).
//!   3. start_joining == start — the joiner override is a no-op now.
//!   4. MUTUAL-DIAL COLLISION — both ends dial; the symmetric tiebreak keeps
//!      the lower-id node's connection on BOTH ends (convergence), and the
//!      losing connection is dropped.

use std::time::Duration;

use super::super::{AcceptedPeer, PeerNetwork};
use super::TestId;
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, KeepaliveRole, PeerConnectionInfo, PeerTransport,
};
use tokio::sync::mpsc;

fn pinfo(id: &str) -> PeerConnectionInfo {
    PeerConnectionInfo {
        secondary_id: id.into(),
        cert: String::new(),
        // Unroutable test address: dial tasks spawned against it just
        // fail in the background after the test ends; the sweep-summary
        // assertions never await the dials.
        ipv4: Some("10.255.255.254".into()),
        ipv6: None,
        port: 59124,
        is_observer: false,
        liveness_port: None,
        slurm_job_id: None,
    }
}

fn keepalive(sender: &str, ts: f64) -> DistributedMessage<TestId> {
    DistributedMessage::Keepalive {
        target: None,
        sender_id: sender.into(),
        timestamp: ts,
        secondary_id: sender.into(),
        active_workers: 0,
        emitter_role: KeepaliveRole::Secondary,
    }
}

/// SWEEP: a node whose id sorts ABOVE every listed peer DIALS each one —
/// the exact case the old lower-id-dials rule parked awaiting-inbound (the
/// connection gap that only healed if the higher-id peer happened to dial).
#[tokio::test(flavor = "current_thread")]
async fn highest_id_dials_every_lower_peer() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut net: PeerNetwork<TestId> = PeerNetwork::start("peer-z", None).await.unwrap();
            let summary = net.connect_to_peers_inner(&[pinfo("peer-a"), pinfo("peer-b")]);
            assert_eq!(summary.listed, 2, "both lower-id peers listed (self excluded)");
            assert_eq!(
                summary.spawned, 2,
                "full-mesh: a higher-id node must dial EVERY lower-id peer; got {summary:#?}"
            );
        })
        .await;
}

/// `start_joining` is now identical to `start`: both dial every peer. Pins
/// that the retired joiner override did not change observable behaviour.
#[tokio::test(flavor = "current_thread")]
async fn start_joining_dials_like_start() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut joining: PeerNetwork<TestId> =
                PeerNetwork::start_joining("peer-z", None).await.unwrap();
            let mut steady: PeerNetwork<TestId> =
                PeerNetwork::start("peer-z", None).await.unwrap();
            let s_join = joining.connect_to_peers_inner(&[pinfo("peer-a"), pinfo("peer-b")]);
            let s_steady = steady.connect_to_peers_inner(&[pinfo("peer-a"), pinfo("peer-b")]);
            assert_eq!(
                s_join.spawned, s_steady.spawned,
                "start_joining and start must spawn the same dials under full mesh"
            );
            assert_eq!(s_join.spawned, 2);
        })
        .await;
}

/// REAL WIRE (case a — a node dials a LOWER-id peer, which the old rule
/// never did): a higher-id node dials a lower-id seed and the leg forms in
/// BOTH directions. Here the seed has NO roster entry for the dialer, so the
/// leg can come ONLY from the higher-id node's dial — the previously-broken
/// direction.
#[tokio::test(flavor = "current_thread")]
async fn higher_id_dials_lower_id_peer_forms_real_leg() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // `seed-z` (higher id) dials `seed-a` (lower id) — the dial the
            // old lower-id-dials rule suppressed.
            let mut dialer: PeerNetwork<TestId> =
                PeerNetwork::start("seed-z", None).await.unwrap();
            let mut seed: PeerNetwork<TestId> =
                PeerNetwork::start("seed-a", None).await.unwrap();

            let seed_info = PeerConnectionInfo {
                secondary_id: "seed-a".into(),
                cert: seed.cert_pem().to_string(),
                ipv4: Some("127.0.0.1".into()),
                ipv6: None,
                port: seed.port(),
                is_observer: false,
                liveness_port: None,
                slurm_job_id: None,
            };

            // The higher-id node dials the lower-id seed. The seed is given
            // NO roster entry for the dialer — it never dials, so the only
            // path to a leg is the higher-id node's dial.
            dialer.connect_to_peers(std::slice::from_ref(&seed_info));

            let deadline = std::time::Instant::now() + Duration::from_secs(15);
            let mut ka_ts = 1.0f64;
            loop {
                let _ = tokio::time::timeout(Duration::from_millis(50), dialer.recv_peer()).await;
                ka_ts += 1.0;
                let _ = dialer.broadcast(keepalive("seed-z", ka_ts)).await;
                let _ = tokio::time::timeout(Duration::from_millis(50), seed.recv_peer()).await;
                seed.drain_new_connections();
                if dialer.connections.contains_key("seed-a")
                    && seed.connections.contains_key("seed-z")
                {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "the higher-id→lower-id leg never formed within 15s — \
                     full-mesh failed to dial a lower-id peer; \
                     dialer_has_seed={} seed_has_dialer={}",
                    dialer.connections.contains_key("seed-a"),
                    seed.connections.contains_key("seed-z"),
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            // The leg is bidirectional: a seed→dialer frame reaches the
            // dialer over the wire the dial built.
            seed.send_to_peer("seed-z", keepalive("seed-a", 99.0))
                .await
                .unwrap();
            let received = tokio::time::timeout(Duration::from_secs(5), dialer.recv_peer())
                .await
                .expect("timeout on seed→dialer frame")
                .expect("seed→dialer frame missing");
            assert_eq!(received.sender_id(), "seed-a");
        })
        .await;
}

/// MUTUAL-DIAL COLLISION — the load-bearing convergence property (cases b
/// and c). Both ends dial each other, so each holds two candidate
/// connections for the pair. The symmetric tiebreak keeps the connection
/// INITIATED BY THE LOWER-ID NODE on BOTH ends, and DROPS the other.
///
/// Pair (seed-a < seed-z). The surviving wire is seed-a's dial of seed-z =
/// seed-z's INBOUND = seed-a's OUTBOUND. We assert BOTH endpoints' local
/// `register_accepted` decisions and that they agree on the same physical
/// wire (no each-keeps-its-own ⇒ no disconnect), and that the loser is
/// dropped.
#[tokio::test(flavor = "current_thread")]
async fn mutual_dial_collision_converges_on_lower_id_initiator() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // ── seed-z's view (the HIGHER-id node) ──
            // It first registers its own OUTBOUND dial of seed-a, then the
            // INBOUND of seed-a's dial collides. Tiebreak: the lower-id
            // initiator (seed-a) wins ⇒ on seed-z the INBOUND must SURVIVE,
            // replacing its own outbound loser.
            let mut hi: PeerNetwork<TestId> = PeerNetwork::start("seed-z", None).await.unwrap();
            let (hi_out_tx, mut hi_out_rx) = mpsc::unbounded_channel();
            hi.new_conn_tx
                .send(AcceptedPeer {
                    peer_id: "seed-a".to_string(),
                    outgoing_tx: hi_out_tx.clone(),
                    inbound: false, // seed-z's outbound dial of seed-a
                })
                .unwrap();
            hi.drain_new_connections();
            let (hi_in_tx, mut hi_in_rx) = mpsc::unbounded_channel();
            hi.new_conn_tx
                .send(AcceptedPeer {
                    peer_id: "seed-a".to_string(),
                    outgoing_tx: hi_in_tx.clone(),
                    inbound: true, // seed-a's dial accepted at seed-z
                })
                .unwrap();
            hi.drain_new_connections();
            // The INBOUND (seed-a-initiated) wins on seed-z.
            assert!(
                hi.connections.get("seed-a").unwrap().same_channel(&hi_in_tx),
                "seed-z must keep the INBOUND (lower-id seed-a's dial)"
            );
            // The loser (seed-z's own outbound) is dropped: its sender is no
            // longer the registered one, and dropping it closes the wire —
            // the held rx now observes the sender count drop.
            hi.connections
                .get("seed-a")
                .unwrap()
                .send(keepalive("seed-z", 1.0))
                .unwrap();
            assert!(hi_in_rx.try_recv().is_ok(), "the survivor is the inbound wire");
            assert!(
                hi_out_rx.try_recv().is_err(),
                "the dropped outbound loser carries no traffic"
            );

            // ── seed-a's view (the LOWER-id node, the initiator) ──
            // It first registers its own OUTBOUND dial of seed-z, then the
            // INBOUND of seed-z's dial collides. Tiebreak: the lower-id
            // initiator (seed-a itself) wins ⇒ on seed-a the OUTBOUND must
            // SURVIVE and the inbound loser is DROPPED.
            let mut lo: PeerNetwork<TestId> = PeerNetwork::start("seed-a", None).await.unwrap();
            let (lo_out_tx, mut lo_out_rx) = mpsc::unbounded_channel();
            lo.new_conn_tx
                .send(AcceptedPeer {
                    peer_id: "seed-z".to_string(),
                    outgoing_tx: lo_out_tx.clone(),
                    inbound: false, // seed-a's outbound dial of seed-z
                })
                .unwrap();
            lo.drain_new_connections();
            let (lo_in_tx, mut lo_in_rx) = mpsc::unbounded_channel();
            lo.new_conn_tx
                .send(AcceptedPeer {
                    peer_id: "seed-z".to_string(),
                    outgoing_tx: lo_in_tx.clone(),
                    inbound: true, // seed-z's dial accepted at seed-a
                })
                .unwrap();
            lo.drain_new_connections();
            assert!(
                lo.connections.get("seed-z").unwrap().same_channel(&lo_out_tx),
                "seed-a must keep its OUTBOUND (it is the lower-id initiator)"
            );
            lo.connections
                .get("seed-z")
                .unwrap()
                .send(keepalive("seed-a", 1.0))
                .unwrap();
            assert!(lo_out_rx.try_recv().is_ok(), "the survivor is the outbound wire");
            assert!(
                lo_in_rx.try_recv().is_err(),
                "the dropped inbound loser carries no traffic"
            );

            // ── CONVERGENCE ── seed-z kept the INBOUND (seed-a's dial);
            // seed-a kept its OUTBOUND (its own dial). Both are the SAME
            // physical connection seed-a→seed-z, so the two endpoints agree
            // and stay connected — exactly one wire survives the pair.
            assert_eq!(PeerTransport::<TestId>::peer_count(&hi), 1);
            assert_eq!(PeerTransport::<TestId>::peer_count(&lo), 1);
        })
        .await;
}

/// COLLISION ORDER-INDEPENDENCE: the tiebreak converges regardless of which
/// orientation arrives FIRST. seed-z accepts seed-a's INBOUND first, then
/// its own OUTBOUND collides — the inbound (lower-id winner) must still
/// survive, and the late-arriving outbound loser is dropped on arrival.
#[tokio::test(flavor = "current_thread")]
async fn mutual_dial_collision_inbound_first_keeps_winner() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut hi: PeerNetwork<TestId> = PeerNetwork::start("seed-z", None).await.unwrap();
            // INBOUND (seed-a's dial) arrives first and registers.
            let (in_tx, mut in_rx) = mpsc::unbounded_channel();
            hi.new_conn_tx
                .send(AcceptedPeer {
                    peer_id: "seed-a".to_string(),
                    outgoing_tx: in_tx.clone(),
                    inbound: true,
                })
                .unwrap();
            hi.drain_new_connections();
            // OUTBOUND (seed-z's own dial) arrives second — it LOSES.
            let (out_tx, mut out_rx) = mpsc::unbounded_channel();
            hi.new_conn_tx
                .send(AcceptedPeer {
                    peer_id: "seed-a".to_string(),
                    outgoing_tx: out_tx,
                    inbound: false,
                })
                .unwrap();
            hi.drain_new_connections();
            assert!(
                hi.connections.get("seed-a").unwrap().same_channel(&in_tx),
                "the inbound winner must survive even when it arrived FIRST"
            );
            hi.connections
                .get("seed-a")
                .unwrap()
                .send(keepalive("seed-z", 1.0))
                .unwrap();
            assert!(in_rx.try_recv().is_ok());
            assert!(out_rx.try_recv().is_err(), "the late outbound loser is dropped");
        })
        .await;
}
