//! Joining-mode dial order: a rosterless late-joiner dials EVERY seed
//! regardless of id order.
//!
//! The latent gap (the lower-id-dials rule's blind spot): the rule is a
//! SIMULTANEOUS-dial dedup for steady-state members who all already know
//! each other — each pair agrees, by id order, who dials. A late joiner
//! is UNKNOWN to the running fleet, so a seed whose id sorts BELOW the
//! joiner would, under the rule, "await the joiner's inbound" — but it
//! never learns the joiner exists to dial it, so that leg parks
//! `awaiting_inbound` forever and the join hangs on relay luck.
//! Production survived only because `observer-<uuid>` sorts below
//! `secondary-*`, so observers dialed everyone; the gap bites the instant
//! the ordering flips.
//!
//! `PeerNetwork::start_joining` sets `dial_all_seeds`, overriding the
//! lower-id rule in the single `dials_outbound_to` predicate so the
//! joiner owns the dial side of EVERY leg. These tests pin:
//!   1. SWEEP — a joiner whose id sorts ABOVE a seed SPAWNS the dial
//!      (steady-state `start` would have parked it `awaiting_inbound`).
//!   2. REAL WIRE — that dial actually forms the leg in both directions
//!      (the joiner sorts above the seed; without joining-mode neither
//!      side would ever dial and the mesh would never form).
//!   3. CROSSED DIAL — once a lower-id seed learns the joiner via the
//!      roster and dials it too, the duplicate inbound is deduped against
//!      the joiner's already-live wire (the accept-side grace window), so
//!      the crossed dial converges to one leg.
//!   4. STEADY STATE — a non-joining `start` node keeps the lower-id
//!      dedup unchanged (the higher-id node parks `awaiting_inbound`).

use std::time::Duration;

use super::super::{ACCEPT_REPLACE_GRACE, AcceptedPeer, PeerNetwork};
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

/// SWEEP (RED before the fix): a joiner whose id sorts ABOVE every seed
/// must SPAWN a dial per seed — the steady-state rule would have parked
/// all of them `awaiting_inbound` (the parked-forever gap). The mirror of
/// `dial_sweep::highest_id_sweep_spawns_zero_dials_and_names_awaiting_inbound`
/// with joining-mode flipping the disposition.
#[tokio::test(flavor = "current_thread")]
async fn joining_highest_id_dials_every_lower_seed() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // `peer-z` sorts ABOVE both seeds — the exact ordering that
            // parks `awaiting_inbound` under the steady-state rule.
            let mut net: PeerNetwork<TestId> =
                PeerNetwork::start_joining("peer-z", None).await.unwrap();
            let summary = net.connect_to_peers_inner(&[pinfo("peer-a"), pinfo("peer-b")]);
            assert_eq!(
                summary.listed, 2,
                "both lower-id seeds are listed (self excluded)"
            );
            assert_eq!(
                summary.spawned, 2,
                "joining-mode must spawn a dial for EVERY seed regardless of \
                 id order; got {summary:#?}"
            );
            assert!(
                summary.awaiting_inbound.is_empty(),
                "joining-mode parks NOTHING awaiting-inbound — that is the gap \
                 it closes; got {:?}",
                summary.awaiting_inbound
            );
        })
        .await;
}

/// STEADY-STATE PIN: a non-joining `start` node with the SAME id ordering
/// keeps the lower-id-dials dedup — the higher-id node spawns zero dials
/// and parks both seeds awaiting-inbound. Guards against the joining-mode
/// override leaking into steady-state members.
#[tokio::test(flavor = "current_thread")]
async fn steady_state_highest_id_keeps_lower_id_dedup() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut net: PeerNetwork<TestId> = PeerNetwork::start("peer-z", None).await.unwrap();
            let summary = net.connect_to_peers_inner(&[pinfo("peer-a"), pinfo("peer-b")]);
            assert_eq!(
                summary.spawned, 0,
                "steady-state highest-id node must spawn ZERO dials (rule unchanged)"
            );
            assert_eq!(
                summary.awaiting_inbound,
                vec!["peer-a".to_string(), "peer-b".to_string()],
                "steady-state highest-id node awaits both lower-id peers' inbound dials"
            );
        })
        .await;
}

/// REAL WIRE (RED before the fix): a joiner that sorts ABOVE the seed
/// forms the leg in BOTH directions, driven by the joiner's own dial.
/// Under the steady-state rule the joiner (higher id) would never dial
/// and the seed never knows the joiner to dial it — so neither side ever
/// connects and the mesh never forms. With joining-mode the joiner dials
/// the seed, the seed's accept loop registers it, and the seed's reply
/// completes the round-trip.
#[tokio::test(flavor = "current_thread")]
async fn joiner_above_seed_forms_real_leg() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // The seed sorts BELOW the joiner: `seed-a` < `seed-z`. The
            // seed is a STEADY-STATE member that does not know the joiner,
            // so it will never dial it; the leg can come ONLY from the
            // joiner's own dial (the gap, closed by joining-mode).
            let mut joiner: PeerNetwork<TestId> =
                PeerNetwork::start_joining("seed-z", None).await.unwrap();
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
            };

            // The joiner dials its seed (joining-mode: it dials a lower-id
            // peer it never would under the steady-state rule). The seed
            // is given NO roster entry for the joiner — it never dials.
            joiner.connect_to_peers(std::slice::from_ref(&seed_info));

            let deadline = std::time::Instant::now() + Duration::from_secs(15);
            let mut ka_ts = 1.0f64;
            loop {
                let _ = tokio::time::timeout(Duration::from_millis(50), joiner.recv_peer()).await;
                ka_ts += 1.0;
                // The joiner's broadcast drains its completed dial
                // registration first, then writes the identifying first
                // frame on the live wire so the seed's accept loop keys
                // the leg under `seed-z`.
                let _ = joiner.broadcast(keepalive("seed-z", ka_ts)).await;
                let _ = tokio::time::timeout(Duration::from_millis(50), seed.recv_peer()).await;
                seed.drain_new_connections();
                if joiner.connections.contains_key("seed-a")
                    && seed.connections.contains_key("seed-z")
                {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "the joiner-above-seed leg never formed within 15s — \
                     joining-mode failed to dial a lower-id seed; \
                     joiner_has_seed={} seed_has_joiner={}",
                    joiner.connections.contains_key("seed-a"),
                    seed.connections.contains_key("seed-z"),
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            // The leg is bidirectional: a seed→joiner frame reaches the
            // joiner over the wire the joiner's dial built.
            seed.send_to_peer("seed-z", keepalive("seed-a", 99.0))
                .await
                .unwrap();
            let received = tokio::time::timeout(Duration::from_secs(5), joiner.recv_peer())
                .await
                .expect("timeout on seed→joiner frame")
                .expect("seed→joiner frame missing");
            assert_eq!(received.sender_id(), "seed-a");
        })
        .await;
}

/// CROSSED DIAL: a joining-mode joiner dialed a lower-id seed (its wire is
/// LIVE). Later the seed learns the joiner via the primary's roster
/// broadcast and — being the lower id — ALSO dials the joiner under its
/// own steady-state rule. That second pipe surfaces as a fresh inbound at
/// the joiner's accept loop while it already holds a healthy entry: the
/// grace-window dedup MUST drop it, so the crossed dial converges to the
/// one live leg (no replace-storm, no leg flap). This is the safety the
/// joining-mode override relies on.
#[tokio::test(flavor = "current_thread")]
async fn crossed_dial_from_lower_id_seed_is_deduped_against_live_wire() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // The joiner sorts ABOVE the seed. Joining-mode made the joiner
            // own the dial; the seed is the lower id, so once it learns the
            // joiner it owns the dial too — the crossed-dial overlap.
            let mut joiner: PeerNetwork<TestId> =
                PeerNetwork::start_joining("seed-z", None).await.unwrap();

            // The joiner's own dial already produced a LIVE wire to the
            // seed (the test stands in for the completed dial registration
            // by inserting the live sender directly — its rx is held so it
            // never reads as closed).
            let (live_tx, mut live_rx) = mpsc::unbounded_channel::<DistributedMessage<TestId>>();
            joiner.connections.insert("seed-a".to_string(), live_tx.clone());

            // The seed's later dial lands as a fresh inbound at the
            // joiner's accept loop. A crossed dial is a one-shot transient
            // of a LIVE wire — the seed dials once on learning the joiner,
            // and the joiner's own traffic then identifies the existing
            // wire at the seed, so the inbound count never climbs past the
            // grace window (the persistence test that distinguishes a
            // crossed dial from a peer genuinely re-dialing a dead leg).
            // Every inbound inside the grace window is dropped; the live
            // wire stands.
            let mut held = Vec::new();
            for i in 0..ACCEPT_REPLACE_GRACE {
                let (tx, rx) = mpsc::unbounded_channel::<DistributedMessage<TestId>>();
                held.push(rx);
                joiner
                    .new_conn_tx
                    .send(AcceptedPeer {
                        peer_id: "seed-a".to_string(),
                        outgoing_tx: tx,
                    })
                    .expect("registration channel open");
                joiner.drain_new_connections();
                assert!(
                    joiner
                        .connections
                        .get("seed-a")
                        .unwrap()
                        .same_channel(&live_tx),
                    "crossed-dial inbound #{} must be deduped against the \
                     joiner's live wire — the seed's dial cannot collapse a \
                     healthy leg",
                    i + 1
                );
            }

            // The original live wire is still the registered one: a
            // joiner→seed send reaches its held receiver, not a dropped
            // crossed-dial pipe.
            joiner
                .connections
                .get("seed-a")
                .unwrap()
                .send(keepalive("seed-z", 1.0))
                .unwrap();
            assert!(
                live_rx.try_recv().is_ok(),
                "the converged leg must be the joiner's original live wire"
            );
        })
        .await;
}
