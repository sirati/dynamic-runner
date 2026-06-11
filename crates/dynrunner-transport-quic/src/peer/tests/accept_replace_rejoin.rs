//! Rejoin-exile heal (#416): a removed-but-alive peer that redials must
//! be RE-ADMITTED by the survivor's accept loop, even when a stale
//! `connections` entry for it survived membership removal.
//!
//! Production (Krater run_20260611_123632): secondary-0 (peer-a, the
//! lexicographically-LOWER id, so the DIAL OWNER under the
//! lower-id-dials rule) was removed from the survivor's (peer-b)
//! membership; `forget_departed` cleared `peer_dial_info` for it, but
//! the stale `connections["peer-a"]` entry SURVIVED (membership removal
//! is mesh-independent by design). peer-a stayed alive and redialed
//! forever; every fresh authenticated inbound landed at peer-b's accept
//! loop in `register_accepted`, where the `contains_key` dedup — seeing
//! the stale entry — SILENTLY DROPPED it. The cooperative escapes were
//! all severed (the redial-request path and the redial nudge both consult
//! `peer_dial_info`, which `forget_departed` emptied), so peer-a was
//! exiled for 45+ min.
//!
//! The fix routes BOTH registration paths (`drain_new_connections` and
//! the `recv_peer` select arm) through the single `register_accepted`
//! disposition, which REPLACES a stale entry with a fresh inbound under a
//! staleness gate (the accept-side analog of `handle_redial_request`'s
//! grace window):
//!   - a PROVABLY-DEAD existing wire (`is_closed()` — its writer task
//!     exited) is replaced at once;
//!   - a still-open existing wire is replaced only when a fresh inbound
//!     PERSISTS past `ACCEPT_REPLACE_GRACE` (the peer keeps re-dialing a
//!     leg it sees as dead) — so a one-shot mesh-forming duplicate of a
//!     healthy wire is dropped (dedup), never collapsing a live wire into
//!     a fleet-wide replace storm.
//!
//! The replacement is generation-safe — the `insert` overwrites the old
//! sender, so a racing disconnect of the OLD wire fails
//! `handle_peer_disconnect`'s `same_channel` check and cannot kill the
//! NEW wire.
//!
//! These tests pin:
//!   1. The real-wire rejoin-exile heal (RED before the fix: peer-b
//!      never re-admits the redialer; GREEN: the fresh inbound replaces
//!      the stale entry and B→A traffic flows over it).
//!   2. The PERSISTENCE gate: a still-open stale entry is replaced only
//!      after a fresh inbound recurs past the grace window.
//!   3. The grace gate PRESERVES the lower-id-dials / mesh-forming dedup:
//!      a one-shot duplicate of a LIVE wire is dropped, the canonical
//!      entry survives untouched.
//!   4. The replacement is generation-checked: a disconnect of the OLD
//!      wire after the replacement is a no-op against the fresh entry.

use std::time::Duration;

use super::super::{ACCEPT_REPLACE_GRACE, PeerNetwork};
use super::TestId;
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, KeepaliveRole, PeerConnectionInfo, PeerTransport,
};
use tokio::sync::mpsc;

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

/// REPRO-FIRST real-wire rejoin-exile (the genuine half-open): peer-b
/// holds a stale `connections["peer-a"]` entry that is STILL OPEN — a
/// membership-removal `forget_departed` cleared `peer_dial_info` but left
/// this transport entry behind, and the per-connection supervisor keeps
/// the writer alive (so peer-b never observes a disconnect and its sends
/// succeed into the dead wire — no send-failure prune). peer-a (the dial
/// owner) re-dials forever; each fresh authenticated inbound recurs at
/// peer-b's accept loop, and once it PERSISTS past the grace window the
/// accept side must REPLACE the stale entry so peer-b can reach peer-a.
///
/// Topology: peer-a (lower id, dial owner) ↔ peer-b (higher id, accept
/// side). The test drives peer-a's "redialed forever" by removing its
/// `connections["peer-b"]` and pulsing its reconnect tick each round
/// (production: the secondary's app-layer keeps re-establishing a leg it
/// believes dead). peer-b ONLY drains the accept loop (never `recv_peer`),
/// so the held entry is never pruned by the disconnect path and its sends
/// never trip the send-failure prune — the heal can come ONLY from the
/// accept-loop replacement.
///
/// RED (pre-fix): the `contains_key` dedup drops every fresh inbound;
/// peer-b's entry stays the held stale sender, so a peer-b→peer-a send
/// lands in the test-held receiver and NEVER reaches peer-a — the bounded
/// wait times out (the permanent exile).
///
/// GREEN: the persistent re-dial crosses the grace window, the accept
/// side replaces the stale entry with the fresh sender, and a
/// peer-b→peer-a send flows over the re-admitted wire to peer-a.
#[tokio::test(flavor = "current_thread")]
async fn rejoiner_fresh_inbound_replaces_stale_entry() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut peer_a: PeerNetwork<TestId> = PeerNetwork::start("peer-a", None).await.unwrap();
            let mut peer_b: PeerNetwork<TestId> = PeerNetwork::start("peer-b", None).await.unwrap();

            let peers = vec![
                PeerConnectionInfo {
                    secondary_id: "peer-a".into(),
                    cert: peer_a.cert_pem().to_string(),
                    ipv4: Some("127.0.0.1".into()),
                    ipv6: None,
                    port: peer_a.port(),
                    is_observer: false,
                    liveness_port: None,
                },
                PeerConnectionInfo {
                    secondary_id: "peer-b".into(),
                    cert: peer_b.cert_pem().to_string(),
                    ipv4: Some("127.0.0.1".into()),
                    ipv6: None,
                    port: peer_b.port(),
                    is_observer: false,
                    liveness_port: None,
                },
            ];
            peer_a.connect_to_peers(&peers);
            peer_b.connect_to_peers(&peers);

            // peer-a's outbound dial lands immediately; peer-b's accept
            // side surfaces only after peer-a writes its first frame.
            tokio::time::sleep(Duration::from_millis(100)).await;
            peer_a.drain_new_connections();
            peer_a.broadcast(keepalive("peer-a", 1.0)).await.unwrap();
            let est_deadline = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                let _ =
                    tokio::time::timeout(Duration::from_millis(50), peer_b.recv_peer()).await;
                if PeerTransport::<TestId>::peer_count(&peer_b) == 1 {
                    break;
                }
                assert!(
                    std::time::Instant::now() < est_deadline,
                    "peer-b should have accepted peer-a's leg"
                );
            }
            while PeerTransport::<TestId>::try_recv_peer(&mut peer_a).is_some() {}

            // ── Model the production stale survivor ── Overwrite peer-b's
            // entry for peer-a with a sender the test holds OPEN: the wire
            // is dead from peer-a's side, but peer-b cannot observe a
            // disconnect (the held rx keeps the entry open, like the live
            // per-connection supervisor) and its sends SUCCEED into the
            // dead wire — exactly the half-open the production stale entry
            // was. peer-b forgets peer-a from its dial roster (the
            // `forget_departed` severing the redial-request / nudge
            // escapes), so ONLY the accept-loop replacement can heal it.
            let (stale_tx, stale_rx) =
                mpsc::unbounded_channel::<DistributedMessage<TestId>>();
            peer_b.connections.insert("peer-a".to_string(), stale_tx);
            let survivor_roster: Vec<PeerConnectionInfo> = peers
                .iter()
                .filter(|p| p.secondary_id == "peer-b")
                .cloned()
                .collect();
            peer_b.connect_to_peers(&survivor_roster);

            // ── peer-a re-dials forever ── Each round: force peer-a to
            // re-dial by clearing its `connections["peer-b"]` and pulsing
            // its reconnect tick (production: the secondary's app layer
            // keeps re-establishing a leg it believes dead), pump peer-a so
            // the dial lands + registers, then have peer-a speak so its
            // first frame on the FRESH wire reaches peer-b's accept loop.
            // peer-b drains the accept loop ONLY (never `recv_peer`), so it
            // never prunes the held entry via the disconnect path and its
            // sends never trip the send-failure prune. Each fresh inbound
            // climbs peer-b's accept-replace evidence; once it crosses the
            // grace window the stale entry is replaced.
            let heal_deadline = std::time::Instant::now() + Duration::from_secs(20);
            let mut ts = 10.0f64;
            loop {
                // Force a re-dial this round.
                let _ = peer_a.connections.remove("peer-b");
                peer_a
                    .reconnect_tick_tx_for_test
                    .send(())
                    .expect("tick channel open");
                for _ in 0..3 {
                    let _ = tokio::time::timeout(Duration::from_millis(40), peer_a.recv_peer())
                        .await;
                    peer_a.drain_new_connections();
                }
                ts += 1.0;
                let _ = peer_a.broadcast(keepalive("peer-a", ts)).await;
                tokio::time::sleep(Duration::from_millis(50)).await;
                // Accept-loop drain ONLY — the stale-entry replace (once
                // the grace window is crossed) happens HERE.
                peer_b.drain_new_connections();

                // The decisive GREEN signal: peer-b can reach peer-a over
                // the re-admitted wire. Pre-fix the entry is still the held
                // stale sender, so this lands in `stale_rx` and peer-a
                // never receives it.
                let _ = peer_b.send_to_peer("peer-a", keepalive("peer-b", ts)).await;
                if let Ok(Some(msg)) =
                    tokio::time::timeout(Duration::from_millis(50), peer_a.recv_peer()).await
                    && msg.sender_id() == "peer-b"
                {
                    break;
                }
                assert!(
                    std::time::Instant::now() < heal_deadline,
                    "rejoin-exile: peer-b never re-admitted the redialing peer-a — \
                     the fresh authenticated inbound was dropped by the stale-entry \
                     dedup and peer-b→peer-a traffic never reached the wire \
                     (run_20260611_123632 shape)"
                );
            }
            // Reaching here = the loop broke = peer-a received peer-b's
            // frame over the freshly re-admitted wire (the heal). Earlier
            // grace-window sends DID land in the held stale receiver — that
            // is the bug window the persistence gate is deliberately
            // bounded to, not a leak.
            drop(stale_rx);
        })
        .await;
}

/// The PERSISTENCE gate (the still-open half-open path): a stale entry
/// whose writer is NOT closed (a blackholed wire whose IDLE_TIMEOUT has
/// not fired on this side) is replaced only after a fresh authenticated
/// inbound RECURS past `ACCEPT_REPLACE_GRACE` — the peer re-dialing a leg
/// it sees as dead. The first `GRACE` inbounds are dropped (they might be
/// a mesh-forming transient); the next one replaces.
///
/// peer-a < peer-b ⇒ peer-b is the accept side for peer-a (`AcceptedPeer`
/// for peer-a is always a fresh inbound off peer-b's accept loop).
#[tokio::test(flavor = "current_thread")]
async fn persistent_redial_past_grace_replaces_open_stale_entry() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut net: PeerNetwork<TestId> = PeerNetwork::start("peer-b", None).await.unwrap();
            assert!(
                !net.dials_outbound_to("peer-a"),
                "peer-b must be the accept side for peer-a"
            );

            // Stale entry whose writer stays OPEN (the test holds the rx) —
            // the blackholed half-open whose disconnect never fires.
            let (stale_tx, _stale_rx) =
                mpsc::unbounded_channel::<DistributedMessage<TestId>>();
            net.connections.insert("peer-a".to_string(), stale_tx.clone());

            // Feed fresh inbounds one at a time (the peer re-dialing every
            // tick). The first `GRACE` are dropped; the entry stays the
            // stale sender. Each fresh inbound's own sender is held so it
            // does not register as is_closed.
            let mut held = Vec::new();
            for i in 0..ACCEPT_REPLACE_GRACE {
                let (tx, rx) = mpsc::unbounded_channel::<DistributedMessage<TestId>>();
                held.push(rx);
                net.new_conn_tx
                    .send(super::super::AcceptedPeer {
                        peer_id: "peer-a".to_string(),
                        outgoing_tx: tx,
                    })
                    .expect("registration channel open");
                net.drain_new_connections();
                assert!(
                    net.connections
                        .get("peer-a")
                        .unwrap()
                        .same_channel(&stale_tx),
                    "inbound #{} inside the grace window must be dropped, \
                     the stale entry kept",
                    i + 1
                );
            }

            // The inbound that crosses the grace window REPLACES the stale
            // entry with the fresh sender.
            let (fresh_tx, mut fresh_rx) =
                mpsc::unbounded_channel::<DistributedMessage<TestId>>();
            net.new_conn_tx
                .send(super::super::AcceptedPeer {
                    peer_id: "peer-a".to_string(),
                    outgoing_tx: fresh_tx.clone(),
                })
                .expect("registration channel open");
            net.drain_new_connections();
            assert!(
                !net.connections
                    .get("peer-a")
                    .unwrap()
                    .same_channel(&stale_tx),
                "the inbound past the grace window must REPLACE the stale entry"
            );
            net.connections
                .get("peer-a")
                .unwrap()
                .send(keepalive("peer-b", 1.0))
                .unwrap();
            assert!(
                fresh_rx.try_recv().is_ok(),
                "the surviving entry must be the fresh sender"
            );
        })
        .await;
}

/// The grace gate PRESERVES the lower-id-dials / mesh-forming dedup: a
/// one-shot duplicate inbound for a peer with a LIVE (open) entry — the
/// establishment dial-race artefact, or a dial-owner's own racing redial
/// — is DROPPED, leaving the canonical entry untouched. (Replacing it
/// would collapse a healthy wire and trigger the fleet-wide replace
/// storm.)
#[tokio::test(flavor = "current_thread")]
async fn mesh_forming_duplicate_of_live_wire_is_deduped() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut net: PeerNetwork<TestId> = PeerNetwork::start("peer-a", None).await.unwrap();

            // Canonical LIVE entry for peer-z (held rx ⇒ open writer).
            let (canonical_tx, mut canonical_rx) =
                mpsc::unbounded_channel::<DistributedMessage<TestId>>();
            net.connections
                .insert("peer-z".to_string(), canonical_tx.clone());

            // A single racing-duplicate AcceptedPeer arrives.
            let (racing_tx, mut racing_rx) =
                mpsc::unbounded_channel::<DistributedMessage<TestId>>();
            net.new_conn_tx
                .send(super::super::AcceptedPeer {
                    peer_id: "peer-z".to_string(),
                    outgoing_tx: racing_tx,
                })
                .expect("registration channel open");
            net.drain_new_connections();

            // The canonical entry survives; a send routes to it, NOT the
            // dropped racing leg.
            assert_eq!(
                PeerTransport::<TestId>::peer_count(&net),
                1,
                "exactly one peer-z entry after deduping the one-shot duplicate"
            );
            net.connections
                .get("peer-z")
                .unwrap()
                .send(keepalive("peer-a", 1.0))
                .unwrap();
            assert!(
                canonical_rx.try_recv().is_ok(),
                "the surviving entry must be the CANONICAL live wire"
            );
            assert!(
                racing_rx.try_recv().is_err(),
                "the one-shot duplicate's sender must have been DROPPED"
            );
        })
        .await;
}

/// Generation check on the REPLACE path: after the accept side replaces a
/// PROVABLY-DEAD stale entry with a fresh sender, a disconnect signal
/// carrying the OLD (replaced) sender is a no-op — it cannot delete the
/// fresh entry. Mirror of
/// `reader_exit_disconnect::stale_disconnect_does_not_prune_reconnected_entry`,
/// driven through `register_accepted`'s replacement so it pins the #416
/// path end to end.
#[tokio::test(flavor = "current_thread")]
async fn replaced_stale_wire_disconnect_does_not_kill_fresh_entry() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut net: PeerNetwork<TestId> = PeerNetwork::start("peer-b", None).await.unwrap();
            assert!(
                !net.dials_outbound_to("peer-a"),
                "peer-b must be the accept side for peer-a"
            );

            // Stale entry whose wire is PROVABLY DEAD: dropping the rx
            // exits the (notional) writer, so the sender is_closed — the
            // immediate-replace path (no grace needed).
            let stale_tx = {
                let (tx, rx) = mpsc::unbounded_channel::<DistributedMessage<TestId>>();
                drop(rx);
                tx
            };
            assert!(stale_tx.is_closed(), "stale wire must be provably dead");
            net.connections.insert("peer-a".to_string(), stale_tx.clone());

            // One fresh authenticated inbound replaces the dead entry at
            // once.
            let (fresh_tx, mut fresh_rx) =
                mpsc::unbounded_channel::<DistributedMessage<TestId>>();
            net.new_conn_tx
                .send(super::super::AcceptedPeer {
                    peer_id: "peer-a".to_string(),
                    outgoing_tx: fresh_tx.clone(),
                })
                .expect("registration channel open");
            net.drain_new_connections();
            assert!(
                net.connections
                    .get("peer-a")
                    .unwrap()
                    .same_channel(&fresh_tx),
                "a provably-dead entry must be replaced by the fresh inbound at once"
            );

            // A late disconnect of the OLD (replaced) wire must NOT prune
            // the fresh entry — the generation check (same_channel) sees
            // the entry is no longer the stale sender.
            net.handle_peer_disconnect("peer-a", &stale_tx);
            assert_eq!(
                PeerTransport::<TestId>::peer_count(&net),
                1,
                "a racing disconnect of the OLD wire must not kill the fresh entry"
            );
            net.connections
                .get("peer-a")
                .unwrap()
                .send(keepalive("peer-b", 1.0))
                .unwrap();
            assert!(
                fresh_rx.try_recv().is_ok(),
                "the surviving entry must be the fresh sender"
            );

            // A disconnect of the FRESH sender DOES prune (it is the live
            // channel).
            net.handle_peer_disconnect("peer-a", &fresh_tx);
            assert_eq!(
                PeerTransport::<TestId>::peer_count(&net),
                0,
                "a disconnect of the live channel prunes the entry"
            );
        })
        .await;
}
