//! Member↔member leg re-establishment when the DIAL OWNER cannot see
//! the death (the run_20260610_221140 / BUG 3.3 production shape).
//!
//! Three peers A/B/C; the lower-id-dials rule makes A the dial owner
//! of the A↔B leg. The leg dies HALF-OPEN: B (the non-owner) loses its
//! outbound entry while A's side still looks healthy — so A never
//! tracks a disconnect and never re-dials, and B structurally never
//! dials (lower-id-dials). Without the redial-request nudge the leg
//! stays dead forever while B→A traffic silently relays via C.
//!
//! The heal under test: B's reconnect tick notices the leg it cannot
//! dial, sends a wire-only `RedialRequest` to A (relay-capable, so it
//! traverses C), A force-prunes its stale entry and re-dials, and the
//! direct leg re-folds. Relay keeps covering directed sends during the
//! outage window (also asserted) — the supervisor restores the direct
//! path, the relay is the meanwhile-fallback.
//!
//! A second test pins the lifecycle stop: a tracked peer that leaves
//! the authoritative roster has its redial tracking forgotten.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::super::{MSG_REDIAL_REQUEST_HONORED, MSG_REDIAL_REQUEST_SENT, PeerNetwork};
use super::TestId;
use super::log_capture::{CaptureLayer, CapturedEvent, pump_b_until};
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, KeepaliveRole, MSG_DIRECT_RESTORED, MSG_RELAY_ENGAGED, PeerConnectionInfo,
    PeerTransport,
};
use tracing_subscriber::Registry;
use tracing_subscriber::layer::SubscriberExt;

/// The production half-open replay (REPRO-FIRST for the member-leg
/// redial fix): the leg dies in a way only the NON-dial-owner can see,
/// and must still re-fold.
///
/// Topology: 3-peer mesh (peer-a, peer-b, peer-c) fully established.
/// We sever the A↔B leg by removing ONLY peer-b's channel entry for
/// peer-a — the half-open shape: B's outbound resolution to A is dead
/// while A's entry for B still looks healthy (A's frames keep arriving
/// at B over the old wire — the production "INBOUND kept arriving
/// while OUTBOUND resolution stayed dead" signature). A therefore
/// never observes a disconnect and never re-dials; B is the higher id
/// and structurally never dials (lower-id-dials rule).
///
/// Phase 1 (relay coverage, the meanwhile-fallback): B.send_to(A)
/// routes via C and is delivered — directed member↔member traffic
/// survives the direct-leg outage.
///
/// Phase 2 (the heal): driving B's reconnect ticks makes B send a
/// `RedialRequest` to A (relayed via C). A — the dial owner —
/// force-prunes its stale entry and re-dials B. The leg re-folds: B
/// regains a live `connections["peer-a"]` entry (A's first frame on
/// the fresh wire identifies it at B's accept loop), and a subsequent
/// B→A directed send takes the Direct path again (the Router's
/// "peer direct link restored" transition log).
///
/// Forwarder (peer-c) runs a `recv_peer()` loop in a dedicated
/// LocalSet task because `process_inbound`'s forwarding only
/// happens on the async path — `try_recv_peer` would drop the
/// Relay-not-for-us envelope with a warn (intentional, see the
/// pre-refactor `try_recv_peer` parity comment in
/// `Router::process_inbound_sync`).
#[tokio::test(flavor = "current_thread")]
async fn half_open_member_leg_heals_via_redial_request() {
    let records: Arc<Mutex<Vec<CapturedEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let layer = CaptureLayer {
        records: records.clone(),
    };
    let subscriber = Registry::default().with(layer);
    // Thread-local default — `current_thread` runtime + LocalSet
    // both run on this thread, so all spawn_local'd accept-loop /
    // dial / handler tasks see this subscriber. Guard drop on
    // function exit clears it without leaking into other tests.
    let _guard = tracing::subscriber::set_default(subscriber);

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Establish a 3-peer mesh.
            let mut peer_a: PeerNetwork<TestId> = PeerNetwork::start("peer-a", None).await.unwrap();
            let mut peer_b: PeerNetwork<TestId> = PeerNetwork::start("peer-b", None).await.unwrap();
            let mut peer_c: PeerNetwork<TestId> = PeerNetwork::start("peer-c", None).await.unwrap();

            let peers = vec![
                PeerConnectionInfo {
                    secondary_id: "peer-a".into(),
                    cert: peer_a.cert_pem().to_string(),
                    ipv4: Some("127.0.0.1".into()),
                    ipv6: None,
                    port: peer_a.port(),
                    is_observer: false,
                    liveness_port: None,
                    slurm_job_id: None,
                },
                PeerConnectionInfo {
                    secondary_id: "peer-b".into(),
                    cert: peer_b.cert_pem().to_string(),
                    ipv4: Some("127.0.0.1".into()),
                    ipv6: None,
                    port: peer_b.port(),
                    is_observer: false,
                    liveness_port: None,
                    slurm_job_id: None,
                },
                PeerConnectionInfo {
                    secondary_id: "peer-c".into(),
                    cert: peer_c.cert_pem().to_string(),
                    ipv4: Some("127.0.0.1".into()),
                    ipv6: None,
                    port: peer_c.port(),
                    is_observer: false,
                    liveness_port: None,
                    slurm_job_id: None,
                },
            ];

            peer_a.connect_to_peers(&peers);
            peer_b.connect_to_peers(&peers);
            peer_c.connect_to_peers(&peers);

            // 3-peer establishment requires a staged unblock
            // dance. Background: the QUIC accept loop's per-
            // connection `accept_bi().await` only resolves once
            // the client has actually written data on the
            // bi-directional stream. With two clients dialing
            // the same server (peer-a → peer-c AND peer-b →
            // peer-c), the server's accept loop blocks on
            // peer-a's accept_bi until peer-a sends — and
            // peer-b's pending dial sits unhandshaken behind
            // it. Existing 2-peer tests don't hit this because
            // each accept loop only ever sees one inbound dial.
            //
            // The unblock dance: poll until the *outbound*
            // dial side completes (peer-a sees both targets,
            // peer-b sees its target), then peer-a broadcasts a
            // keepalive. That broadcast is what writes the
            // first stream-frame on peer-a's connections,
            // which unblocks peer-c's accept_bi for peer-a and
            // lets peer-c iterate to handshake peer-b. After
            // that handshake, peer-b's dial completes and
            // registers peer-c. Finally peer-b broadcasts to
            // unblock peer-c's accept_bi for peer-b. Now the
            // mesh is fully observable from every peer.
            //
            // Each poll uses a 5s deadline; localhost QUIC
            // handshakes complete in single-digit ms.

            // Stage 1: outbound dials complete. peer-a dials
            // peer-b and peer-c (both lower-id-dials targets);
            // peer-b dials peer-c. peer-c dials no one.
            let stage1_deadline = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                peer_a.drain_new_connections();
                peer_b.drain_new_connections();
                let pa = <PeerNetwork<TestId> as PeerTransport<TestId>>::peer_count(&peer_a);
                if pa >= 2 {
                    break;
                }
                assert!(
                    std::time::Instant::now() < stage1_deadline,
                    "peer-a outbound dials did not complete within 5s; pa={pa}"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }

            // Stage 2: peer-a broadcasts a keepalive so its
            // outbound stream-frames hit peer-b and peer-c.
            // This unsticks peer-c's quic_accept_loop, which
            // can now iterate to accept peer-b's pending dial.
            peer_a
                .broadcast(DistributedMessage::Keepalive {
                    target: None,
                    sender_id: "peer-a".into(),
                    timestamp: 1.0,
                    secondary_id: "peer-a".into(),
                    active_workers: 0,
                    emitter_role: KeepaliveRole::Secondary,
                })
                .await
                .unwrap();

            // Stage 3: poll until peer-b's dial to peer-c
            // lands. Now peer-b.connections has peer-c.
            let stage3_deadline = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                peer_b.drain_new_connections();
                let pb = <PeerNetwork<TestId> as PeerTransport<TestId>>::peer_count(&peer_b);
                if pb >= 1 {
                    break;
                }
                assert!(
                    std::time::Instant::now() < stage3_deadline,
                    "peer-b dial to peer-c did not complete within 5s; pb={pb}"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }

            // Stage 4: peer-b broadcasts so its outbound
            // stream-frame hits peer-c, unsticking peer-c's
            // accept_bi for peer-b. peer-c's accept loop can
            // now process the spawned handle_accepted_quic
            // for peer-b past its first-message recv,
            // surfacing the AcceptedPeer through new_conn_tx.
            peer_b
                .broadcast(DistributedMessage::Keepalive {
                    target: None,
                    sender_id: "peer-b".into(),
                    timestamp: 1.0,
                    secondary_id: "peer-b".into(),
                    active_workers: 0,
                    emitter_role: KeepaliveRole::Secondary,
                })
                .await
                .unwrap();

            // Stage 5: poll until the full mesh is observable.
            // peer-a sees b+c (own dials, already done).
            // peer-b sees a (accept-side from peer-a's
            // broadcast) + c (own dial). peer-c sees a+b
            // (accept-side from both broadcasts).
            let stage5_deadline = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                peer_a.drain_new_connections();
                peer_b.drain_new_connections();
                peer_c.drain_new_connections();
                let pa = <PeerNetwork<TestId> as PeerTransport<TestId>>::peer_count(&peer_a);
                let pb = <PeerNetwork<TestId> as PeerTransport<TestId>>::peer_count(&peer_b);
                let pc = <PeerNetwork<TestId> as PeerTransport<TestId>>::peer_count(&peer_c);
                if pa == 2 && pb == 2 && pc == 2 {
                    break;
                }
                assert!(
                    std::time::Instant::now() < stage5_deadline,
                    "3-peer mesh did not fully establish within 5s; \
                     peer_a={pa} peer_b={pb} peer_c={pc}"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }

            // Drain the primes from each inbox so they don't
            // pollute the post-partition delivery count below.
            // peer-c is also drained here (rather than relying
            // on the forwarder task's recv loop) so the
            // "did peer-c forward msg1?" signal is unambiguous.
            while <PeerNetwork<TestId> as PeerTransport<TestId>>::try_recv_peer(&mut peer_a)
                .is_some()
            {}
            while <PeerNetwork<TestId> as PeerTransport<TestId>>::try_recv_peer(&mut peer_b)
                .is_some()
            {}
            while <PeerNetwork<TestId> as PeerTransport<TestId>>::try_recv_peer(&mut peer_c)
                .is_some()
            {}

            // Warmup direct send B→A so B's `route_state` for
            // peer-a is `Direct`. The first observation of a
            // peer's route is silent by design (the `None` arm in
            // Router::observe_relay) — without this warmup, the
            // post-sever relay observation would also be silent
            // and the "peer relay engaged" warn would never fire.
            let warmup: DistributedMessage<TestId> = DistributedMessage::Keepalive {
                target: None,
                sender_id: "peer-b".into(),
                timestamp: 1.5,
                secondary_id: "peer-b".into(),
                active_workers: 1,
                emitter_role: KeepaliveRole::Secondary,
            };
            peer_b
                .send_to_peer("peer-a", warmup)
                .await
                .expect("warmup direct send should succeed");
            // Drive the runtime briefly so the warmup actually
            // arrives, then drain peer-a's inbox.
            tokio::time::sleep(Duration::from_millis(50)).await;
            peer_a.drain_new_connections();
            while <PeerNetwork<TestId> as PeerTransport<TestId>>::try_recv_peer(&mut peer_a)
                .is_some()
            {}

            // HALF-OPEN sever: remove ONLY peer-b's channel entry
            // for peer-a. A's entry for B stays live (its frames
            // still arrive at B over the old wire — the production
            // inbound-alive/outbound-dead signature), so A — the
            // dial owner under the lower-id-dials rule — never
            // observes a disconnect and never re-dials on its own.
            assert!(peer_b.connections.remove("peer-a").is_some());
            assert_eq!(
                <PeerNetwork<TestId> as PeerTransport<TestId>>::peer_count(&peer_a),
                2,
                "A's side of the leg must still look healthy (half-open)"
            );
            assert_eq!(
                <PeerNetwork<TestId> as PeerTransport<TestId>>::peer_count(&peer_b),
                1
            );

            // Move peer_c into a forwarder task. Its sole job is
            // to run `recv_peer()` in a loop so inbound Relay
            // envelopes addressed to peer-a get forwarded
            // through `Router::process_inbound`'s
            // `apply_forward_decision`. We don't need its
            // returned messages — peer-c is a forwarder, not an
            // endpoint; any keepalive it itself receives is
            // discarded silently.
            let forwarder_handle = tokio::task::spawn_local(async move {
                while let Some(m) = peer_c.recv_peer().await {
                    tracing::debug!(target: "test_debug", "peer-c forwarder received: {m:?}");
                }
            });

            // ── Phase 1: relay covers directed sends during the
            // outage. B→A routes via C (B has no direct entry) and
            // is delivered to A — the meanwhile-fallback the redial
            // supervisor must not fight. ──
            let msg1: DistributedMessage<TestId> = DistributedMessage::Keepalive {
                target: None,
                sender_id: "peer-b".into(),
                timestamp: 2.0,
                secondary_id: "peer-b".into(),
                active_workers: 5,
                emitter_role: KeepaliveRole::Secondary,
            };
            peer_b
                .send_to_peer("peer-a", msg1)
                .await
                .expect("send_to_peer should route via peer-c relay");

            // Pump until A receives the relayed payload (proves
            // forwarder C did its job AND Router::process_inbound
            // unwrapped the Relay-for-self envelope on A).
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            let n1 = pump_b_until(&mut peer_a, &mut peer_b, deadline, |n| n >= 1)
                .await
                .unwrap_or_else(|| {
                    let trace = records.lock().unwrap().clone();
                    panic!(
                        "relayed message should reach peer-a within 5s; \
                         peer_a.peer_count={} peer_b.peer_count={} captured={:#?}",
                        <PeerNetwork<TestId> as PeerTransport<TestId>>::peer_count(&peer_a),
                        <PeerNetwork<TestId> as PeerTransport<TestId>>::peer_count(&peer_b),
                        trace
                    )
                });
            assert_eq!(n1, 1, "exactly one relayed message");

            // ── Phase 2: the heal. Drive B's reconnect ticks (the
            // test backdoor stands in for the production 5s ticker)
            // and pump both ends. Each B tick: the tracker tracks
            // the missing peer-a leg and — because B is NOT the
            // dial owner — sends a `RedialRequest` to A via the
            // relay. A force-prunes its stale entry and re-dials;
            // A's keepalive broadcast then identifies the fresh
            // wire at B's accept loop, restoring B's entry.
            //
            // Without the fix this loop can never converge: B
            // never dials (lower-id-dials), A never notices (its
            // entry looks healthy) — the leg stays dead forever,
            // which is exactly the production shape this test
            // pins (RED before the fix).
            let deadline = std::time::Instant::now() + Duration::from_secs(15);
            let mut ka_ts = 10.0f64;
            loop {
                peer_b
                    .reconnect_tick_tx_for_test
                    .send(())
                    .expect("tick channel open");
                // One bounded recv turn per side: B processes the
                // tick (tracker + redial-request emission); A
                // processes the inbound RedialRequest (force-prune
                // + re-dial) and drains its dial registrations.
                let _ = tokio::time::timeout(Duration::from_millis(50), peer_b.recv_peer()).await;
                let _ = tokio::time::timeout(Duration::from_millis(50), peer_a.recv_peer()).await;
                // A speaks so a freshly re-dialed wire identifies
                // itself at B's accept loop (production: the
                // anti-entropy digest / keepalive cadence).
                ka_ts += 1.0;
                let _ = peer_a
                    .broadcast(DistributedMessage::Keepalive {
                        target: None,
                        sender_id: "peer-a".into(),
                        timestamp: ka_ts,
                        secondary_id: "peer-a".into(),
                        active_workers: 0,
                        emitter_role: KeepaliveRole::Secondary,
                    })
                    .await;
                peer_b.drain_new_connections();
                if peer_b.connections.contains_key("peer-a") {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "the half-open member leg never re-folded: B still has no \
                     direct entry for peer-a after 15s of reconnect ticks — \
                     the dial owner was never nudged to re-dial \
                     (production run_20260610_221140 shape); captured trace: {:#?}",
                    records.lock().unwrap().clone()
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            // ── Phase 3: a directed B→A send takes the Direct path
            // again — the Router observes the restored route and
            // logs the transition. ──
            let msg2: DistributedMessage<TestId> = DistributedMessage::Keepalive {
                target: None,
                sender_id: "peer-b".into(),
                timestamp: 3.0,
                secondary_id: "peer-b".into(),
                active_workers: 6,
                emitter_role: KeepaliveRole::Secondary,
            };
            peer_b
                .send_to_peer("peer-a", msg2)
                .await
                .expect("send_to_peer should now go direct");

            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            // A may also still receive late relayed keepalives from
            // the heal loop; one direct delivery is what we need.
            let n2 = pump_b_until(&mut peer_a, &mut peer_b, deadline, |n| n >= 1)
                .await
                .expect("direct message should reach peer-a within 5s");
            assert!(n2 >= 1, "direct message after heal must be delivered");

            // Tear down forwarder cleanly so leaked tasks don't
            // outlive the LocalSet (which would keep the runtime
            // alive past the test).
            forwarder_handle.abort();

            // ── Assertions on captured log trace ──
            //
            // 1. "peer relay engaged" warn fired on B's outage
            //    observation (target=dynrunner_relay).
            // 2. "peer direct link restored" info fired on B's heal
            //    observation (target=dynrunner_relay).
            // 3. The redial-request handshake is narrated on both
            //    sides: B names the request it sent (first attempt at
            //    INFO), A names the force-prune + re-dial it honored
            //    (a state transition — the silent-branch rule).
            let captured = records.lock().unwrap().clone();

            assert!(
                captured
                    .iter()
                    .any(|e| e.target == "dynrunner_relay"
                        && e.message.contains(MSG_RELAY_ENGAGED)),
                "relay-engaged warn must fire on B's severed leg; trace: {captured:#?}"
            );
            assert!(
                captured
                    .iter()
                    .any(|e| e.target == "dynrunner_relay"
                        && e.message.contains(MSG_DIRECT_RESTORED)),
                "direct-restored info must fire after the leg re-folds; \
                 trace: {captured:#?}"
            );
            assert!(
                captured
                    .iter()
                    .any(|e| e.message.contains(MSG_REDIAL_REQUEST_SENT)),
                "B must narrate the redial request it sent; trace: {captured:#?}"
            );
            assert!(
                captured
                    .iter()
                    .any(|e| e.message.contains(MSG_REDIAL_REQUEST_HONORED)),
                "A must narrate the stale-entry prune + re-dial it honored; \
                 trace: {captured:#?}"
            );
        })
        .await;
}

/// Lifecycle stop: redial tracking for a peer ENDS when the peer
/// leaves the authoritative roster (genuine departure) — the
/// membership-replacement semantics `connect_to_peers` documents.
/// Before the fix the tracker entry survived the roster replacement
/// and kept ticking (milestone WARNs + redial-request emission against
/// a retired peer, forever).
#[tokio::test(flavor = "current_thread")]
async fn member_redial_tracking_stops_on_membership_departure() {
    // This test asserts on TRACKER STATE, not logs — but it hits the
    // same tracing callsites (`observe_disconnect`,
    // `send_redial_request`) as the log-asserting heal test running in
    // a parallel test thread. A thread with NO dispatcher evaluates
    // those callsites against `NoSubscriber`, and tracing's GLOBAL
    // callsite-interest cache then suppresses the events for every
    // thread — including the heal test's capture layer (its log
    // asserts fail only when both tests run together). Registering a
    // (discarded) capture subscriber here keeps the callsite interest
    // honest. Same shape as every other log-touching test in this
    // tree: one thread-local subscriber per test.
    let records: Arc<Mutex<Vec<CapturedEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let layer = CaptureLayer { records };
    let subscriber = Registry::default().with(layer);
    let _guard = tracing::subscriber::set_default(subscriber);

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut peer_b: PeerNetwork<TestId> = PeerNetwork::start("peer-b", None).await.unwrap();

            // Roster names peer-a (lower id — B never dials it, so the
            // leg can only be tracked-disconnected: peer-a does not
            // exist). The cert content is irrelevant on the non-dialing
            // side; reuse B's own PEM.
            let roster = vec![
                PeerConnectionInfo {
                    secondary_id: "peer-a".into(),
                    cert: peer_b.cert_pem().to_string(),
                    ipv4: Some("127.0.0.1".into()),
                    ipv6: None,
                    port: 1,
                    is_observer: false,
                    liveness_port: None,
                    slurm_job_id: None,
                },
                PeerConnectionInfo {
                    secondary_id: "peer-b".into(),
                    cert: peer_b.cert_pem().to_string(),
                    ipv4: Some("127.0.0.1".into()),
                    ipv6: None,
                    port: peer_b.port(),
                    is_observer: false,
                    liveness_port: None,
                    slurm_job_id: None,
                },
            ];
            peer_b.connect_to_peers(&roster);

            // One tick: the reconciliation tracks the missing peer-a leg.
            peer_b
                .reconnect_tick_tx_for_test
                .send(())
                .expect("tick channel open");
            let _ = tokio::time::timeout(Duration::from_millis(50), peer_b.recv_peer()).await;
            assert_eq!(
                peer_b.reconnect_tracker.tracked_count(),
                1,
                "the missing peer-a leg must be tracked"
            );

            // Membership replacement: the new authoritative roster no
            // longer contains peer-a — its redial tracking must stop
            // HERE (the genuine-departure stop), not keep ticking
            // forever against a retired peer.
            let shrunk: Vec<PeerConnectionInfo> = roster
                .into_iter()
                .filter(|p| p.secondary_id == "peer-b")
                .collect();
            peer_b.connect_to_peers(&shrunk);
            assert_eq!(
                peer_b.reconnect_tracker.tracked_count(),
                0,
                "a peer dropped from the authoritative roster must have its \
                 redial tracking forgotten"
            );

            // And a subsequent tick neither re-tracks nor emits redial
            // nudges for the departed peer.
            peer_b
                .reconnect_tick_tx_for_test
                .send(())
                .expect("tick channel open");
            let _ = tokio::time::timeout(Duration::from_millis(50), peer_b.recv_peer()).await;
            assert_eq!(peer_b.reconnect_tracker.tracked_count(), 0);
        })
        .await;
}
