//! `silent_reconnect_partition_heals_with_two_transition_logs` —
//! the canonical end-to-end silent-reconnect scenario. Three peers
//! A/B/C. A partition forces B→A traffic to relay via C; on heal,
//! the trace must contain exactly two state-transition log lines
//! (relay engaged, direct restored) and nothing about
//! redial/reconnect on the relay path. Uses the shared `log_capture`
//! helpers + `pump_b_until` driver.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::super::PeerNetwork;
use super::log_capture::{pump_b_until, CaptureLayer, CapturedEvent};
use super::TestId;
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, PeerConnectionInfo, PeerTransport, MSG_DIRECT_RESTORED, MSG_RELAY_ENGAGED,
};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

/// Silent-reconnect: a partitioned A↔B link self-heals when the
/// Router emits a redial signal, and the operator-visible log
/// trace is exactly two state-transition lines (one warn on the
/// outage, one info on the restore). No "redial" or "reconnect"
/// token appears anywhere in the captured record stream — the
/// reconnect happens silently behind the relay-path log target.
///
/// Topology: 3-peer mesh (peer-a, peer-b, peer-c) fully
/// established. We sever A↔B by removing both sides' channel
/// entries (mirroring the partition; otherwise the half-closed
/// pipe would race with the redial — see the lower-id-dials
/// commentary in `peer/mod.rs::connect_to_peers`). A.send_to(B)
/// then routes via C; that observation is the partition warn.
/// Router emits `redial_target=Some("peer-b")`; A is lower-id so
/// `spawn_redial` fires. After the dial completes (poll on
/// `peer_count()` after draining), a second A.send_to(B) takes
/// the Direct path; that observation is the heal info.
///
/// Forwarder (peer-c) runs a `recv_peer()` loop in a dedicated
/// LocalSet task because `process_inbound`'s forwarding only
/// happens on the async path — `try_recv_peer` would drop the
/// Relay-not-for-us envelope with a warn (intentional, see the
/// pre-refactor `try_recv_peer` parity comment in
/// `Router::process_inbound_sync`).
#[tokio::test(flavor = "current_thread")]
async fn silent_reconnect_partition_heals_with_two_transition_logs() {
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
            let mut peer_a: PeerNetwork<TestId> =
                PeerNetwork::start("peer-a").await.unwrap();
            let mut peer_b: PeerNetwork<TestId> =
                PeerNetwork::start("peer-b").await.unwrap();
            let mut peer_c: PeerNetwork<TestId> =
                PeerNetwork::start("peer-c").await.unwrap();

            let peers = vec![
                PeerConnectionInfo {
                    secondary_id: "peer-a".into(),
                    cert: peer_a.cert_pem().to_string(),
                    ipv4: Some("127.0.0.1".into()),
                    ipv6: None,
                    port: peer_a.port(),
                    is_observer: false,
                },
                PeerConnectionInfo {
                    secondary_id: "peer-b".into(),
                    cert: peer_b.cert_pem().to_string(),
                    ipv4: Some("127.0.0.1".into()),
                    ipv6: None,
                    port: peer_b.port(),
                    is_observer: false,
                },
                PeerConnectionInfo {
                    secondary_id: "peer-c".into(),
                    cert: peer_c.cert_pem().to_string(),
                    ipv4: Some("127.0.0.1".into()),
                    ipv6: None,
                    port: peer_c.port(),
                    is_observer: false,
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
                    sender_id: "peer-a".into(),
                    timestamp: 1.0,
                    secondary_id: "peer-a".into(),
                    active_workers: 0,
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
                    sender_id: "peer-b".into(),
                    timestamp: 1.0,
                    secondary_id: "peer-b".into(),
                    active_workers: 0,
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
            while <PeerNetwork<TestId> as PeerTransport<TestId>>::try_recv_peer(
                &mut peer_a,
            )
            .is_some()
            {}
            while <PeerNetwork<TestId> as PeerTransport<TestId>>::try_recv_peer(
                &mut peer_b,
            )
            .is_some()
            {}
            while <PeerNetwork<TestId> as PeerTransport<TestId>>::try_recv_peer(
                &mut peer_c,
            )
            .is_some()
            {}

            // Pre-partition direct send so A's `route_state` for
            // peer-b is `Direct`. The first observation of a
            // peer's route is silent by design (the `None` arm in
            // Router::observe_relay) — without this warmup, the
            // post-partition relay observation would also be
            // silent and the "peer relay engaged" warn would
            // never fire.
            let warmup: DistributedMessage<TestId> = DistributedMessage::Keepalive {
                sender_id: "peer-a".into(),
                timestamp: 1.5,
                secondary_id: "peer-a".into(),
                active_workers: 1,
            };
            peer_a
                .send_to_peer("peer-b", warmup)
                .await
                .expect("warmup direct send should succeed");
            // Drain peer-b's inbox so the warmup keepalive
            // doesn't pollute the post-partition delivery
            // count. We also drive the runtime briefly so the
            // warmup actually arrives at peer-b.
            tokio::time::sleep(Duration::from_millis(50)).await;
            peer_b.drain_new_connections();
            while <PeerNetwork<TestId> as PeerTransport<TestId>>::try_recv_peer(
                &mut peer_b,
            )
            .is_some()
            {}

            // Partition A↔B by removing both sides' channel
            // entries. Only-removing-A's-side would leave B's
            // half-closed pipe alive; B's later AcceptedPeer-
            // dedup on the redial would drop the new
            // outgoing_tx and tear the freshly-dialed pipe back
            // down (the duplicate-WSS scenario documented in
            // `connect_to_peers`).
            assert!(peer_a.connections.remove("peer-b").is_some());
            assert!(peer_b.connections.remove("peer-a").is_some());
            assert_eq!(
                <PeerNetwork<TestId> as PeerTransport<TestId>>::peer_count(&peer_a),
                1
            );
            assert_eq!(
                <PeerNetwork<TestId> as PeerTransport<TestId>>::peer_count(&peer_b),
                1
            );

            // Move peer_c into a forwarder task. Its sole job is
            // to run `recv_peer()` in a loop so inbound Relay
            // envelopes addressed to peer-b get forwarded
            // synchronously through `Router::process_inbound`'s
            // `apply_forward_decision`. We don't need its
            // returned messages — peer-c is a forwarder, not an
            // endpoint; any keepalive it itself receives is
            // discarded silently.
            let forwarder_handle = tokio::task::spawn_local(async move {
                while let Some(m) = peer_c.recv_peer().await {
                    tracing::warn!(target: "test_debug", "peer-c forwarder received: {m:?}");
                }
            });

            // Send #1: A→B partitioned; Router routes via C and
            // emits redial(B). spawn_redial fires (A lower-id
            // than B), dialing B in the background.
            let msg1: DistributedMessage<TestId> = DistributedMessage::Keepalive {
                sender_id: "peer-a".into(),
                timestamp: 2.0,
                secondary_id: "peer-a".into(),
                active_workers: 5,
            };
            peer_a
                .send_to_peer("peer-b", msg1)
                .await
                .expect("send_to_peer should route via peer-c relay");

            // Pump until B receives the relayed payload (proves
            // forwarder C did its job AND Router::process_inbound
            // unwrapped the Relay-for-self envelope on B).
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            let n1 = pump_b_until(&mut peer_b, &mut peer_a, deadline, |n| n >= 1)
                .await
                .unwrap_or_else(|| {
                    let trace = records.lock().unwrap().clone();
                    panic!(
                        "relayed message should reach peer-b within 5s; \
                         peer_a.peer_count={} peer_b.peer_count={} captured={:#?}",
                        <PeerNetwork<TestId> as PeerTransport<TestId>>::peer_count(
                            &peer_a
                        ),
                        <PeerNetwork<TestId> as PeerTransport<TestId>>::peer_count(
                            &peer_b
                        ),
                        trace
                    )
                });
            assert_eq!(n1, 1, "exactly one relayed message");

            // Wait for the redial to land. A's spawned dial task
            // pushes through `new_conn_tx` on success; we drain
            // each iteration so peer_count reflects the new
            // entry. Up to 100×50ms = 5s — generous for
            // localhost QUIC handshakes.
            let mut healed = false;
            for _ in 0..100 {
                peer_a.drain_new_connections();
                if <PeerNetwork<TestId> as PeerTransport<TestId>>::peer_count(&peer_a)
                    == 2
                {
                    healed = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            assert!(
                healed,
                "redial should have re-established A's connection to peer-b"
            );

            // Send #2: A→B Direct now (Router sees peer-b in
            // connections again). observe_direct fires the
            // "peer direct link restored" info — the heal log.
            let msg2: DistributedMessage<TestId> = DistributedMessage::Keepalive {
                sender_id: "peer-a".into(),
                timestamp: 3.0,
                secondary_id: "peer-a".into(),
                active_workers: 6,
            };
            peer_a
                .send_to_peer("peer-b", msg2)
                .await
                .expect("send_to_peer should now go direct");

            // Wait for the direct payload AND for B's accept-
            // side handler to surface its newly-accepted A
            // pipe. The first send through the new pipe is what
            // triggers B's accept handler past its
            // `MessageReceiver::recv` await — see
            // `accept::handle_accepted_quic`.
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            let n2 = pump_b_until(&mut peer_b, &mut peer_a, deadline, |n| n >= 1)
                .await
                .expect("direct message should reach peer-b within 5s");
            assert_eq!(n2, 1, "exactly one direct message after heal");

            // Tear down forwarder cleanly so leaked tasks don't
            // outlive the LocalSet (which would keep the runtime
            // alive past the test).
            forwarder_handle.abort();

            // ── Assertions on captured log trace ──
            //
            // 1. "peer relay engaged" warn fired on the partition
            //    observation (target=dynrunner_relay).
            // 2. "peer direct link restored" info fired on the
            //    heal observation (target=dynrunner_relay).
            // 3. NO captured event from our own crates
            //    (target starts with `dynrunner`) has a message
            //    containing "redial" or "reconnect" tokens
            //    (case-insensitive). The silent-reconnect
            //    property: the redial signal is a side-effect of
            //    the routing decision, not a separately-logged
            //    event, so neither `spawn_redial` nor any
            //    sub-callee should leak a "redialing" /
            //    "reconnecting" trace. Third-party protocol
            //    vocabulary (e.g. quinn's `RetireConnectionId`
            //    frame name, which lower-cases to a string
            //    happening to contain the substring "reconnect")
            //    is out of scope: it's not operator-visible
            //    framework output and we don't control it.
            let captured = records.lock().unwrap().clone();

            // Tightened: exactly two dynrunner_relay events in this
            // order. Catches a future regression where the Router
            // emits an extra event during the partition or heal
            // (e.g. forwarder-changed info, a debug log during dial,
            // a stray try_recv-drop-relay warn from the sync path).
            let relay_events: Vec<&CapturedEvent> = captured
                .iter()
                .filter(|e| e.target == "dynrunner_relay")
                .collect();
            assert_eq!(
                relay_events.len(),
                2,
                "expected exactly 2 dynrunner_relay events; got {relay_events:#?}; \
                 full trace: {captured:#?}"
            );
            assert!(
                relay_events[0].message.contains(MSG_RELAY_ENGAGED),
                "first dynrunner_relay event must be the relay-engaged warn; got {:?}",
                relay_events[0]
            );
            assert!(
                relay_events[1].message.contains(MSG_DIRECT_RESTORED),
                "second dynrunner_relay event must be the direct-restored info; got {:?}",
                relay_events[1]
            );

            for ev in captured.iter().filter(|e| e.target.starts_with("dynrunner")) {
                let lower = ev.message.to_lowercase();
                assert!(
                    !lower.contains("redial"),
                    "redial token leaked into framework log trace — silent-reconnect violated; \
                     event: {ev:?}; full trace: {captured:#?}"
                );
                assert!(
                    !lower.contains("reconnect"),
                    "reconnect token leaked into framework log trace — silent-reconnect violated; \
                     event: {ev:?}; full trace: {captured:#?}"
                );
            }
        })
        .await;
}
