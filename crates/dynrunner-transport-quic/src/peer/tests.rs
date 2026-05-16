use super::util::parse_cert_pem;
use super::{EitherPeerTransport, NoPeerTransport, PeerNetwork};
use crate::certs::CertPair;
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, PeerConnectionInfo, PeerTransport, MSG_DIRECT_RESTORED,
    MSG_RELAY_ENGAGED,
};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::Registry;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct TestId(String);

#[test]
fn parse_cert_pem_works() {
    let cert = CertPair::generate("test").unwrap();
    let der = parse_cert_pem(&cert.cert_pem);
    assert!(der.is_some());
    assert_eq!(der.unwrap().as_ref(), cert.cert_der.as_ref());
}

#[test]
fn parse_cert_pem_empty_returns_none() {
    assert!(parse_cert_pem("").is_none());
    assert!(parse_cert_pem("not a cert").is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn two_peers_exchange_messages() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Start two peer networks
            let mut peer_a: PeerNetwork<TestId> = PeerNetwork::start("peer-a").await.unwrap();
            let mut peer_b: PeerNetwork<TestId> = PeerNetwork::start("peer-b").await.unwrap();

            let port_a = peer_a.port();
            let port_b = peer_b.port();
            let cert_pem_a = peer_a.cert_pem().to_string();
            let cert_pem_b = peer_b.cert_pem().to_string();

            // Create peer info for both
            let peers = vec![
                PeerConnectionInfo {
                    secondary_id: "peer-a".into(),
                    cert: cert_pem_a,
                    ipv4: Some("127.0.0.1".into()),
                    ipv6: None,
                    port: port_a,
                    is_observer: false,
                },
                PeerConnectionInfo {
                    secondary_id: "peer-b".into(),
                    cert: cert_pem_b,
                    ipv4: Some("127.0.0.1".into()),
                    ipv6: None,
                    port: port_b,
                    is_observer: false,
                },
            ];

            // Each peer kicks off outgoing dials. Non-blocking — the
            // actual handshakes run on spawned tasks; the sleep below
            // gives them time to complete before we drain.
            peer_a.connect_to_peers(&peers);
            peer_b.connect_to_peers(&peers);

            // Give accept loops time to register incoming connections
            tokio::time::sleep(Duration::from_millis(100)).await;
            peer_a.drain_new_connections();
            peer_b.drain_new_connections();

            // Peer A broadcasts a message
            let msg: DistributedMessage<TestId> = DistributedMessage::Keepalive {
                sender_id: "peer-a".into(),
                timestamp: 1.0,
                secondary_id: "peer-a".into(),
                active_workers: 2,
            };
            peer_a.broadcast(msg).await.unwrap();

            // Peer B should receive it
            let received = tokio::time::timeout(Duration::from_secs(5), peer_b.recv_peer())
                .await
                .expect("timeout waiting for peer message")
                .expect("no message received");

            assert_eq!(received.sender_id(), "peer-a");
            match received {
                DistributedMessage::Keepalive { active_workers, .. } => {
                    assert_eq!(active_workers, 2);
                }
                _ => panic!("expected Keepalive"),
            }
        })
        .await;
}

/// Lower-id-dials: only the lexicographically-lower peer initiates
/// the connection; the higher-id peer relies on its accept loop. This
/// test exercises the asymmetry by having a HIGHER-id peer call
/// `connect_to_peers` first — it must NOT dial, and the connection
/// must still establish via the LOWER-id peer's later dial. Pre-fix
/// both peers dialed concurrently, leaving the duplicate connection
/// (and the resulting drop-tear-down cascade) as the root cause of
/// the "all peers disconnected during broadcast" bug both consumers
/// hit on Krater.
#[tokio::test(flavor = "current_thread")]
async fn higher_id_does_not_dial_lower_id() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut peer_low: PeerNetwork<TestId> = PeerNetwork::start("peer-a").await.unwrap();
            let mut peer_high: PeerNetwork<TestId> = PeerNetwork::start("peer-b").await.unwrap();
            let port_low = peer_low.port();
            let port_high = peer_high.port();
            let cert_low = peer_low.cert_pem().to_string();
            let cert_high = peer_high.cert_pem().to_string();

            let peers = vec![
                PeerConnectionInfo {
                    secondary_id: "peer-a".into(),
                    cert: cert_low,
                    ipv4: Some("127.0.0.1".into()),
                    ipv6: None,
                    port: port_low,
                    is_observer: false,
                },
                PeerConnectionInfo {
                    secondary_id: "peer-b".into(),
                    cert: cert_high,
                    ipv4: Some("127.0.0.1".into()),
                    ipv6: None,
                    port: port_high,
                    is_observer: false,
                },
            ];

            // Higher-id peer attempts connect_to_peers FIRST. The
            // skip-on-higher-id rule must keep it from dialing
            // peer-a, otherwise both sides would race.
            peer_high.connect_to_peers(&peers);
            // Brief pause so any (incorrect) dial-spawn would have
            // a chance to land before peer-a starts its dial.
            tokio::time::sleep(Duration::from_millis(50)).await;
            peer_low.connect_to_peers(&peers);

            // Settle the dial; peer-low's connect-side AcceptedPeer
            // (no message exchange needed) lands in its own
            // new_conn_rx and gets drained immediately. peer-high's
            // accept-side blocks on the first incoming message — its
            // AcceptedPeer doesn't surface until peer-low actually
            // sends data over the WSS pipe. So we have to broadcast
            // before peer-high's peer_count can reflect the
            // accepted connection.
            tokio::time::sleep(Duration::from_millis(100)).await;
            peer_low.drain_new_connections();
            assert_eq!(
                PeerTransport::<TestId>::peer_count(&peer_low),
                1,
                "lower-id peer should have one connection (its outbound dial)"
            );

            // Broadcast triggers peer-high's accept-side to read the
            // first message, identify peer-low, and queue its
            // AcceptedPeer. recv_peer's internal drain then
            // populates peer-high's connections.
            let msg: DistributedMessage<TestId> = DistributedMessage::Keepalive {
                sender_id: "peer-a".into(),
                timestamp: 1.0,
                secondary_id: "peer-a".into(),
                active_workers: 2,
            };
            peer_low.broadcast(msg).await.unwrap();
            let received = tokio::time::timeout(Duration::from_secs(5), peer_high.recv_peer())
                .await
                .expect("timeout waiting for peer message")
                .expect("no message received");
            assert_eq!(received.sender_id(), "peer-a");

            assert_eq!(
                PeerTransport::<TestId>::peer_count(&peer_high),
                1,
                "higher-id peer should have one connection (accepted from lower-id)"
            );
            // No "peer disconnected during broadcast" warns: the
            // single-WSS topology has nothing to tear down. peer_low
            // still sees its single connection after broadcasting.
            assert_eq!(
                PeerTransport::<TestId>::peer_count(&peer_low),
                1,
                "lower-id peer's connection survived the broadcast"
            );
        })
        .await;
}

/// Regression: the reconnect-tick channel must survive an outer
/// caller dropping the `recv_peer` future mid-poll. Pre-fix,
/// `recv_peer` opened with `let mut tick_rx =
/// self.reconnect_tick_rx.take();` and only restored
/// `self.reconnect_tick_rx` inside each arm body. If the outer
/// caller's `tokio::select!` dropped the `recv_peer` future while
/// the inner select was pending, the stack-local `tick_rx` (still
/// holding the receiver) dropped together with the outer future and
/// `self.reconnect_tick_rx` stayed `None` forever — silently
/// disabling the periodic reconnect tick for the lifetime of the
/// coordinator.
///
/// This test pins the contract directly:
/// 1. Pre-arm a fake peer in `peer_dial_info` (NOT in `connections`)
///    so a fired tick has an observable side effect — the
///    reconnect tracker registers the peer's "disconnect" and
///    `tracked_count` goes from 0 → 1.
/// 2. Race a `recv_peer` against a short timeout, letting the
///    timeout win. `recv_peer` is polled (entering the inner
///    `select!`) and then dropped. Pre-fix this destroys the tick
///    receiver; post-fix the receiver stays in the field.
/// 3. Inject a synthetic tick through the test-only sender clone.
/// 4. Race a second `recv_peer` against a slightly longer timeout.
///    The buffered tick must be consumed: the tick arm fires,
///    `process_reconnect_tick` runs, and the tracker registers the
///    fake peer. Pre-fix the tick arm would be `pending().await`
///    (because `tick_rx.take()` returned `None`) and the tracker
///    would stay empty.
#[tokio::test(flavor = "current_thread")]
async fn recv_peer_tick_survives_outer_drop() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut peer: PeerNetwork<TestId> =
                PeerNetwork::start("peer-a").await.unwrap();

            // Pre-arm a fake peer entry so `process_reconnect_tick`
            // has work to do. The fake peer id sorts higher than
            // ours so the lower-id-dials rule lets `spawn_redial`
            // reach `spawn_dial_task`; the dial itself fails
            // silently because no server is bound — irrelevant to
            // the side effect we assert on (the tracker increment).
            peer.peer_dial_info.insert(
                "peer-z".into(),
                PeerConnectionInfo {
                    secondary_id: "peer-z".into(),
                    cert: peer.cert_pem().to_string(),
                    ipv4: Some("127.0.0.1".into()),
                    ipv6: None,
                    port: 1,
                    is_observer: false,
                },
            );
            assert_eq!(peer.reconnect_tracker.tracked_count(), 0);

            // Step 1: race recv_peer against a short timeout so
            // recv_peer is polled (entering the inner select with
            // all three arms Pending) and then dropped. The
            // timeout is much shorter than the natural 5s tick
            // cadence so no real tick can sneak in and complete
            // the recv_peer before the drop.
            tokio::select! {
                _ = peer.recv_peer() => {
                    panic!("recv_peer should not resolve in this race");
                }
                _ = tokio::time::sleep(Duration::from_millis(50)) => {}
            }

            // Pre-fix invariant check: tracker still empty.
            // (Both pre- and post-fix this should hold — no tick
            // fired yet.)
            assert_eq!(
                peer.reconnect_tracker.tracked_count(),
                0,
                "no tick should have fired during the dropped recv_peer"
            );

            // Step 2: inject a synthetic tick via the test-only
            // sender. Two failure modes the contract guards
            // against, both rolled into this assertion:
            //   - Channel closed: pre-fix the original receiver
            //     was moved into a stack-local inside recv_peer
            //     and dropped along with the dropped future, so
            //     the underlying mpsc channel has no receiver and
            //     `send` returns `Err`.
            //   - Channel alive but receiver detached from
            //     `self.reconnect_tick_rx`: a hypothetical fix
            //     that kept the channel alive but stashed the
            //     receiver somewhere `recv_peer` no longer polls
            //     would let the send succeed but produce the
            //     same silent-disable. Step 3 below catches that
            //     variant.
            peer.reconnect_tick_tx_for_test
                .send(())
                .expect(
                    "tick channel must survive recv_peer drop; \
                     pre-fix this fails because the receiver was \
                     moved into the dropped future",
                );

            // Step 3: race a second recv_peer against a longer
            // timeout. Post-fix: the tick arm picks up the
            // buffered tick, runs `process_reconnect_tick`,
            // tracker registers "peer-z" disconnect (count → 1),
            // recv_peer loops back to await again, and the
            // timeout eventually wins. Pre-fix: the tick arm is
            // wired to a `None`-taken receiver via
            // `pending::<Option<()>>().await`, the buffered tick
            // is never observed, and the tracker stays empty.
            tokio::select! {
                _ = peer.recv_peer() => {
                    panic!("recv_peer should not resolve in this race");
                }
                _ = tokio::time::sleep(Duration::from_millis(200)) => {}
            }

            assert_eq!(
                peer.reconnect_tracker.tracked_count(),
                1,
                "buffered tick must be observed after outer recv_peer drop; \
                 pre-fix this is 0 because the tick receiver was destroyed",
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn no_peer_transport_never_receives() {
    let mut noop = NoPeerTransport;
    noop.broadcast(DistributedMessage::<TestId>::Keepalive {
        sender_id: "x".into(),
        timestamp: 0.0,
        secondary_id: "x".into(),
        active_workers: 0,
    })
    .await
    .unwrap();
    assert_eq!(PeerTransport::<TestId>::peer_count(&noop), 0);
    assert!(PeerTransport::<TestId>::try_recv_peer(&mut noop).is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn either_peer_transport_disabled_acts_like_no_peer() {
    // `EitherPeerTransport::Disabled` must behave identically to a
    // bare `NoPeerTransport`: zero peers, broadcasts succeed silently,
    // try_recv_peer returns None. This is the contract the secondary
    // relies on when `--disable-peer-overlay` is set.
    let mut either: EitherPeerTransport<TestId> =
        EitherPeerTransport::Disabled(NoPeerTransport);

    either
        .broadcast(DistributedMessage::Keepalive {
            sender_id: "x".into(),
            timestamp: 0.0,
            secondary_id: "x".into(),
            active_workers: 0,
        })
        .await
        .unwrap();
    assert_eq!(PeerTransport::<TestId>::peer_count(&either), 0);
    assert!(PeerTransport::<TestId>::try_recv_peer(&mut either).is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn either_peer_transport_real_round_trips_a_message() {
    // Mirror `two_peers_exchange_messages` but route everything through
    // `EitherPeerTransport::Real(...)` to prove the enum doesn't drop
    // the active variant's behavior.
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let pn_a: PeerNetwork<TestId> = PeerNetwork::start("peer-a").await.unwrap();
            let pn_b: PeerNetwork<TestId> = PeerNetwork::start("peer-b").await.unwrap();

            let port_a = pn_a.port();
            let port_b = pn_b.port();
            let cert_pem_a = pn_a.cert_pem().to_string();
            let cert_pem_b = pn_b.cert_pem().to_string();

            let mut peer_a: EitherPeerTransport<TestId> = EitherPeerTransport::Real(pn_a);
            let mut peer_b: EitherPeerTransport<TestId> = EitherPeerTransport::Real(pn_b);

            let peers = vec![
                PeerConnectionInfo {
                    secondary_id: "peer-a".into(),
                    cert: cert_pem_a,
                    ipv4: Some("127.0.0.1".into()),
                    ipv6: None,
                    port: port_a,
                    is_observer: false,
                },
                PeerConnectionInfo {
                    secondary_id: "peer-b".into(),
                    cert: cert_pem_b,
                    ipv4: Some("127.0.0.1".into()),
                    ipv6: None,
                    port: port_b,
                    is_observer: false,
                },
            ];

            // Per-peer dials run as spawned tasks; the sleep gives them
            // time to land before we broadcast.
            peer_a.connect_to_peers(&peers).await;
            peer_b.connect_to_peers(&peers).await;
            tokio::time::sleep(Duration::from_millis(100)).await;

            let msg: DistributedMessage<TestId> = DistributedMessage::Keepalive {
                sender_id: "peer-a".into(),
                timestamp: 1.0,
                secondary_id: "peer-a".into(),
                active_workers: 2,
            };
            peer_a.broadcast(msg).await.unwrap();

            let received = tokio::time::timeout(Duration::from_secs(5), peer_b.recv_peer())
                .await
                .expect("timeout waiting for peer message")
                .expect("no message received");

            assert_eq!(received.sender_id(), "peer-a");
            match received {
                DistributedMessage::Keepalive { active_workers, .. } => {
                    assert_eq!(active_workers, 2);
                }
                _ => panic!("expected Keepalive"),
            }
        })
        .await;
}

/// One captured `tracing::Event` reduced to the two fields the
/// silent-reconnect assertions care about: the event's `target`
/// metadata (so we can scope to `dynrunner_relay`) and its
/// formatted message text (so we can match on substrings).
///
/// Visiting fields is the only way to extract the message body
/// from an `Event` without a full `fmt::Layer`. The `tracing`
/// macros encode the message as a field named `message` whose
/// value is rendered through `record_debug` for the typical
/// `tracing::warn!("...")` / `tracing::info!(target: "...", "...")`
/// invocations on the relay path.
#[derive(Debug, Clone)]
struct CapturedEvent {
    target: String,
    message: String,
}

/// `tracing` field visitor: extract the formatted message body of
/// an event into a `String`. The macros render the message field
/// via `record_debug`; `record_str` is wired up too for forward
/// compatibility with future tracing versions that prefer it.
struct MessageVisitor(String);

impl Visit for MessageVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.0 = value.to_string();
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

/// `tracing-subscriber` Layer that appends every `Event` it sees
/// (regardless of target / level) to a shared buffer. Captured
/// records are inspected after the scenario completes — the
/// silent-reconnect property is "the only relay-path log lines
/// between partition and heal are exactly the two state-transition
/// observers; nothing anywhere mentions redial/reconnect", which
/// requires looking at every event rather than pre-filtering by
/// target inside the layer.
struct CaptureLayer {
    records: Arc<Mutex<Vec<CapturedEvent>>>,
}

impl<S: tracing::Subscriber> Layer<S> for CaptureLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let target = event.metadata().target().to_string();
        let mut visitor = MessageVisitor(String::new());
        event.record(&mut visitor);
        // Lock can poison if a concurrent test panics, but this
        // layer is only ever installed via `set_default` for the
        // duration of a single test on a current_thread runtime —
        // a poisoned mutex here means the scenario already failed,
        // so we just swallow the error rather than masking it.
        if let Ok(mut buf) = self.records.lock() {
            buf.push(CapturedEvent {
                target,
                message: visitor.0,
            });
        }
    }
}

/// Round-robin pump that calls `try_recv_peer` on each of the
/// passed peers until either `done(received_count)` returns
/// `true` or the wall-clock deadline expires. We use
/// `try_recv_peer` per peer instead of `recv_peer` because each
/// async call would borrow `&mut peer` exclusively for the
/// duration of the `await`, blocking the round-robin from
/// advancing other peers' state.
///
/// Caveat: `try_recv_peer` runs the **synchronous** Router path
/// which drops Relay envelopes that aren't for self with a warn
/// (see `Router::process_inbound_sync`). For a forwarder C, that
/// would defeat the test. So in this scenario the forwarder
/// (peer-c) is driven by a dedicated `recv_peer()` task spawned
/// inside the LocalSet — see `silent_reconnect_*` below.
///
/// Returns `Some(n)` with `n` = number of payload messages
/// delivered to peer-b on success; `None` on timeout.
async fn pump_b_until<F>(
    peer_b: &mut PeerNetwork<TestId>,
    peer_a_drain: &mut PeerNetwork<TestId>,
    deadline: std::time::Instant,
    mut done: F,
) -> Option<usize>
where
    F: FnMut(usize) -> bool,
{
    let mut received = 0usize;
    while std::time::Instant::now() < deadline {
        // Cooperative tick — yields to the runtime so accept-loop
        // tasks, redial dial tasks, and the forwarder's recv_peer
        // task can make progress.
        tokio::time::sleep(Duration::from_millis(20)).await;
        // Drain freshly-accepted connections on A so the redial's
        // `AcceptedPeer` is observable via `peer_count()` without
        // having to call `recv_peer` (which would consume a
        // payload and complicate the assertion logic below).
        peer_a_drain.drain_new_connections();
        // Drain B's accept-loop pending registrations the same
        // way before we look at its incoming inbox.
        peer_b.drain_new_connections();
        while let Some(msg) =
            <PeerNetwork<TestId> as PeerTransport<TestId>>::try_recv_peer(peer_b)
        {
            // Sanity: the relayed envelope's inner is delivered
            // unwrapped (Router::process_inbound_sync's Relay-for-
            // self arm). Anything else means the Router or accept
            // loop misrouted.
            assert!(
                matches!(msg, DistributedMessage::Keepalive { .. }),
                "unexpected delivered variant on peer-b: {msg:?}"
            );
            received += 1;
            if done(received) {
                return Some(received);
            }
        }
        if done(received) {
            return Some(received);
        }
    }
    None
}

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
                loop {
                    match peer_c.recv_peer().await {
                        Some(m) => {
                            tracing::warn!(target: "test_debug", "peer-c forwarder received: {m:?}");
                        }
                        None => break,
                    }
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
