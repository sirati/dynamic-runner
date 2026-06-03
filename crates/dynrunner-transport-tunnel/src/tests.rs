//! Unit tests for [`TunneledPeerTransport`]. Driven by hand-
//! constructed channel pairs that stand in for the per-secondary
//! tunnel writers + inbound demux. The pattern mirrors
//! `dynrunner_manager_distributed::primary::test_helpers::setup_test`,
//! which is what the integration test in
//! `crates/dynrunner-manager-distributed/tests/network_integration.rs`
//! actually uses; here we exercise the transport in isolation
//! with no manager coordinator wrapped around it.
use crate::{InboundTap, PeerRegistration, RegistrationSink, TunneledPeerTransport};
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, DistributedMessage, KeepaliveRole, PeerId, PeerTransport,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct TestId(String);

fn keepalive(sender: &str) -> DistributedMessage<TestId> {
    DistributedMessage::Keepalive {
        sender_id: sender.into(),
        timestamp: 1.0,
        secondary_id: sender.into(),
        active_workers: 0,
        emitter_role: KeepaliveRole::Secondary,
    }
}

/// 1 primary + 2 secondaries fixture: pre-register both
/// secondaries' writers in the shared outgoing table BEFORE the
/// transport is asked to send, mirroring what `NetworkServer`'s
/// accept-loop `drain_new_connections` would do as each secondary
/// completes handshake. Returns the transport plus the two
/// per-secondary receivers so the test can assert on what each
/// secondary actually received.
// The 4-tuple shape is locked by the fixture contract and only
// used inside this test module; factoring would split the test
// setup across modules for no maintainability gain.
#[allow(clippy::type_complexity)]
fn fixture() -> (
    TunneledPeerTransport<TestId>,
    mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    InboundTap<TestId>,
) {
    // The registration sink is dropped: this fixture registers writers
    // DIRECTLY into the shared `outgoing` table (the in-process / test
    // path), so the `recv_peer` demux's `new_conn_rx` arm parks.
    let (transport, outgoing, inbound_tap, _reg_sink) =
        TunneledPeerTransport::<TestId>::new("primary".into());
    let (sec_a_tx, sec_a_rx) = mpsc::unbounded_channel();
    let (sec_b_tx, sec_b_rx) = mpsc::unbounded_channel();
    outgoing.borrow_mut().insert("sec-A".into(), sec_a_tx);
    outgoing.borrow_mut().insert("sec-B".into(), sec_b_tx);
    (transport, sec_a_rx, sec_b_rx, inbound_tap)
}

/// `send_to_peer(id, msg)` reaches exactly the writer for `id`
/// and nothing else. The submitter primary's `task::handle_task_request`
/// relay arm sits on top of this (the coordinator edge resolves a
/// typed `Destination` to the peer-id, then calls `send_to_peer`).
#[tokio::test(flavor = "current_thread")]
async fn send_to_peer_reaches_only_target() {
    let (mut transport, mut sec_a_rx, mut sec_b_rx, _tap) = fixture();
    transport
        .send_to_peer("sec-A", keepalive("primary"))
        .await
        .unwrap();
    assert!(sec_a_rx.try_recv().is_ok(), "sec-A must receive");
    assert!(sec_b_rx.try_recv().is_err(), "sec-B must NOT receive");
}

/// `broadcast(msg)` reaches every writer in the table.
#[tokio::test(flavor = "current_thread")]
async fn broadcast_reaches_all() {
    let (mut transport, mut sec_a_rx, mut sec_b_rx, _tap) = fixture();
    transport.broadcast(keepalive("primary")).await.unwrap();
    assert!(sec_a_rx.try_recv().is_ok());
    assert!(sec_b_rx.try_recv().is_ok());
}

/// `recv_peer()` returns whatever the legacy transport's tap
/// pushed through `inbound_tap`. Pre-Step-6 nothing consumes this
/// in production; the unit assertion here pins the wire path so
/// Step 6's `select! { peer_transport.recv_peer() }` arm has a
/// load-bearing channel underneath.
#[tokio::test(flavor = "current_thread")]
async fn recv_peer_yields_tapped_inbound() {
    let (mut transport, _sec_a_rx, _sec_b_rx, tap) = fixture();
    tap.send(keepalive("sec-A")).unwrap();
    let got = transport.recv_peer().await.expect("must receive tapped");
    assert_eq!(got.sender_id(), "sec-A");
}

/// `try_recv_peer()` returns `None` when the tap queue is empty.
#[tokio::test(flavor = "current_thread")]
async fn try_recv_peer_empty_returns_none() {
    let (mut transport, _sec_a_rx, _sec_b_rx, _tap) = fixture();
    assert!(transport.try_recv_peer().is_none());
}

/// `peer_count()` reflects the shared outgoing table size — the
/// gate `peer_transport.peer_count() > 0` Step 6 will use to relax
/// the demoted-primary disconnect detection needs this to be
/// accurate against the same writer table the legacy transport
/// populates.
#[tokio::test(flavor = "current_thread")]
async fn peer_count_reflects_outgoing_table() {
    let (transport, outgoing, _tap, _reg_sink) =
        TunneledPeerTransport::<TestId>::new("primary".into());
    assert_eq!(transport.peer_count(), 0);
    let (a_tx, _a_rx) = mpsc::unbounded_channel::<DistributedMessage<TestId>>();
    outgoing.borrow_mut().insert("sec-A".into(), a_tx);
    assert_eq!(transport.peer_count(), 1);
    let (b_tx, _b_rx) = mpsc::unbounded_channel::<DistributedMessage<TestId>>();
    outgoing.borrow_mut().insert("sec-B".into(), b_tx);
    assert_eq!(transport.peer_count(), 2);
}

/// Wrap a payload in a `Relay` envelope as another peer would put it
/// on the wire toward `target` (originator path = `[sender]`).
fn relay_envelope(
    sender: &str,
    target: &str,
    inner: DistributedMessage<TestId>,
) -> DistributedMessage<TestId> {
    DistributedMessage::Relay {
        sender_id: sender.into(),
        timestamp: 1.0,
        target_id: target.into(),
        relay_id: 0,
        path: vec![sender.into()],
        inner: Box::new(inner),
    }
}

/// The submitter behaves as a real relay peer: a `Relay` envelope from
/// sec-A addressed to sec-B (which the submitter has a direct writer
/// for) is FORWARDED through the submitter to sec-B and consumed —
/// never surfaced from `recv_peer`. This is the relay capability the
/// `Router` wiring grants: the submitter forwards a secondary-A→
/// secondary-B frame through itself, exactly like `PeerNetwork` does.
#[tokio::test(flavor = "current_thread")]
async fn relay_envelope_forwarded_through_submitter_to_target() {
    let (mut transport, mut sec_a_rx, mut sec_b_rx, tap) = fixture();
    // sec-A reaches the submitter (via its tunnel) carrying a Relay for
    // sec-B's writer, then a plain frame addressed at the submitter.
    tap.send(relay_envelope("sec-A", "sec-B", keepalive("sec-A")))
        .unwrap();
    tap.send(keepalive("sec-A")).unwrap();
    // recv_peer forwards the relay internally (loop, no yield) and then
    // delivers the trailing direct frame — so the FIRST yield is the
    // trailing frame, proving the relay was consumed not yielded.
    // Fully deterministic: no timer, both frames are already queued.
    let got = transport.recv_peer().await.expect("trailing frame delivers");
    assert!(
        matches!(got, DistributedMessage::Keepalive { .. }),
        "first yielded frame must be the trailing direct keepalive, \
         not the forwarded relay: {got:?}"
    );
    // The inner payload landed on sec-B's writer (the forward target).
    let forwarded = sec_b_rx
        .try_recv()
        .expect("sec-B must receive the forwarded inner");
    assert_eq!(forwarded.sender_id(), "sec-A");
    // sec-A (the originator) does not receive its own relay back.
    assert!(
        sec_a_rx.try_recv().is_err(),
        "originator must not receive the forward"
    );
}

/// A `Relay` envelope addressed to the submitter ITSELF is unwrapped
/// and the inner payload is delivered up from `recv_peer` — the
/// receiver-side of the same `Router` fabric.
#[tokio::test(flavor = "current_thread")]
async fn relay_envelope_addressed_to_self_is_unwrapped() {
    let (mut transport, _sec_a_rx, _sec_b_rx, tap) = fixture();
    tap.send(relay_envelope("sec-A", "primary", keepalive("sec-A")))
        .unwrap();
    let got = transport.recv_peer().await.expect("must deliver unwrapped inner");
    assert_eq!(got.sender_id(), "sec-A");
    assert!(
        matches!(got, DistributedMessage::Keepalive { .. }),
        "delivered frame must be the unwrapped inner, not the Relay wrapper: {got:?}"
    );
}

/// A global-state (CRDT) broadcast frame: a `ClusterMutation` carrying
/// one `PrimaryChanged` — the canonical bootstrap global-state mutation
/// (the plan's exactly-once subject). The submitter fans this out over
/// `Destination::All` / `broadcast`.
fn cluster_mutation(epoch: u64) -> DistributedMessage<TestId> {
    DistributedMessage::ClusterMutation {
        sender_id: "primary".into(),
        timestamp: 1.0,
        mutations: vec![ClusterMutation::PrimaryChanged {
            new: "primary".into(),
            epoch,
            reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
        }],
    }
}

/// Count every frame currently queued on `rx` (non-blocking). The
/// exactly-once assertion is at the recv/wire layer: COUNT deliveries
/// per peer == 1 — NOT merely that the idempotent CRDT apply is a
/// no-op (a double-send would still leave count == 2 here, which an
/// apply-idempotency check would never catch).
fn drain_count(rx: &mut mpsc::UnboundedReceiver<DistributedMessage<TestId>>) -> usize {
    let mut n = 0;
    while rx.try_recv().is_ok() {
        n += 1;
    }
    n
}

/// EXACTLY-ONCE at the wire layer: a `Destination::All` global-state
/// (CRDT) broadcast lands on each peer's writer EXACTLY ONCE. The
/// guarantee is structural — `broadcast` is a single direct fan-out
/// over the Router-backed `outgoing` table (one `msg.clone()` per
/// connection, not a relayed `Relay` envelope), so no peer is reached
/// both directly and via relay. Asserting the per-peer delivery COUNT
/// is what catches a double-send; an idempotent-apply check would not.
#[tokio::test(flavor = "current_thread")]
async fn broadcast_global_state_delivered_exactly_once_per_peer() {
    let (mut transport, mut sec_a_rx, mut sec_b_rx, _tap) = fixture();
    transport.broadcast(cluster_mutation(1)).await.unwrap();
    assert_eq!(
        drain_count(&mut sec_a_rx),
        1,
        "sec-A must receive the CRDT broadcast EXACTLY once"
    );
    assert_eq!(
        drain_count(&mut sec_b_rx),
        1,
        "sec-B must receive the CRDT broadcast EXACTLY once"
    );
}

/// A peer that handshaked since the last `recv_peer` poll — its writer
/// still queued on the registration sink (`new_conn_rx`), NOT yet in
/// `outgoing` — is NOT silently skipped by a broadcast, and still
/// receives it EXACTLY once. `broadcast` drains pending registrations
/// first (mirror of `PeerNetwork::broadcast`'s leading
/// `drain_new_connections`), so the freshly-joined peer is part of the
/// one fan-out. Without the drain this peer's count would be 0 (missed
/// delivery); a naive double-drain would make it 2.
#[tokio::test(flavor = "current_thread")]
async fn broadcast_includes_freshly_registered_peer_exactly_once() {
    // Hold the registration sink this time (the standard fixture drops
    // it). sec-A is pre-registered directly into `outgoing`; sec-C
    // arrives ONLY as a pending registration on the sink and is never
    // drained before the broadcast call.
    let (mut transport, outgoing, _inbound_tap, reg_sink): (
        TunneledPeerTransport<TestId>,
        _,
        InboundTap<TestId>,
        RegistrationSink<TestId>,
    ) = TunneledPeerTransport::<TestId>::new("primary".into());
    let (sec_a_tx, mut sec_a_rx) = mpsc::unbounded_channel();
    outgoing.borrow_mut().insert("sec-A".into(), sec_a_tx);

    let (sec_c_tx, mut sec_c_rx) = mpsc::unbounded_channel();
    reg_sink
        .send(PeerRegistration {
            peer_id: "sec-C".into(),
            outgoing_tx: sec_c_tx,
        })
        .expect("registration sink must accept");

    // No `recv_peer` / `try_recv_peer` ran, so sec-C is still only on
    // the sink — broadcast must drain it in before fanning out.
    transport.broadcast(cluster_mutation(1)).await.unwrap();

    assert_eq!(
        drain_count(&mut sec_a_rx),
        1,
        "already-registered sec-A must receive EXACTLY once"
    );
    assert_eq!(
        drain_count(&mut sec_c_rx),
        1,
        "freshly-registered sec-C must receive EXACTLY once (drained in, not skipped)"
    );
    assert_eq!(
        transport.peer_count(),
        2,
        "broadcast drained the pending registration into the writer table"
    );
}

/// A dead writer (the secondary went away) is pruned from the table on
/// broadcast detection — keeping membership (`peer_count` / `has_peer`)
/// accurate and ensuring a later broadcast does not re-attempt it. The
/// submitter has no dial path, so removal is the whole disposition (no
/// redial, unlike `PeerNetwork::broadcast`).
#[tokio::test(flavor = "current_thread")]
async fn broadcast_prunes_dead_writer() {
    let (mut transport, sec_a_rx, mut sec_b_rx, _tap) = fixture();
    // Drop sec-A's receiver: its writer is now closed.
    drop(sec_a_rx);
    assert_eq!(transport.peer_count(), 2, "both registered pre-broadcast");

    transport.broadcast(cluster_mutation(1)).await.unwrap();

    assert_eq!(
        transport.peer_count(),
        1,
        "dead sec-A writer pruned; live sec-B retained"
    );
    assert!(
        !transport.has_peer(&PeerId::from("sec-A")),
        "dead sec-A no longer a member"
    );
    assert!(
        transport.has_peer(&PeerId::from("sec-B")),
        "live sec-B still a member"
    );
    // The live peer still got exactly one frame despite the dead peer.
    assert_eq!(
        drain_count(&mut sec_b_rx),
        1,
        "live sec-B receives the broadcast EXACTLY once"
    );
}
