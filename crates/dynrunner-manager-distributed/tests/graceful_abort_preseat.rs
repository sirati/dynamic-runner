//! Pre-seat SIGUSR2 graceful-abort tests — the production specimen replay:
//! a late-joiner observer received the CLI-documented graceful-abort signal
//! DURING its bootstrap window (handler not yet armed) and died via the
//! kernel's default disposition. These tests pin the fix: the
//! [`GracefulAbortTrigger`] armed at process entry survives a pre-seat
//! SIGUSR2, latches it, delivers it through the REAL coordinator run loop
//! on seat exactly like a post-seat signal, and narrates an undelivered
//! latched intent when the bootstrap fails.
//!
//! # Why a separate integration binary
//!
//! Signals are process-global: raising SIGUSR2 here would feed every armed
//! `SignalKind::user_defined2()` stream in the process, and the unit-test
//! binary runs many `ObserverCoordinator::run` loops (each arming one) in
//! parallel — a raise there would inject spurious graceful-abort triggers
//! into unrelated tests. This binary is its own process and only these
//! tests run in it; within it, the raising tests are serialized behind a
//! single `Mutex` (the `panik_watcher` SIGTERM harness style).
//!
//! The mesh harness (`transport_with_peers` / `observer_mesh`) is the
//! faithful copy of the unit suite's
//! (`src/observer/coordinator/tests.rs`) — see its docs there.

use std::collections::HashMap;
use std::time::Duration;

use dynrunner_protocol_primary_secondary::cluster_mutation::ClusterMutation;
use dynrunner_protocol_primary_secondary::{
    Destination, DistributedMessage, PrimaryChangeReason,
};
use dynrunner_transport_channel::ChannelPeerTransport;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use dynrunner_manager_distributed::GracefulAbortTrigger;
use dynrunner_manager_distributed::cluster_state::ClusterState;
use dynrunner_manager_distributed::observer::{
    ObserverConfig, ObserverCoordinator, ObserverTerminal,
};
use dynrunner_manager_distributed::process::{LocalRole, Mesh, MeshClient, RoleInbox};
use dynrunner_protocol_primary_secondary::address::PeerId;

/// Minimal serializable identifier for the observer tests.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct TestId(String);

/// Default observer config with short backstop windows so wall-clock
/// tests finish quickly. `fleet_dead_timeout` doubles as the loop's
/// re-check cadence, so it is kept small to drive the silence backstop too.
fn observer_config(node_id: &str) -> ObserverConfig {
    ObserverConfig {
        node_id: node_id.to_string(),
        fleet_dead_timeout: Duration::from_millis(50),
        peer_timeout: Duration::from_secs(300),
        panik_watcher_paths: Vec::new(),
        panik_watcher_poll_interval: Duration::from_secs(60),
        // LONG (the production default, 20 min) so the rc-B
        // report-and-retry pins in this suite never trip the last-resort
        // terminal inside their wall-clock windows; the fleet-death tests
        // shrink it explicitly.
        fleet_death_presumption: ObserverConfig::DEFAULT_FLEET_DEATH_PRESUMPTION,
    }
}

/// Live peer receivers that must be kept alive for the duration of a test:
/// the router PRUNES a peer outbox whose receiver was dropped (a closed
/// channel) on the first failed send, which would silently drop
/// `peer_count()` to zero and trip the fleet-dead grace. Tests bind this so
/// the dummy peers stay "connected" for as long as the observer runs.
type PeerKeepalive = Vec<mpsc::UnboundedReceiver<DistributedMessage<TestId>>>;

/// Build a `ChannelPeerTransport` with `peer_count` dummy peer outboxes
/// and an inbound receiver fed by the returned sender. The peers are keyed
/// `peer-{i}` (never `"primary"`), so `Destination::Primary` is
/// unrouteable unless the test also wires a primary-keyed outbox. The
/// returned [`PeerKeepalive`] MUST be held by the test (see its doc) so the
/// dummy peers are not pruned on a failed send. `recv_peer` pends until the
/// returned `inbound_tx` is fed.
fn transport_with_peers(
    node_id: &str,
    peer_count: usize,
) -> (
    ChannelPeerTransport<TestId>,
    mpsc::UnboundedSender<DistributedMessage<TestId>>,
    PeerKeepalive,
) {
    let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
    let mut outgoing = HashMap::new();
    let mut keepalive = Vec::new();
    for i in 0..peer_count {
        let (peer_tx, peer_rx) = mpsc::unbounded_channel();
        outgoing.insert(format!("peer-{i}"), peer_tx);
        keepalive.push(peer_rx);
    }
    let transport = ChannelPeerTransport::from_raw_channels(node_id.into(), outgoing, inbound_rx);
    (transport, inbound_tx, keepalive)
}

/// Mint the Observer mesh trio over `transport` and hand back the
/// coordinator's `(client, inbox)` plus a PUMP future to drive concurrently
/// on the same `LocalSet` as `observer.run()`.
///
/// Post-Phase-C the observer never names a transport: it reaches the mesh
/// through a [`MeshClient`] (egress) + a [`RoleInbox`] (ingress), both
/// minted by [`Mesh::register_local_role`] (the C0 `process/tests`
/// pattern). The pump is a faithful single-role mirror of C-NODE's real
/// mesh-pump — it is what bridges the test's `ChannelPeerTransport` to the
/// detached client/inbox:
///   - INGRESS: a frame off the wire (`recv_peer`, fed by a test's
///     `inbound_tx`) is demuxed to the observer's slot via
///     `deliver_local(Observer)` → the observer's `inbox.recv()`. (These
///     tests are single-observer, so every inbound frame is observer-bound;
///     this is exactly what `route_incoming` would do for an
///     `Observer`/`All`-stamped frame, without needing the test to stamp.)
///   - EGRESS: a queued `client.send` (`next_local_dispatch`) is applied
///     via `apply_local_dispatch` → the wire (`send_to_peer`/`broadcast`),
///     so a test draining a peer outbox still observes the observer's sends.
///   - MEMBERSHIP: the pump publishes the live transport cardinality each
///     cycle (and ONCE upfront, before the observer's first visibility
///     check, so the visibility classifier reads the real peer count rather
///     than the fresh-view 0). The channel transport's membership is static
///     for a test's lifetime (peers held by the `PeerKeepalive`).
#[allow(clippy::type_complexity)]
fn observer_mesh(
    transport: ChannelPeerTransport<TestId>,
    node_id: &str,
) -> (
    MeshClient<TestId>,
    RoleInbox<TestId>,
    std::pin::Pin<Box<dyn std::future::Future<Output = ()>>>,
) {
    let mut mesh = Mesh::<TestId, _>::new(transport);
    let (slot, client, inbox) =
        mesh.register_local_role(LocalRole::Observer, PeerId::from(node_id));
    // Seed the membership view BEFORE the observer's run loop reads it, so a
    // resident-peer test sees `peer_count > 0` on the first `evaluate_exit`.
    mesh.publish_membership();
    let pump = async move {
        // Keep `slot` alive for the pump's lifetime: dropping it would let
        // the mesh `Weak` lapse and close the observer's inbox prematurely.
        let _slot = slot;
        // `recv_peer` and `next_local_dispatch`/`apply_local_dispatch` both
        // take `&mut mesh` (they share the transport), so they cannot be two
        // live branches of one `select!`. Drive them as SEQUENTIAL
        // zero-timeout polls each tick instead — one `&mut mesh` borrow at a
        // time. A 1ms cadence keeps both directions responsive for the
        // wall-clock-bounded tests; the membership republish rides the same
        // tick (its value is always a live transport read).
        let mut wire_open = true;
        loop {
            // INGRESS: demux any ready inbound frame to the observer slot
            // (the single local role in these tests). A closed wire latches
            // `wire_open = false` so we stop polling it.
            if wire_open
                && let Ok(maybe) = tokio::time::timeout(Duration::ZERO, mesh.recv_peer()).await
            {
                match maybe {
                    Some(frame) => {
                        mesh.deliver_local(LocalRole::Observer, frame);
                    }
                    None => wire_open = false,
                }
            }
            // EGRESS: apply every queued client send against the live slots
            // / the wire (so a test draining a peer outbox observes the
            // observer's sends).
            while let Ok(Some(item)) =
                tokio::time::timeout(Duration::ZERO, mesh.next_local_dispatch()).await
            {
                let _ = mesh.apply_local_dispatch(item).await;
            }
            // MEMBERSHIP: republish the live transport cardinality.
            mesh.publish_membership();
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    (client, inbox, Box::pin(pump))
}

// ----- SIGUSR2-raising tests -----
//
// SIGUSR2 is process-global; raising it delivers to EVERY armed
// `user_defined2` stream in this process. All raising tests are
// serialized behind one lock so a raise can never leak into a sibling's
// trigger (the `panik_watcher` SIGTERM harness style). `tokio::sync::Mutex`
// because the guard is held across `.await` (the workspace
// `clippy::await_holding_lock` lint denies `std` guards there).
static SIGUSR2_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Raise SIGUSR2 against this process — the operator's `kill -USR2`.
fn raise_sigusr2() {
    nix::sys::signal::raise(nix::sys::signal::Signal::SIGUSR2).expect("raise SIGUSR2");
}

/// THE PRODUCTION REPLAY (run_20260611_200548): SIGUSR2 lands while
/// NOTHING is consuming the trigger — the late-joiner's pre-seat
/// bootstrap window. With the trigger armed at entry the process
/// survives (pre-fix: kernel default disposition killed it — this very
/// test process would die here) and the delivery is LATCHED: the first
/// consumer poll, arbitrarily later, receives it.
#[tokio::test(flavor = "current_thread")]
async fn preseat_sigusr2_is_survived_and_latched() {
    let _lock = SIGUSR2_TEST_LOCK.lock().await;
    let mut trigger = GracefulAbortTrigger::arm();

    // Pre-seat window: the signal arrives with no recv in flight.
    raise_sigusr2();

    // Surviving to this line IS the headline assertion. Now seat the
    // consumer: the latched delivery must surface on the first poll.
    let latched = tokio::time::timeout(Duration::from_millis(500), trigger.recv())
        .await
        .expect("latched pre-seat delivery must surface on the first poll");
    assert_eq!(latched, Some(()));
}

/// The latched pre-seat abort delivers ON SEAT exactly like a post-seat
/// one: a trigger armed + signalled BEFORE the observer exists is injected
/// via `set_graceful_abort_trigger`, and the REAL run loop's first poll of
/// its graceful-abort arm sends the typed `GracefulAbortRequest` to the
/// recognized primary.
#[tokio::test(flavor = "current_thread")]
async fn latched_preseat_abort_delivers_on_seat_to_primary() {
    let _lock = SIGUSR2_TEST_LOCK.lock().await;
    tokio::time::timeout(Duration::from_secs(10), async {
        // Pre-seat: arm at "process entry", then the operator signals
        // while the (simulated) bootstrap is still in flight.
        let trigger = GracefulAbortTrigger::arm();
        raise_sigusr2();

        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                let (transport, inbound, mut peers) = transport_with_peers("obs", 1);
                let mut cs = ClusterState::<TestId>::new();
                // The recognized primary is the wired peer, so
                // `Destination::Primary` routes onto `peers[0]`.
                cs.apply(ClusterMutation::PrimaryChanged {
                    new: "peer-0".into(),
                    epoch: 1,
                    reason: PrimaryChangeReason::Election,
                });
                let (client, inbox, pump) = observer_mesh(transport, "obs");
                tokio::task::spawn_local(pump);
                let mut observer =
                    ObserverCoordinator::new(client, inbox, cs, observer_config("obs"));
                // Seat: hand the entry-armed trigger to the coordinator
                // (the late-joiner's step 6b) and drive the real loop.
                observer.set_graceful_abort_trigger(trigger);
                let run = tokio::task::spawn_local(async move { observer.run().await });

                // The latched signal must reach the primary as ONE typed
                // GracefulAbortRequest. Scan the outbox (the observer may
                // interleave its own frames, e.g. holdings announces).
                let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
                let mut delivered = false;
                while tokio::time::Instant::now() < deadline {
                    while let Ok(frame) = peers[0].try_recv() {
                        if let DistributedMessage::GracefulAbortRequest {
                            target, sender_id, ..
                        } = frame
                        {
                            assert_eq!(sender_id, "obs");
                            assert!(
                                matches!(target, Some(Destination::Primary)),
                                "the frame must be stamped Destination::Primary, \
                                 got {target:?}"
                            );
                            delivered = true;
                        }
                    }
                    if delivered {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                assert!(
                    delivered,
                    "the pre-seat latched abort must be delivered to the \
                     primary by the seated run loop"
                );

                // Wind the run down: the primary's drain terminal.
                let _ = inbound.send(DistributedMessage::ClusterMutation {
                    target: None,
                    sender_id: "peer-0".into(),
                    timestamp: 0.0,
                    mutations: vec![
                        ClusterMutation::GracefulAbortRequested,
                        ClusterMutation::RunComplete { counts: Default::default() },
                    ],
                });
                let terminal = run
                    .await
                    .expect("run task")
                    .expect("Ok on the drain terminal");
                assert!(
                    matches!(terminal, ObserverTerminal::GracefulAbort),
                    "got {terminal:?}"
                );
            })
            .await;
    })
    .await
    .expect("test must finish within budget");
}

/// Failed bootstrap with a latched abort: the exit path narrates the
/// undelivered intent (returns true — the exact message is pinned by the
/// trigger module's importance-capture unit test); without a signal there
/// is nothing to narrate (returns false).
#[tokio::test(flavor = "current_thread")]
async fn failed_bootstrap_reports_latched_abort_undelivered() {
    let _lock = SIGUSR2_TEST_LOCK.lock().await;

    // A latched pre-seat abort + a bootstrap that never seats.
    let trigger = GracefulAbortTrigger::arm();
    raise_sigusr2();
    assert!(
        trigger.report_undelivered().await,
        "a latched abort must be found and narrated on the failed-bootstrap exit"
    );

    // Fresh trigger, no signal: nothing latched, nothing narrated. (The
    // raise above was consumed by the first trigger and predates this
    // stream's registration, so it cannot leak in.)
    let clean = GracefulAbortTrigger::arm();
    assert!(
        !clean.report_undelivered().await,
        "a clean bootstrap failure must not invent an abort intent"
    );
}
