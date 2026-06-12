//! Behaviour tests for the standalone [`ObserverCoordinator`].
//!
//! These re-create the observer-behaviour contract Wave 0 removed from
//! `relocate_observe.rs` / `crdt_convergence.rs`, now targeting the
//! standalone coordinator: run-complete / run-aborted / panik exits, the
//! BUG-B lost-visibility contract (visibility loss is reported + retried,
//! NEVER a run verdict — the observer keeps observing and exits only on the
//! primary's observed terminal), CRDT narration, snapshot recovery, and the
//! BUG-1/4/5/7 fixes. Each test builds the observer via [`ObserverCoordinator::new`]
//! over a real [`ChannelPeerTransport`] (or a minimal feed), drives the
//! single `run()` loop, and asserts on its terminal / `Err` / emitted
//! narration. The relocation/handoff e2e is a LATER wave's concern.

use std::collections::HashMap;
use std::time::Duration;

use dynrunner_core::{ErrorType, PhaseId, TaskInfo, TypeId};
use dynrunner_protocol_primary_secondary::cluster_mutation::ClusterMutation;
use dynrunner_protocol_primary_secondary::{
    Destination, DistributedMessage, KeepaliveRole, PrimaryChangeReason,
};
use dynrunner_transport_channel::ChannelPeerTransport;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use super::{ObserverConfig, ObserverCoordinator, ObserverTerminal};
use crate::cluster_state::ClusterState;
use crate::process::{LocalRole, Mesh, MeshClient, RoleInbox};
use dynrunner_protocol_primary_secondary::address::PeerId;

/// Minimal serializable identifier for the observer tests.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct TestId(String);

/// A `TaskInfo` in `phase` with id `id` and the given fully-qualified
/// `(dep_phase, dep_task_id)` prerequisites.
fn task(phase: &str, id: &str, deps: &[(&str, &str)]) -> TaskInfo<TestId> {
    TaskInfo {
        path: std::path::PathBuf::from(format!("/tmp/{id}")),
        size: 1,
        identifier: TestId(id.to_string()),
        phase_id: PhaseId::from(phase),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: id.to_string(),
        task_depends_on: deps
            .iter()
            .map(|(dp, dt)| dynrunner_core::TaskDep {
                task_id: (*dt).to_string(),
                phase_id: PhaseId::from(*dp),
                inherit_outputs: false,
            })
            .collect(),
        preferred_secondaries: Default::default(),
        resolved_path: None,
        preferred_version: Default::default(),
    }
}

fn add(state: &mut ClusterState<TestId>, t: &TaskInfo<TestId>) {
    state.apply(ClusterMutation::TaskAdded {
        hash: t.task_id.clone(),
        task: t.clone(),
    });
}

fn complete(state: &mut ClusterState<TestId>, hash: &str) {
    state.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: hash.to_string(),
        result_data: None,
    });
}

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

/// A `PeerTransport` whose connected-id set is a shared cell the test
/// flips, modelling a leg that re-registers then dies (the production `-R`
/// rebuild-success-then-die flap) WITHOUT ever delivering an application
/// frame. `recv_peer` pends forever (no inbound), so `last_inbound_at`
/// never advances — the whole point of the #415 (b2) repro. Used only by
/// `fleet_death_terminal_fires_under_flapping_leg_with_no_delivery`.
struct FlappingMembershipPeer {
    connected: std::rc::Rc<std::cell::RefCell<std::collections::HashSet<String>>>,
}

impl dynrunner_protocol_primary_secondary::PeerTransport<TestId> for FlappingMembershipPeer {
    async fn broadcast(&mut self, _msg: DistributedMessage<TestId>) -> Result<(), String> {
        Ok(())
    }
    async fn send_to_peer(
        &mut self,
        _peer_id: &str,
        _msg: DistributedMessage<TestId>,
    ) -> Result<(), String> {
        Ok(())
    }
    async fn recv_peer(&mut self) -> Option<DistributedMessage<TestId>> {
        std::future::pending().await
    }
    fn try_recv_peer(&mut self) -> Option<DistributedMessage<TestId>> {
        None
    }
    fn peer_count(&self) -> usize {
        self.connected.borrow().len()
    }
    fn has_peer(&self, id: &PeerId) -> bool {
        self.connected.borrow().contains(id.as_str())
    }
    fn connected_ids(&self) -> Vec<PeerId> {
        self.connected
            .borrow()
            .iter()
            .map(|s| PeerId::from(s.as_str()))
            .collect()
    }
    async fn connect_to_peers(
        &mut self,
        _peers: &[dynrunner_protocol_primary_secondary::PeerConnectionInfo],
    ) {
    }
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

/// `run_complete` already applied ⇒ the observer returns `Done` (exit 0)
/// immediately, without arming any backstop.
#[tokio::test(flavor = "current_thread")]
async fn observer_returns_on_run_complete() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _inbound, _peers) = transport_with_peers("obs", 1);
            let mut cs = ClusterState::<TestId>::new();
            cs.apply(ClusterMutation::RunComplete);
            let (client, inbox, pump) = observer_mesh(transport, "obs");
            tokio::task::spawn_local(pump);
            let mut observer = ObserverCoordinator::new(client, inbox, cs, observer_config("obs"));
            let terminal = observer.run().await.expect("Ok on run_complete");
            assert!(
                matches!(terminal, ObserverTerminal::Done),
                "got {terminal:?}"
            );
        })
        .await;
}

/// The composed graceful-abort verdict: `run_complete ∧
/// graceful_abort_requested` already applied ⇒ the observer returns the
/// DISTINCT `GracefulAbort` terminal (never plain `Done` — the verdict
/// must not narrate as a clean success).
#[tokio::test(flavor = "current_thread")]
async fn observer_reports_graceful_abort_verdict() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _inbound, _peers) = transport_with_peers("obs", 1);
            let mut cs = ClusterState::<TestId>::new();
            cs.apply(ClusterMutation::GracefulAbortRequested);
            cs.apply(ClusterMutation::RunComplete);
            let (client, inbox, pump) = observer_mesh(transport, "obs");
            tokio::task::spawn_local(pump);
            let mut observer = ObserverCoordinator::new(client, inbox, cs, observer_config("obs"));
            let terminal = observer.run().await.expect("Ok on graceful verdict");
            assert!(
                matches!(terminal, ObserverTerminal::GracefulAbort),
                "run_complete + graceful latch must report the DISTINCT \
                 GracefulAbort terminal, got {terminal:?}"
            );
        })
        .await;
}

/// The graceful latch ALONE (no run terminal) is NOT an exit: the
/// observer keeps observing the drain and terminates only on the
/// primary's eventual `RunComplete` — then with the graceful verdict.
#[tokio::test(flavor = "current_thread")]
async fn observer_keeps_observing_until_drain_terminal() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, inbound, _peers) = transport_with_peers("obs", 1);
            let mut cs = ClusterState::<TestId>::new();
            cs.apply(ClusterMutation::GracefulAbortRequested);
            let (client, inbox, pump) = observer_mesh(transport, "obs");
            tokio::task::spawn_local(pump);
            let mut observer = ObserverCoordinator::new(client, inbox, cs, observer_config("obs"));
            // Deliver the drain terminal over the wire a beat later; the
            // observer must still be running to consume it.
            tokio::task::spawn_local(async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let _ = inbound.send(DistributedMessage::ClusterMutation {
                    target: None,
                    sender_id: "primary".into(),
                    timestamp: 0.0,
                    mutations: vec![ClusterMutation::RunComplete],
                });
            });
            let terminal = observer.run().await.expect("Ok on the drain terminal");
            assert!(
                matches!(terminal, ObserverTerminal::GracefulAbort),
                "got {terminal:?}"
            );
        })
        .await;
}

/// `request_graceful_abort` (the operator-trigger seam — the SIGUSR2 arm
/// calls this) sends ONE typed `GracefulAbortRequest` frame routed to the
/// recognized primary, stamped `Destination::Primary`.
#[tokio::test(flavor = "current_thread")]
async fn request_graceful_abort_sends_typed_frame_to_primary() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _inbound, mut peers) = transport_with_peers("obs", 1);
            let mut cs = ClusterState::<TestId>::new();
            // The recognized primary is the wired peer.
            cs.apply(ClusterMutation::PrimaryChanged {
                new: "peer-0".into(),
                epoch: 1,
                reason: PrimaryChangeReason::Election,
            });
            let (client, inbox, pump) = observer_mesh(transport, "obs");
            tokio::task::spawn_local(pump);
            let mut observer = ObserverCoordinator::new(client, inbox, cs, observer_config("obs"));

            observer.request_graceful_abort().await;
            // Let the pump drain the queued egress onto the wire.
            tokio::time::sleep(Duration::from_millis(20)).await;

            let frame = peers[0].try_recv().expect("the request reaches the primary peer");
            match frame {
                DistributedMessage::GracefulAbortRequest {
                    target, sender_id, ..
                } => {
                    assert_eq!(sender_id, "obs");
                    assert!(
                        matches!(
                            target,
                            Some(dynrunner_protocol_primary_secondary::Destination::Primary)
                        ),
                        "the frame must be stamped Destination::Primary, got {target:?}"
                    );
                }
                other => panic!("expected GracefulAbortRequest, got {other:?}"),
            }
        })
        .await;
}

/// BUG-B: zero peers + no RunComplete (the observer lost ALL visibility —
/// its `-R` setup tunnel dropped) must NOT collapse the run. The observer
/// carries zero authority: it reports lost + keeps observing, and
/// terminates ONLY on the PRIMARY's observed RunComplete. Asserts (a) the
/// observer does NOT return early / strand while the fleet is empty, (b) it
/// exits `Ok(Done)` once RunComplete converges over the (later-arriving)
/// inbound — NEVER `Err(ClusterCollapsed)`.
///
/// Drives the REAL run loop end-to-end (no pre-built shortcut): the
/// observer starts with zero peers and no terminal, sits through several
/// fleet-empty re-check ticks (the window that USED to strand), then the
/// test feeds a live `RunComplete` over the inbound — the observer's own
/// recv-arm applies it and exits Done. A revert (re-adding the §5/§6
/// strand-exit) makes this fail: the observer would `Err(ClusterCollapsed)`
/// before the RunComplete arrives, so the `expect("…Done")` trips.
#[tokio::test(flavor = "current_thread")]
async fn observer_does_not_collapse_on_dead_fleet_exits_on_observed_run_complete() {
    use dynrunner_protocol_primary_secondary::cluster_mutation::ClusterMutation as CM;
    tokio::time::timeout(Duration::from_secs(5), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                // Zero peers: the observer's transport view is empty for the
                // whole run (the by-design setup-tunnel drop).
                let (transport, inbound, _peers) = transport_with_peers("obs", 0);
                let cs = ClusterState::<TestId>::new();
                let mut config = observer_config("obs");
                // Tiny re-check cadence so many fleet-empty ticks elapse in
                // the window the pre-fix strand grace would have fired in —
                // proving the observer rides through instead of stranding.
                config.fleet_dead_timeout = Duration::from_millis(10);
                let (client, inbox, pump) = observer_mesh(transport, "obs");
                tokio::task::spawn_local(pump);
                let mut observer = ObserverCoordinator::new(client, inbox, cs, config);

                // Feed RunComplete over the LIVE inbound after a delay that is
                // many times the (former) strand grace, so a regressed
                // strand-exit would have already fired by the time it lands.
                tokio::task::spawn_local(async move {
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    let _ = inbound.send(DistributedMessage::ClusterMutation {
                        target: None,
                        sender_id: "primary".into(),
                        timestamp: 0.0,
                        mutations: vec![CM::RunComplete],
                    });
                });

                let terminal = observer.run().await.expect(
                    "a fleet-empty observer must NOT collapse the run — it keeps observing \
                     and exits on the PRIMARY's observed RunComplete, never \
                     Err(ClusterCollapsed)",
                );
                assert!(
                    matches!(terminal, ObserverTerminal::Done),
                    "the run verdict is the primary's RunComplete (Done), not the observer's \
                     view: got {terminal:?}"
                );
            })
            .await;
    })
    .await
    .expect("the fleet-empty observer must observe RunComplete + exit, not hang");
}

/// BUG-B: a NAMED primary going silent (resident peer, no RunComplete) past
/// `peer_timeout` must NOT strand the run. A silent primary from the
/// observer's vantage means the observer lost ITS path to the primary's
/// signals — NOT that the cluster died (the primary is reachable from its
/// own mesh). The observer reports lost-visibility and keeps observing,
/// terminating only on the primary's observed RunComplete — never
/// `Err(ClusterCollapsed)`. peer_count == 1 throughout so this exercises the
/// half-open-link silence path specifically.
///
/// Drives the REAL run loop: the observer sits through the silence window
/// (which the pre-fix §6 backstop would have stranded in), then the test
/// feeds a live `RunComplete` and the observer exits Done. Revert (re-add
/// the §6 strand-exit) → the observer Errs before RunComplete lands → the
/// `expect("…Done")` trips.
#[tokio::test(flavor = "current_thread")]
async fn observer_does_not_strand_on_silent_primary_exits_on_observed_run_complete() {
    use dynrunner_protocol_primary_secondary::cluster_mutation::ClusterMutation as CM;
    tokio::time::timeout(Duration::from_secs(5), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (transport, inbound, _peers) = transport_with_peers("obs", 1);
                let mut cs = ClusterState::<TestId>::new();
                cs.apply(ClusterMutation::PrimaryChanged {
                    new: "sec-0".into(),
                    epoch: 1,
                    reason: PrimaryChangeReason::Transferred,
                });
                let mut config = observer_config("obs");
                // Short silence threshold + smaller re-check cadence so the
                // observer crosses the (former) §6 strand window well before
                // the RunComplete arrives.
                config.peer_timeout = Duration::from_millis(40);
                config.fleet_dead_timeout = Duration::from_millis(10);
                let (client, inbox, pump) = observer_mesh(transport, "obs");
                tokio::task::spawn_local(pump);
                let mut observer = ObserverCoordinator::new(client, inbox, cs, config);

                // RunComplete lands AFTER the silence window — the observer
                // must have ridden through it (report-and-retry), not
                // stranded, to still be alive to observe it.
                tokio::task::spawn_local(async move {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    let _ = inbound.send(DistributedMessage::ClusterMutation {
                        target: None,
                        sender_id: "sec-0".into(),
                        timestamp: 0.0,
                        mutations: vec![CM::RunComplete],
                    });
                });

                let terminal = observer.run().await.expect(
                    "a silent-primary observer must NOT strand — it keeps observing and exits \
                     on the PRIMARY's observed RunComplete, never Err(ClusterCollapsed)",
                );
                assert!(
                    matches!(terminal, ObserverTerminal::Done),
                    "the run verdict is the primary's RunComplete (Done): got {terminal:?}"
                );
            })
            .await;
    })
    .await
    .expect("the silent-primary observer must ride through + exit on RunComplete, not hang");
}

/// The observer narrates phases + exactly one completion summary from the
/// CRDT (items 9/14). Two-phase chain (build → compile), mixed outcomes
/// (2 succeeded, 1 failed-final), RunComplete applied ⇒ both phases
/// narrated started + complete and one run-complete summary.
///
/// The narration is captured through the observer's OWN narration seam —
/// a [`RunNarrator`] driven synchronously over the converged ledger the
/// observer's `run()` exited on (its `cluster_state()`) — NOT by scraping
/// the process-global tracing dispatcher. The observer's `run()` loop
/// narrates by seeding `RunNarrator::with_started_phases(self.started_phases)`
/// (empty for `new()`) and calling `observe()` against `self.cluster_state`;
/// a single `observe()` over a pre-converged ledger reproduces exactly the
/// lines that loop emits. Capturing that synchronous drive — inside a
/// `with_default` closure with no `.await` between subscriber install and
/// emission, the proven non-flaky idiom from `run_narrator.rs` — makes the
/// importance assertion independent of the `tracing` per-callsite `Interest`
/// cache, which is process-global and concurrently re-poisoned by sibling
/// tests that install a `fmt::try_init` global subscriber (a thread-local
/// `set_default` held across `run().await` races that shared cache).
#[tokio::test(flavor = "current_thread")]
async fn observer_narrates_phases_and_one_completion_summary() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _inbound, _peers) = transport_with_peers("obs", 1);
            let mut cs = ClusterState::<TestId>::new();
            cs.apply(ClusterMutation::PhaseDepsSet {
                deps: HashMap::from([(PhaseId::from("compile"), vec![PhaseId::from("build")])]),
            });
            let toolchain = task("build", "toolchain", &[]);
            let ok = task("compile", "ok", &[]);
            let bad = task("compile", "bad", &[]);
            for b in [&toolchain, &ok, &bad] {
                add(&mut cs, b);
            }
            complete(&mut cs, "toolchain");
            complete(&mut cs, "ok");
            cs.apply(ClusterMutation::TaskFailed {
                attempt: 0,
                hash: "bad".to_string(),
                kind: ErrorType::NonRecoverable,
                error: "boom".into(),
                version: Default::default(),
            });
            cs.apply(ClusterMutation::RunComplete);

            // Drive the real observer to its terminal so the narration is
            // asserted over the ledger `run()` actually converged + exited on.
            let (client, inbox, pump) = observer_mesh(transport, "obs");
            tokio::task::spawn_local(pump);
            let mut observer = ObserverCoordinator::new(client, inbox, cs, observer_config("obs"));
            let terminal = observer.run().await.expect("Ok on run_complete");
            assert!(
                matches!(terminal, ObserverTerminal::Done),
                "got {terminal:?}"
            );

            // Re-derive the observer's narration synchronously over its
            // converged ledger, capturing through the narrator's own emit
            // path under a thread-local subscriber (no await between install
            // and emit → the per-callsite Interest is evaluated under THIS
            // subscriber, immune to the cross-test global cache poisoning).
            let events = crate::test_capture::capture_important(|| {
                crate::run_narrator::RunNarrator::new().observe(observer.cluster_state());
            });

            let started: std::collections::HashSet<&str> = events
                .iter()
                .filter(|e| e.message.contains("starting job phase"))
                .filter_map(|e| e.fields.get("phase").map(String::as_str))
                .collect();
            assert_eq!(
                started,
                std::collections::HashSet::from(["build", "compile"]),
                "both phases must narrate started: {events:?}"
            );
            let done: std::collections::HashSet<&str> = events
                .iter()
                .filter(|e| e.message.contains("phase complete"))
                .filter_map(|e| e.fields.get("phase").map(String::as_str))
                .collect();
            assert_eq!(
                done,
                std::collections::HashSet::from(["build", "compile"]),
                "both phases must narrate complete: {events:?}"
            );
            let summary: Vec<_> = events
                .iter()
                .filter(|e| e.message.contains("run complete"))
                .collect();
            assert_eq!(
                summary.len(),
                1,
                "exactly one completion summary: {events:?}"
            );
            assert_eq!(
                summary[0].fields.get("succeeded").map(String::as_str),
                Some("2")
            );
            assert_eq!(
                summary[0].fields.get("fail_final").map(String::as_str),
                Some("1")
            );
        })
        .await;
}

/// A legitimate failover must NOT trip the silence backstop: the relocated
/// primary dies, a surviving secondary re-elects (PrimaryChanged), the new
/// primary emits a `Primary` keepalive (refreshes `primary_last_seen`),
/// and the observer rides through, exiting `Ok` on the new primary's
/// RunComplete. Drives the refresh + RunComplete over the real inbound so
/// the recv-arm's liveness + apply path is exercised.
#[tokio::test(flavor = "current_thread")]
async fn observer_rides_through_failover_and_exits_on_run_complete() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (transport, inbound, _peers) = transport_with_peers("obs", 1);
                let mut cs = ClusterState::<TestId>::new();
                cs.apply(ClusterMutation::PrimaryChanged {
                    new: "sec-0".into(),
                    epoch: 1,
                    reason: PrimaryChangeReason::Transferred,
                });
                let mut config = observer_config("obs");
                config.peer_timeout = Duration::from_millis(120);
                // Re-check cadence well under peer_timeout so the backstop
                // WOULD fire by ~120ms if the refresh didn't reset it.
                config.fleet_dead_timeout = Duration::from_millis(40);
                let (client, inbox, pump) = observer_mesh(transport, "obs");
                tokio::task::spawn_local(pump);
                let mut observer = ObserverCoordinator::new(client, inbox, cs, config);

                tokio::task::spawn_local(async move {
                    tokio::time::sleep(Duration::from_millis(60)).await;
                    inbound
                        .send(DistributedMessage::ClusterMutation {
                            target: None,
                            sender_id: "sec-1".into(),
                            timestamp: 0.0,
                            mutations: vec![ClusterMutation::PrimaryChanged {
                                new: "sec-1".into(),
                                epoch: 2,
                                reason: PrimaryChangeReason::Election,
                            }],
                        })
                        .expect("inbound open");
                    inbound
                        .send(DistributedMessage::Keepalive {
                            target: None,
                            sender_id: "sec-1".into(),
                            timestamp: 0.0,
                            secondary_id: "sec-1".into(),
                            active_workers: 0,
                            emitter_role: KeepaliveRole::Primary,
                        })
                        .expect("inbound open");
                    // Past the ORIGINAL 120ms deadline: reaching RunComplete
                    // proves the refresh reset the clock.
                    tokio::time::sleep(Duration::from_millis(90)).await;
                    inbound
                        .send(DistributedMessage::ClusterMutation {
                            target: None,
                            sender_id: "sec-1".into(),
                            timestamp: 0.0,
                            mutations: vec![ClusterMutation::RunComplete],
                        })
                        .expect("inbound open");
                });

                let terminal = observer
                    .run()
                    .await
                    .expect("a legitimate failover must NOT trip the silence backstop");
                assert!(
                    matches!(terminal, ObserverTerminal::Done),
                    "got {terminal:?}"
                );
                assert_eq!(
                    observer.cluster_state().current_primary(),
                    Some("sec-1"),
                    "the failover re-elected sec-1"
                );
            })
            .await;
    })
    .await
    .expect("the failover-ride-through observer must terminate");
}

/// Bootstrap recovery (§6 / item 2): an empty observer with a named
/// primary recovers from a `ClusterSnapshot` reply fed over the inbound,
/// restoring the completed-task count + the RunComplete latch ⇒ exit `Ok`.
#[tokio::test(flavor = "current_thread")]
async fn observer_recovers_from_snapshot_reply() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                // Donor snapshot: two completed tasks + RunComplete.
                let snapshot_json = {
                    let mut donor = ClusterState::<TestId>::new();
                    for name in ["t1", "t2"] {
                        let t = task("p", name, &[]);
                        add(&mut donor, &t);
                        complete(&mut donor, name);
                    }
                    donor.apply(ClusterMutation::RunComplete);
                    serde_json::to_string(&donor.snapshot()).expect("snapshot serializes")
                };

                // Observer transport: a `"promoted-sec"`-keyed outbox so the
                // recovery RPC's Destination::Primary resolves + sends, plus
                // an inbound we feed the ClusterSnapshot reply into.
                let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
                let (to_primary_tx, _to_primary_rx) = mpsc::unbounded_channel();
                let mut outgoing = HashMap::new();
                outgoing.insert("promoted-sec".to_string(), to_primary_tx);
                let transport =
                    ChannelPeerTransport::from_raw_channels("obs".into(), outgoing, inbound_rx);

                let mut cs = ClusterState::<TestId>::new();
                cs.apply(ClusterMutation::PrimaryChanged {
                    new: "promoted-sec".into(),
                    epoch: 2,
                    reason: PrimaryChangeReason::Election,
                });

                // Pre-feed the snapshot reply so the loop's recv arm picks
                // it up immediately on entry.
                inbound_tx
                    .send(DistributedMessage::ClusterSnapshot {
                        target: None,
                        sender_id: "promoted-sec".into(),
                        timestamp: 0.0,
                        snapshot_json,
                    })
                    .unwrap();

                let (client, inbox, pump) = observer_mesh(transport, "obs");
                tokio::task::spawn_local(pump);
                let mut observer =
                    ObserverCoordinator::new(client, inbox, cs, observer_config("obs"));
                assert_eq!(
                    observer.cluster_state().outcome_counts().succeeded,
                    0,
                    "pre-recovery the observer's ledger is empty"
                );
                let terminal = observer.run().await.expect("Ok after recovery");
                assert!(
                    matches!(terminal, ObserverTerminal::Done),
                    "got {terminal:?}"
                );
                assert_eq!(
                    observer.cluster_state().outcome_counts().succeeded,
                    2,
                    "recovery must restore the completed-task count"
                );
            })
            .await;
    })
    .await
    .expect("the recovery observer must terminate");
}

/// BUG-1: `run_aborted` ⇒ non-zero exit (Aborted terminal), checked
/// BEFORE `run_complete` so an aborted run never exits as completed.
#[tokio::test(flavor = "current_thread")]
async fn observer_run_aborted_exits_non_zero() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _inbound, _peers) = transport_with_peers("obs", 1);
            let mut cs = ClusterState::<TestId>::new();
            // Both aborted AND complete latched: aborted must win.
            cs.apply(ClusterMutation::RunAborted {
                reason: "duplicate task id in initial batch".into(),
            });
            cs.apply(ClusterMutation::RunComplete);
            let (client, inbox, pump) = observer_mesh(transport, "obs");
            tokio::task::spawn_local(pump);
            let mut observer = ObserverCoordinator::new(client, inbox, cs, observer_config("obs"));
            let terminal = observer
                .run()
                .await
                .expect("aborted is a terminal, not an Err");
            match terminal {
                ObserverTerminal::Aborted { reason } => {
                    assert!(
                        reason.contains("duplicate"),
                        "carries the abort reason: {reason}"
                    );
                }
                other => panic!("aborted must win over complete: got {other:?}"),
            }
        })
        .await;
}

/// BUG-4: a working panik arm. A sentinel panik file triggers the watcher;
/// the run loop's panik arm consumes the signal and returns the Panik
/// terminal (→ exit 137 at the boundary). Built via the cold-join factory
/// so the real watcher is wired.
#[tokio::test(flavor = "current_thread")]
async fn observer_panik_arm_returns_panik_terminal() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let tmp = tempfile::TempDir::new().unwrap();
                let panik_path = tmp.path().join("observer.panik");
                let (transport, _inbound, _peers) = transport_with_peers("obs", 1);
                let mut config = observer_config("obs");
                config.panik_watcher_paths = vec![panik_path.clone()];
                config.panik_watcher_poll_interval = Duration::from_millis(20);
                // Resident peer (peer_count==1) so visibility stays Visible;
                // slow re-check cadence so nothing competes with the panik
                // arm. The panik arm is the only exit path here.
                config.fleet_dead_timeout = Duration::from_secs(60);
                config.peer_timeout = Duration::from_secs(60);

                let (client, inbox, pump) = observer_mesh(transport, "obs");
                tokio::task::spawn_local(pump);
                let mut observer = super::build_cold_join_observer(
                    client,
                    inbox,
                    ClusterState::<TestId>::new(),
                    config,
                    Vec::new(),
                    std::collections::HashSet::new(),
                );

                tokio::task::spawn_local(async move {
                    tokio::time::sleep(Duration::from_millis(60)).await;
                    std::fs::write(&panik_path, b"stop").unwrap();
                });

                let terminal = observer
                    .run()
                    .await
                    .expect("panik is a terminal, not an Err");
                match terminal {
                    ObserverTerminal::Panik { matched_path } => {
                        assert!(
                            matched_path.ends_with("observer.panik"),
                            "panik terminal carries the matched path: {matched_path:?}"
                        );
                    }
                    other => panic!("panik file must drive the Panik terminal: got {other:?}"),
                }
            })
            .await;
    })
    .await
    .expect("the panik observer must terminate via the panik arm");
}

/// BUG-5: `primary_last_seen` is refreshed when a restore-driven snapshot
/// re-points `current_primary`. A snapshot that newly names a primary is a
/// liveness assertion; without the refresh the silence backstop would fire
/// against a primary the observer only just learned of via snapshot.
///
/// Setup: peer_timeout short; the observer starts with NO named primary
/// (so the silence backstop is gated off initially). A snapshot naming
/// `promoted-sec` AND carrying RunComplete arrives just before the
/// peer_timeout window — the observer applies it, refreshes the clock, and
/// exits `Ok` on RunComplete rather than firing the silence backstop. The
/// proof is the green exit: had the restore NOT refreshed the clock, the
/// silence window measured from the (named) primary would already be blown
/// by the time the loop re-checks.
#[tokio::test(flavor = "current_thread")]
async fn observer_refreshes_primary_clock_on_restore_repoint() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let snapshot_json = {
                    let mut donor = ClusterState::<TestId>::new();
                    let t = task("p", "t1", &[]);
                    add(&mut donor, &t);
                    complete(&mut donor, "t1");
                    donor.apply(ClusterMutation::PrimaryChanged {
                        new: "promoted-sec".into(),
                        epoch: 3,
                        reason: PrimaryChangeReason::Election,
                    });
                    // NOT complete yet: we want the loop to keep running on
                    // the named primary so the silence backstop is the
                    // hazard the refresh must defuse, then a later
                    // RunComplete provides the clean exit.
                    serde_json::to_string(&donor.snapshot()).expect("snapshot serializes")
                };

                let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
                let (to_primary_tx, _to_primary_rx) = mpsc::unbounded_channel();
                let mut outgoing = HashMap::new();
                outgoing.insert("promoted-sec".to_string(), to_primary_tx);
                let transport =
                    ChannelPeerTransport::from_raw_channels("obs".into(), outgoing, inbound_rx);

                // Start with NO named primary so the silence backstop is
                // gated off until the restore re-points it.
                let cs = ClusterState::<TestId>::new();
                let mut config = observer_config("obs");
                config.peer_timeout = Duration::from_millis(80);
                config.fleet_dead_timeout = Duration::from_millis(30);

                let (client, inbox, pump) = observer_mesh(transport, "obs");
                tokio::task::spawn_local(pump);
                let mut observer = ObserverCoordinator::new(client, inbox, cs, config);

                tokio::task::spawn_local(async move {
                    // Land the re-pointing snapshot well into the run (past
                    // one re-check tick). It names promoted-sec → arms the
                    // silence backstop, and the BUG-5 refresh resets the
                    // clock to "now".
                    tokio::time::sleep(Duration::from_millis(60)).await;
                    inbound_tx
                        .send(DistributedMessage::ClusterSnapshot {
                            target: None,
                            sender_id: "promoted-sec".into(),
                            timestamp: 0.0,
                            snapshot_json,
                        })
                        .expect("inbound open");
                    // Past the ORIGINAL 80ms window measured from t=0: if
                    // the restore had not refreshed the clock the backstop
                    // would already have fired Err. A later RunComplete
                    // gives the clean exit.
                    tokio::time::sleep(Duration::from_millis(70)).await;
                    inbound_tx
                        .send(DistributedMessage::ClusterMutation {
                            target: None,
                            sender_id: "promoted-sec".into(),
                            timestamp: 0.0,
                            mutations: vec![ClusterMutation::RunComplete],
                        })
                        .expect("inbound open");
                });

                let terminal = observer
                    .run()
                    .await
                    .expect("the restore-driven re-point must refresh the clock, not strand");
                assert!(
                    matches!(terminal, ObserverTerminal::Done),
                    "got {terminal:?}"
                );
                assert_eq!(
                    observer.cluster_state().current_primary(),
                    Some("promoted-sec"),
                    "the snapshot re-pointed the primary"
                );
            })
            .await;
    })
    .await
    .expect("the restore-refresh observer must terminate");
}

/// The shared inbound + bookkeeping a [`build_test_handoff`] hands back.
struct HandoffTestRig {
    handoff: super::ObserverHandoff<TestId>,
    /// The mesh-pump the test must `spawn_local` alongside `observer.run()`
    /// — it carries the SAME mesh whose `(client, inbox)` rode the handoff,
    /// so the observer's ingress/egress/membership are driven exactly as the
    /// `new`-path tests (the retag preserves the slot/channel — H5).
    pump: std::pin::Pin<Box<dyn std::future::Future<Output = ()>>>,
    /// Feed mesh frames the observer's run loop applies.
    inbound: mpsc::UnboundedSender<DistributedMessage<TestId>>,
    /// Count of events the INHERITED (primary) dispatcher received. The
    /// `from_handoff` reconciliation REPLACES the inherited sender, so this
    /// must stay 0 for any event emitted after construction — proof the
    /// inherited dispatcher was superseded by the observer's own fresh one.
    inherited_event_count: std::rc::Rc<std::cell::Cell<usize>>,
    /// Held so the single dummy peer is not pruned for the test's lifetime.
    _peers: PeerKeepalive,
}

/// Build an [`super::ObserverHandoff`] over a moved-in transport +
/// cluster_state, mirroring what the relocated submitter's
/// `into_observer_handoff` produces: the cluster_state already carries an
/// installed task-completed sender (the inherited primary fabric), and two
/// dummy dispatcher tasks stand in for the inherited dispatcher handles. The
/// inherited task-completed dispatcher counts every event it receives into
/// `inherited_event_count`.
fn build_test_handoff(
    node_id: &str,
    cluster_state: ClusterState<TestId>,
    config: ObserverConfig,
) -> HandoffTestRig {
    let (transport, inbound, peers) = transport_with_peers(node_id, 1);
    // Mint the SAME mesh trio the primary held + retagged to observer (H5):
    // the handoff carries `client + inbox`, and the test drives the pump.
    let (client, inbox, pump) = observer_mesh(transport, node_id);

    // The inherited primary fabric: a task-completed channel already
    // installed on the moved-in cluster_state, with a dummy dispatcher
    // counting events on its receiver. `from_handoff` REPLACES this sender
    // with a fresh one (so this dispatcher is orphaned + receives nothing
    // further) and carries the handle only to abort it via single-teardown.
    let mut cluster_state = cluster_state;
    let (inherited_tx, mut inherited_rx) =
        mpsc::unbounded_channel::<crate::task_completed::TaskCompletedEvent>();
    cluster_state.install_task_completed_sender(inherited_tx);
    let inherited_event_count = std::rc::Rc::new(std::cell::Cell::new(0usize));
    let count_for_task = inherited_event_count.clone();
    let inherited_task_completed_dispatcher = tokio::task::spawn_local(async move {
        while inherited_rx.recv().await.is_some() {
            count_for_task.set(count_for_task.get() + 1);
        }
    });
    // A dummy peer-lifecycle dispatcher handle (no observer consumer; carried
    // only so single-teardown aborts it).
    let lifecycle_dispatcher_handle =
        tokio::task::spawn_local(async { std::future::pending::<()>().await });

    let handoff = super::ObserverHandoff {
        client,
        inbox,
        cluster_state,
        node_id: node_id.to_string(),
        deadlines: config.clone(),
        started_phases: std::collections::HashSet::new(),
        panik_signal_rx: None,
        task_completed_dispatcher_handle: inherited_task_completed_dispatcher,
        lifecycle_dispatcher_handle,
        holdings: std::collections::HashSet::new(),
        reconnector: None,
        respawn_provider: None,
    };
    HandoffTestRig {
        handoff,
        pump,
        inbound,
        inherited_event_count,
        _peers: peers,
    }
}

/// A relocation hands off transport + cluster_state BY VALUE: the observer
/// resumes over the moved-in mesh (peer set intact, no re-dial) and the
/// moved-in ledger, and exits cleanly on a `RunComplete` already present in
/// that ledger. Pins the core relocation mechanic at the observer seam +
/// proves the post-run accounting is re-sourced from the moved-in ledger.
#[tokio::test(flavor = "current_thread")]
async fn from_handoff_resumes_moved_in_state_and_exits_on_run_complete() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // The moved-in ledger already carries the run's terminal +
            // accounting (two completions) — exactly what the submitter's
            // converged cluster_state would hold at relocation.
            let mut cs = ClusterState::<TestId>::new();
            for id in ["a", "b"] {
                let t = task("p", id, &[]);
                add(&mut cs, &t);
                complete(&mut cs, id);
            }
            cs.apply(ClusterMutation::RunComplete);

            let rig = build_test_handoff("obs", cs, observer_config("obs"));
            tokio::task::spawn_local(rig.pump);
            let mut observer = ObserverCoordinator::from_handoff(rig.handoff);

            let terminal = observer
                .run()
                .await
                .expect("Ok on the moved-in run_complete");
            assert!(
                matches!(terminal, ObserverTerminal::Done),
                "got {terminal:?}"
            );

            // Post-run accounting is re-sourced from the observer's moved-in
            // (converged) ledger — the surface the relocated `PrimaryRunOutcome`
            // reads after the submitter binding is consumed.
            assert_eq!(
                observer.completed_count(),
                2,
                "completions off the moved-in ledger"
            );
            assert_eq!(observer.failed_count(), 0);
            assert_eq!(observer.stranded_count(), 0, "an observer strands nothing");
        })
        .await;
}

/// `from_handoff` reconciliation: the observer installs a FRESH task-completed
/// channel on the moved-in cluster_state, REPLACING the inherited primary
/// sender. Proof: an inbound `TaskCompleted` applied AFTER `from_handoff`
/// (through the observer's run loop) routes to the FRESH sender — the inherited
/// (orphaned) primary dispatcher receives ZERO post-handoff events, even
/// though it was live and counting before the swap. (The observer's own fresh
/// dispatcher carries the Policy B/D listeners; the inherited handle is
/// carried only so single-teardown aborts it.)
#[tokio::test(flavor = "current_thread")]
async fn from_handoff_fresh_sender_supersedes_inherited_dispatcher() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let mut cs = ClusterState::<TestId>::new();
                let t = task("p", "x", &[]);
                add(&mut cs, &t);

                let rig = build_test_handoff("obs", cs, observer_config("obs"));
                let inherited_count = rig.inherited_event_count.clone();
                let inbound = rig.inbound.clone();
                tokio::task::spawn_local(rig.pump);
                let mut observer = ObserverCoordinator::from_handoff(rig.handoff);

                // Apply a TaskCompleted AFTER from_handoff (so it routes via
                // the FRESH sender), then complete the run. The observer's run
                // loop applies these inbound frames.
                tokio::task::spawn_local(async move {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    inbound
                        .send(DistributedMessage::ClusterMutation {
                            target: None,
                            sender_id: "p".into(),
                            timestamp: 0.0,
                            mutations: vec![ClusterMutation::TaskCompleted {
                                attempt: 0,
                                hash: "x".into(),
                                result_data: None,
                            }],
                        })
                        .expect("inbound open");
                    inbound
                        .send(DistributedMessage::ClusterMutation {
                            target: None,
                            sender_id: "p".into(),
                            timestamp: 0.0,
                            mutations: vec![ClusterMutation::RunComplete],
                        })
                        .expect("inbound open");
                });

                let terminal = observer.run().await.expect("Ok on RunComplete");
                assert!(
                    matches!(terminal, ObserverTerminal::Done),
                    "got {terminal:?}"
                );
                // The completion was observed on the FRESH ledger.
                assert_eq!(
                    observer.completed_count(),
                    1,
                    "completion applied via fresh sender"
                );
                // The INHERITED dispatcher received NOTHING after the swap —
                // its sender was replaced by the fresh one in from_handoff.
                assert_eq!(
                    inherited_count.get(),
                    0,
                    "the inherited primary dispatcher must be superseded by the \
                     observer's own fresh dispatcher (0 post-handoff events)"
                );
            })
            .await;
    })
    .await
    .expect("the fresh-sender observer must terminate (not hang)");
}

/// Announcer-ordering fix: `build_cold_join_observer` attaches the
/// resource-holdings announcer's role-change hook BEFORE it restores the
/// bootstrap snapshot, so the restore's `PrimaryChanged` apply fires the
/// hook into the registered channel and the INITIAL holdings announce is
/// NOT dropped. Pre-fix the announcer was attached only in `run`, AFTER
/// the factory's restore had already fired the (then-unregistered) hook,
/// so the first announce never went out.
///
/// Proof: a cold-join observer with non-empty `holdings`, restoring a
/// snapshot that names a primary (so the restore re-points
/// `current_primary` and fires the role-change hook), broadcasts exactly
/// the restore-driven `PeerResourceHoldingsUpdated` carrying those
/// holdings to `Destination::Primary` — captured on the primary-keyed
/// outbox — before the run completes.
#[tokio::test(flavor = "current_thread")]
async fn cold_join_announces_initial_holdings_after_restore() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                // Donor snapshot: names a primary (epoch 4) so the restore's
                // `primary_epoch > local` branch fires the role-change hook.
                // NOT complete — the loop must keep running so the announcer
                // task gets a turn to drain its trigger onto the outbox.
                let snapshot = {
                    let mut donor = ClusterState::<TestId>::new();
                    donor.apply(ClusterMutation::PrimaryChanged {
                        new: "promoted-sec".into(),
                        epoch: 4,
                        reason: PrimaryChangeReason::Election,
                    });
                    donor.snapshot()
                };

                // Observer transport: a `"promoted-sec"`-keyed outbox so the
                // announce's `Destination::Primary` resolves + sends, plus an
                // inbound we feed RunComplete into after capturing the announce.
                let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
                let (to_primary_tx, mut to_primary_rx) = mpsc::unbounded_channel();
                let mut outgoing = HashMap::new();
                outgoing.insert("promoted-sec".to_string(), to_primary_tx);
                let transport =
                    ChannelPeerTransport::from_raw_channels("obs".into(), outgoing, inbound_rx);

                let holdings: std::collections::HashSet<String> =
                    ["/nix/store/aaa".to_string(), "/nix/store/bbb".to_string()]
                        .into_iter()
                        .collect();

                let mut config = observer_config("obs");
                config.peer_timeout = Duration::from_secs(60);
                config.fleet_dead_timeout = Duration::from_secs(60);

                let (client, inbox, pump) = observer_mesh(transport, "obs");
                tokio::task::spawn_local(pump);
                let mut observer = super::build_cold_join_observer(
                    client,
                    inbox,
                    ClusterState::<TestId>::new(),
                    config,
                    vec![snapshot],
                    holdings,
                );

                // Drain frames to the primary until the restore-driven
                // holdings announce arrives, then complete the run. The
                // observer also fires a bootstrap `RequestClusterSnapshot` to
                // the named primary at loop entry (§6); skip it — only the
                // `PeerResourceHoldingsUpdated` announce is under test.
                tokio::task::spawn_local(async move {
                    loop {
                        let frame = to_primary_rx.recv().await.expect(
                            "the restore-driven initial announce must reach the primary outbox",
                        );
                        match frame {
                            DistributedMessage::RequestClusterSnapshot { .. } => continue,
                            DistributedMessage::ClusterMutation { mutations, .. } => {
                                assert_eq!(mutations.len(), 1, "one mutation per announce");
                                match &mutations[0] {
                                    ClusterMutation::PeerResourceHoldingsUpdated {
                                        peer_id,
                                        holdings,
                                        epoch,
                                    } => {
                                        assert_eq!(peer_id, "obs");
                                        assert_eq!(
                                            holdings,
                                            &vec![
                                                "/nix/store/aaa".to_string(),
                                                "/nix/store/bbb".to_string()
                                            ],
                                            "the initial announce carries the cold-join holdings"
                                        );
                                        assert_eq!(
                                            *epoch, 4,
                                            "the announce stamps the restored primary_epoch"
                                        );
                                    }
                                    other => panic!(
                                        "expected PeerResourceHoldingsUpdated; got {other:?}"
                                    ),
                                }
                                break;
                            }
                            other => panic!("unexpected frame to primary: got {other:?}"),
                        }
                    }
                    // Now finish the run.
                    inbound_tx
                        .send(DistributedMessage::ClusterMutation {
                            target: None,
                            sender_id: "promoted-sec".into(),
                            timestamp: 0.0,
                            mutations: vec![ClusterMutation::RunComplete],
                        })
                        .expect("inbound open");
                });

                let terminal = observer
                    .run()
                    .await
                    .expect("Ok after the announce + RunComplete");
                assert!(
                    matches!(terminal, ObserverTerminal::Done),
                    "got {terminal:?}"
                );
            })
            .await;
    })
    .await
    .expect("the cold-join-announce observer must terminate");
}

/// D-C / D3 (§5.3): a steady-state WARN-DROPPED snapshot decode keeps the
/// anti-entropy LIVE — the AE-3 recovery cadence RE-PULLS a fresh snapshot
/// and the observer converges + exits `Done` within ≤ one cadence period.
///
/// ISOLATION of the TIMER-driven recovery (not the reactive digest arm):
/// the ahead digest arrives EXACTLY ONCE. The observer's reactive
/// `on_state_digest` issues the FIRST snapshot pull; the driver answers it
/// with a MALFORMED snapshot (WARN-dropped, not fatal). After that there is
/// NO further inbound digest, so the reactive arm is exhausted — the ONLY
/// thing that can re-pull is the timer-driven AE-3 recovery cadence, which
/// fires off the RECORDED peer digest. The driver answers that SECOND pull
/// with the GOOD snapshot. Without the recovery cadence this test HANGS
/// (no second pull ever comes) and trips the 5s timeout — that is the
/// fail-before / pass-after proof that the cadence (not the reactive arm)
/// drives convergence here.
#[tokio::test(flavor = "current_thread")]
async fn warn_dropped_decode_is_repulled_and_converges_via_recovery() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                // The good donor snapshot the recovery re-pull heals from:
                // one completed task + RunComplete.
                let good_snapshot_json = {
                    let mut donor = ClusterState::<TestId>::new();
                    let t = task("p", "t1", &[]);
                    add(&mut donor, &t);
                    complete(&mut donor, "t1");
                    donor.apply(ClusterMutation::RunComplete);
                    serde_json::to_string(&donor.snapshot()).expect("snapshot serializes")
                };
                // The (single) digest the named primary broadcasts — ahead of
                // the observer's empty ledger, so the observer is `is_behind`.
                let ahead_digest = {
                    let mut donor = ClusterState::<TestId>::new();
                    let t = task("p", "t1", &[]);
                    add(&mut donor, &t);
                    complete(&mut donor, "t1");
                    donor.digest()
                };

                // `promoted-sec`-keyed outbox so BOTH the reactive pull
                // (Destination::Secondary(promoted-sec)) and the recovery pull
                // resolve + send; we capture each RequestClusterSnapshot.
                let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
                let (to_primary_tx, mut to_primary_rx) = mpsc::unbounded_channel();
                let mut outgoing = HashMap::new();
                outgoing.insert("promoted-sec".to_string(), to_primary_tx);
                let transport =
                    ChannelPeerTransport::from_raw_channels("obs".into(), outgoing, inbound_rx);

                let mut cs = ClusterState::<TestId>::new();
                cs.apply(ClusterMutation::PrimaryChanged {
                    new: "promoted-sec".into(),
                    epoch: 2,
                    reason: PrimaryChangeReason::Election,
                });

                let mut config = observer_config("obs");
                // Short peer_timeout bounds the recovery period small (the
                // recovery interval is tick_period.min(peer_timeout)); the
                // driver keeps the primary clock fresh so the SILENCE backstop
                // never fires within the test.
                config.peer_timeout = Duration::from_millis(50);
                config.fleet_dead_timeout = Duration::from_secs(60);

                let (client, inbox, pump) = observer_mesh(transport, "obs");
                tokio::task::spawn_local(pump);
                let mut observer = ObserverCoordinator::new(client, inbox, cs, config);

                let inbound_for_driver = inbound_tx.clone();
                tokio::task::spawn_local(async move {
                    // Keep the primary-silence clock alive throughout (a
                    // Primary keepalive every 20ms < peer_timeout 50ms), so a
                    // strand never fires — ONLY the recovery path can heal.
                    let inbound_ka = inbound_for_driver.clone();
                    let keepalive_pump = tokio::task::spawn_local(async move {
                        loop {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                            if inbound_ka
                                .send(DistributedMessage::Keepalive {
                                    target: None,
                                    sender_id: "promoted-sec".into(),
                                    timestamp: 0.0,
                                    secondary_id: "promoted-sec".into(),
                                    active_workers: 0,
                                    emitter_role: KeepaliveRole::Primary,
                                })
                                .is_err()
                            {
                                break;
                            }
                        }
                    });

                    // Feed the ahead digest EXACTLY ONCE — this is the only
                    // event that drives the reactive pull; the recorded digest
                    // is what the timer-driven recovery later re-pulls off.
                    inbound_for_driver
                        .send(DistributedMessage::StateDigest {
                            target: None,
                            sender_id: "promoted-sec".into(),
                            timestamp: 0.0,
                            digest: ahead_digest,
                        })
                        .expect("inbound open");

                    // Two pulls are NOT timer-driven: the at-entry bootstrap
                    // request (fired synchronously before the loop, to the
                    // named primary) and the ONE reactive pull the single
                    // inbound digest triggers. Answer BOTH of those with a
                    // MALFORMED snapshot (WARN-dropped — the observer stays
                    // behind). Since no further digest ever arrives, EVERY
                    // pull after those two can ONLY be the TIMER-driven AE-3
                    // recovery cadence (it re-pulls off the recorded digest);
                    // answer those with the GOOD snapshot → converges. This
                    // is the isolation: without the recovery cadence there is
                    // no third pull and the test hangs to its 5s timeout.
                    let mut non_timer_pulls_left = 2u8;
                    while let Some(frame) = to_primary_rx.recv().await {
                        if let DistributedMessage::RequestClusterSnapshot { target: _, .. } = frame
                        {
                            let reply = if non_timer_pulls_left > 0 {
                                non_timer_pulls_left -= 1;
                                "{ this is not valid snapshot json".to_string()
                            } else {
                                good_snapshot_json.clone()
                            };
                            inbound_for_driver
                                .send(DistributedMessage::ClusterSnapshot {
                                    target: None,
                                    sender_id: "promoted-sec".into(),
                                    timestamp: 0.0,
                                    snapshot_json: reply,
                                })
                                .expect("inbound open");
                        }
                    }
                    keepalive_pump.abort();
                });

                let terminal = observer
                    .run()
                    .await
                    .expect("a WARN-dropped decode must NOT strand; recovery re-pulls + heals");
                assert!(
                    matches!(terminal, ObserverTerminal::Done),
                    "got {terminal:?}"
                );
                assert_eq!(
                    observer.cluster_state().outcome_counts().succeeded,
                    1,
                    "the recovery re-pull restored the completed-task count"
                );
                drop(inbound_tx);
            })
            .await;
    })
    .await
    .expect("the WARN-drop-then-recovery observer must terminate");
}

/// C9 quiesce (§5.3): the recovery cadence does NOT pull when the observer
/// is converged with every known peer it has heard a digest from. Setup: a
/// known primary broadcasts a digest the observer is ALREADY converged with
/// (the observer's ledger matches it), so `plan_recovery_pull` returns
/// `None` every tick. Proof: across a window spanning multiple recovery
/// ticks, ZERO `RequestClusterSnapshot` frames are emitted by the recovery
/// cadence (only the at-entry bootstrap request, which we account for).
#[tokio::test(flavor = "current_thread")]
async fn recovery_cadence_quiesces_when_converged() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                // The observer and the primary share the SAME ledger shape, so
                // the digest the primary broadcasts is one the observer is NOT
                // behind — the C9 quiesce case.
                let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
                let (to_primary_tx, mut to_primary_rx) = mpsc::unbounded_channel();
                let mut outgoing = HashMap::new();
                outgoing.insert("promoted-sec".to_string(), to_primary_tx);
                let transport =
                    ChannelPeerTransport::from_raw_channels("obs".into(), outgoing, inbound_rx);

                // Observer ledger: a named primary + one completed task.
                let mut cs = ClusterState::<TestId>::new();
                cs.apply(ClusterMutation::PrimaryChanged {
                    new: "promoted-sec".into(),
                    epoch: 2,
                    reason: PrimaryChangeReason::Election,
                });
                let t = task("p", "t1", &[]);
                add(&mut cs, &t);
                complete(&mut cs, "t1");
                // The converged digest the primary will echo back (identical
                // to the observer's own ⇒ not is_behind ⇒ quiesce).
                let converged_digest = cs.digest();

                let mut config = observer_config("obs");
                config.peer_timeout = Duration::from_millis(40);
                config.fleet_dead_timeout = Duration::from_secs(60);

                let (client, inbox, pump) = observer_mesh(transport, "obs");
                tokio::task::spawn_local(pump);
                let mut observer = ObserverCoordinator::new(client, inbox, cs, config);

                let inbound_for_driver = inbound_tx.clone();
                let recovery_requests = std::rc::Rc::new(std::cell::Cell::new(0usize));
                let recovery_requests_drv = recovery_requests.clone();
                tokio::task::spawn_local(async move {
                    // Keep the silence clock fresh so the silence backstop
                    // never fires; feed the converged digest so the observer
                    // records a known-peer digest it is NOT behind.
                    let inbound_ka = inbound_for_driver.clone();
                    let pump = tokio::task::spawn_local(async move {
                        loop {
                            tokio::time::sleep(Duration::from_millis(15)).await;
                            let _ = inbound_ka.send(DistributedMessage::Keepalive {
                                target: None,
                                sender_id: "promoted-sec".into(),
                                timestamp: 0.0,
                                secondary_id: "promoted-sec".into(),
                                active_workers: 0,
                                emitter_role: KeepaliveRole::Primary,
                            });
                            let _ = inbound_ka.send(DistributedMessage::StateDigest {
                                target: None,
                                sender_id: "promoted-sec".into(),
                                timestamp: 0.0,
                                digest: converged_digest,
                            });
                        }
                    });

                    // Count any RequestClusterSnapshot the recovery cadence
                    // emits over a multi-tick window. A drain task tallies
                    // them; with C9 quiesce there is only the at-entry
                    // bootstrap request (1), never a recovery re-pull.
                    let count_task = tokio::task::spawn_local(async move {
                        while let Some(frame) = to_primary_rx.recv().await {
                            if let DistributedMessage::RequestClusterSnapshot {
                                target: _, ..
                            } = frame
                            {
                                recovery_requests_drv.set(recovery_requests_drv.get() + 1);
                            }
                        }
                    });

                    // Let several recovery ticks (period ≈ 40ms) elapse, then
                    // complete the run cleanly.
                    tokio::time::sleep(Duration::from_millis(180)).await;
                    pump.abort();
                    inbound_for_driver
                        .send(DistributedMessage::ClusterMutation {
                            target: None,
                            sender_id: "promoted-sec".into(),
                            timestamp: 0.0,
                            mutations: vec![ClusterMutation::RunComplete],
                        })
                        .expect("inbound open");
                    // Give the drain a beat to observe any straggler frame.
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    count_task.abort();
                });

                let terminal = observer.run().await.expect("Ok on RunComplete");
                assert!(
                    matches!(terminal, ObserverTerminal::Done),
                    "got {terminal:?}"
                );
                // The ONLY snapshot request is the at-entry bootstrap one;
                // the converged recovery cadence emitted ZERO re-pulls.
                assert!(
                    recovery_requests.get() <= 1,
                    "a converged observer's recovery cadence must quiesce \
                     (≤1 request = the at-entry bootstrap only), got {}",
                    recovery_requests.get()
                );
                drop(inbound_tx);
            })
            .await;
    })
    .await
    .expect("the converged-quiesce observer must terminate");
}

/// L2a: a departed peer's AE-3 recovery digest is PRUNED from
/// `peer_digests` when its `PeerRemoved` mutation is applied, so the store
/// stays bounded by the live roster over the run's lifetime. A still-live
/// peer's entry is untouched.
#[tokio::test(flavor = "current_thread")]
async fn peer_digests_pruned_on_peer_removed() {
    use dynrunner_protocol_primary_secondary::RemovalCause;
    use dynrunner_protocol_primary_secondary::StateDigest;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, inbound_tx, _keepalive) = transport_with_peers("obs", 1);
            let cs = ClusterState::<TestId>::new();
            // This test drives `on_cluster_mutation` synchronously (no run
            // loop), so the mesh-pump is not needed — only a valid trio.
            let (client, inbox, _pump) = observer_mesh(transport, "obs");
            let mut observer = ObserverCoordinator::new(client, inbox, cs, observer_config("obs"));

            // Two recorded last-seen digests, one of which is about to depart.
            observer
                .peer_digests
                .insert("departing-sec".to_string(), StateDigest::default());
            observer
                .peer_digests
                .insert("live-sec".to_string(), StateDigest::default());

            let mut primary_last_seen = std::time::Instant::now();
            observer.on_cluster_mutation(
                vec![ClusterMutation::PeerRemoved {
                    id: "departing-sec".to_string(),
                    cause: RemovalCause::KeepaliveMiss,
                    member_gen: 0,
                }],
                &mut primary_last_seen,
            );

            assert!(
                !observer.peer_digests.contains_key("departing-sec"),
                "the departed peer's recovery digest must be pruned"
            );
            assert!(
                observer.peer_digests.contains_key("live-sec"),
                "a still-live peer's recovery digest must be retained"
            );
            assert_eq!(
                observer.peer_digests.len(),
                1,
                "only the live peer's entry remains"
            );
            drop(inbound_tx);
        })
        .await;
}

/// #235 observer half (primitive): `emit_terminal_reason_important` lands
/// exactly one event on the importance channel, carrying the terminal
/// reason. This is the single emit site the observer's LOCAL terminal arms
/// (fatal-policy, panik) route through; the synchronous,
/// yield-free drive (no `.await` between subscriber install and emit) makes
/// the importance assertion immune to the cross-test per-callsite Interest
/// cache poisoning — the documented requirement for `capture_important`.
#[tokio::test(flavor = "current_thread")]
async fn emit_terminal_reason_lands_on_important_channel() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _inbound, _peers) = transport_with_peers("obs", 0);
            let cs = ClusterState::<TestId>::new();
            let (client, inbox, _pump) = observer_mesh(transport, "obs");
            let observer = ObserverCoordinator::new(client, inbox, cs, observer_config("obs"));

            let events = crate::test_capture::capture_important(|| {
                observer.emit_terminal_reason_important("fleet-dead: every peer left");
            });

            assert_eq!(
                events.len(),
                1,
                "exactly one important event per terminal emit: {events:?}"
            );
            assert!(
                events[0].message.contains("run terminated")
                    && events[0].message.contains("fleet-dead: every peer left"),
                "the terminal reason must reach the important channel: {events:?}"
            );
        })
        .await;
}

/// BUG-B (report content): when the observer loses visibility past the
/// 5-minute wake threshold, the report it emits on the importance channel
/// is the operator-facing "connection down — retrying" notice, and
/// crucially NOT a "run terminated" reason — visibility loss is not a
/// terminal. This drives the EXACT emits the run loop produces: the
/// loop's lost-visibility side effects are
/// `LostVisibilityReporter::observe(Lost { .. }, now)` plus the
/// `on_wake_deadline` threshold arm, so driving them synchronously
/// reproduces the loop's emission. Captured through the
/// narrator's own `capture_important` idiom (a SYNCHRONOUS, yield-free
/// drive with no `.await` between subscriber install and emit — the
/// documented non-flaky requirement, immune to the cross-test per-callsite
/// Interest cache poisoning that a `set_default`-across-`.await` capture
/// flakes on).
///
/// The end-to-end "does not collapse + exits on the primary's RunComplete"
/// behaviour is asserted separately by
/// `observer_does_not_collapse_on_dead_fleet_exits_on_observed_run_complete`
/// (which drives the real async `run()` loop). This test isolates the
/// report CONTENT so the importance assertion stays deterministic.
#[test]
fn lost_visibility_report_is_retry_notice_not_a_run_terminal() {
    use crate::observer::lost_visibility::{
        LostVisibilityReporter, MeshLiveness, Visibility, WakeNoteSlot,
    };

    // The wake stream is threshold-gated (5 minutes): the IMMEDIATE loss
    // observe emits nothing important — only at the threshold mark does
    // the wake loss land, and it is a retry notice, never a terminal.
    let t0 = tokio::time::Instant::now();
    let events = crate::test_capture::capture_important(|| {
        let mut reporter = LostVisibilityReporter::new(WakeNoteSlot::default());
        reporter.observe(
            &Visibility::Lost {
                reason: "no reachable peer".to_string(),
                mesh_liveness: MeshLiveness::Unknown,
            },
            t0,
        );
        // The 5-minute mark: the wake-stream loss fires.
        reporter.on_wake_deadline(t0 + std::time::Duration::from_secs(300));
    });

    assert!(
        events
            .iter()
            .any(|e| e.message.contains("has been down for")),
        "a ≥5-minute lost-visibility episode must report on the importance \
         channel: {events:?}"
    );
    assert!(
        !events.iter().any(|e| e.message.contains("run terminated")),
        "visibility loss is NOT a run terminal — no 'run terminated' reason must be \
         emitted for it: {events:?}"
    );
}

/// Recording [`TunnelReconnector`] stub: captures every set of peer ids the
/// observer asked to reconnect, so a test can assert the observer DROVE the
/// reconnect (the `-R` tunnel rebuild) on lost visibility — the BUG-B2
/// contract. The `reconnect` is otherwise a no-op (a real impl rebuilds the
/// ssh tunnel; the unit boundary is "the observer triggered it with the
/// right roster"). Recording state is behind a `std::sync::Mutex` so the
/// stub is genuinely `Send + Sync` (the trait-object bound), with no unsafe.
#[derive(Default)]
struct RecordingReconnector {
    calls: std::sync::Mutex<Vec<Vec<String>>>,
}

#[async_trait::async_trait(?Send)]
impl crate::observer::TunnelReconnector for RecordingReconnector {
    async fn reconnect(&self, peer_ids: &[String]) {
        self.calls
            .lock()
            .expect("recording mutex not poisoned")
            .push(peer_ids.to_vec());
    }
}

/// BUG-B2 (the reconnect the prior agent omitted): a relocated observer
/// that loses ALL visibility (its `-R` reverse tunnels dropped, peer_count
/// == 0) must ACTIVELY trigger a tunnel rebuild for its CRDT roster — not
/// merely report lost. Proof: with a recording [`TunnelReconnector`] wired
/// on the handoff and a named primary in the ledger, the observer's first
/// lost loop calls `reconnect(["sec-0"])`; the observer does NOT hang or
/// strand, and exits `Done` once the primary's `RunComplete` converges over
/// the (later-arriving) inbound.
///
/// Revert check: dropping the `trigger_reconnect()` call (or the
/// `RetryDirective::ReconnectDue` wiring) makes `reconnect_calls` stay empty
/// — the observer would report lost but never rebuild the tunnel, the exact
/// gap this fix closes. The end-to-end "does not collapse + exits on
/// RunComplete" is shared with the dead-fleet test; this one adds the
/// reconnect-was-DRIVEN assertion.
#[tokio::test(flavor = "current_thread")]
async fn lost_visibility_drives_tunnel_reconnect_with_roster() {
    use dynrunner_protocol_primary_secondary::cluster_mutation::ClusterMutation as CM;
    tokio::time::timeout(Duration::from_secs(5), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                // Zero peers ⇒ the observer's transport view is empty for the
                // whole run (the dropped `-R` tunnels). A named primary so
                // the roster the observer asks to reconnect is non-empty.
                let (transport, inbound, _peers) = transport_with_peers("obs", 0);
                let mut cs = ClusterState::<TestId>::new();
                cs.apply(ClusterMutation::PrimaryChanged {
                    new: "sec-0".into(),
                    epoch: 1,
                    reason: PrimaryChangeReason::Transferred,
                });
                let mut config = observer_config("obs");
                // Tiny re-check cadence so the first lost loop (which fires
                // the reconnect) runs well before the RunComplete lands.
                config.fleet_dead_timeout = Duration::from_millis(10);

                let (client, inbox, pump) = observer_mesh(transport, "obs");
                tokio::task::spawn_local(pump);

                // Build the handoff WITH a recording reconnector wired on it
                // (mirrors `into_observer_handoff` carrying the submitter's
                // `tunnel_reconnector`). A minimal inherited-fabric stand-in
                // so `from_handoff` is exercised end-to-end.
                let reconnector = std::sync::Arc::new(RecordingReconnector::default());
                let (inherited_tx, mut inherited_rx) =
                    mpsc::unbounded_channel::<crate::task_completed::TaskCompletedEvent>();
                cs.install_task_completed_sender(inherited_tx);
                let inherited_dispatcher = tokio::task::spawn_local(async move {
                    while inherited_rx.recv().await.is_some() {}
                });
                let lifecycle_dispatcher =
                    tokio::task::spawn_local(async { std::future::pending::<()>().await });
                let handoff = super::ObserverHandoff {
                    client,
                    inbox,
                    cluster_state: cs,
                    node_id: "obs".to_string(),
                    deadlines: config,
                    started_phases: std::collections::HashSet::new(),
                    panik_signal_rx: None,
                    task_completed_dispatcher_handle: inherited_dispatcher,
                    lifecycle_dispatcher_handle: lifecycle_dispatcher,
                    holdings: std::collections::HashSet::new(),
                    reconnector: Some(reconnector.clone()),
                    respawn_provider: None,
                };
                let mut observer = ObserverCoordinator::from_handoff(handoff);

                // Land RunComplete after the observer has had several
                // lost-visibility ticks — long enough to have fired ≥1
                // reconnect, short enough to keep the test snappy.
                tokio::task::spawn_local(async move {
                    tokio::time::sleep(Duration::from_millis(120)).await;
                    let _ = inbound.send(DistributedMessage::ClusterMutation {
                        target: None,
                        sender_id: "sec-0".into(),
                        timestamp: 0.0,
                        mutations: vec![CM::RunComplete],
                    });
                });

                let terminal = observer.run().await.expect(
                    "a lost-visibility observer must keep observing + exit on the primary's \
                     RunComplete, never strand",
                );
                assert!(
                    matches!(terminal, ObserverTerminal::Done),
                    "got {terminal:?}"
                );

                // The observer DROVE the reconnect: at least one call, each
                // carrying the named primary id (the roster it expects to
                // reach over the rebuilt tunnel).
                let calls = reconnector.calls.lock().expect("mutex");
                assert!(
                    !calls.is_empty(),
                    "a lost-visibility observer must TRIGGER the tunnel reconnect, not just \
                     report lost — got zero reconnect calls"
                );
                assert!(
                    calls.iter().all(|ids| ids.contains(&"sec-0".to_string())),
                    "each reconnect must target the observer's roster (the named primary \
                     sec-0): {calls:?}"
                );
            })
            .await;
    })
    .await
    .expect("the reconnecting observer must terminate, not hang");
}

/// The relocated submitter-observer is the ONLY replica holding the
/// relocation `PrimaryChanged` fact when the live broadcast is lost
/// mid-setup (the run_20260610_185621 leaderless wedge). Its anti-entropy
/// digest makes a behind secondary pull a snapshot FROM IT — so the
/// observer MUST answer `RequestClusterSnapshot` (serve-only: the reply is
/// a read of its converged mirror, never a mutation origination — the
/// zero-authority contract is preserved; unlike the secondary's responder
/// it originates NO `PeerJoined`).
///
/// REVERT-CHECK: pre-fix the request lands in `on_inbound`'s ignore-arm
/// and the pull goes unanswered forever — the wedged fleet can never heal
/// off the only ahead replica.
#[tokio::test(flavor = "current_thread")]
async fn observer_answers_snapshot_pull_from_behind_secondary() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                // The requesting (behind, still-in-setup) secondary's outbox.
                let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
                let (sec1_tx, mut sec1_rx) = mpsc::unbounded_channel();
                let mut outgoing = HashMap::new();
                outgoing.insert("sec-1".to_string(), sec1_tx);
                let transport =
                    ChannelPeerTransport::from_raw_channels("setup".into(), outgoing, inbound_rx);

                // The relocated submitter's converged mirror: it applied the
                // relocation locally before the broadcast was lost.
                let mut cs = ClusterState::<TestId>::new();
                cs.apply(ClusterMutation::PrimaryChanged {
                    new: "sec-0".into(),
                    epoch: 1,
                    reason: PrimaryChangeReason::Transferred,
                });

                let mut config = observer_config("setup");
                config.fleet_dead_timeout = Duration::from_secs(60);

                let (client, inbox, pump) = observer_mesh(transport, "setup");
                tokio::task::spawn_local(pump);
                let mut observer = ObserverCoordinator::new(client, inbox, cs, config);
                let run_task = tokio::task::spawn_local(async move {
                    let _ = observer.run().await;
                });

                // The behind secondary's anti-entropy pull (its reactive
                // digest arm targets the proven-ahead SENDER — this observer).
                inbound_tx
                    .send(DistributedMessage::RequestClusterSnapshot {
                        target: None,
                        sender_id: "sec-1".into(),
                        timestamp: 0.0,
                        is_observer: false,
                        can_be_primary: true,
                    })
                    .expect("inbound open");

                // The observer MUST answer with its snapshot, carrying the
                // relocation fact the requester is missing.
                let (reply, reply_target) = loop {
                    let frame = sec1_rx
                        .recv()
                        .await
                        .expect("observer transport stays open");
                    if let DistributedMessage::ClusterSnapshot {
                        target,
                        snapshot_json,
                        ..
                    } = frame
                    {
                        break (snapshot_json, target);
                    }
                };
                // The reply is typed off the requester's self-declared role:
                // a WORKER requester (is_observer=false) gets a
                // Secondary-typed reply (the ingress demux selector).
                assert_eq!(
                    reply_target,
                    Some(Destination::Secondary(PeerId::from("sec-1"))),
                    "a worker requester's snapshot reply must be Secondary-typed"
                );
                let snap: crate::cluster_state::ClusterStateSnapshot<TestId> =
                    serde_json::from_str(&reply).expect("served snapshot decodes");
                let mut healed = ClusterState::<TestId>::new();
                healed.restore(snap);
                assert_eq!(
                    healed.current_primary(),
                    Some("sec-0"),
                    "the served snapshot carries the relocation fact"
                );
                assert_eq!(healed.primary_epoch(), 1);

                run_task.abort();
            })
            .await;
    })
    .await
    .expect(
        "REVERT-CHECK: the observer never answered the snapshot pull — the only \
         ahead replica is mute and the wedged fleet cannot heal",
    );
}

/// The observer responder's reply is TYPED off the requester's
/// self-declared role: an OBSERVER requester (`is_observer: true` on the
/// request frame) is answered under `Destination::Observer(requester)` —
/// the receiver-side ingress demux selector (the PeerId already selects
/// the host at egress). The worker-requester half is pinned by
/// `observer_answers_snapshot_pull_from_behind_secondary` above.
#[tokio::test(flavor = "current_thread")]
async fn observer_answers_observer_requester_with_observer_typed_reply() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                // The requesting (behind) PEER OBSERVER's outbox.
                let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
                let (obs1_tx, mut obs1_rx) = mpsc::unbounded_channel();
                let mut outgoing = HashMap::new();
                outgoing.insert("obs-1".to_string(), obs1_tx);
                let transport =
                    ChannelPeerTransport::from_raw_channels("setup".into(), outgoing, inbound_rx);

                let mut cs = ClusterState::<TestId>::new();
                cs.apply(ClusterMutation::PrimaryChanged {
                    new: "sec-0".into(),
                    epoch: 1,
                    reason: PrimaryChangeReason::Transferred,
                });

                let mut config = observer_config("setup");
                config.fleet_dead_timeout = Duration::from_secs(60);

                let (client, inbox, pump) = observer_mesh(transport, "setup");
                tokio::task::spawn_local(pump);
                let mut observer = ObserverCoordinator::new(client, inbox, cs, config);
                let run_task = tokio::task::spawn_local(async move {
                    let _ = observer.run().await;
                });

                // The behind peer observer's anti-entropy pull declares its
                // own role on the request frame.
                inbound_tx
                    .send(DistributedMessage::RequestClusterSnapshot {
                        target: None,
                        sender_id: "obs-1".into(),
                        timestamp: 0.0,
                        is_observer: true,
                        can_be_primary: false,
                    })
                    .expect("inbound open");

                let reply_target = loop {
                    let frame = obs1_rx
                        .recv()
                        .await
                        .expect("observer transport stays open");
                    if let DistributedMessage::ClusterSnapshot { target, .. } = frame {
                        break target;
                    }
                };
                assert_eq!(
                    reply_target,
                    Some(Destination::Observer(PeerId::from("obs-1"))),
                    "an observer requester's snapshot reply must be Observer-typed \
                     (the requester's self-declared role is the ingress demux selector)"
                );

                run_task.abort();
            })
            .await;
    })
    .await
    .expect("the observer never answered the peer observer's snapshot pull");
}

/// #370 fires-under-real-topology: the PRODUCTION observer EMITS its
/// anti-entropy digest on the wire — through the real `run()` loop, the
/// real `Mesh`, and a real transport with a registered peer — and KEEPS
/// emitting (the cadence recurs) while the loop's other periodic arms
/// (the 50ms visibility-recheck tick, the recovery tick) stay busy.
///
/// The prior relocation-heal fix tested only the APPLY/PULL sides with a
/// synthetic observer pushing digest frames by hand; nobody pinned that
/// the production observer's `ae_tick` actually puts `StateDigest`
/// frames onto the wire (the generalized watchdog-needs-a-fires-test
/// law). The jittered period is 15–25s, so this runs under paused time.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn observer_digest_cadence_emits_on_the_wire() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _inbound, mut peers) = transport_with_peers("obs", 1);
            let mut peer_rx = peers.pop().expect("one registered peer");
            // No terminal in the ledger — the observer keeps observing for
            // the whole assertion window.
            let cs = ClusterState::<TestId>::new();
            let (client, inbox, pump) = observer_mesh(transport, "obs");
            tokio::task::spawn_local(pump);
            let mut observer = ObserverCoordinator::new(client, inbox, cs, observer_config("obs"));

            // TWO digests prove the cadence RECURS (a single emit could be a
            // one-shot); both must reach the registered peer's wire end.
            let assertion = async {
                let mut digests_seen = 0u32;
                while digests_seen < 2 {
                    let frame = peer_rx.recv().await.expect("peer wire open");
                    if matches!(frame, DistributedMessage::StateDigest { ref sender_id, .. } if sender_id == "obs")
                    {
                        digests_seen += 1;
                    }
                }
            };

            tokio::select! {
                run = observer.run() => {
                    panic!("the observer must keep observing, got {run:?}");
                }
                () = assertion => { /* two digest broadcasts reached the wire */ }
                _ = tokio::time::sleep(Duration::from_secs(120)) => {
                    panic!(
                        "SILENT CADENCE: the production observer's anti-entropy \
                         tick never put a StateDigest on the wire within 120s \
                         (≥4 jittered periods) — the relocation-handoff heal \
                         has no emitter"
                    );
                }
            }
        })
        .await;
}

/// THE asm-dataset LMU stale-snapshot spin replay: every SLURM job is
/// dead (zero transport legs, zero inbound — forever) but the observer's
/// last converged CRDT snapshot still shows live worker-secondary
/// members (a fleet that dies without originating `PeerRemoved` freezes
/// the membership ledger alive-looking). Pre-fix the observer looped
/// "last CRDT snapshot still shows N live worker-secondary members …
/// mesh running autonomously" FOREVER — stale-snapshot reassurance, no
/// verdict, the submitter never exits.
///
/// The fix (`observer::fleet_death`): once nothing has been received
/// from ANY member for the LONG `fleet_death_presumption` threshold,
/// with the transport showing zero legs and ≥3 driven reconnect
/// recovery cycles failed, the observer reports
/// fleet-unreachable-presumed-dead loudly and exits non-zero
/// (`RunError::FatalPolicyExit`) — a bounded LAST-RESORT terminal. The
/// rc-B report-and-retry machinery (immediate full-log diagnostics, the
/// 5-minute wake gate, the ~60s recurrence) runs untouched ahead of it —
/// this paused-clock test rides through MANY of those cycles (3+ virtual
/// minutes of recurrences) before the presumption may trip.
///
/// REVERT-CHECK: pre-fix `run()` never returns here — the outer
/// virtual-time bound (1000s, 2.5× the presumption window) expires with
/// the observer still spinning on the stale snapshot.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn stale_snapshot_fleet_death_is_bounded_loud_terminal() {
    use crate::primary::RunError;
    use dynrunner_core::{ResourceAmount, ResourceKind};

    tokio::time::timeout(Duration::from_secs(1000), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                // Zero peers, inbound held open but NEVER fed — the
                // whole-fleet-dead transport view.
                let (transport, _inbound, _peers) = transport_with_peers("obs", 0);

                // The STALE snapshot: 10 worker-secondaries joined +
                // advertised capacity, none ever removed — exactly what
                // the production observer's ledger held while reassuring
                // the operator.
                let mut cs = ClusterState::<TestId>::new();
                for i in 0..10 {
                    cs.apply(ClusterMutation::PeerJoined {
                        peer_id: format!("sec-{i}"),
                        is_observer: false,
                        can_be_primary: true,
                        cap_version: Default::default(),
                        member_gen: 0,
                    });
                    cs.apply(ClusterMutation::SecondaryCapacity {
                        secondary: format!("sec-{i}"),
                        worker_count: 4,
                        resources: vec![ResourceAmount {
                            kind: ResourceKind::memory(),
                            amount: 1024 * 1024 * 1024,
                        }],
                    });
                }
                assert!(
                    cs.alive_secondary_members().count() == 10,
                    "the stale snapshot must read alive (the production smell)"
                );

                let mut config = observer_config("obs");
                config.fleet_dead_timeout = Duration::from_millis(50);
                // LONG relative to every cadence in the loop (recurrence
                // 60s, wake gate 300s) but bounded: the presumption trips
                // at 400s of total virtual silence — after the wake-loss
                // gate fired and ≥3 recovery cycles failed.
                config.fleet_death_presumption = Duration::from_secs(400);

                let (client, inbox, pump) = observer_mesh(transport, "obs");
                tokio::task::spawn_local(pump);
                let mut observer = ObserverCoordinator::new(client, inbox, cs, config);

                let err = observer.run().await.expect_err(
                    "a fleet dead past the presumption threshold (zero legs, \
                     zero inbound, failed reconnect cycles) must be a BOUNDED \
                     non-zero terminal — never an infinite stale-snapshot spin",
                );
                match err {
                    RunError::FatalPolicyExit { reason } => {
                        assert!(
                            reason.contains("presumed dead"),
                            "the verdict carries the distinct fleet-death \
                             wording: {reason}"
                        );
                    }
                    other => panic!(
                        "the fleet-death terminal must be the structured \
                         FatalPolicyExit (the PyO3 boundary raises it \
                         non-zero), got {other:?}"
                    ),
                }
            })
            .await;
    })
    .await
    .expect(
        "BOUNDED terminal: the observer must exit within the presumption \
         window — the pre-fix behaviour spins on the stale snapshot forever",
    );
}

/// #415 face (b2) — the presumed-dead terminal under a FLAPPING leg, the
/// production-vs-#394-test gap.
///
/// run_20260611_155305: the observer sat blind past 20 min WITHOUT ever
/// rendering the presumed-dead terminal, while it cycled `-R` tunnel
/// rebuilds (193 "observer rebuilt reverse tunnel" SUCCESSES). The #394
/// test (`stale_snapshot_fleet_death_is_bounded_loud_terminal`) passes
/// because its leg view is FLAT zero forever — but production's was not.
/// A SUCCESSFUL `-R` rebuild re-opens the worker-side listener; a stale /
/// retrying compute peer (or a lingering wire) momentarily re-registers a
/// writer in the observer's `outgoing` table, so `peer_count()` flaps
/// 0→N→0 each rebuild cycle WITHOUT a single application frame ever
/// arriving (the run is over; no terminal is delivered).
///
/// That transient `peer_count > 0` is the bug: the detector treats ANY
/// wired leg as a full episode reset (`reconnect_attempts = 0`, silence
/// baseline restarts), so a leg that flaps up faster than the presumption
/// window keeps resetting the clock — the terminal never fires and the
/// observer spins on its stale snapshot forever. The fix must require a
/// leg to actually DELIVER (a fresh inbound frame), not merely register,
/// before it counts as a sign of life that resets the death presumption.
///
/// REVERT-CHECK: with the fix reverted the flapping leg resets the episode
/// every ~60s and the 4000s outer bound expires with no terminal — the
/// production blackout verbatim.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fleet_death_terminal_fires_under_flapping_leg_with_no_delivery() {
    use crate::primary::RunError;
    use dynrunner_core::{ResourceAmount, ResourceKind};

    tokio::time::timeout(Duration::from_secs(4000), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                // The whole-fleet-dead transport view: a leg that flaps
                // present↔absent (the FlappingMembershipPeer below), inbound
                // never fed — no application frame ever arrives (run is over).
                let mut cs = ClusterState::<TestId>::new();
                for i in 0..10 {
                    cs.apply(ClusterMutation::PeerJoined {
                        peer_id: format!("sec-{i}"),
                        is_observer: false,
                        can_be_primary: true,
                        cap_version: Default::default(),
                        member_gen: 0,
                    });
                    cs.apply(ClusterMutation::SecondaryCapacity {
                        secondary: format!("sec-{i}"),
                        worker_count: 4,
                        resources: vec![ResourceAmount {
                            kind: ResourceKind::memory(),
                            amount: 1024 * 1024 * 1024,
                        }],
                    });
                }

                let mut config = observer_config("obs");
                config.fleet_dead_timeout = Duration::from_millis(50);
                config.fleet_death_presumption = Duration::from_secs(1200);

                // The mesh's membership is read off the transport each pump
                // cycle. To FLAP `peer_count` (a leg that re-registers on a
                // rebuild then dies again, delivering nothing) we drive a
                // shared cell the test toggles 0↔1 on a ~60s cadence — the
                // rebuild-success-then-die rhythm — directly on the slot the
                // pump republishes from. The flap is faster than the 1200s
                // presumption window, so a reset-on-any-wired-leg detector
                // never accumulates the silence.
                let connected: std::rc::Rc<std::cell::RefCell<std::collections::HashSet<String>>> =
                    std::rc::Rc::new(std::cell::RefCell::new(std::collections::HashSet::new()));
                let peer = FlappingMembershipPeer {
                    connected: connected.clone(),
                };
                let mut mesh = Mesh::<TestId, _>::new(peer);
                let (slot, client, inbox) =
                    mesh.register_local_role(LocalRole::Observer, PeerId::from("obs"));
                mesh.publish_membership();
                tokio::task::spawn_local(async move {
                    let _slot = slot;
                    let (_control, control_rx) = crate::process::pump::control_channel::<TestId>();
                    crate::process::pump::run_pump(mesh, control_rx).await;
                });
                // The flapper: every 60s the leg "re-registers" for a beat
                // (peer_count 0→1) then drops again — no frame delivered.
                let flap = connected.clone();
                tokio::task::spawn_local(async move {
                    loop {
                        tokio::time::sleep(Duration::from_secs(58)).await;
                        flap.borrow_mut().insert("sec-0".to_string());
                        // Held just long enough for the ~100ms pump publish +
                        // a top-of-loop observe to read it, then dropped.
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        flap.borrow_mut().clear();
                    }
                });

                let mut observer = ObserverCoordinator::new(client, inbox, cs, config);

                let err = observer.run().await.expect_err(
                    "the presumed-dead terminal MUST fire within a bounded \
                     window even though the leg flaps 0→1→0 every ~60s — a \
                     re-registering leg that never DELIVERS a frame is not a \
                     sign of life (the run is over). Pre-fix the flap reset \
                     the episode each cycle and the observer spun forever \
                     (the run_20260611_155305 blackout).",
                );
                match err {
                    RunError::FatalPolicyExit { reason } => {
                        assert!(
                            reason.contains("presumed dead"),
                            "the verdict carries the fleet-death wording: {reason}"
                        );
                    }
                    other => panic!("expected the bounded FatalPolicyExit, got {other:?}"),
                }
            })
            .await;
    })
    .await
    .expect(
        "BOUNDED terminal under a flapping no-delivery leg: the observer must \
         exit within a bounded window — the pre-fix production blackout spun \
         forever because each rebuild's transient leg reset the presumption",
    );
}
