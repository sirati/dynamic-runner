//! Primary-side SIGUSR2 graceful-abort tests — the operator signals the
//! submitter / standalone primary directly.
//!
//! A PRIMARY that receives the operator's SIGUSR2 IS the abort authority: it
//! does NOT send a `GracefulAbortRequest` over the mesh (the observer's arm
//! does that), it short-circuits straight into the same
//! `GracefulAbortRequested` latch the wire handler drives. Before this fix
//! SIGUSR2 was armed only on the observer paths, so the WHOLE primary tenure
//! (submitter bootstrap + standalone primary) left SIGUSR2 on its kernel
//! default — terminate. These tests pin the fix:
//!
//!   1. A SIGUSR2 latched before the operational loop seats survives (pre-fix
//!      the kernel default killed this very test process — exit 140) and the
//!      primary's own arm INITIATES the graceful abort (broadcasts the
//!      `GracefulAbortRequested` mutation + surfaces `RunError::GracefulAbort`)
//!      on the loop's first poll — no `GracefulAbortRequest` is ever sent.
//!   2. The relocation handover: a trigger injected into a primary that
//!      RELOCATES rides `into_observer_handoff` onto the standalone observer,
//!      so a signal latched during the primary tenure surfaces on the
//!      relocated observer's first poll (it sends ONE `GracefulAbortRequest`
//!      to the recognized primary) instead of killing the relocated process.
//!
//! # Why a separate integration binary
//!
//! Signals are process-global: raising SIGUSR2 feeds EVERY armed
//! `SignalKind::user_defined2()` stream in the process, and the unit-test
//! binary runs many role loops (each arming one) in parallel — a raise there
//! would inject spurious graceful-abort triggers into unrelated tests. This
//! binary is its own process and only these tests run in it; within it the
//! raising tests serialize behind a single `Mutex` (the gabort-preseat /
//! panik SIGTERM harness style).
//!
//! The mesh harness mirrors `graceful_abort_preseat.rs` (the observer side)
//! and the in-crate primary suite's channel-mesh fixtures.

use std::collections::HashMap;
use std::time::Duration;

use dynrunner_protocol_primary_secondary::cluster_mutation::ClusterMutation;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PrimaryChangeReason};
use dynrunner_transport_channel::ChannelPeerTransport;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use dynrunner_core::{
    PhaseId, ResourceAmount, ResourceKind, ResourceMap, SoftPreferredSecondaries, TaskInfo, TypeId,
};
use dynrunner_manager_distributed::cluster_state::ClusterState;
use dynrunner_manager_distributed::process::{LocalRole, Mesh, MeshClient, RoleInbox};
use dynrunner_manager_distributed::{
    GracefulAbortTrigger, PrimaryConfig, PrimaryCoordinator, PrimaryRunOutcome, RunError,
};
use dynrunner_protocol_primary_secondary::address::PeerId;
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_scheduler_api::ResourceEstimator;

/// Minimal serializable identifier for the primary tests.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct TestId(String);

/// A fixed-budget estimator (every task reserves the same memory) — the
/// integration-binary stand-in for the in-crate `FixedEstimator`.
struct FixedEstimator(u64);
impl ResourceEstimator<TestId> for FixedEstimator {
    fn estimate(&self, _task: &TaskInfo<TestId>) -> ResourceMap {
        let mut m = ResourceMap::new();
        m.insert(ResourceKind::memory(), self.0);
        m
    }
}

/// One advertised-memory resource amount (the live capacity shape).
fn mem(bytes: u64) -> Vec<ResourceAmount> {
    vec![ResourceAmount {
        kind: ResourceKind::memory(),
        amount: bytes,
    }]
}

/// A single-phase pending `TaskInfo` whose `task_id == name`, on the
/// implicit zero-deps `"default"` phase (no explicit `PhaseDepsSet` needed —
/// hydrate treats an unmentioned phase as a single zero-deps phase).
fn plain_task(name: &str) -> TaskInfo<TestId> {
    TaskInfo {
        path: std::path::PathBuf::from(name),
        size: 100,
        identifier: TestId(name.to_string()),
        phase_id: PhaseId::from("default"),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: name.to_string(),
        task_depends_on: Vec::new(),
        preferred_secondaries: SoftPreferredSecondaries::default(),
        preferred_version: Default::default(),
        kind: Default::default(),
        setup_affinity: None,
        upload_file: None,
        required_files: None,
        resolved_path: None,
    }
}

/// Live peer receivers kept alive for the test's duration: the router PRUNES
/// a peer outbox whose receiver was dropped on the first failed send, which
/// would silently drop the membership view. Tests bind this so the dummy
/// peers stay "connected".
type PeerKeepalive = Vec<mpsc::UnboundedReceiver<DistributedMessage<TestId>>>;

/// Build a `ChannelPeerTransport` with `peer_count` dummy peer outboxes keyed
/// `peer-{i}` and an inbound fed by the returned sender. Mirrors the
/// gabort-preseat observer harness.
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

/// Mint the Primary mesh trio over `transport` and hand back the
/// coordinator's `(client, inbox)` plus a PUMP future to drive concurrently
/// on the same `LocalSet` as `primary.run()`. The faithful primary mirror of
/// the gabort-preseat `observer_mesh`: it bridges the test's
/// `ChannelPeerTransport` to the detached client/inbox the coordinator holds.
#[allow(clippy::type_complexity)]
fn primary_mesh(
    transport: ChannelPeerTransport<TestId>,
    node_id: &str,
) -> (
    MeshClient<TestId>,
    RoleInbox<TestId>,
    std::pin::Pin<Box<dyn std::future::Future<Output = ()>>>,
) {
    let mut mesh = Mesh::<TestId, _>::new(transport);
    let (slot, client, inbox) =
        mesh.register_local_role(LocalRole::Primary, PeerId::from(node_id));
    mesh.publish_membership();
    let pump = async move {
        // Keep `slot` alive for the pump's lifetime so the mesh `Weak` keeps
        // upgrading the primary's inbox.
        let _slot = slot;
        let mut wire_open = true;
        loop {
            // INGRESS: demux any ready inbound frame to the primary slot.
            if wire_open
                && let Ok(maybe) = tokio::time::timeout(Duration::ZERO, mesh.recv_peer()).await
            {
                match maybe {
                    Some(frame) => {
                        mesh.deliver_local(LocalRole::Primary, frame);
                    }
                    None => wire_open = false,
                }
            }
            // EGRESS: apply every queued client send against the live slots /
            // the wire (so a test draining a peer outbox observes the
            // primary's broadcasts).
            while let Ok(Some(item)) =
                tokio::time::timeout(Duration::ZERO, mesh.next_local_dispatch()).await
            {
                let _ = mesh.apply_local_dispatch(item).await;
            }
            mesh.publish_membership();
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    (client, inbox, Box::pin(pump))
}

/// A `PromotionSnapshot`-ready `ClusterState`: `node_id` is the recognized
/// primary (so `initiate_graceful_abort` / `graceful_abort_tick` act), a
/// `peer-0` secondary capacity record exists, and ONE pending task is seeded
/// (so the operational loop's entry run-complete check is false and the loop
/// actually polls its `select!` arms at least once — otherwise an empty
/// ledger trips run-complete on entry before the graceful-abort arm fires).
fn operational_snapshot(node_id: &str) -> ClusterState<TestId> {
    let mut cs = ClusterState::<TestId>::new();
    cs.apply(ClusterMutation::PrimaryChanged {
        new: node_id.into(),
        epoch: 1,
        reason: PrimaryChangeReason::Election,
    });
    cs.apply(ClusterMutation::SecondaryCapacity {
        secondary: "peer-0".into(),
        worker_count: 1,
        resources: mem(8 * 1024 * 1024 * 1024),
    });
    cs.apply(ClusterMutation::TaskAdded {
        hash: "t-0".into(),
        task: plain_task("t-0"),
    });
    cs
}

/// A `PrimaryConfig` with `num_secondaries: 0` (so `wait_for_connections`
/// returns immediately — no real secondary to stand up) and a LONG
/// `fleet_dead_timeout` so the brief sub-second window the latched SIGUSR2
/// needs cannot be misclassified as a strand.
fn primary_config(node_id: &str) -> PrimaryConfig {
    PrimaryConfig {
        node_id: node_id.to_string(),
        num_secondaries: 0,
        connect_timeout: Duration::from_secs(5),
        peer_timeout: Duration::from_secs(300),
        keepalive_interval: Duration::from_millis(50),
        fleet_dead_timeout: Duration::from_secs(60),
        mesh_ready_timeout: Duration::from_secs(60),
        ..PrimaryConfig::default()
    }
}

/// No-op phase hooks (the run carries no real phase work).
#[allow(clippy::type_complexity)]
fn noop_phase_hooks() -> (
    dynrunner_manager_distributed::primary::OnPhaseStart,
    dynrunner_manager_distributed::primary::OnPhaseEnd,
) {
    (Box::new(|_: &PhaseId| {}), Box::new(|_, _, _, _| {}))
}

// SIGUSR2 is process-global; all raising tests serialize behind one lock so a
// raise can never leak into a sibling's trigger. `tokio::sync::Mutex` because
// the guard is held across `.await`.
static SIGUSR2_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Raise SIGUSR2 against this process — the operator's `kill -USR2`.
fn raise_sigusr2() {
    nix::sys::signal::raise(nix::sys::signal::Signal::SIGUSR2).expect("raise SIGUSR2");
}

/// THE primary headline: a SIGUSR2 latched before the operational loop seats
/// is SURVIVED (pre-fix the kernel default disposition killed this very test
/// process — exit 140) and the primary's own arm INITIATES the graceful abort
/// — it broadcasts the `GracefulAbortRequested` mutation (it is the abort
/// authority — it never sends itself a `GracefulAbortRequest`) and surfaces
/// the structured `RunError::GracefulAbort` terminal.
#[tokio::test(flavor = "current_thread")]
async fn primary_sigusr2_initiates_graceful_abort_and_survives() {
    let _lock = SIGUSR2_TEST_LOCK.lock().await;
    tokio::time::timeout(Duration::from_secs(10), async {
        // Pre-seat: arm at "process entry", then the operator signals while
        // the (simulated) bootstrap is still in flight.
        let trigger = GracefulAbortTrigger::arm();
        raise_sigusr2();
        // Surviving past this raise is already the load-bearing fact.

        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                let (transport, _inbound, mut peers) = transport_with_peers("pri", 1);
                let (client, inbox, pump) = primary_mesh(transport, "pri");
                tokio::task::spawn_local(pump);
                let (_demote_tx, demote_rx) = mpsc::unbounded_channel::<()>();
                let mut primary = PrimaryCoordinator::new(
                    primary_config("pri"),
                    client,
                    inbox,
                    demote_rx,
                    ResourceStealingScheduler::memory(),
                    FixedEstimator(100),
                );
                primary.seed_from_promotion_snapshot(operational_snapshot("pri").snapshot());
                // Inject the entry-armed trigger (the PyO3 entry's step) and
                // drive the REAL operational loop.
                primary.register_graceful_abort_trigger(trigger);

                let (ops, ope) = noop_phase_hooks();
                let terminal = primary
                    .run(
                        dynrunner_manager_distributed::process::SeedSource::PromotionSnapshot { kind: dynrunner_manager_distributed::process::BootstrapKind::Failover },
                        ops,
                        ope,
                    )
                    .await;

                // The primary initiated the abort itself: the run returns the
                // structured graceful-abort terminal.
                assert!(
                    matches!(terminal, Err(RunError::GracefulAbort { .. })),
                    "the SIGUSR2-driven primary must surface RunError::GracefulAbort; \
                     got {terminal:?}"
                );

                // It broadcast the `GracefulAbortRequested` latch (apply +
                // fleet-wide), so the wired peer observed it — and it NEVER
                // sent itself a `GracefulAbortRequest` (a primary is the
                // authority, not a requester).
                let mut saw_latch = false;
                let mut saw_self_request = false;
                while let Ok(frame) = peers[0].try_recv() {
                    match frame {
                        DistributedMessage::ClusterMutation { mutations, .. }
                            if mutations
                                .iter()
                                .any(|m| matches!(m, ClusterMutation::GracefulAbortRequested)) =>
                        {
                            saw_latch = true;
                        }
                        DistributedMessage::GracefulAbortRequest { .. } => {
                            saw_self_request = true;
                        }
                        _ => {}
                    }
                }
                assert!(
                    saw_latch,
                    "the primary must broadcast the GracefulAbortRequested latch \
                     fleet-wide when SIGUSR2 fires"
                );
                assert!(
                    !saw_self_request,
                    "a primary is the abort authority — it must NOT send itself a \
                     GracefulAbortRequest"
                );
            })
            .await;
    })
    .await
    .expect("test must finish within budget");
}

/// A primary with NO trigger injected (the un-injected default) parks its
/// graceful-abort arm and NEVER self-arms a second `user_defined2` stream:
/// the run reaches a NON-graceful terminal under the same fixture. This is
/// the single-owner-rule control — proof the arm is inert without injection
/// (so the primary never races the one process trigger the entry path owns).
/// No signal is raised, so this test does not take the SIGUSR2 lock.
#[tokio::test(flavor = "current_thread")]
async fn un_injected_primary_arm_is_inert() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                let (transport, _inbound, _peers) = transport_with_peers("pri2", 1);
                let (client, inbox, pump) = primary_mesh(transport, "pri2");
                tokio::task::spawn_local(pump);
                let (_demote_tx, demote_rx) = mpsc::unbounded_channel::<()>();
                let mut primary = PrimaryCoordinator::new(
                    primary_config("pri2"),
                    client,
                    inbox,
                    demote_rx,
                    ResourceStealingScheduler::memory(),
                    FixedEstimator(100),
                );
                // Empty ledger: recognized-self primary, no pending work →
                // the entry run-complete check trips and the run is Done
                // (clean) — the un-injected graceful-abort arm never fires.
                let mut cs = ClusterState::<TestId>::new();
                cs.apply(ClusterMutation::PrimaryChanged {
                    new: "pri2".into(),
                    epoch: 1,
                    reason: PrimaryChangeReason::Election,
                });
                primary.seed_from_promotion_snapshot(cs.snapshot());
                // Deliberately NO register_graceful_abort_trigger.

                let (ops, ope) = noop_phase_hooks();
                let terminal = primary
                    .run(
                        dynrunner_manager_distributed::process::SeedSource::PromotionSnapshot { kind: dynrunner_manager_distributed::process::BootstrapKind::Failover },
                        ops,
                        ope,
                    )
                    .await;
                assert!(
                    !matches!(terminal, Err(RunError::GracefulAbort { .. })),
                    "an un-injected primary must NOT reach a graceful-abort terminal \
                     (the arm is inert without an injected trigger); got {terminal:?}"
                );
            })
            .await;
    })
    .await
    .expect("test must finish within budget");
}

/// The relocation handover (the submitter lifecycle: primary tenure →
/// relocation → observer `from_handoff`). A trigger injected into a primary
/// must (a) RIDE the by-value relocation across the public
/// `ObserverHandoff.graceful_abort_trigger` field — the same handoff
/// `run_consuming`'s demote arm produces and the `Node` drives — so it is NOT
/// orphaned by the role swap, and (b) keep the relocated process ALIVE under
/// a post-relocation operator SIGUSR2: the observer built `from_handoff`
/// consumes the SAME stream, so a `kill -USR2` after relocation is absorbed
/// by the observer's run loop instead of hitting the kernel default and
/// killing the process. These are the exact two guarantees the fix adds for
/// the relocation tail; that the observer then ROUTES the consumed signal to
/// the recognized primary as a `GracefulAbortRequest` is the observer's own
/// contract, pinned by `graceful_abort_preseat`.
///
/// (The pre-relocation-latched case is the primary's own concern: whichever
/// loop is active when the signal lands consumes it — the setup peer relocates
/// at bootstrap WITHOUT running the operational loop, so a pre-relocation
/// delivery rides the handoff; a promoted primary running the loop consumes it
/// directly. Both are correct.)
#[tokio::test(flavor = "current_thread")]
async fn relocation_hands_the_trigger_to_the_responding_observer() {
    let _lock = SIGUSR2_TEST_LOCK.lock().await;
    tokio::time::timeout(Duration::from_secs(10), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                let (transport, inbound, _peers) = transport_with_peers("setup", 1);
                let (client, inbox, pump) = primary_mesh(transport, "setup");
                tokio::task::spawn_local(pump);
                let (demote_tx, demote_rx) = mpsc::unbounded_channel::<()>();
                let mut primary = PrimaryCoordinator::new(
                    primary_config("setup"),
                    client,
                    inbox,
                    demote_rx,
                    ResourceStealingScheduler::memory(),
                    FixedEstimator(100),
                );
                // A remote recognized primary in the seed (`peer-0`) — the
                // relocation target a real submitter hands the role to.
                let mut cs = ClusterState::<TestId>::new();
                cs.apply(ClusterMutation::PrimaryChanged {
                    new: "peer-0".into(),
                    epoch: 1,
                    reason: PrimaryChangeReason::Election,
                });
                primary.seed_from_promotion_snapshot(cs.snapshot());
                // Inject the entry-armed trigger (NO pre-relocation signal — the
                // post-relocation responder is what this pins deterministically).
                primary.register_graceful_abort_trigger(GracefulAbortTrigger::arm());

                // Fire the demote signal so `run_consuming` relinquishes the
                // primary by value into a `Relocated` outcome — the real
                // relocation path. Fired from a DELAYED task so the pipeline
                // future has entered `run_pipeline` and spawned its
                // run-dispatchers first (`into_observer_handoff` requires both
                // dispatcher handles, exactly as a real relocation: the demote
                // arrives mid-run, never before the run starts).
                tokio::task::spawn_local(async move {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    let _ = demote_tx.send(());
                });
                let (ops, ope) = noop_phase_hooks();
                let outcome = primary
                    .run_consuming(
                        dynrunner_manager_distributed::process::SeedSource::PromotionSnapshot { kind: dynrunner_manager_distributed::process::BootstrapKind::Failover },
                        ops,
                        ope,
                    )
                    .await
                    .expect("run_consuming must return Ok on the relocate path");
                let handoff = match outcome {
                    PrimaryRunOutcome::Relocated { handoff } => handoff,
                    PrimaryRunOutcome::Local { .. } => {
                        panic!("the demote must relocate the primary, not stay Local")
                    }
                };
                // (a) The trigger crossed the handoff — it is NOT lost.
                assert!(
                    handoff.graceful_abort_trigger.is_some(),
                    "the relocation handoff must carry the injected trigger across \
                     (the trigger must not be orphaned by the role swap)"
                );
                let mut observer = dynrunner_manager_distributed::observer::ObserverCoordinator::<
                    TestId,
                >::from_handoff(*handoff);
                let run = tokio::task::spawn_local(async move { observer.run().await });

                // (b) The relocated process SURVIVES a post-relocation operator
                // SIGUSR2 — the exact hole this closes for the relocation tail.
                // Pre-fix the relocated process had NO armed handler (arming was
                // observer-`run`-start only and the relocated observer inherits
                // an EMPTY trigger), so a `kill -USR2` hit the kernel default
                // and KILLED the process (exit 140). With the handed-over
                // trigger the observer's run loop ABSORBS the signal: the
                // process is still alive after, and the run loop is still
                // running (not killed). (That the observer then sends a
                // `GracefulAbortRequest` to the recognized primary is the
                // observer's own contract, pinned by `graceful_abort_preseat`.)
                tokio::time::sleep(Duration::from_millis(20)).await;
                raise_sigusr2();
                tokio::time::sleep(Duration::from_millis(50)).await;
                assert!(
                    !run.is_finished(),
                    "the relocated observer must SURVIVE a post-relocation SIGUSR2 \
                     (the handed-over trigger absorbs it; pre-fix the kernel default \
                     would have killed the process)"
                );

                // Wind the observer run down on the primary's drain terminal.
                let _ = inbound.send(DistributedMessage::ClusterMutation {
                    target: None,
                    sender_id: "peer-0".into(),
                    timestamp: 0.0,
                    mutations: vec![
                        ClusterMutation::GracefulAbortRequested,
                        ClusterMutation::RunComplete { counts: Default::default() },
                    ],
                });
                let _ = tokio::time::timeout(Duration::from_secs(5), run).await;
            })
            .await;
    })
    .await
    .expect("test must finish within budget");
}
