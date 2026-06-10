//! Worker-ready (capacity-growth) dispatch coverage: dispatch is a pure
//! function of (ready-tasks ∩ idle-worker-capacity), re-evaluated on the
//! WORKER-READY event (a newly-applied `SecondaryCapacity`), not only on
//! the task-added event. Closes the relocated-primary bring-up race where
//! a worker becomes ready AFTER `perform_initial_assignment`'s snapshot
//! (assigned=0) and the ready task strands because no path rebuilt the
//! roster or re-ran the dispatch recheck.
//!
//! All deterministic — no operational loop raced against a wall clock.
//! The bus is driven synchronously (install a `WorkerMgmtSignal` sender,
//! drive the apply path, drain the coalesced batch, run the reaction)
//! exactly as the dispatch-decoupling tests do.
//!
//! Three real stalls + revert-checks:
//!   (a) STARTUP via wire — a `SecondaryCapacity` arrives over the mesh
//!       (`handle_cluster_mutation`) after assignment; the roster grows
//!       and the ready task IS dispatched. Revert-check: a redundant
//!       NoOp re-emit neither re-grows the roster nor re-emits.
//!   (b) MID-RUN via local origination — a worker becomes ready through
//!       the originator path (`apply_and_broadcast_cluster_mutations`,
//!       the `handle_welcome` channel); the roster grows + the recheck
//!       dispatches. Revert-check: a re-origination NoOps.
//!   (c) WAIT SELF-RECOVERY — the pre-loop bus-drain
//!       (`drain_and_react_to_pending_worker_signals`, the mechanism
//!       `wait_for_mesh_ready` runs inline) services a queued `TasksAdded`
//!       and dispatches, so a capacity that lands during the wait does
//!       NOT deadlock on the never-emitted `MeshReady`. Revert-check:
//!       without the drain the freed worker sits idle.

use super::*;

use dynrunner_core::{PhaseId, ResourceAmount, ResourceKind, TypeId};

use crate::primary::wire::compute_task_hash;
use crate::worker_signal::{WorkerMgmtSignal, recv_worker_signal_batch};

type TestPrimary = PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>;

/// One advertised-memory resource amount (bytes), the live welcome shape.
fn mem(bytes: u64) -> Vec<ResourceAmount> {
    vec![ResourceAmount {
        kind: ResourceKind::memory(),
        amount: bytes,
    }]
}

/// A single zero-dep task in phase "p", type "default".
fn one_task(name: &str) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 100);
    t.phase_id = PhaseId::from("p");
    t.type_id = TypeId::from("default");
    t
}

/// A `SecondaryCapacity` wire batch naming `secondary` with `n` workers —
/// the shape a remote peer forwards / the primary itself originates on
/// welcome. Paired with the `PeerJoined` that the welcome originates
/// alongside it, so the apply makes the secondary a known + alive member.
fn capacity_batch(secondary: &str, n: u32) -> DistributedMessage<TestId> {
    DistributedMessage::ClusterMutation {
        target: None,
        sender_id: "setup".into(),
        timestamp: 0.0,
        mutations: vec![
            ClusterMutation::PeerJoined {
                peer_id: secondary.into(),
                is_observer: false,
                can_be_primary: true,
                cap_version: Default::default(),
            },
            ClusterMutation::SecondaryCapacity {
                secondary: secondary.into(),
                worker_count: n,
                resources: mem(8 * 1024 * 1024 * 1024),
            },
        ],
    }
}

/// Drain every `TaskAssignment` `task_id` queued on a secondary's wire
/// (non-blocking). `task_id == name` for `one_task`.
fn assigned_ids(rx: &mut tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>) -> Vec<String> {
    let mut ids = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        if let DistributedMessage::TaskAssignment { binary_info, .. } = msg {
            ids.push(binary_info.task_id);
        }
    }
    ids
}

/// Build a single-secondary primary whose CRDT holds ONE ready zero-dep
/// task hydrated into the pool, but with an EMPTY worker roster (no
/// `SecondaryCapacity` applied yet — the relocated-primary state at the
/// instant `perform_initial_assignment` ran and found assigned=0).
///
/// The coordinator's PRODUCTION worker-management bus (installed at
/// construction; receiver on `self.worker_mgmt_rx`) is left intact, so
/// `drain_and_react_to_pending_worker_signals` — the in-wait drain — works
/// end-to-end. A test that wants to OBSERVE the emit installs its own
/// sender (replacing the internal one) AFTER calling this.
#[allow(clippy::type_complexity)]
fn primary_one_task_no_worker() -> (
    TestPrimary,
    Vec<(
        String,
        tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
        tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    )>,
    String,
    PrimaryMeshKeepalive,
) {
    let (transport, ends) = setup_test(1);
    let (mut primary, _mesh) = build_test_primary(
        test_primary_config(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    let task = one_task("t");
    let hash = compute_task_hash(&task);
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PhaseDepsSet {
            deps: HashMap::from([(PhaseId::from("p"), vec![])]),
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: hash.clone(),
            task,
        });
    }
    primary.hydrate_from_cluster_state();
    // The roster is EMPTY: no capacity record applied, so
    // `reconstruct_workers_from_cluster_state` would build 0 slots — the
    // assigned=0 snapshot state.
    assert_eq!(
        primary.alive_worker_count_for_test(),
        0,
        "fixture precondition: empty roster (the assigned=0 race state)"
    );
    (primary, ends, hash, _mesh)
}

/// Replace the coordinator's worker-management bus sender with a fresh
/// one whose receiver the caller keeps, so the reaction's `TasksAdded`
/// emit is observable in-test. Use ONLY in tests that drain the emit
/// directly (a/b) — never in the in-wait-drain test (c), which needs the
/// internal `self.worker_mgmt_rx` pair intact.
fn install_observer_bus(primary: &mut TestPrimary) -> tokio_mpsc::UnboundedReceiver<WorkerMgmtSignal>
{
    let (wm_tx, wm_rx) = tokio_mpsc::unbounded_channel::<WorkerMgmtSignal>();
    primary
        .cluster_state_mut_for_test()
        .install_worker_mgmt_sender(wm_tx);
    wm_rx
}

/// (a) STARTUP via wire. A `SecondaryCapacity` arrives over the mesh
/// AFTER the (assigned=0) initial assignment. The wire-receive apply path
/// (`handle_cluster_mutation`) must (1) grow the roster and (2) emit
/// `TasksAdded`; running the recheck must dispatch the ready task to the
/// freshly-rostered idle worker — no stall.
#[tokio::test(flavor = "current_thread")]
async fn late_capacity_over_wire_grows_roster_and_dispatches_ready_task() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, mut ends, _hash, _mesh) = primary_one_task_no_worker();
            let mut wm_rx = install_observer_bus(&mut primary);

            // The late worker-ready event: sec-0's capacity arrives over
            // the mesh well after the assigned=0 assignment snapshot.
            primary
                .handle_cluster_mutation(capacity_batch("sec-0", 1))
                .await;

            // (1) The roster grew from the now-applied capacity record.
            assert_eq!(
                primary.alive_worker_count_for_test(),
                1,
                "the wire SecondaryCapacity must rebuild the roster with sec-0's idle slot"
            );

            // (2) A `TasksAdded` was emitted onto the bus — the recheck cue.
            let batch = recv_worker_signal_batch(&mut wm_rx)
                .await
                .expect("a roster-growing SecondaryCapacity must emit a batch");
            assert!(
                batch.signals.contains(&WorkerMgmtSignal::TasksAdded),
                "capacity growth must emit TasksAdded; got {:?}",
                batch.signals
            );

            // Running the recheck (what the operational loop / a pre-loop
            // wait does off the bus) dispatches the ready task to the new
            // idle worker — the stall is closed.
            primary.react_to_worker_signal_batch(batch).await;
            settle_pump().await;
            assert_eq!(
                assigned_ids(&mut ends[0].1),
                vec!["t".to_string()],
                "the ready task must dispatch to the freshly-rostered idle worker"
            );
        })
        .await;
}

/// (a) REVERT-CHECK. A redundant re-emit of the SAME capacity (the
/// set-once apply NoOps) must NOT re-grow the roster or re-emit
/// `TasksAdded` — the `Applied`-gated detection is one-shot.
#[tokio::test(flavor = "current_thread")]
async fn redundant_capacity_reemit_is_a_noop() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, _ends, _hash, _mesh) = primary_one_task_no_worker();
            let mut wm_rx = install_observer_bus(&mut primary);

            // First apply: roster grows, one TasksAdded.
            primary
                .handle_cluster_mutation(capacity_batch("sec-0", 1))
                .await;
            assert_eq!(primary.alive_worker_count_for_test(), 1);
            let first = recv_worker_signal_batch(&mut wm_rx)
                .await
                .expect("first apply emits a batch");
            assert!(first.signals.contains(&WorkerMgmtSignal::TasksAdded));

            // Redundant re-emit: set-once apply ⇒ NoOp ⇒ no growth, no
            // emit. The bus is empty; a non-blocking drain yields None.
            primary
                .handle_cluster_mutation(capacity_batch("sec-0", 1))
                .await;
            assert_eq!(
                primary.alive_worker_count_for_test(),
                1,
                "a NoOp re-emit must not re-grow the roster"
            );
            assert!(
                crate::worker_signal::try_collect_worker_signal_batch(&mut wm_rx).is_none(),
                "a NoOp re-emit must not re-emit TasksAdded"
            );
        })
        .await;
}

/// (b) MID-RUN via local origination. A worker becomes ready through the
/// originator path (`apply_and_broadcast_cluster_mutations` — the channel
/// `handle_welcome` uses). The roster must grow + a `TasksAdded` must be
/// emitted so the recheck dispatches. Models the type-shift-respawned
/// memmap worker becoming ready after its phase's assignment.
#[tokio::test(flavor = "current_thread")]
async fn locally_originated_capacity_growth_grows_roster_and_dispatches() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, mut ends, _hash, _mesh) = primary_one_task_no_worker();
            let mut wm_rx = install_observer_bus(&mut primary);

            // Originate the capacity locally (the `handle_welcome` path).
            primary
                .apply_and_broadcast_cluster_mutations(vec![
                    ClusterMutation::PeerJoined {
                        peer_id: "sec-0".into(),
                        is_observer: false,
                        can_be_primary: true,
                        cap_version: Default::default(),
                    },
                    ClusterMutation::SecondaryCapacity {
                        secondary: "sec-0".into(),
                        worker_count: 1,
                        resources: mem(8 * 1024 * 1024 * 1024),
                    },
                ])
                .await;

            assert_eq!(
                primary.alive_worker_count_for_test(),
                1,
                "local origination of a new capacity must rebuild the roster"
            );
            let batch = recv_worker_signal_batch(&mut wm_rx)
                .await
                .expect("local capacity growth must emit a batch");
            assert!(
                batch.signals.contains(&WorkerMgmtSignal::TasksAdded),
                "local capacity growth must emit TasksAdded; got {:?}",
                batch.signals
            );
            primary.react_to_worker_signal_batch(batch).await;
            settle_pump().await;
            // The capacity-batch broadcast itself rides the same wire
            // (sec-0's egress); filter to TaskAssignment only.
            assert_eq!(
                assigned_ids(&mut ends[0].1),
                vec!["t".to_string()],
                "the recheck must dispatch the ready task to the new worker"
            );
        })
        .await;
}

/// (c) WAIT SELF-RECOVERY. The pre-loop bus-drain that
/// `wait_for_mesh_ready` runs inline after every `dispatch_message`
/// (`drain_and_react_to_pending_worker_signals`) must SERVICE a queued
/// `TasksAdded` — dispatching the ready work so a secondary whose
/// capacity landed during the wait gets a `TaskAssignment`, goes
/// operational, and can finally emit the `MeshReady` the wait blocks on.
/// The capacity event is NOT dropped by the wait.
#[tokio::test(flavor = "current_thread")]
async fn wait_inline_drain_services_queued_capacity_signal() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Keep the coordinator's PRODUCTION worker-mgmt bus intact (no
            // observer install) so the in-wait drain
            // (`drain_and_react_to_pending_worker_signals`, which reads
            // `self.worker_mgmt_rx`) services the real queued signal.
            let (mut primary, mut ends, _hash, _mesh) = primary_one_task_no_worker();

            // A capacity lands (e.g. a SecondaryWelcome handled during the
            // wait): the apply grows the roster and queues `TasksAdded` on
            // the bus — but the operational loop, the usual drain, has not
            // started. This is the in-wait state.
            primary
                .handle_cluster_mutation(capacity_batch("sec-0", 1))
                .await;
            assert_eq!(primary.alive_worker_count_for_test(), 1);
            settle_pump().await;
            // Nothing dispatched yet: the recheck has not been run (the
            // signal is parked on the bus).
            assert!(
                assigned_ids(&mut ends[0].1).is_empty(),
                "precondition: no dispatch before the inline drain runs the recheck"
            );

            // The inline drain the wait performs services the queued
            // signal and dispatches — self-recovery.
            let reacted = primary.drain_and_react_to_pending_worker_signals().await;
            assert!(reacted, "the queued TasksAdded must be drained + reacted to");
            settle_pump().await;
            assert_eq!(
                assigned_ids(&mut ends[0].1),
                vec!["t".to_string()],
                "the in-wait inline drain must dispatch the ready task (self-recovery)"
            );
        })
        .await;
}

/// (c) REVERT-CHECK. WITHOUT the inline drain, the queued `TasksAdded`
/// sits unserviced: the freshly-rostered worker stays idle and the ready
/// task is NOT dispatched — proving the drain is load-bearing (the wait
/// would otherwise deadlock on a `MeshReady` that never comes).
#[tokio::test(flavor = "current_thread")]
async fn without_inline_drain_queued_capacity_signal_strands() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, mut ends, _hash, _mesh) = primary_one_task_no_worker();

            primary
                .handle_cluster_mutation(capacity_batch("sec-0", 1))
                .await;
            assert_eq!(primary.alive_worker_count_for_test(), 1);
            settle_pump().await;

            // No inline drain is run. The signal stays parked; nothing
            // re-runs the dispatch recheck, so the worker sits idle.
            assert!(
                assigned_ids(&mut ends[0].1).is_empty(),
                "without the inline drain the ready task strands at the idle worker"
            );
        })
        .await;
}
