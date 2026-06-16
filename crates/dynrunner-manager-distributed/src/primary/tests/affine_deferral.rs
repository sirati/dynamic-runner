//! #497 affine-deferral PRIMARY-HANDLER round trip — the never-wired-handler
//! loop the fix closes.
//!
//! Production sequence replayed:
//!   1. A work task `B` is assigned to an affine secondary the normal way:
//!      CRDT `InFlight` + tracked in `self.in_flight` + its worker slot
//!      `Assigned`.
//!   2. The secondary finds `B` depends on a SecondaryAffine gate that is
//!      `AffineReady` but not yet locally imported, so it DEFERS `B` (parks
//!      it in `affine_running`, NOT `active_tasks`) and reports
//!      `TaskQueuedAfterLocalDependency`.
//!   3. THE FIX: the primary's handler originates `QueuedAfterLocalDependencySet`
//!      (CRDT `InFlight → QueuedAfterLocalDependency`) AND removes `B` from
//!      `self.in_flight` — so the reconciliation probe, whose view is built
//!      SOLELY from `self.in_flight`, never sees `B` and cannot loop on it.
//!   4. The import completes; the secondary self-dispatches `B` and reports
//!      `LocalDependencyReleased`. The primary re-originates the EXISTING
//!      `TaskAssigned` (`→ InFlight`) and re-enters `B` into `self.in_flight`.
//!
//! REVERT-CHECK: on trunk (no handler) the reports hit the `dispatch_message`
//! catch-all, so `B` stays `InFlight` AND in `self.in_flight` forever — the
//! probe poll below would still surface `B` as probeable. The post-defer
//! `poll` assertion (no probe fires for `B`) is the load-bearing pin.

use super::*;

use crate::primary::reconciliation_probe::ReconciliationProber;
use crate::primary::wire::compute_task_hash;
use dynrunner_core::{ErrorType, PhaseId, ResourceAmount, ResourceKind, TypeId};
use std::time::{Duration, Instant};

/// One advertised-memory resource amount (bytes), the live welcome shape.
fn mem(bytes: u64) -> Vec<ResourceAmount> {
    vec![ResourceAmount {
        kind: ResourceKind::memory(),
        amount: bytes,
    }]
}

/// The member's `MeshReady` confirmation — without it the proactive dispatch
/// recheck withholds (the mesh-confirmation gate).
fn mesh_ready_from(secondary_id: &str) -> DistributedMessage<TestId> {
    DistributedMessage::MeshReady {
        target: None,
        sender_id: secondary_id.into(),
        timestamp: 0.0,
        secondary_id: secondary_id.into(),
        peer_count: 1,
    }
}

/// Build a 1-secondary, 1-worker primary, hydrate it, and LIVE-DISPATCH one
/// work task `B` onto sec-0/worker-0 the real way — so the phase in-flight
/// counter, the worker slot (`Assigned`), the in-flight ledger entry, AND the
/// per-type concurrency slot are all populated exactly as production. The
/// task's `default` type is given a concurrency cap so its type slot is
/// reserved (uncapped types are not tracked), letting the terminal test assert
/// the release is symmetric. Returns the primary, the mesh keepalive, and B's
/// hash.
async fn primary_with_live_dispatched_dependent() -> (
    PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    PrimaryMeshKeepalive,
    String,
) {
    let (transport, _ends) = setup_test(1);
    let config = PrimaryConfig {
        max_concurrent_per_type: HashMap::from([(TypeId::from("default"), 4)]),
        ..test_primary_config()
    };
    let (mut primary, mesh) = build_test_primary(
        config,
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    let task = make_binary("affine-dependent-B", 100);
    let hash = compute_task_hash(&task);
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PhaseDepsSet {
            deps: HashMap::from([(PhaseId::from("default"), vec![])]),
        });
        cs.apply(ClusterMutation::PeerJoined {
            peer_id: "sec-0".into(),
            is_observer: false,
            can_be_primary: true,
            cap_version: Default::default(),
            member_gen: 0,
        });
        cs.apply(ClusterMutation::SecondaryCapacity {
            secondary: "sec-0".into(),
            worker_count: 1,
            resources: mem(8 * 1024 * 1024 * 1024),
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: hash.clone(),
            task,
        });
    }
    primary
        .hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");
    primary.handle_mesh_ready(mesh_ready_from("sec-0"));
    primary
        .dispatch_to_idle_workers(true)
        .await
        .expect("dispatch recheck");
    settle_pump().await;
    assert!(
        matches!(
            primary.cluster_state_for_test().task_state(&hash),
            Some(crate::cluster_state::TaskState::InFlight { .. })
        ),
        "fixture precondition: B is dispatched InFlight"
    );
    assert_eq!(
        primary.pool().in_flight(&PhaseId::from("default")),
        1,
        "fixture precondition: phase in-flight counter is 1"
    );
    assert_eq!(
        primary.in_flight_per_type_for_test(&TypeId::from("default")),
        1,
        "fixture precondition: the type slot is reserved"
    );
    assert_eq!(
        primary.active_workers_for_test(),
        1,
        "fixture precondition: the worker slot is busy (Assigned)"
    );
    (primary, mesh, hash)
}

/// An affine-import failure terminal as the secondary emits it via
/// `report_deferred_task_failed` (`secondary/affine_exec.rs` →
/// `secondary/resource.rs`). `error_message` is the caller's choice: the
/// stable #509 gate-absent re-route marker (RE-ROUTE, no budget burn) vs a
/// genuine gate-body failure reason (CHARGED).
fn deferred_failure(
    secondary: &str,
    worker_id: u32,
    hash: &str,
    error_message: &str,
) -> DistributedMessage<TestId> {
    DistributedMessage::TaskFailed {
        target: None,
        sender_id: secondary.into(),
        timestamp: 0.0,
        secondary_id: secondary.into(),
        worker_id,
        task_hash: hash.into(),
        error_type: ErrorType::Recoverable,
        error_message: error_message.into(),
        delivery_seq: None,
        msgs_posted_through: None,
    }
}

/// Build a 1-secondary primary and seat work task `B` as `InFlight` on
/// sec-0's worker exactly as a live dispatch would: the worker slot
/// `Assigned`, the CRDT `InFlight` fact, and the `self.in_flight` ledger
/// entry. Returns the primary, the mesh keepalive, and `B`'s hash.
async fn primary_with_inflight_dependent() -> (
    PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    PrimaryMeshKeepalive,
    String,
) {
    let (transport, _ends) = setup_test(1);
    let (mut primary, mesh) = build_test_primary(
        test_primary_config(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    let task = make_binary("affine-dependent-B", 100);
    let hash = compute_task_hash(&task);
    // Seed the CRDT entry (TaskAdded → InFlight) the live dispatch wrote.
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::TaskAdded {
            hash: hash.clone(),
            task: task.clone(),
        });
    }
    // Seat B on sec-0's worker (slot Assigned + ledger entry + type slot),
    // then mirror the CRDT InFlight fact the originate would have written.
    let staged = primary.stage_in_flight_for_test("sec-0".into(), 0, task.clone());
    assert_eq!(staged, hash);
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::TaskAssigned {
            hash: hash.clone(),
            secondary: "sec-0".into(),
            worker: 0,
            version: Default::default(),
            attempt: 0,
        });
    }
    assert!(
        primary.in_flight_for_test().contains_key(&hash),
        "fixture precondition: B is tracked InFlight in the ledger"
    );
    (primary, mesh, hash)
}

fn queued_report(secondary: &str, task_hash: &str) -> DistributedMessage<TestId> {
    DistributedMessage::TaskQueuedAfterLocalDependency {
        target: None,
        sender_id: secondary.into(),
        timestamp: 0.0,
        secondary_id: secondary.into(),
        task_hash: task_hash.into(),
        affine_hash: "affine-gate-I".into(),
    }
}

fn released_report(secondary: &str, task_hash: &str, worker_id: u32) -> DistributedMessage<TestId> {
    DistributedMessage::LocalDependencyReleased {
        target: None,
        sender_id: secondary.into(),
        timestamp: 0.0,
        secondary_id: secondary.into(),
        task_hash: task_hash.into(),
        worker_id,
    }
}

/// Count the `QueuedAfterLocalDependencySet` mutations carried by the
/// `ClusterMutation` frames drained from a secondary end, and the number of
/// distinct `ClusterMutation` FRAMES that carried at least one. Returns
/// `(frames, mutations)`. Mesh keepalives / digests are a different
/// `DistributedMessage` variant, so they are ignored.
fn count_queued_set_broadcasts(
    rx: &mut tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) -> (usize, usize) {
    let mut frames = 0;
    let mut mutations = 0;
    while let Ok(msg) = rx.try_recv() {
        if let DistributedMessage::ClusterMutation { mutations: ms, .. } = msg {
            let n = ms
                .iter()
                .filter(|m| {
                    matches!(m, ClusterMutation::QueuedAfterLocalDependencySet { .. })
                })
                .count();
            if n > 0 {
                frames += 1;
                mutations += n;
            }
        }
    }
    (frames, mutations)
}

/// COALESCING (Commit 3): a contiguous run of N `TaskQueuedAfterLocalDependency`
/// reports dispatched through `dispatch_inbox_batch_coalescing_deferrals`
/// produces exactly ONE `ClusterMutation` broadcast carrying all N rank-drops
/// — not N broadcasts. The build_compilers affine burst (S × M reports) is
/// thereby O(1) broadcasts per inbox drain instead of O(N), so it no longer
/// floods ingest and trips the self-starvation false-election.
#[tokio::test(flavor = "current_thread")]
async fn deferral_burst_coalesces_into_one_broadcast() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, mut ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Seat THREE distinct work tasks as InFlight on sec-0 (the
            // build_compilers burst shape — many dependents parked behind one
            // import), exactly as a live dispatch would.
            let mut hashes = Vec::new();
            for (slot, name) in ["dep-A", "dep-B", "dep-C"].iter().enumerate() {
                let task = make_binary(name, 100);
                let hash = compute_task_hash(&task);
                {
                    let cs = primary.cluster_state_mut_for_test();
                    cs.apply(ClusterMutation::TaskAdded {
                        hash: hash.clone(),
                        task: task.clone(),
                    });
                }
                let staged =
                    primary.stage_in_flight_for_test("sec-0".into(), slot as u32, task.clone());
                assert_eq!(staged, hash);
                {
                    let cs = primary.cluster_state_mut_for_test();
                    cs.apply(ClusterMutation::TaskAssigned {
                        hash: hash.clone(),
                        secondary: "sec-0".into(),
                        worker: slot as u32,
                        version: Default::default(),
                        attempt: 0,
                    });
                }
                hashes.push(hash);
            }

            // Drain any setup/keepalive frames already queued so the count
            // below reflects only the deferral burst's broadcasts.
            settle_pump().await;
            let (_, rx, _) = &mut ends[0];
            while rx.try_recv().is_ok() {}

            // The burst: three TaskQueuedAfterLocalDependency reports in one
            // inbox-drain batch, dispatched through the coalescing path.
            let batch: Vec<DistributedMessage<TestId>> = hashes
                .iter()
                .map(|h| queued_report("sec-0", h))
                .collect();
            primary
                .dispatch_inbox_batch_coalescing_deferrals(batch, &mut None)
                .await
                .expect("coalescing dispatch succeeds");

            // All three parked dependents STAY in the ledger flagged deferred
            // (the per-report local effect is unchanged by coalescing): the
            // slot↔ledger symmetry the fix restores, so the probe excludes
            // them while the terminal/recovery paths still resolve them.
            for h in &hashes {
                let entry = primary
                    .in_flight_for_test()
                    .get(h)
                    .expect("each parked dependent must stay in the in-flight ledger");
                assert!(
                    entry.deferred,
                    "each parked dependent must be marked deferred"
                );
            }

            settle_pump().await;
            let (_, rx, _) = &mut ends[0];
            let (frames, mutations) = count_queued_set_broadcasts(rx);
            assert_eq!(
                frames, 1,
                "the deferral burst must coalesce into exactly ONE ClusterMutation \
                 broadcast frame (got {frames}); O(1) per inbox drain, not O(N)"
            );
            assert_eq!(
                mutations, 3,
                "the single coalesced frame must carry all three rank-drops (got \
                 {mutations})"
            );
        })
        .await;
}

/// Build the reconciliation-probe view EXACTLY as
/// [`PrimaryCoordinator::reconciliation_probe_tick`] does — every NON-deferred
/// in-flight entry as `(hash, holder)`. The deferred-filter is THE behaviour
/// the probe-skip test pins, so the test mirrors the production view-build
/// instead of reaching for a private helper.
fn probe_view(
    primary: &PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
) -> Vec<(String, String)> {
    primary
        .in_flight_for_test()
        .iter()
        .filter(|(_, e)| !e.deferred)
        .map(|(h, e)| (h.clone(), e.secondary_id.clone()))
        .collect()
}

/// THE round trip + the SYMMETRY the fix restores: defer marks B deferred
/// (CRDT QueuedAfterLocalDependency, but B STAYS in the in-flight ledger so
/// terminal/recovery still resolve it by hash) while the reconciliation probe
/// EXCLUDES it (no loop); release un-defers it (CRDT InFlight + flag cleared,
/// the same slot it never left).
///
/// REVERT-CHECK vs trunk: trunk REMOVED B from `self.in_flight` at defer, so
/// `in_flight_for_test().contains_key(&hash)` was `false`. This test asserts
/// the OPPOSITE — B present-but-deferred — so it FAILS on trunk and PASSES
/// with the fix. The probe-skip (no probe fires for the deferred B) is the
/// no-loop protection trunk's remove was the only guard for.
#[tokio::test(flavor = "current_thread")]
async fn defer_keeps_in_flight_deferred_then_release_un_defers() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, _mesh, hash) = primary_with_inflight_dependent().await;

            // (defer) The secondary's TaskQueuedAfterLocalDependency report.
            primary
                .handle_task_queued_after_local_dependency(queued_report("sec-0", &hash))
                .await;

            // CRDT: B is now QueuedAfterLocalDependency on sec-0.
            match primary.cluster_state_for_test().task_state(&hash) {
                Some(crate::cluster_state::TaskState::QueuedAfterLocalDependency {
                    secondary,
                    ..
                }) => assert_eq!(secondary, "sec-0"),
                other => panic!("expected QueuedAfterLocalDependency, got {other:?}"),
            }
            // Ledger SYMMETRY (FAILS on trunk, which removed B): B STAYS in the
            // ledger, flagged deferred, against the slot it never left — so the
            // terminal + dead-holder-recovery paths still resolve it by hash.
            let entry = primary
                .in_flight_for_test()
                .get(&hash)
                .expect("the defer handler must KEEP B in the in-flight ledger \
                         (slot↔ledger symmetry) — trunk removed it");
            assert!(
                entry.deferred,
                "the kept entry must be marked deferred so the probe excludes it"
            );
            assert_eq!(entry.local_worker_id, Some(0), "slot identity preserved");

            // Probe-skip (the original no-loop protection): the probe view
            // EXCLUDES the deferred B, so a poll past the deadline fires NO
            // probe for it (on trunk the remove protected this; the flag now
            // does, without blinding the terminal paths).
            let view = probe_view(&primary);
            let view_refs: Vec<(&str, &str)> =
                view.iter().map(|(h, s)| (h.as_str(), s.as_str())).collect();
            assert!(
                view_refs.is_empty(),
                "the deferred dependent must be excluded from the probe view"
            );
            let mut prober = ReconciliationProber::new(
                Duration::from_secs(600),
                Duration::from_secs(15),
                Duration::from_secs(3600),
            );
            let t0 = Instant::now();
            let _ = prober.poll(t0, &view_refs);
            let tick = prober.poll(t0 + Duration::from_secs(601), &view_refs);
            assert!(
                tick.probes.is_empty(),
                "no reconciliation probe may fire for a parked dependent"
            );

            // (release) The secondary's LocalDependencyReleased report.
            primary
                .handle_local_dependency_released(released_report("sec-0", &hash, 0))
                .await;

            // CRDT: B is InFlight again on sec-0/worker 0.
            match primary.cluster_state_for_test().task_state(&hash) {
                Some(crate::cluster_state::TaskState::InFlight {
                    secondary, worker, ..
                }) => {
                    assert_eq!(secondary, "sec-0");
                    assert_eq!(*worker, 0);
                }
                other => panic!("expected InFlight after release, got {other:?}"),
            }
            // Ledger: B is un-deferred (probe + death seam cover it again),
            // still against the same slot.
            let entry = primary
                .in_flight_for_test()
                .get(&hash)
                .expect("B must remain in the in-flight ledger after release");
            assert_eq!(entry.secondary_id, "sec-0");
            assert_eq!(entry.local_worker_id, Some(0));
            assert!(
                !entry.deferred,
                "release must clear the deferred flag so the probe sees B again"
            );
            assert!(
                !probe_view(&primary).is_empty(),
                "the un-deferred B is back in the probe view"
            );
        })
        .await;
}

/// SYMMETRY #1 — affine-import FAILURE terminal after a defer. On trunk B
/// was removed from `self.in_flight` at defer, so `free_slot_on_terminal`
/// returned `None`: the worker slot stayed phantom-busy, the type slot was
/// never released, and `note_item_failed` was skipped (phase counter
/// desynced) — yet `failed_tasks` + the CRDT terminal still fired. With the
/// fix B stays in the ledger (deferred), so the terminal flows through the
/// NORMAL path: slot → Idle, type slot released, phase counter decremented.
/// For the #509 gate-absent marker the failure RE-ROUTES (requeue, NO
/// retry-budget burn), per #495.
#[tokio::test(flavor = "current_thread")]
async fn defer_then_gate_absent_failure_frees_slot_and_re_routes() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, _mesh, hash) = primary_with_live_dispatched_dependent().await;

            // (defer) park B behind the local import.
            primary
                .handle_task_queued_after_local_dependency(queued_report("sec-0", &hash))
                .await;
            assert!(
                primary.in_flight_for_test().get(&hash).unwrap().deferred,
                "B is deferred but STILL in the ledger"
            );

            // (terminal) the #509 gate-absent re-route failure for the
            // deferred dependent.
            primary
                .handle_task_failed(
                    deferred_failure(
                        "sec-0",
                        0,
                        &hash,
                        crate::secondary::affine_exec::AFFINE_GATE_ABSENT_WIRE_MESSAGE,
                    ),
                    &mut None,
                )
                .await;

            // SLOT freed (FAILS on trunk — phantom-busy leak): the worker
            // returns to Idle.
            assert_eq!(
                primary.active_workers_for_test(),
                0,
                "the affine-import failure must free B's worker slot back to \
                 Idle (trunk left it phantom-busy because B was absent from \
                 the ledger)"
            );
            // TYPE SLOT released (FAILS on trunk — never released).
            assert_eq!(
                primary.in_flight_per_type_for_test(&TypeId::from("default")),
                0,
                "the type slot must be released symmetric with dispatch"
            );
            // LEDGER entry gone.
            assert!(
                !primary.in_flight_for_test().contains_key(&hash),
                "the terminal must remove B from the in-flight ledger"
            );
            // PHASE COUNTER decremented (FAILS on trunk — note_item_failed /
            // the requeue decrement was skipped). The #509 RE-ROUTE requeues
            // B, and `pool.requeue` decrements the phase in-flight counter.
            assert_eq!(
                primary.pool().in_flight(&PhaseId::from("default")),
                0,
                "the phase in-flight counter must be decremented (trunk desynced \
                 it because the slot-free returned None)"
            );
            // RE-ROUTED, not permanently failed (the #495 intent): B is back
            // in the pool with NO retry-budget burn.
            assert_eq!(
                primary.pool().iter().count(),
                1,
                "the gate-absent failure must RE-ROUTE B into the pool"
            );
            assert!(
                !primary.failed_tasks.contains_key(&hash),
                "the #509 gate-absent re-route must NOT consume retry budget \
                 (not a terminal failure)"
            );
            // The replicated ledger mirrors the requeue (InFlight → Pending).
            assert!(
                matches!(
                    primary.cluster_state_for_test().task_state(&hash),
                    Some(crate::cluster_state::TaskState::Pending { .. })
                ),
                "the re-route originates TaskRequeued (Pending), got {:?}",
                primary.cluster_state_for_test().task_state(&hash)
            );
        })
        .await;
}

/// SYMMETRY #2 — the holding secondary DIES (no failover) after a defer. On
/// trunk B was absent from `self.in_flight`, so
/// `recover_inflight_for_dead_secondary` (which iterates the ledger by
/// secondary) never saw it: B was STRANDED in QueuedAfterLocalDependency. With
/// the fix B stays in the ledger, so the dead-secondary recovery requeues it.
#[tokio::test(flavor = "current_thread")]
async fn defer_then_dead_secondary_requeues_the_deferred_dependent() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, _mesh, hash) = primary_with_live_dispatched_dependent().await;

            // (defer) park B behind the local import.
            primary
                .handle_task_queued_after_local_dependency(queued_report("sec-0", &hash))
                .await;
            assert!(
                primary.in_flight_for_test().get(&hash).unwrap().deferred,
                "B is deferred but STILL in the ledger"
            );

            // sec-0 dies: the dead-secondary recovery iterates the ledger by
            // secondary and requeues everything it held.
            let mutations = primary.recover_inflight_for_dead_secondary("sec-0");

            // RECOVERED (FAILS on trunk — B was absent, so stranded): B is
            // requeued into the pool and a `TaskRequeued` is returned for the
            // replicated InFlight → Pending.
            assert!(
                !primary.in_flight_for_test().contains_key(&hash),
                "dead-secondary recovery must remove B from the ledger"
            );
            assert_eq!(
                primary.pool().iter().count(),
                1,
                "the deferred dependent must be requeued (trunk stranded it in \
                 QueuedAfterLocalDependency)"
            );
            assert!(
                mutations.iter().any(|m| matches!(
                    m,
                    ClusterMutation::TaskRequeued { hash: h, .. } if *h == hash
                )),
                "recovery must originate TaskRequeued for the deferred dependent"
            );
            // Type slot released + phase counter decremented (symmetry).
            assert_eq!(
                primary.in_flight_per_type_for_test(&TypeId::from("default")),
                0,
                "the type slot must be released on recovery"
            );
            assert_eq!(
                primary.pool().in_flight(&PhaseId::from("default")),
                0,
                "the phase in-flight counter must be decremented on recovery"
            );
        })
        .await;
}

/// SYMMETRY #4 (happy path unchanged) — defer → release → genuine terminal
/// frees the slot EXACTLY ONCE (no double-free, no type-slot underflow). The
/// externally-observable defer→release→terminal contract is identical to
/// pre-fix when imports succeed.
#[tokio::test(flavor = "current_thread")]
async fn defer_release_then_complete_frees_slot_exactly_once() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, _mesh, hash) = primary_with_live_dispatched_dependent().await;

            // defer → release: B is un-deferred, tracked InFlight again.
            primary
                .handle_task_queued_after_local_dependency(queued_report("sec-0", &hash))
                .await;
            primary
                .handle_local_dependency_released(released_report("sec-0", &hash, 0))
                .await;
            assert!(
                !primary.in_flight_for_test().get(&hash).unwrap().deferred,
                "release un-deferred B"
            );
            // Still exactly one of everything (no double-count from re-entry).
            assert_eq!(
                primary.in_flight_per_type_for_test(&TypeId::from("default")),
                1,
                "release must NOT re-reserve the type slot (it never left)"
            );
            assert_eq!(primary.active_workers_for_test(), 1, "slot still Assigned");
            assert_eq!(primary.pool().in_flight(&PhaseId::from("default")), 1);

            // Genuine terminal: TaskComplete frees the slot exactly once.
            let complete = DistributedMessage::TaskComplete {
                target: None,
                sender_id: "sec-0".into(),
                timestamp: 0.0,
                secondary_id: "sec-0".into(),
                worker_id: 0,
                task_hash: hash.clone(),
                result_data: None,
                delivery_seq: None,
                msgs_posted_through: None,
            };
            primary.handle_task_complete(complete, &mut None).await;

            assert_eq!(
                primary.active_workers_for_test(),
                0,
                "the terminal frees the slot back to Idle"
            );
            assert_eq!(
                primary.in_flight_per_type_for_test(&TypeId::from("default")),
                0,
                "the terminal releases the type slot exactly once (no underflow)"
            );
            assert!(
                !primary.in_flight_for_test().contains_key(&hash),
                "the terminal removes the ledger entry"
            );
            assert_eq!(
                primary.pool().in_flight(&PhaseId::from("default")),
                0,
                "the phase in-flight counter is decremented exactly once"
            );

            // A duplicate/stale terminal is a safe no-op (no double-free,
            // no type-slot underflow below zero).
            let dup = DistributedMessage::TaskComplete {
                target: None,
                sender_id: "sec-0".into(),
                timestamp: 0.0,
                secondary_id: "sec-0".into(),
                worker_id: 0,
                task_hash: hash.clone(),
                result_data: None,
                delivery_seq: None,
                msgs_posted_through: None,
            };
            primary.handle_task_complete(dup, &mut None).await;
            assert_eq!(
                primary.in_flight_per_type_for_test(&TypeId::from("default")),
                0,
                "a duplicate terminal must not underflow the type slot"
            );
            assert_eq!(primary.active_workers_for_test(), 0);
        })
        .await;
}
