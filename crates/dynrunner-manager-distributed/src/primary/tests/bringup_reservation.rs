//! #494 bring-up preliminary-reservation regression (asm-tokenizer
//! run_20260610_121427 family): cold/staggered bring-up, 28 tasks across
//! 15 secondaries. The #382 mesh-veto admits only the first-confirmed
//! members; the greedy idle-worker recheck then pops from the GLOBAL pool
//! with no per-member cap, so the two early confirmers drain all 28 tasks
//! (the captured 14/14/0×13 pack) before the other 13 confirm 70-90s
//! later — by which point the pool is empty and there is no reclaim of an
//! already-sent assignment.
//!
//! THE FIX: a POOL-SIDE reservation partitions the initial pool across
//! the FULL EXPECTED member set via the projected-load interleave
//! (`seed_bringup_reservation`). `dispatch_view_for_worker` scopes each
//! member's view to its reserved share, so a first-confirmed member
//! drains only its ~2 reserved tasks and the late confirmers' shares stay
//! HELD until they arrive. The veto in `should_skip_worker_for_dispatch`
//! is untouched — nothing is ever sent to an unconfirmed member; the
//! reservation only caps what a CONFIRMED member may pull.
//!
//! Deterministic direct-handler tests (the `mesh_readiness_gate` shape):
//! register the roster + reservation, confirm members in stages, run the
//! `TasksAdded` recheck, and assert each member's wire receives only its
//! share.

use super::*;

use dynrunner_core::{PhaseId, ResourceAmount, ResourceKind, ResourceMap, TypeId};

use crate::state::{SecondaryConnection, SecondaryConnectionState};
use crate::worker_signal::{WorkerMgmtSignal, recv_worker_signal_batch};

type TestPrimary = PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>;

/// A single zero-dep task in phase "p", type "default", with a stable
/// task_id == its name.
fn one_task(name: &str) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 100);
    t.phase_id = PhaseId::from("p");
    t.type_id = TypeId::from("default");
    t.task_id = name.into();
    t
}

/// Drain every `TaskAssignment` `task_id` queued on a secondary's wire.
fn assigned_ids(rx: &mut tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>) -> Vec<String> {
    let mut ids = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        if let DistributedMessage::TaskAssignment { binary_info, .. } = msg {
            ids.push(binary_info.task_id);
        }
    }
    ids
}

/// Run one `TasksAdded` recheck (the operational-loop worker-management
/// arm's body) so `dispatch_to_idle_workers` re-evaluates every free
/// worker against the live pool (and reservation overlay).
async fn run_dispatch_recheck(primary: &mut TestPrimary) {
    let (wm_tx, mut wm_rx) = tokio_mpsc::unbounded_channel::<WorkerMgmtSignal>();
    primary
        .cluster_state_mut_for_test()
        .install_worker_mgmt_sender(wm_tx);
    primary
        .cluster_state_mut_for_test()
        .emit_worker_mgmt(WorkerMgmtSignal::TasksAdded);
    let batch = recv_worker_signal_batch(&mut wm_rx)
        .await
        .expect("emit must produce a batch");
    primary.react_to_worker_signal_batch(batch, &mut None).await;
}

/// Seed `n_tasks` zero-dep tasks into the pool through the real hydrate
/// path, then register `n_members` × `workers_each` idle workers in the
/// round-robin shape `reconstruct_workers_from_cluster_state` produces
/// (round-major: round 0 = every member's worker-0, round 1 = worker-1,
/// …). Every member starts mesh-CONFIRMED (`register_idle_worker_for_test`
/// marks it so); the caller unconfirms the late ones.
///
/// Returns the seeded task names in pool order (their `task_id`s).
fn seed_roster_and_tasks(
    primary: &mut TestPrimary,
    n_members: usize,
    workers_each: u32,
    n_tasks: usize,
) -> Vec<String> {
    let names: Vec<String> = (0..n_tasks).map(|i| format!("t{i}")).collect();
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PhaseDepsSet {
            deps: HashMap::from([(PhaseId::from("p"), vec![])]),
        });
        for name in &names {
            let task = one_task(name);
            cs.apply(ClusterMutation::TaskAdded {
                hash: crate::primary::wire::compute_task_hash(&task),
                task,
            });
        }
    }
    primary
        .hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");

    // Round-major worker registration so the global ids + per-secondary
    // ordering match `reconstruct_workers_from_cluster_state`, the shape
    // `dispatch_order` interleaves over.
    let budget = ResourceMap::from([(ResourceKind::memory(), 8 * 1024 * 1024 * 1024u64)]);
    let mut global_id = 0u32;
    for _round in 0..workers_each {
        for m in 0..n_members {
            primary.register_idle_worker_for_test(format!("sec-{m}"), global_id, budget.clone());
            global_id += 1;
        }
    }

    // COLD-BRING-UP connection typestate: each member parked in
    // `InitialAssigning` (provably awaiting the initial batch) — the
    // `is_cold_bringup` fact `seed_bringup_reservation` gates on. Without
    // this, the reservation (correctly) treats the roster as a
    // failover-promoted survivor set and stays closed.
    for m in 0..n_members {
        let id = format!("sec-{m}");
        let conn = SecondaryConnection::new(id.clone())
            .receive_welcome(
                workers_each,
                vec![ResourceAmount {
                    kind: ResourceKind::memory(),
                    amount: 8 * 1024 * 1024 * 1024,
                }],
                "host".into(),
                0,
                None,
                false,
                false,
            )
            .receive_cert_exchange(String::new(), None, None, 0, None)
            .begin_peer_discovery()
            .peers_ready();
        primary
            .secondaries
            .insert(id, SecondaryConnectionState::InitialAssigning(conn));
    }
    names
}

/// THE HEADLINE REPRO. 15 expected members × 2 workers, 28-task cold
/// pool. Two members (sec-0, sec-1) confirm early; the other 13 confirm
/// late. With the reservation each confirming member dispatches ONLY its
/// reserved share — never the 14/14/0×13 pack — and the late 13 each
/// receive their share at their own confirmation edge. Final spread is
/// even (1-2 per member), nothing stranded.
#[tokio::test(flavor = "current_thread")]
async fn staggered_confirms_dispatch_only_each_members_reserved_share() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let n_members = 15usize;
            let (transport, mut ends) = setup_test(n_members as u32);
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            seed_roster_and_tasks(&mut primary, n_members, 2, 28);

            // EARLY confirmers: sec-0, sec-1. LATE: sec-2..sec-14 (drop
            // them from the confirmation set — the only fact the veto and
            // the reservation-hold both turn on).
            for m in 2..n_members {
                primary.mark_member_mesh_unconfirmed_for_test(&format!("sec-{m}"));
            }

            // OPEN the reservation over the full 15-member roster, BEFORE
            // any dispatch — the pre-loop pipeline order.
            primary.seed_bringup_reservation();
            assert!(
                primary.pool().reservation_active(),
                "the 28-task cold pool over 15 members must open a window"
            );

            // ── Stage 1: only sec-0, sec-1 confirmed. Recheck. ──
            run_dispatch_recheck(&mut primary).await;
            settle_pump().await;

            let mut per_member: Vec<usize> = (0..n_members)
                .map(|m| assigned_ids(&mut ends[m].1).len())
                .collect();

            // The two early confirmers drained ONLY their reserved share
            // (≤ ceil(28/15)=2 each), NOT 14 each. The 13 unconfirmed got
            // ZERO (the veto withholds the send AND their share is held).
            assert!(
                per_member[0] >= 1 && per_member[0] <= 2,
                "sec-0 must take only its reserved share (1-2), not the \
                 14/14 pack; got {}",
                per_member[0]
            );
            assert!(
                per_member[1] >= 1 && per_member[1] <= 2,
                "sec-1 must take only its reserved share (1-2); got {}",
                per_member[1]
            );
            for (m, got) in per_member.iter().enumerate().take(n_members).skip(2) {
                assert_eq!(
                    *got, 0,
                    "unconfirmed sec-{m} must receive NOTHING (its share is \
                     held, the veto withholds the send); got {got}"
                );
            }
            let early_total: usize = per_member.iter().sum();
            assert!(
                early_total <= 4,
                "the two early confirmers together took only their reserved \
                 shares (≤4), not the whole 28-task pool; per-member: {per_member:?}"
            );

            // ── Stage 2: the late 13 confirm (each its own edge). Recheck
            //    after each so the confirmation-edge wakeup flows its share. ──
            for m in 2..n_members {
                primary.confirm_member_mesh_for_test(&format!("sec-{m}"));
            }
            run_dispatch_recheck(&mut primary).await;
            settle_pump().await;

            for m in 0..n_members {
                per_member[m] += assigned_ids(&mut ends[m].1).len();
            }

            // EVERY member received its reserved share; the full pool
            // dispatched; final spread is even (1-2 each), no member starved.
            let total: usize = per_member.iter().sum();
            assert_eq!(
                total, 28,
                "the whole 28-task pool must dispatch once every member \
                 confirms; per-member: {per_member:?}"
            );
            let max = *per_member.iter().max().unwrap();
            let min = *per_member.iter().min().unwrap();
            assert!(
                min >= 1 && max <= 2,
                "28 tasks over 15 members must spread to 1..=2 each (the \
                 production pack was 14/14/0×13); per-member: {per_member:?}"
            );
            assert_eq!(
                primary.pool().iter().count(),
                0,
                "no task may strand queued — the reserved shares all dispatched"
            );
            assert!(
                !primary.pool().reservation_active(),
                "the window self-closes once every reserved task drained"
            );
        })
        .await;
}

/// REDISTRIBUTE. A reserved member never confirms and is then DECLARED
/// DEAD (the genuine member-removal path). Its reserved share folds onto
/// the surviving fleet — no strand, no permanent idle pool. CASCADE: two
/// members die; both shares fold into the survivors.
#[tokio::test(flavor = "current_thread")]
async fn dead_member_share_redistributes_onto_survivors() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let n_members = 4usize;
            let (transport, mut ends) = setup_test(n_members as u32);
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // 8 tasks / 4 members × 8 workers → reserved 2 each, and the
            // survivors keep ample idle slots so a single post-death
            // recheck drains the whole redistributed share (the cascade
            // round-robin over-assigns to the least-loaded survivor — a
            // member at WORKER capacity holds the overflow for the next
            // free worker, which the wide-survivor shape avoids so the
            // pin is single-recheck-deterministic).
            seed_roster_and_tasks(&mut primary, n_members, 8, 8);

            // sec-2 and sec-3 are the members that never confirm.
            primary.mark_member_mesh_unconfirmed_for_test("sec-2");
            primary.mark_member_mesh_unconfirmed_for_test("sec-3");
            primary.seed_bringup_reservation();
            assert!(primary.pool().reservation_active());

            // Confirmed sec-0, sec-1 drain their own shares first.
            run_dispatch_recheck(&mut primary).await;
            settle_pump().await;
            let confirmed_first: usize =
                assigned_ids(&mut ends[0].1).len() + assigned_ids(&mut ends[1].1).len();
            assert!(
                confirmed_first <= 4,
                "sec-0/sec-1 took only their own reserved shares first (≤4); \
                 got {confirmed_first}"
            );
            // The unconfirmed pair got nothing and their 4 reserved tasks
            // are still held in the pool.
            assert!(assigned_ids(&mut ends[2].1).is_empty());
            assert!(assigned_ids(&mut ends[3].1).is_empty());
            let held = primary.pool().iter().count();
            assert!(
                held >= 1,
                "the unconfirmed members' reserved shares stay HELD in the \
                 pool (no re-clump onto the confirmed members); held={held}"
            );

            // sec-2 dies → declared dead (the genuine member-removal path).
            // Its reserved share must fold onto the survivors.
            primary
                .requeue_dead_secondary_for_test("sec-2")
                .await
                .expect("dead-member cleanup");
            // sec-3 ALSO dies (cascade) → its share folds onto the
            // remaining survivors too.
            primary
                .requeue_dead_secondary_for_test("sec-3")
                .await
                .expect("cascade dead-member cleanup");

            // After both deaths the redistributed shares are reserved to
            // survivors (sec-0, sec-1); a recheck dispatches them there.
            run_dispatch_recheck(&mut primary).await;
            settle_pump().await;

            let survivor_total = assigned_ids(&mut ends[0].1).len()
                + assigned_ids(&mut ends[1].1).len()
                + confirmed_first;
            assert_eq!(
                survivor_total, 8,
                "every task — including the two dead members' redistributed \
                 shares — dispatches onto the survivors; nothing stranded"
            );
            assert_eq!(
                primary.pool().iter().count(),
                0,
                "no task strands in a reservation for a member that can no \
                 longer take it"
            );
        })
        .await;
}

/// #382 PRESERVED. An unconfirmed member is NEVER sent a `TaskAssignment`
/// even though it has a reserved share — the share is HELD in the pool,
/// not dispatched. (The reservation caps what a confirmed member pulls;
/// the veto independently withholds the send to an unconfirmed one.)
#[tokio::test(flavor = "current_thread")]
async fn unconfirmed_member_with_reserved_share_is_never_sent_an_assignment() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let n_members = 3usize;
            let (transport, mut ends) = setup_test(n_members as u32);
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            seed_roster_and_tasks(&mut primary, n_members, 2, 6);

            // sec-2 is the half-joined member: reserved a share but never
            // confirmed its mesh leg.
            primary.mark_member_mesh_unconfirmed_for_test("sec-2");
            primary.seed_bringup_reservation();

            run_dispatch_recheck(&mut primary).await;
            settle_pump().await;

            assert!(
                assigned_ids(&mut ends[2].1).is_empty(),
                "the unconfirmed member must receive NO TaskAssignment — its \
                 terminals would swallow on its half-formed egress leg (#382); \
                 its reserved share stays HELD in the pool"
            );
            assert!(
                primary.slot_is_idle_for_test("sec-2", 0),
                "sec-2's workers stay idle: the veto withheld every send"
            );
            // Its share is still queued (held), not dispatched elsewhere.
            assert!(
                primary.pool().iter().count() >= 1,
                "the held share is still queued for sec-2 to claim on a late \
                 MeshReady"
            );
        })
        .await;
}
