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

use dynrunner_core::{PhaseId, ResourceAmount, ResourceKind, ResourceMap, TaskDep, TypeId};

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
                def_id: None,
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
            primary.seed_bringup_reservation(crate::process::BootstrapKind::BootstrapRelocation);
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
            primary.seed_bringup_reservation(crate::process::BootstrapKind::BootstrapRelocation);
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
            primary.seed_bringup_reservation(crate::process::BootstrapKind::BootstrapRelocation);

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

// ── #507: the operational primary is a PromotedDestination whose members
//    hydrate to Operational (never InitialAssigning). These tests exercise
//    the REAL mesh-always shape the old `is_cold_bringup` gate (any-member-
//    InitialAssigning) silently no-op'd on — the dead-on-arrival #494
//    reservation. The new gate keys on the inherited LEDGER being UNSTARTED
//    (`run_is_unstarted()`), so it opens on a bootstrap-relocation cold
//    target and stays closed on a failover survivor-inherit. ────────────

/// A `build`-phase task with explicit deps; `task_id == name`.
fn dep_task(name: &str, phase: &str, depends_on: &[(&str, &str)]) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 100);
    t.phase_id = PhaseId::from(phase);
    t.type_id = TypeId::from("default");
    t.task_id = name.into();
    t.task_depends_on = depends_on
        .iter()
        .map(|(dep_phase, dep_id)| TaskDep {
            task_id: (*dep_id).to_string(),
            phase_id: PhaseId::from(*dep_phase),
            inherit_outputs: false,
        })
        .collect();
    t
}

/// PromotedDestination shape: register `n_members × workers_each` idle
/// workers as OPERATIONAL (mesh-confirmed) members — NOT InitialAssigning
/// (the connection table is left empty, exactly as a relocate target's
/// `reconstruct_secondaries_from_cluster_state` seeds metadata-only
/// `Operational` members; `register_idle_worker_for_test` mirrors that by
/// touching only `self.workers` + `mesh_ready_secondaries`, never
/// `self.secondaries`). The caller seeds the CRDT (started vs unstarted).
fn register_operational_roster(primary: &mut TestPrimary, n_members: usize, workers_each: u32) {
    let budget = ResourceMap::from([(ResourceKind::memory(), 8 * 1024 * 1024 * 1024u64)]);
    let mut global_id = 0u32;
    for _round in 0..workers_each {
        for m in 0..n_members {
            primary.register_idle_worker_for_test(format!("sec-{m}"), global_id, budget.clone());
            global_id += 1;
        }
    }
}

/// Seed an UNSTARTED multi-phase ledger: a terminal `build` prereq plus
/// `n_run` zero-dep `run` tasks AND one `run` DEPENDENT that lands
/// CRDT-`Blocked` at seed (its `build` prereq is still Pending). Hydrates
/// the pool. Returns the dispatchable (`Pending`) run-task names.
///
/// The Blocked-at-seed dependent is the LOAD-BEARING fixture for the #507
/// Blocked-classification: `run_is_unstarted()` must read `true` DESPITE a
/// CRDT-`Blocked` entry (a Blocked task has never been dispatched — counting
/// it as post-dispatch would keep the reservation closed on exactly this
/// multi-phase shape).
fn seed_unstarted_multiphase(primary: &mut TestPrimary, n_run: usize) -> Vec<String> {
    let names: Vec<String> = (0..n_run).map(|i| format!("t{i}")).collect();
    {
        let cs = primary.cluster_state_mut_for_test();
        // `build` is the zero-dep FIRST phase (its tasks are dispatchable
        // now); `run` depends on `build` (its dependent stays Blocked until
        // build finishes).
        cs.apply(ClusterMutation::PhaseDepsSet {
            deps: HashMap::from([
                (PhaseId::from("build"), vec![]),
                (PhaseId::from("run"), vec![PhaseId::from("build")]),
            ]),
        });
        // The zero-dep build tasks — the dispatchable Pending pool the
        // reservation partitions (the first-phase work).
        for name in &names {
            let task = dep_task(name, "build", &[]);
            cs.apply(ClusterMutation::TaskAdded {
                hash: crate::primary::wire::compute_task_hash(&task),
                task,
                def_id: None,
            });
        }
        // A run task that DEPENDS on a still-Pending build task (`t0`), then
        // transitioned to CRDT-Blocked via `TaskBlocked` (Pending→Blocked,
        // apply.rs) — the cascade-pause a dependent lands in when its
        // prereq has not finished. This is a NEVER-DISPATCHED entry that
        // run_is_unstarted() must NOT count as post-dispatch (#507).
        let prereq_hash = {
            let p = dep_task("t0", "build", &[]);
            crate::primary::wire::compute_task_hash(&p)
        };
        let blocked = dep_task("blocked-dependent", "run", &[("build", "t0")]);
        let blocked_hash = crate::primary::wire::compute_task_hash(&blocked);
        cs.apply(ClusterMutation::TaskAdded {
            hash: blocked_hash.clone(),
            task: blocked,
            def_id: None,
        });
        cs.apply(ClusterMutation::TaskBlocked {
            hash: blocked_hash,
            on: prereq_hash,
        });
    }
    primary
        .hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");
    names
}

/// TEST A — the #507 headline (revert-confirmed). A PromotedDestination at
/// cold bring-up (Operational-seeded members, UNSTARTED ledger that
/// INCLUDES a CRDT-Blocked-at-seed dependent) with a co-located, high-worker
/// lowest-id node and a small first-phase pool. The reservation MUST open
/// (despite the Blocked entry — run_is_unstarted is true) and the pool MUST
/// spread across the fleet instead of packing the high-worker node.
///
/// REVERT-CONFIRM (documented, asserted via the gate fact): the OLD gate
/// (`any member InitialAssigning`) is FALSE here — no member is in the
/// connection table — so the OLD gate would NOT open the reservation, and
/// the greedy recheck would let the 16-worker co-located node pack the
/// whole pool. The NEW gate opens it; the spread assertion below is what
/// the old code fails.
#[tokio::test(flavor = "current_thread")]
async fn bootstrap_cold_unstarted_opens_reservation_and_spreads_despite_blocked() {
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

            // Seed the CRDT + hydrate the pool FIRST — hydrate rebuilds
            // `self.workers` from CRDT capacity, so the test-registered
            // workers below MUST come after it (mirrors `seed_roster_and_tasks`).
            let run_names = seed_unstarted_multiphase(&mut primary, 6);
            assert_eq!(run_names.len(), 6);

            // The co-located lowest-id node (sec-0) has MANY workers; the
            // peers have few — the #507 asymmetry. 6 tasks < total capacity
            // (16+4+4+4), so every task pairs a distinct worker. 4 workers
            // each, then 12 EXTRA on sec-0 so it ALONE could greedily pack
            // all 6 with no reservation (the high-worker co-located node).
            register_operational_roster(&mut primary, n_members, 4);
            let budget = ResourceMap::from([(ResourceKind::memory(), 8 * 1024 * 1024 * 1024u64)]);
            for extra in 0..12u32 {
                primary.register_idle_worker_for_test("sec-0".into(), 100 + extra, budget.clone());
            }

            // THE load-bearing classification: a CRDT-Blocked-at-seed
            // dependent is present, yet the run is UNSTARTED.
            assert!(
                primary.cluster_state_mut_for_test().counts().blocked >= 1,
                "fixture must seed a CRDT-Blocked dependent so the test \
                 pins Blocked ∉ post-dispatch"
            );
            assert!(
                primary.cluster_state_mut_for_test().run_is_unstarted(),
                "a Blocked-at-seed dependent must NOT make the run look \
                 started — Blocked is never-dispatched (#507 load-bearing)"
            );

            // OPEN the reservation — the pre-loop pipeline order.
            primary.seed_bringup_reservation(crate::process::BootstrapKind::BootstrapRelocation);
            assert!(
                primary.pool().reservation_active(),
                "a PromotedDestination at cold bring-up (unstarted ledger) \
                 MUST open the reservation — the old InitialAssigning gate \
                 left it CLOSED here (the #507 dead-on-arrival pack)"
            );

            // All members confirmed (Operational-seeded). Recheck: with the
            // reservation each member drains only its capacity-bounded share.
            run_dispatch_recheck(&mut primary).await;
            settle_pump().await;

            let per_member: Vec<usize> = (0..n_members)
                .map(|m| assigned_ids(&mut ends[m].1).len())
                .collect();
            let total: usize = per_member.iter().sum();
            assert_eq!(
                total, 6,
                "the whole 6-task pool dispatched; per-member: {per_member:?}"
            );
            // THE anti-pack assertion: sec-0 (16 workers) did NOT pack all 6.
            // One task per distinct worker over the load-interleave spreads
            // them — every member gets at least one.
            assert!(
                per_member[0] <= 3,
                "the high-worker co-located node must NOT pack the pool \
                 (it would take all 6 with no reservation); got {} on sec-0; \
                 per-member: {per_member:?}",
                per_member[0]
            );
            for (m, got) in per_member.iter().enumerate() {
                assert!(
                    *got >= 1,
                    "every member gets a reserved share — sec-{m} got {got}; \
                     per-member: {per_member:?}"
                );
            }
        })
        .await;
}

/// TEST B — the failover exclusion. A PromotedDestination whose inherited
/// ledger is STARTED (≥1 InFlight task from the prior operational primary)
/// must NOT open the reservation — the inherited pool dispatches through the
/// re-announce flow and a pre-partitioned share would wedge it.
#[tokio::test(flavor = "current_thread")]
async fn failover_started_ledger_does_not_open_reservation() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let n_members = 3usize;
            let (transport, _ends) = setup_test(n_members as u32);
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Seed a STARTED ledger: two Pending tasks plus one the prior
            // primary already dispatched (InFlight) — the failover signal.
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::PhaseDepsSet {
                    deps: HashMap::from([(PhaseId::from("p"), vec![])]),
                });
                for name in ["t0", "t1", "inflight"] {
                    let task = one_task(name);
                    cs.apply(ClusterMutation::TaskAdded {
                        hash: crate::primary::wire::compute_task_hash(&task),
                        task,
                        def_id: None,
                    });
                }
                // Drive `inflight` to InFlight (a worker outcome the prior
                // operational primary produced — the post-dispatch proof).
                let inflight = one_task("inflight");
                cs.apply(ClusterMutation::TaskAssigned {
                    hash: crate::primary::wire::compute_task_hash(&inflight),
                    secondary: "sec-0".into(),
                    worker: 0,
                    version: Default::default(),
                    attempt: 0,
                });
            }
            primary
                .hydrate_from_cluster_state()
                .expect("test fixture: composed task graph is valid");
            // Register workers AFTER hydrate (it rebuilds self.workers), so
            // a non-empty roster proves the gate — not an empty roster — is
            // what keeps the reservation closed.
            register_operational_roster(&mut primary, n_members, 2);

            assert!(
                primary.cluster_state_mut_for_test().counts().in_flight >= 1,
                "fixture must seed an InFlight task (the started/failover signal)"
            );
            assert!(
                !primary.cluster_state_mut_for_test().run_is_unstarted(),
                "a started ledger (InFlight) must read NOT-unstarted"
            );

            primary.seed_bringup_reservation(crate::process::BootstrapKind::Failover);
            assert!(
                !primary.pool().reservation_active(),
                "a failover survivor-inherit (started ledger) must NOT open \
                 the reservation — the inherited-pool wedge the exclusion \
                 prevents"
            );
        })
        .await;
}

/// TEST B'' — the graceful-abort relocation exclusion (the `run_is_unstarted`
/// AND-branch in isolation). A graceful abort relocates the primary MID-RUN
/// via the SAME `PrimaryChangeReason::Transferred` a bootstrap relocation uses
/// (`graceful_abort` -> `relocate_primary_to`), so its target hydrates as
/// `kind = BootstrapRelocation` — but its inherited ledger is STARTED. The
/// gate `kind == BootstrapRelocation && run_is_unstarted()` must NOT open: the
/// kind branch passes (Relocation), only the `run_is_unstarted()` branch
/// declines (started), preventing the inherited-pool wedge. This ISOLATES the
/// `run_is_unstarted` guard — a kind-only gate would wrongly open and wedge a
/// graceful-abort relocation, which TEST B (Failover, where the kind branch
/// already declines) cannot catch.
#[tokio::test(flavor = "current_thread")]
async fn relocation_started_ledger_does_not_open_reservation() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let n_members = 3usize;
            let (transport, _ends) = setup_test(n_members as u32);
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Seed a STARTED ledger (as a mid-run graceful-abort relocate
            // inherits): two Pending tasks plus one the prior primary already
            // dispatched (InFlight — the post-dispatch / started proof).
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::PhaseDepsSet {
                    deps: HashMap::from([(PhaseId::from("p"), vec![])]),
                });
                for name in ["t0", "t1", "inflight"] {
                    let task = one_task(name);
                    cs.apply(ClusterMutation::TaskAdded {
                        hash: crate::primary::wire::compute_task_hash(&task),
                        task,
                        def_id: None,
                    });
                }
                let inflight = one_task("inflight");
                cs.apply(ClusterMutation::TaskAssigned {
                    hash: crate::primary::wire::compute_task_hash(&inflight),
                    secondary: "sec-0".into(),
                    worker: 0,
                    version: Default::default(),
                    attempt: 0,
                });
            }
            primary
                .hydrate_from_cluster_state()
                .expect("test fixture: composed task graph is valid");
            register_operational_roster(&mut primary, n_members, 2);

            assert!(
                !primary.cluster_state_mut_for_test().run_is_unstarted(),
                "a started ledger (InFlight) must read NOT-unstarted"
            );

            // kind = BootstrapRelocation (the Transferred graceful-abort path),
            // but the inherited ledger is STARTED: only the run_is_unstarted
            // AND-branch keeps the reservation closed here.
            primary.seed_bringup_reservation(crate::process::BootstrapKind::BootstrapRelocation);
            assert!(
                !primary.pool().reservation_active(),
                "a graceful-abort relocation (kind=BootstrapRelocation but a \
                 STARTED inherited ledger) must NOT open the reservation — only \
                 the run_is_unstarted AND-guard prevents the inherited-pool \
                 wedge here; a kind-only gate would wrongly open"
            );
        })
        .await;
}

/// TEST B' — THE CRUX (the all-Pending early-failover edge). A FAILOVER
/// whose inherited pool is ENTIRELY Pending (the prior primary dispatched
/// the initial batch, died, and the survivor re-queued every InFlight→
/// Pending) is LEDGER-INDISTINGUISHABLE from a bootstrap-relocation cold
/// target: run_is_unstarted() reads TRUE on it. This is exactly the case
/// that broke `run_is_unstarted`-alone (it opened the reservation on a
/// failover and wedged the inherited pool). The BootstrapKind is what
/// saves it: kind=Failover keeps the reservation CLOSED despite the
/// unstarted ledger.
#[tokio::test(flavor = "current_thread")]
async fn failover_all_pending_pool_does_not_open_reservation_despite_unstarted() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let n_members = 3usize;
            let (transport, _ends) = setup_test(n_members as u32);
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // An ALL-PENDING inherited ledger — no post-dispatch task at all,
            // so it reads as unstarted (the early-failover re-queue shape).
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::PhaseDepsSet {
                    deps: HashMap::from([(PhaseId::from("p"), vec![])]),
                });
                for name in ["t0", "t1", "t2"] {
                    let task = one_task(name);
                    cs.apply(ClusterMutation::TaskAdded {
                        hash: crate::primary::wire::compute_task_hash(&task),
                        task,
                        def_id: None,
                    });
                }
            }
            primary
                .hydrate_from_cluster_state()
                .expect("test fixture: composed task graph is valid");
            register_operational_roster(&mut primary, n_members, 2);

            // The ledger IS unstarted — run_is_unstarted alone would (wrongly)
            // open the reservation here. This is the misclassification.
            assert!(
                primary.cluster_state_mut_for_test().run_is_unstarted(),
                "an all-Pending inherited pool reads as unstarted — the exact \
                 ambiguity the BootstrapKind resolves"
            );

            // kind=Failover keeps it CLOSED despite the unstarted ledger.
            primary.seed_bringup_reservation(crate::process::BootstrapKind::Failover);
            assert!(
                !primary.pool().reservation_active(),
                "an all-Pending FAILOVER must NOT open the reservation — the \
                 kind is the authoritative discriminator the unstarted-ledger \
                 read cannot provide (would wedge the inherited pool)"
            );
        })
        .await;
}

/// TEST D — over-subscription. tasks > total idle capacity: each member's
/// reserved share is bounded to its idle-worker count, and the surplus
/// stays UNRESERVED (free for any member). No member holds more than it can
/// drain (no steal, no strand).
#[tokio::test(flavor = "current_thread")]
async fn over_subscription_bounds_share_to_capacity_and_leaves_surplus_free() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let n_members = 2usize;
            let (transport, _ends) = setup_test(n_members as u32);
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // 2 members × 1 worker = 2 idle workers; 5 tasks → 2 reserved
            // (one per worker), 3 surplus unreserved. Seed+hydrate FIRST
            // (hydrate rebuilds self.workers), then register workers.
            let run_names = seed_unstarted_multiphase(&mut primary, 5);
            assert_eq!(run_names.len(), 5);
            register_operational_roster(&mut primary, n_members, 1);

            primary.seed_bringup_reservation(crate::process::BootstrapKind::BootstrapRelocation);
            assert!(primary.pool().reservation_active());

            // Exactly 2 tasks are reserved (one per idle worker); the other
            // 3 are unreserved. Count reserved by asking: a task is reserved
            // iff some member is admitted AND another is not.
            let pool_tasks: Vec<TaskInfo<TestId>> = primary.pool().iter().cloned().collect();
            let mut reserved = 0usize;
            let mut unreserved = 0usize;
            for task in &pool_tasks {
                let sec0_ok = primary.pool().reservation_admits("sec-0", task);
                let sec1_ok = primary.pool().reservation_admits("sec-1", task);
                if sec0_ok && sec1_ok {
                    unreserved += 1;
                } else {
                    reserved += 1;
                }
            }
            assert_eq!(
                reserved, 2,
                "exactly one task reserved per idle worker (2 workers); \
                 reserved={reserved} unreserved={unreserved}"
            );
            assert_eq!(
                unreserved, 3,
                "the surplus beyond total idle capacity stays unreserved \
                 (free for any member); unreserved={unreserved}"
            );

            // No member is reserved MORE than one task (its single worker's
            // capacity) — the no-over-subscription invariant.
            for m in 0..n_members {
                let sec = format!("sec-{m}");
                let held = pool_tasks
                    .iter()
                    .filter(|task| {
                        // reserved to THIS member iff it sees it but the
                        // other member does not.
                        let other = format!("sec-{}", 1 - m);
                        primary.pool().reservation_admits(&sec, task)
                            && !primary.pool().reservation_admits(&other, task)
                    })
                    .count();
                assert!(
                    held <= 1,
                    "sec-{m} must not be reserved more than its 1 worker can \
                     drain; reserved-to-it={held}"
                );
            }
        })
        .await;
}
