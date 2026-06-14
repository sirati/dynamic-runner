//! #517 primary side: the `IllegallyAssignedToNonidleWorker` bounce
//! handler RECONCILES the diverged `(secondary, worker_id)` occupancy +
//! REQUEUES the bounced task — and NEVER accounts it as a failure. Plus
//! the enforced assign-guard: a commit onto a non-idle slot is REFUSED
//! (skip, not a silent overwrite).

use super::*;

use crate::primary::wire::compute_task_hash;
use dynrunner_core::{PhaseId, ResourceAmount, ResourceKind, ResourceMap, TaskInfo, TypeId};

fn mem(bytes: u64) -> Vec<ResourceAmount> {
    vec![ResourceAmount {
        kind: ResourceKind::memory(),
        amount: bytes,
    }]
}

/// QUEUED pool items only (`PendingPool::len` folds in-flight + blocked).
fn queued(
    primary: &PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
) -> usize {
    primary.pool().iter().count()
}

fn work_task(name: &str) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 100);
    t.phase_id = PhaseId::from("work");
    t.type_id = TypeId::from("default");
    t
}

/// The secondary's illegal-assignment bounce, mirroring the emitter
/// (`secondary/dispatch/helpers.rs`): names the assigned (bounced) task +
/// the incumbent the busy slot is running.
fn illegal_bounce(
    secondary_id: &str,
    worker_id: u32,
    assigned_hash: &str,
    assigned_id: &str,
    incumbent: Option<(&str, &str)>,
) -> DistributedMessage<TestId> {
    DistributedMessage::IllegallyAssignedToNonidleWorker {
        target: None,
        sender_id: secondary_id.into(),
        timestamp: 0.0,
        secondary_id: secondary_id.into(),
        worker_id,
        assigned: dynrunner_protocol_primary_secondary::AssignedTaskRef {
            hash: assigned_hash.into(),
            task_id: TestId(assigned_id.into()),
        },
        incumbent: incumbent.map(|(h, id)| dynrunner_protocol_primary_secondary::AssignedTaskRef {
            hash: h.into(),
            task_id: TestId(id.into()),
        }),
    }
}

/// TEST (b) + REVERT-CHECK: the bounce handler reconciles occupancy +
/// requeues the bounced task and burns NO failure budget. (Revert: a
/// TaskFailed-shaped bounce would have routed through `handle_task_failed`
/// and burned retry budget / recorded a terminal.)
#[tokio::test(flavor = "current_thread")]
async fn illegal_bounce_reconciles_occupancy_and_requeues_without_failure() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let phase = PhaseId::from("work");
            let pool = dynrunner_scheduler_api::PendingPool::<TestId>::new(
                [phase.clone()],
                HashMap::new(),
            )
            .expect("work-phase pool");
            primary.pending = Some(pool);

            // The INCUMBENT the worker physically runs, and the ASSIGNED task
            // the primary illegally committed onto the SAME slot (its diverged
            // belief). Seed both in the CRDT ledger (production-shaped).
            let incumbent = work_task("incumbent");
            let incumbent_hash = compute_task_hash(&incumbent);
            let assigned = work_task("assigned");
            let assigned_hash = compute_task_hash(&assigned);
            for t in [&incumbent, &assigned] {
                primary.cluster_state.apply(ClusterMutation::TaskAdded {
                    hash: compute_task_hash(t),
                    task: t.clone(),
                });
            }
            // The incumbent is in the ledger but NOT held on a slot here (its
            // slot belief is what the bounce will repair). The ASSIGNED task
            // is what the primary committed onto (sec-0, worker 0): stage it
            // in-flight there (the diverged state).
            primary.cluster_state.apply(ClusterMutation::SecondaryCapacity {
                secondary: "sec-0".into(),
                worker_count: 1,
                resources: mem(8 * 1024 * 1024 * 1024),
            });
            // Track the incumbent in the ledger too (the primary dispatched it
            // earlier; only the SLOT belief drifted), so the reconcile can
            // re-seat the slot from the ledger's task body.
            primary.seed_inflight(
                incumbent_hash.clone(),
                phase.clone(),
                "sec-0".into(),
                0,
                incumbent.clone(),
            );
            // Stage the ASSIGNED task as the slot's current (diverged) holder
            // + its ledger entry, via the real commit lifecycle.
            let staged = primary.stage_in_flight_for_test("sec-0".into(), 0, assigned.clone());
            assert_eq!(staged, assigned_hash, "fixture stages the assigned hash");
            assert!(
                primary.slot_holds_hash_for_test("sec-0", 0, &assigned_hash),
                "fixture precondition: the slot holds the (illegally) assigned task"
            );

            let failed_before = primary.failed_tasks.len();

            // Deliver the bounce: worker 0 is actually running the incumbent.
            primary
                .handle_illegally_assigned(illegal_bounce(
                    "sec-0",
                    0,
                    &assigned_hash,
                    "assigned",
                    Some((&incumbent_hash, "incumbent")),
                ))
                .await;

            // REQUEUE: the assigned task is back in the pool, dropped from the
            // in-flight ledger, and the CRDT shows it Pending (TaskRequeued).
            assert_eq!(
                queued(&primary),
                1,
                "the bounced task must be requeued into the pool"
            );
            assert!(
                !primary.in_flight.contains_key(&assigned_hash),
                "the bounced task must be removed from the in-flight ledger"
            );
            assert!(
                matches!(
                    primary.cluster_state_for_test().task_state(&assigned_hash),
                    Some(crate::cluster_state::TaskState::Pending { .. })
                ),
                "the bounced task's replicated state must be Pending (TaskRequeued); got {:?}",
                primary.cluster_state_for_test().task_state(&assigned_hash)
            );

            // NO FAILURE: never accounted as a terminal.
            assert_eq!(
                primary.failed_tasks.len(),
                failed_before,
                "the bounce must NOT touch failed_tasks (no retry-budget burn)"
            );
            assert!(
                !primary.failed_tasks.contains_key(&assigned_hash),
                "the bounced task must never be a failure"
            );

            // RECONCILE: the slot now reflects the INCUMBENT (the primary
            // stops believing the worker is idle / running the bounced task).
            assert!(
                primary.slot_holds_hash_for_test("sec-0", 0, &incumbent_hash),
                "the slot must be reconciled to hold the incumbent the \
                 secondary reported (loop-breaker: the primary no longer \
                 re-assigns the busy slot)"
            );
            assert!(
                primary.in_flight.contains_key(&incumbent_hash),
                "the incumbent stays in the in-flight ledger"
            );
        })
        .await;
}

/// TEST (b2): an out-of-range bounce (no incumbent) still requeues the
/// task and burns no failure budget — the degenerate case.
#[tokio::test(flavor = "current_thread")]
async fn illegal_bounce_without_incumbent_still_requeues_no_failure() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let phase = PhaseId::from("work");
            primary.pending = Some(
                dynrunner_scheduler_api::PendingPool::<TestId>::new([phase], HashMap::new())
                    .expect("work-phase pool"),
            );
            let assigned = work_task("oor-assigned");
            let assigned_hash = compute_task_hash(&assigned);
            primary.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: assigned_hash.clone(),
                task: assigned.clone(),
            });
            primary.cluster_state.apply(ClusterMutation::SecondaryCapacity {
                secondary: "sec-0".into(),
                worker_count: 1,
                resources: mem(8 * 1024 * 1024 * 1024),
            });
            let staged = primary.stage_in_flight_for_test("sec-0".into(), 0, assigned.clone());
            assert_eq!(staged, assigned_hash);

            primary
                .handle_illegally_assigned(illegal_bounce(
                    "sec-0",
                    0,
                    &assigned_hash,
                    "oor-assigned",
                    None,
                ))
                .await;

            assert_eq!(queued(&primary), 1, "the bounced task is requeued");
            assert!(!primary.in_flight.contains_key(&assigned_hash));
            assert!(
                !primary.failed_tasks.contains_key(&assigned_hash),
                "no failure accounting for a no-incumbent bounce"
            );
            // No incumbent ⇒ the slot is left idle (the requeue freed it);
            // nothing to reconcile onto it.
            assert!(
                primary.slot_is_idle_for_test("sec-0", 0),
                "with no incumbent the freed slot stays idle"
            );
        })
        .await;
}

/// TEST (#531 Fix A): a handled bounce reconcile logs the per-event line at
/// DEBUG, NOT ERROR — the bounce is expected, no-loss optimistic-dispatch
/// churn at scale. Captured with a per-test scoped `TargetCapture` subscriber
/// (unfiltered, `Interest::always` — safe to hold across `.await`, no
/// #422-class warn-capture poisoning) on the reconcile module's target.
#[tokio::test(flavor = "current_thread")]
async fn handled_bounce_reconcile_logs_at_debug_not_error() {
    let log = crate::test_capture::TargetCapture::for_target(
        "dynrunner_manager_distributed::primary::task::illegal_assignment",
    );
    let _guard = {
        use tracing_subscriber::layer::SubscriberExt;
        tracing::subscriber::set_default(tracing_subscriber::Registry::default().with(log.clone()))
    };
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let phase = PhaseId::from("work");
            primary.pending = Some(
                dynrunner_scheduler_api::PendingPool::<TestId>::new(
                    [phase.clone()],
                    HashMap::new(),
                )
                .expect("work-phase pool"),
            );
            let incumbent = work_task("incumbent");
            let incumbent_hash = compute_task_hash(&incumbent);
            let assigned = work_task("assigned");
            let assigned_hash = compute_task_hash(&assigned);
            for t in [&incumbent, &assigned] {
                primary.cluster_state.apply(ClusterMutation::TaskAdded {
                    hash: compute_task_hash(t),
                    task: t.clone(),
                });
            }
            primary.cluster_state.apply(ClusterMutation::SecondaryCapacity {
                secondary: "sec-0".into(),
                worker_count: 1,
                resources: mem(8 * 1024 * 1024 * 1024),
            });
            primary.seed_inflight(
                incumbent_hash.clone(),
                phase.clone(),
                "sec-0".into(),
                0,
                incumbent.clone(),
            );
            primary.stage_in_flight_for_test("sec-0".into(), 0, assigned.clone());

            primary
                .handle_illegally_assigned(illegal_bounce(
                    "sec-0",
                    0,
                    &assigned_hash,
                    "assigned",
                    Some((&incumbent_hash, "incumbent")),
                ))
                .await;

            let events = log.events();
            // The per-event reconcile line ("secondary bounced an ILLEGAL
            // assignment ...") fired, and at DEBUG.
            let reconcile_line = events.iter().find(|e| {
                e.event
                    .message
                    .contains("secondary bounced an ILLEGAL assignment")
            });
            let reconcile_line =
                reconcile_line.expect("the per-event reconcile line must be emitted");
            assert_eq!(
                reconcile_line.level,
                tracing::Level::DEBUG,
                "the handled, no-loss bounce reconcile must log at DEBUG, not \
                 ERROR (#531: expected optimistic-dispatch churn at scale)"
            );
            // NOTHING on this target is ERROR (the downgrade is complete; no
            // stray ERROR slipped through on the reconcile path).
            assert!(
                events.iter().all(|e| e.level != tracing::Level::ERROR),
                "a handled bounce reconcile must emit NO ERROR on the reconcile \
                 path; got {:?}",
                events
                    .iter()
                    .map(|e| (e.level, e.event.message.clone()))
                    .collect::<Vec<_>>()
            );
        })
        .await;
}

/// TEST (#531 Fix B — the re-seat cross-member guard): when the incumbent a
/// member bounces is authoritatively held by a DIFFERENT member in the ledger
/// (the #518 cross-member re-seat repointed the hash onto the original holder
/// A, while a duplicate copy keeps running on the bouncing member B), the
/// reconcile must NOT re-seat B's slot onto that hash. Re-seating B's slot
/// Inherited-holding-A's-hash would later corrupt A's ledger entry via
/// `reconcile_inherited_slot` (it removes the hash's ledger entry — A's — and
/// requeues A's still-running task). The guard leaves B's slot Idle and leaves
/// A's ledger entry untouched.
#[tokio::test(flavor = "current_thread")]
async fn bounce_does_not_reseat_b_slot_onto_a_held_incumbent() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let phase = PhaseId::from("work");
            primary.pending = Some(
                dynrunner_scheduler_api::PendingPool::<TestId>::new(
                    [phase.clone()],
                    HashMap::new(),
                )
                .expect("work-phase pool"),
            );

            // The cross-member duplicate hash. Authoritatively held by member A
            // (sec-a, worker 0) in the ledger — the post-#518-re-seat state.
            let dup = work_task("dup");
            let dup_hash = compute_task_hash(&dup);
            // The NEW task B's slot was (illegally) assigned, which B bounces.
            let assigned = work_task("assigned-to-b");
            let assigned_hash = compute_task_hash(&assigned);
            for t in [&dup, &assigned] {
                primary.cluster_state.apply(ClusterMutation::TaskAdded {
                    hash: compute_task_hash(t),
                    task: t.clone(),
                });
            }
            // Both members have capacity. A holds `dup` in the ledger; B is the
            // bouncing member whose slot the new task was committed onto.
            for sec in ["sec-a", "sec-b"] {
                primary.cluster_state.apply(ClusterMutation::SecondaryCapacity {
                    secondary: sec.into(),
                    worker_count: 1,
                    resources: mem(8 * 1024 * 1024 * 1024),
                });
            }
            // A is the authoritative holder of `dup` in the ledger (the #518
            // re-seat repointed the entry onto A). Register A's slot idle and
            // seed the ledger entry under sec-a.
            primary.register_idle_worker_for_test(
                "sec-a".into(),
                0,
                ResourceMap::from([(ResourceKind::memory(), 8 * 1024 * 1024 * 1024u64)]),
            );
            primary.seed_inflight(
                dup_hash.clone(),
                phase.clone(),
                "sec-a".into(),
                0,
                dup.clone(),
            );
            // B's slot holds the (illegally) assigned new task — the diverged
            // belief the bounce will requeue.
            primary.stage_in_flight_for_test("sec-b".into(), 0, assigned.clone());

            // B bounces: its worker 0 is physically running `dup` (the
            // not-withdrawn duplicate), so the bounce names `dup` as incumbent.
            primary
                .handle_illegally_assigned(illegal_bounce(
                    "sec-b",
                    0,
                    &assigned_hash,
                    "assigned-to-b",
                    Some((&dup_hash, "dup")),
                ))
                .await;

            // GUARD: B's slot must NOT be re-seated onto `dup` (the requeue of
            // the bounced task freed it; the cross-member guard leaves it Idle
            // rather than seeding it Inherited-holding-A's-hash).
            assert!(
                primary.slot_is_idle_for_test("sec-b", 0),
                "B's slot must stay Idle: re-seating it onto a hash the ledger \
                 attributes to A would later corrupt A's ledger entry"
            );
            assert!(
                !primary.slot_holds_hash_for_test("sec-b", 0, &dup_hash),
                "B's slot must NOT hold the A-attributed incumbent hash"
            );
            // A's ledger entry is untouched: still present and still attributed
            // to sec-a (the authoritative holder owns the hash).
            let dup_entry = primary
                .in_flight
                .get(&dup_hash)
                .expect("A's ledger entry for the duplicate must survive");
            assert_eq!(
                dup_entry.secondary_id, "sec-a",
                "the duplicate hash must stay attributed to the authoritative \
                 holder A; the bounce must not repoint or drop it"
            );
            // The bounced new task is still requeued (the requeue half is
            // unaffected by the re-seat guard).
            assert_eq!(
                queued(&primary),
                1,
                "the bounced task is requeued regardless of the re-seat guard"
            );
        })
        .await;
}

/// TEST (c): the enforced assign-guard. A second commit onto a slot that
/// already holds a task is REFUSED (returns false), the held task's slot
/// state is PRESERVED (never silently overwritten), and the ledger/type
/// budget are untouched.
#[tokio::test(flavor = "current_thread")]
async fn assign_guard_refuses_commit_onto_nonidle_slot() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            primary.register_idle_worker_for_test(
                "sec-0".into(),
                0,
                ResourceMap::from([(ResourceKind::memory(), 1024 * 1024 * 1024u64)]),
            );
            let idx = primary
                .worker_idx_for("sec-0", 0)
                .expect("worker exists");

            let first = work_task("first");
            let first_hash = compute_task_hash(&first);
            // First commit takes (slot was idle).
            assert!(
                primary.commit_assignment(idx, first.clone(), first_hash.clone(), ResourceMap::new()),
                "the first commit onto an idle slot must take"
            );
            assert!(primary.slot_holds_hash_for_test("sec-0", 0, &first_hash));
            let in_flight_after_first = primary.in_flight_len_for_test();

            // Second commit onto the SAME (now busy) slot is REFUSED.
            let second = work_task("second");
            let second_hash = compute_task_hash(&second);
            assert!(
                !primary.commit_assignment(idx, second.clone(), second_hash.clone(), ResourceMap::new()),
                "a commit onto a NON-idle slot must be REFUSED (not silently \
                 overwritten) — the #517 enforced idle-guard"
            );
            // The held task's slot state is PRESERVED.
            assert!(
                primary.slot_holds_hash_for_test("sec-0", 0, &first_hash),
                "the refused commit must PRESERVE the held task's slot state"
            );
            assert!(
                !primary.slot_holds_hash_for_test("sec-0", 0, &second_hash),
                "the refused task must NOT overwrite the slot"
            );
            // No ledger / type-slot side effect from the refused commit.
            assert_eq!(
                primary.in_flight_len_for_test(),
                in_flight_after_first,
                "a refused commit must not insert a ledger entry (atomic triple)"
            );
            assert!(
                !primary.in_flight.contains_key(&second_hash),
                "the refused task must not be in the in-flight ledger"
            );
        })
        .await;
}
