//! #519 pipeline-depth dispatch bias (`next_prefer_dependency`): the gate
//! arithmetic, the deterministic per-decision toggle, and the within-class
//! selection bias the dispatch view applies when armed.
//!
//! The bias is the within-class reorder `dispatch_view_for_worker(idx, true)`
//! threads through the pool's `view_for_worker` preference slot — a ready
//! DIRECT prerequisite of a LIVE blocked task floats to the front of its
//! soft-pin class so completing it refills the ready pool (deepening the
//! pipeline). `dispatch_view_for_worker(idx, false)` is byte-identical to the
//! pre-#519 path (the revert-confirm control).
//!
//! All deterministic: the toggle is a bool (no RNG), and the view order is
//! pinned, so the alternation + the bias are asserted exactly.

use super::*;

use dynrunner_core::{PhaseId, ResourceMap, ResourceKind, TaskDep, TypeId};

use crate::primary::wire::compute_task_hash;

type TestPrimary = PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>;

/// A free-pool Work task in phase `phase` with an explicit id + task-level
/// deps (each dep names its prerequisite's full `(phase, id)` identity).
fn dep_task(id: &str, phase: &str, size: u64, deps: &[(&str, &str)]) -> TaskInfo<TestId> {
    let mut t = make_binary(id, size);
    t.phase_id = PhaseId::from(phase);
    t.type_id = TypeId::from("default");
    t.affinity_id = None; // free pool — the bias's reorder class
    t.task_depends_on = deps
        .iter()
        .map(|(dp, di)| TaskDep {
            task_id: (*di).to_string(),
            phase_id: PhaseId::from(*dp),
            inherit_outputs: false,
            def_id: None,
        })
        .collect();
    t
}

fn one_gib() -> ResourceMap {
    ResourceMap::from([(ResourceKind::memory(), 1024 * 1024 * 1024u64)])
}

/// Hydrate a single-secondary primary from a CRDT carrying `tasks` (with
/// per-phase deps `phase_deps`), then register `workers` idle workers on
/// `sec-0`. The pool reflects the dep graph: a task whose `task_depends_on`
/// prereq has not completed hydrates into the blocked map.
fn primary_with_dag(
    tasks: Vec<TaskInfo<TestId>>,
    phase_deps: &[(&str, &[&str])],
    workers: u32,
) -> (TestPrimary, PrimaryMeshKeepalive) {
    let (transport, _ends) = setup_test(1);
    let (mut primary, mesh) = build_test_primary(
        test_primary_config(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    {
        let cs = primary.cluster_state_mut_for_test();
        let mut deps_map = HashMap::new();
        for (child, parents) in phase_deps {
            deps_map.insert(
                PhaseId::from(*child),
                parents.iter().map(|p| PhaseId::from(*p)).collect::<Vec<_>>(),
            );
        }
        cs.apply(ClusterMutation::PhaseDepsSet { deps: deps_map });
        // Route the TaskAdded batch through the ORIGINATOR stamp pass (as
        // production does) so each task's def id AND each dep's resolved
        // prereq def id are stamped over the WHOLE batch BEFORE apply —
        // making an intra-batch forward-ref (a dependent listed before its
        // prerequisite, e.g. `cons` before `prod0`) resolve, exactly as the
        // live wire does (L5/CL-A8). A raw per-task `cs.apply` would leave
        // every dep unstamped and lose a forward-ref.
        let batch: Vec<ClusterMutation<TestId>> = tasks
            .into_iter()
            .map(|t| ClusterMutation::TaskAdded {
                hash: compute_task_hash(&t),
                task: t,
                def_id: None,
            })
            .collect();
        crate::cluster_state::apply_locally_for_broadcast(cs, batch);
    }
    primary
        .hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");
    for w in 0..workers {
        primary.register_idle_worker_for_test("sec-0".into(), w, one_gib());
    }
    (primary, mesh)
}

/// Ordered task_ids of a worker's dispatch view (the slice the scheduler
/// chooses index 0 from).
fn view_ids(primary: &TestPrimary, worker_idx: usize, prefer_dependency: bool) -> Vec<String> {
    primary
        .dispatch_view_for_worker(worker_idx, prefer_dependency)
        .as_slice()
        .iter()
        .map(|t| t.task_id.clone())
        .collect()
}

// ───────────────────── per-decision bias: alternation + cadence ────────────

/// While the gate is armed, consecutive decisions alternate
/// false,true,false,true … — the deterministic ~50% split (no RNG). With
/// W=1 the gate is re-evaluated EVERY decision, so the cadence guard never
/// suppresses the flip; the alternation is the toggle alone.
#[test]
fn armed_decisions_alternate_deterministically() {
    // prod (ready) → cons (blocked, live). 1 ready < 4×1 → gate armed.
    let tasks = vec![
        dep_task("prod", "p", 50, &[]),
        dep_task("cons", "p", 50, &[("p", "prod")]),
    ];
    let (mut primary, _mesh) = primary_with_dag(tasks, &[], 1);
    assert!(primary.prefer_dependency_gate_holds(), "gate armed precondition");
    let seq: Vec<bool> = (0..4)
        .map(|_| primary.prefer_dependency_for_decision())
        .collect();
    assert_eq!(
        seq,
        vec![false, true, false, true],
        "armed decisions alternate deterministically (no RNG)"
    );
}

/// A DISARMED gate never flips the toggle and always returns `false` — and
/// it does NOT advance the alternation, so the first armed decision after a
/// disarmed run still starts from `false`.
#[test]
fn disarmed_decisions_return_false_without_flipping() {
    // Nothing blocked → clause 2 fails → gate disarmed.
    let tasks = vec![dep_task("a", "p", 50, &[]), dep_task("b", "p", 50, &[])];
    let (mut primary, _mesh) = primary_with_dag(tasks, &[], 1);
    assert!(!primary.prefer_dependency_gate_holds(), "gate disarmed precondition");
    let seq: Vec<bool> = (0..4)
        .map(|_| primary.prefer_dependency_for_decision())
        .collect();
    assert_eq!(
        seq,
        vec![false, false, false, false],
        "a disarmed gate never prefers dependency and never flips the toggle"
    );
}

/// PERF: the gate is re-evaluated at most once per W DECISIONS, not per
/// decision. With W=4, a freshly-armed verdict is cached for 4 decisions:
/// arming the gate, then DRAINING it to disarmed mid-window, must NOT flip
/// the cached verdict until the next W-boundary — proving the gate read is
/// amortized, not per-decision.
#[test]
fn gate_reevaluated_at_most_once_per_w_decisions() {
    // 4 workers → W=4, threshold 16. 1 ready producer + 1 blocked consumer:
    // 1 ready < 16 AND cons live → armed at decision 0 (count 0 % 4 == 0).
    let tasks = vec![
        dep_task("prod", "p", 50, &[]),
        dep_task("cons", "p", 50, &[("p", "prod")]),
    ];
    let (mut primary, _mesh) = primary_with_dag(tasks, &[], 4);
    // Decision 0: re-eval (count 0 % 4 == 0) → armed → flip → returns false,
    // toggle now true.
    assert!(!primary.prefer_dependency_for_decision(), "d0: armed, toggle was false");
    // Now DISARM the live source by completing prod's would-be path: remove
    // the blocked cons so the gate WOULD be disarmed if re-evaluated. Pop
    // prod (ready→in-flight) and finish it → cons unblocks → nothing blocked.
    let _ = primary.pool_mut().take_first_match(|t| t.task_id == "prod");
    primary.pool_mut().mark_in_flight(&PhaseId::from("p"));
    primary
        .pool_mut()
        .on_item_finished(&PhaseId::from("p"), Some("prod"));
    assert_eq!(primary.pool().blocked_len(), 0, "cons unblocked → gate WOULD disarm");
    assert!(
        !primary.prefer_dependency_gate_holds(),
        "the live gate read is now disarmed (no blocked task)"
    );
    // Decisions 1,2,3: still inside the W=4 window (counts 1,2,3 — not a
    // multiple of 4) → the CACHED armed verdict is read, NOT re-evaluated, so
    // the toggle keeps alternating from its armed state despite the live gate
    // being disarmed (the bounded staleness the 3W margin tolerates).
    assert!(primary.prefer_dependency_for_decision(), "d1: cached armed, toggle was true");
    assert!(!primary.prefer_dependency_for_decision(), "d2: cached armed, toggle was false");
    assert!(primary.prefer_dependency_for_decision(), "d3: cached armed, toggle was true");
    // Decision 4 (count 4 % 4 == 0): re-evaluate → now disarmed → false, and
    // the toggle is left untouched from here on.
    assert!(
        !primary.prefer_dependency_for_decision(),
        "d4: W-boundary re-eval picks up the disarmed verdict → false"
    );
    assert!(
        !primary.prefer_dependency_for_decision(),
        "d5: still disarmed"
    );
}

// ──────────────────────────────── the gate ────────────────────────────────

/// GATE HOLDS: ready pool shallow (< 4×workers) AND a live blocked task
/// exists. produce→consume DAG within one phase, 1 worker (threshold 4).
#[test]
fn gate_holds_when_ready_shallow_with_live_blocked() {
    // prod (ready) → cons (blocked on prod). 1 ready < 4×1; cons is live.
    let tasks = vec![
        dep_task("prod", "p", 50, &[]),
        dep_task("cons", "p", 50, &[("p", "prod")]),
    ];
    let (primary, _mesh) = primary_with_dag(tasks, &[], 1);
    assert_eq!(primary.pool().blocked_len(), 1, "cons hydrates blocked");
    assert!(
        primary.prefer_dependency_gate_holds(),
        "1 ready < 4×1 AND cons is a live blocked task → gate holds"
    );
}

/// GATE OFF (clause 1): the ready pool is DEEP (≥ 4×workers) even though a
/// live blocked task exists. Many ready producers, 1 worker (threshold 4).
#[test]
fn gate_off_when_ready_pool_is_deep() {
    let mut tasks = vec![dep_task("cons", "p", 50, &[("p", "prod0")])];
    // 5 ready producers (≥ 4×1). cons depends only on prod0 → still live.
    for i in 0..5 {
        tasks.push(dep_task(&format!("prod{i}"), "p", 50, &[]));
    }
    let (primary, _mesh) = primary_with_dag(tasks, &[], 1);
    assert!(
        !primary.prefer_dependency_gate_holds(),
        "5 ready ≥ 4×1 → clause 1 fails → gate off (deep breadth pool)"
    );
}

/// GATE OFF (clause 2): the ready pool is shallow but there is NO live
/// blocked task (nothing blocked at all).
#[test]
fn gate_off_when_no_live_blocked() {
    let tasks = vec![dep_task("a", "p", 50, &[]), dep_task("b", "p", 50, &[])];
    let (primary, _mesh) = primary_with_dag(tasks, &[], 1);
    assert_eq!(primary.pool().blocked_len(), 0, "nothing blocked");
    assert!(
        !primary.prefer_dependency_gate_holds(),
        "no live blocked task → clause 2 fails → gate off"
    );
}

/// GATE OFF: a 0-worker fleet makes the threshold 0, so clause 1 is
/// vacuously false — no workers, no starvation to prevent.
#[test]
fn gate_off_with_zero_workers() {
    let tasks = vec![
        dep_task("prod", "p", 50, &[]),
        dep_task("cons", "p", 50, &[("p", "prod")]),
    ];
    let (primary, _mesh) = primary_with_dag(tasks, &[], 0);
    assert!(
        !primary.prefer_dependency_gate_holds(),
        "0 workers → threshold 0 → clause 1 vacuously false → gate off"
    );
}

// ─────────────────────── the within-class selection bias ───────────────────

/// CORE + REVERT-CONFIRM. Two ready producers in the SAME free-pool class:
/// `prod_dep` (a direct prerequisite of the live blocked `cons`) and
/// `prod_free` (no dependent). With the bias armed the view floats
/// `prod_dep` to the front; WITHOUT the bias the view keeps FIFO order
/// (`prod_free` was extended first → leads). The revert-confirm is the
/// `false` call producing the un-biased order.
#[test]
fn armed_view_prefers_prerequisite_unarmed_keeps_fifo() {
    // Hydrate sorts size-DESC, so the LARGER `prod_free` (60) naturally leads
    // the free-pool class and the smaller `prod_dep` (50) trails. That makes
    // the un-biased order put the NON-prerequisite first, so the bias's
    // reorder (floating the prerequisite ahead) is observable.
    let tasks = vec![
        dep_task("prod_free", "p", 60, &[]),
        dep_task("prod_dep", "p", 50, &[]),
        dep_task("cons", "p", 50, &[("p", "prod_dep")]),
    ];
    let (primary, _mesh) = primary_with_dag(tasks, &[], 1);
    assert_eq!(primary.pool().blocked_len(), 1, "cons blocked on prod_dep");
    assert!(
        primary.prefer_dependency_gate_holds(),
        "2 ready < 4×1 AND cons live → gate holds (the bias is meaningful)"
    );

    // UNARMED (revert-confirm): the pre-#519 size-DESC order — prod_free (60)
    // leads, prod_dep (50) trails. cons is blocked → never in the view.
    let unarmed = view_ids(&primary, 0, false);
    assert_eq!(
        unarmed,
        vec!["prod_free".to_string(), "prod_dep".to_string()],
        "without the bias the view is the pre-#519 size-DESC order"
    );

    // ARMED: prod_dep (prerequisite of the live blocked cons) floats first,
    // OVERTAKING the larger prod_free that led the un-biased order — the
    // observable proof the bias reordered within the free-pool class.
    let armed = view_ids(&primary, 0, true);
    assert_eq!(
        armed,
        vec!["prod_dep".to_string(), "prod_free".to_string()],
        "the bias floats the prerequisite of a live blocked task to the front \
         of its soft-pin class, overtaking the larger non-prerequisite"
    );
}

/// The bias does NOT change the view when no candidate is a prerequisite of
/// a live blocked task (armed == unarmed). Pairs with the core test to pin
/// that the reorder is SPECIFIC to prerequisites, never a blanket shuffle.
#[test]
fn armed_view_unchanged_when_no_candidate_is_a_prerequisite() {
    // cons depends on a SEPARATE producer `root` that is already in flight
    // (popped), so neither queued task `x`/`y` is a prerequisite of cons.
    let tasks = vec![
        dep_task("root", "p", 50, &[]),
        dep_task("x", "p", 40, &[]),
        dep_task("y", "p", 30, &[]),
        dep_task("cons", "p", 50, &[("p", "root")]),
    ];
    let (mut primary, _mesh) = primary_with_dag(tasks, &[], 1);
    // Pop `root` so it leaves the queue (in flight): cons stays blocked on
    // the now-in-flight root; x and y are not prerequisites of anything.
    let _ = primary.pool_mut().take_first_match(|t| t.task_id == "root");
    primary.pool_mut().mark_in_flight(&PhaseId::from("p"));
    assert!(primary.prefer_dependency_gate_holds(), "x,y ready; cons live");
    let unarmed = view_ids(&primary, 0, false);
    let armed = view_ids(&primary, 0, true);
    assert_eq!(
        armed, unarmed,
        "no queued candidate is a prerequisite of the live blocked cons → \
         the armed view is identical to the unarmed view"
    );
    // And the order is the pinned size-DESC FIFO (x=40 before y=30).
    assert_eq!(armed, vec!["x".to_string(), "y".to_string()]);
}

/// END-TO-END refill: armed dispatch picks the prerequisite, and completing
/// it UNBLOCKS the consumer into the ready pool (the pipeline deepens →
/// breadth refills). The revert path (no prerequisite preference) is the
/// core test's unarmed branch; here we prove the downstream refill effect.
#[tokio::test(flavor = "current_thread")]
async fn armed_dispatch_of_prerequisite_refills_ready_pool() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // prod_free (60) leads the un-biased size-DESC order; the armed
            // bias floats the smaller prerequisite prod_dep (50) to index 0.
            let tasks = vec![
                dep_task("prod_free", "p", 60, &[]),
                dep_task("prod_dep", "p", 50, &[]),
                dep_task("cons", "p", 50, &[("p", "prod_dep")]),
            ];
            let (mut primary, _mesh) = primary_with_dag(tasks, &[], 1);
            assert_eq!(primary.pool().blocked_len(), 1, "cons starts blocked");

            // Armed view → prod_dep leads. Consume it (the scheduler picks
            // index 0) by taking the selected slot directly.
            let view = primary.dispatch_view_for_worker(0, true);
            let selection = view.select(0);
            let taken = primary.pool_mut().take_selected(selection);
            assert_eq!(
                taken.task_id, "prod_dep",
                "the armed view's index-0 candidate is the prerequisite"
            );

            // Completing prod_dep resolves cons's only dep → cons moves
            // blocked → ready, refilling the breadth pool.
            primary
                .pool_mut()
                .on_item_finished(&PhaseId::from("p"), Some("prod_dep"));
            assert_eq!(
                primary.pool().blocked_len(),
                0,
                "cons unblocked: the pipeline deepened and the ready pool refilled"
            );
            // cons is now a ready dispatchable candidate (alongside prod_free).
            let ready_now = view_ids(&primary, 0, false);
            assert!(
                ready_now.contains(&"cons".to_string()),
                "the freshly-unblocked cons is now in the ready dispatch view"
            );
        })
        .await;
}
