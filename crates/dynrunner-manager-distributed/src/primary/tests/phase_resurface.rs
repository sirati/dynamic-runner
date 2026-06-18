//! Phase-drain LEVEL-NET: `process_phase_lifecycle` is purely event-driven
//! (its only callers are the bootstrap cascade and the `note_item_*` /
//! `note_affine_terminal` completion hooks). A phase whose LAST event left it
//! all-clear but stranded short of its drain edge — a momentarily-unsettled
//! counter at that instant, or a flipped-`Drained`-then-consumed race — has no
//! further event to re-surface it and would strand forever (the consumer's
//! matrix_eval freeze). These tests pin the restored level-net:
//!
//! - `next_phase_resurface_expiry` arms a BOUNDED wake while a phase is
//!   stranded and DISARMS (parks on `pending()`, no hot-spin) when none remain.
//! - the empty-poll re-surface inside `process_phase_lifecycle` re-flips /
//!   re-pushes a stranded phase and completes it through the EXISTING
//!   `on_phase_end` / `mark_phase_done` block (a 2-phase matrix_eval→dep_graph
//!   mirror: phase1's lost surface is recovered, phase1 ends, the dependent
//!   phase2 activates).

use super::*;

use dynrunner_core::{PhaseId, TaskDep, TypeId};
use dynrunner_scheduler_api::PhaseState;

/// A task with an explicit phase + caller-chosen id and fully-qualified deps.
fn cross_binary(phase: &str, id: &str, deps: &[(&str, &str)]) -> TaskInfo<TestId> {
    let mut t = make_binary(id, 100);
    t.phase_id = PhaseId::from(phase);
    t.type_id = TypeId::from("default");
    t.task_id = id.to_string();
    t.task_depends_on = deps
        .iter()
        .map(|(dp, dt)| TaskDep {
            task_id: (*dt).to_string(),
            phase_id: PhaseId::from(*dp),
            inherit_outputs: false,
            def_id: None,
        })
        .collect();
    t
}

fn make_primary() -> (
    PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    PrimaryMeshKeepalive,
) {
    let (transport, _ends) = setup_test(1);
    build_test_primary(
        PrimaryConfig::default(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    )
}

/// NO-HOT-SPIN (c): the level-trigger expiry is `None` (arm DISARMED, parks on
/// `pending()`) when no phase is stranded, and `Some(bounded)` while one is.
#[test]
fn phase_resurface_expiry_disarms_when_nothing_stuck() {
    let (mut primary, _mesh) = make_primary();

    // A phase carrying a single LIVE (Pending) task: not all-clear, so the
    // arm is DISARMED — re-evaluating the drain gate must not surface it.
    let live = cross_binary("compile", "live", &[]);
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::TaskAdded {
            hash: "live".into(),
            task: live,
            def_id: None,
        });
    }
    primary
        .hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");
    assert!(
        primary.next_phase_resurface_expiry().is_none(),
        "no stranded phase ⇒ the level-trigger arm parks on pending() (no hot-spin)"
    );

    // Drive the phase all-clear: dispatch (take it out of its bucket +
    // mark_in_flight) then complete its only task. WITHOUT the cascade running
    // (we never call process_phase_lifecycle), the phase reaches Drained via
    // the pool but its surface is stranded — the arm must ARM.
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::TaskCompleted {
            attempt: 0,
            hash: "live".into(),
            result_data: None,
        });
    }
    // `pop_for_worker` removes the item from its bucket AND bumps in_flight
    // (via `take_at`), so the phase is now (queued=0, in_flight=1) = Draining.
    let item = primary
        .pool_mut()
        .pop_for_worker(1)
        .expect("the live task is dispatchable");
    assert_eq!(item.task_id, "live");
    // The completion decrements in_flight back to 0 → all-clear → Drained.
    primary
        .pool_mut()
        .on_item_finished(&PhaseId::from("compile"), Some("live"));
    // Consume the surface to model the lost-surface race (Drained-not-Done,
    // nothing queued, no event to revive it).
    let _ = primary.pool_mut().poll_drain_transitions();
    assert_eq!(
        primary.pool().phase_state(&PhaseId::from("compile")),
        Some(PhaseState::Drained),
    );
    assert!(
        primary.next_phase_resurface_expiry().is_some(),
        "a stranded drain edge ⇒ the level-trigger arms a bounded wake"
    );
    // Bounded: the wake is within one re-poll interval of now (never `now`
    // raw, never unbounded) — the no-hot-spin property the arm relies on.
    let due = primary.next_phase_resurface_expiry().unwrap();
    assert!(
        due <= std::time::Instant::now() + primary.pool().phase_resurface_repoll_interval(),
        "the wake is bounded by the re-poll interval"
    );
}

/// 2-PHASE matrix_eval→dep_graph MIRROR (e): phase1 reaches all-clear (its work
/// completed) but its drain SURFACE is lost — the manager's
/// `poll_drain_transitions` consumed it without its cascade completing
/// `mark_phase_done`. The level-trigger re-enters `process_phase_lifecycle`,
/// whose empty-poll re-surface re-pushes phase1; the EXISTING drain-edge block
/// fires `on_phase_end(phase1)` and `mark_phase_done`, which activates phase2
/// (which depends on phase1). The end-to-end repro of the frozen run.
#[tokio::test]
async fn lost_surface_phase1_resurfaces_and_activates_phase2() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, _mesh) = make_primary();

            // matrix_eval has one completed work task; dep_graph depends on it
            // and owns a pending work task (the dispatch target once phase1
            // ends). No PhaseEnded(matrix_eval) fact ⇒ phase1 flows through the
            // live cascade (it is NOT seeded Done).
            let eval = cross_binary("matrix_eval", "eval-1", &[]);
            let graph = cross_binary("dep_graph", "graph-1", &[("matrix_eval", "eval-1")]);
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::PhaseDepsSet {
                    deps: HashMap::from([(
                        PhaseId::from("dep_graph"),
                        vec![PhaseId::from("matrix_eval")],
                    )]),
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: "eval-1".into(),
                    task: eval,
                    def_id: None,
                });
                cs.apply(ClusterMutation::TaskCompleted {
                    attempt: 0,
                    hash: "eval-1".into(),
                    result_data: None,
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: "graph-1".into(),
                    task: graph,
                    def_id: None,
                });
            }

            // Capture the on_phase_end firings.
            let ends = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
            let ends_cb = ends.clone();
            primary.on_phase_end = Some(Box::new(move |p: &PhaseId, _c, _f, _o| {
                ends_cb.lock().unwrap().push(p.to_string());
            }));

            primary
                .hydrate_from_cluster_state()
                .expect("test fixture: composed task graph is valid");

            // matrix_eval is all-terminal (one completion), no live affine, no
            // predecessor — it flips Drained on the empty-active drain.
            primary.pool_mut().drain_empty_active_phases();
            assert_eq!(
                primary.pool().phase_state(&PhaseId::from("matrix_eval")),
                Some(PhaseState::Drained),
            );
            assert_eq!(
                primary.pool().phase_state(&PhaseId::from("dep_graph")),
                Some(PhaseState::Blocked),
                "dep_graph waits on matrix_eval's Done"
            );

            // LOST SURFACE: consume matrix_eval off drained_pending WITHOUT
            // running the cascade — the flipped-then-consumed race that strands
            // the phase Drained-but-not-Done with no event to revive it.
            let consumed = primary.pool_mut().poll_drain_transitions();
            assert_eq!(consumed, vec![PhaseId::from("matrix_eval")]);
            assert!(!primary.pool().has_drained_pending());

            // Pre-fix: `process_phase_lifecycle`'s ordinary poll returns empty
            // and it breaks — matrix_eval is stranded forever, dep_graph never
            // activates, graph-1 never dispatches (the freeze). The level-net
            // recovers it via the IDLE `ARM_PHASE_RESURFACE` path: re-push the
            // flipped-then-consumed Drained phase, then re-enter the cascade so
            // the existing drain-edge block completes it. (Mirrors the arm body
            // in `operational_loop.rs`: `resurface_drained_pending` then
            // `process_phase_lifecycle`.)
            primary.pool_mut().resurface_drained_pending();
            primary.process_phase_lifecycle(&mut None).await;

            let fired = ends.lock().unwrap().clone();
            assert!(
                fired.iter().any(|p| p == "matrix_eval"),
                "the stranded phase's on_phase_end fired via the level-net; fired: {fired:?}"
            );
            assert_eq!(
                primary.pool().phase_state(&PhaseId::from("matrix_eval")),
                Some(PhaseState::Done),
                "matrix_eval reaches Done after the re-surface"
            );
            assert_eq!(
                primary.pool().phase_state(&PhaseId::from("dep_graph")),
                Some(PhaseState::Active),
                "dep_graph activates — its ready task can now dispatch"
            );
            // The freeze is over: dep_graph's work is dispatchable.
            assert!(
                !primary.pool().is_empty(),
                "graph-1 is real outstanding work the activated phase can dispatch"
            );
        })
        .await;
}
