//! `phase_boundary_open` predicate (#584): the single phase-boundary-policy
//! seam consumed by `RunNarrator`'s start/complete gates and by the
//! coordinator's `phase_can_proceed` / `fire_initial_phase_starts`.
//!
//! Pins:
//!   * a phase with NO entry in `phase_deps` is vacuously open (the strict
//!     initial-active root);
//!   * a phase with ≥1 dep is open IFF every direct dep's `PhaseEnded` fact
//!     is in `phases_ended` (set-membership AND);
//!   * the predicate is STRICT regardless of `phase_no_barrier`: barrier=False
//!     is the I3 dispatch-authorization (the runtime-spawn interlock in
//!     `apply_spawn_tasks` and the pool's `set_no_barrier_phases` own that
//!     path), NOT a relaxation of the formal boundary;
//!   * the predicate reads only `phase_deps` + `phases_ended` — no live-bit
//!     consultation, no transitive walk (transitivity is implicit: a dep's
//!     own `PhaseEnded` could only have fired against ITS own boundary, so
//!     the predicate is closed under the dep chain by induction).

use super::*;

/// T1 (correctness): a phase with no `phase_deps` entry is vacuously open;
/// a phase whose lone dep has its `PhaseEnded` applied is open; a phase
/// whose lone dep has NOT had `PhaseEnded` applied is closed; a multi-dep
/// phase opens only once EVERY direct dep is ended (set-membership AND).
#[test]
fn phase_boundary_open_correctness() {
    let mut s = ClusterState::<RunnerIdentifier>::new();

    // No phase_deps entry → vacuously open.
    let root = PhaseId::from("root");
    assert!(
        s.phase_boundary_open(&root),
        "a phase with no declared deps has nothing to wait on"
    );

    // One dep, not yet ended → closed.
    let mid = PhaseId::from("mid");
    let tail = PhaseId::from("tail");
    s.apply(ClusterMutation::PhaseDepsSet {
        deps: HashMap::from([
            (mid.clone(), vec![root.clone()]),
            (tail.clone(), vec![mid.clone(), root.clone()]),
        ]),
    });
    assert!(
        !s.phase_boundary_open(&mid),
        "mid depends on root; root's PhaseEnded has not fired"
    );
    assert!(
        !s.phase_boundary_open(&tail),
        "tail depends on mid AND root; neither has fired"
    );

    // Ending only root → mid opens; tail still waits on mid.
    s.apply(ClusterMutation::PhaseEnded { phase: root.clone() });
    assert!(s.phase_boundary_open(&mid), "mid's lone dep ended");
    assert!(
        !s.phase_boundary_open(&tail),
        "tail still needs mid's PhaseEnded — AND semantics"
    );

    // Ending mid as well → tail opens (both deps ended).
    s.apply(ClusterMutation::PhaseEnded { phase: mid.clone() });
    assert!(
        s.phase_boundary_open(&tail),
        "every direct dep ended ⇒ multi-dep phase opens"
    );
}

/// barrier=False does NOT relax the formal-boundary predicate. The opt-in
/// authorizes I3 (early DISPATCH of P's tasks) via the runtime-spawn
/// interlock and the pool's `set_no_barrier_phases`; this predicate (the
/// I1/I2 enforcer) reads only `phase_deps` + `phases_ended` and ignores
/// the barrier set.
#[test]
fn phase_boundary_open_ignores_phase_no_barrier() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let build = PhaseId::from("build");
    let matrix_eval = PhaseId::from("matrix_eval");
    s.apply(ClusterMutation::PhaseDepsSet {
        deps: HashMap::from([(matrix_eval.clone(), vec![build.clone()])]),
    });
    // The consumer opts matrix_eval in to barrier=False (I3 authorization).
    s.apply(ClusterMutation::PhaseNoBarrierSet {
        phases: vec![matrix_eval.clone()],
    });
    assert!(
        s.phase_no_barrier(&matrix_eval),
        "the opt-in landed in the barrier set"
    );
    assert!(
        !s.phase_boundary_open(&matrix_eval),
        "barrier=False is I3 dispatch authorization, NOT I1/I2 boundary relaxation"
    );

    // The boundary opens only once the predecessor formally ends.
    s.apply(ClusterMutation::PhaseEnded {
        phase: build.clone(),
    });
    assert!(s.phase_boundary_open(&matrix_eval));
}
