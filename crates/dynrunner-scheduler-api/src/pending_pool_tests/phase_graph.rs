//! Tests for `PendingPool::new` — phase-graph validation (cycle
//! detection, unknown dep rejection) and the initial `PhaseState`
//! assignment based on the dependency closure.

use std::collections::HashMap;

use dynrunner_core::PhaseId;

use super::{PendingPool, PendingPoolError, PhaseState, phase, pool_with};

#[test]
fn new_rejects_dependency_cycle() {
    let mut deps: HashMap<PhaseId, Vec<PhaseId>> = HashMap::new();
    deps.insert(phase("B"), vec![phase("A")]);
    deps.insert(phase("C"), vec![phase("B")]);
    deps.insert(phase("A"), vec![phase("C")]);
    let res = PendingPool::<()>::new([phase("A"), phase("B"), phase("C")], deps);
    assert!(matches!(res, Err(PendingPoolError::DependencyCycle(_))));
}

#[test]
fn new_rejects_unknown_dependency() {
    let mut deps: HashMap<PhaseId, Vec<PhaseId>> = HashMap::new();
    deps.insert(phase("B"), vec![phase("Z")]);
    let res = PendingPool::<()>::new([phase("A"), phase("B")], deps);
    assert!(matches!(res, Err(PendingPoolError::UnknownDependency(_))));
}

#[test]
fn new_initial_states_active_for_zero_deps_blocked_otherwise() {
    let p = pool_with(&["A", "B", "C"], &[("B", &["A"]), ("C", &["B"])]);
    assert_eq!(p.phase_state(&phase("A")), Some(PhaseState::Active));
    assert_eq!(p.phase_state(&phase("B")), Some(PhaseState::Blocked));
    assert_eq!(p.phase_state(&phase("C")), Some(PhaseState::Blocked));
}

#[test]
fn set_no_barrier_phases_flips_blocked_to_active() {
    // A normally-`Blocked` phase (it has deps) flips to `Active` when
    // declared no-barrier — the `PhaseSpec(barrier=False)` opt-in. Its
    // sibling (still barrier=True) stays `Blocked`. The zero-dep phase
    // was already `Active`; the no-barrier flag is a no-op for it
    // (idempotent on `Active`).
    let mut p = pool_with(&["A", "B", "C"], &[("B", &["A"]), ("C", &["B"])]);
    p.set_no_barrier_phases([phase("B")]);
    assert_eq!(p.phase_state(&phase("A")), Some(PhaseState::Active));
    assert_eq!(p.phase_state(&phase("B")), Some(PhaseState::Active));
    assert_eq!(p.phase_state(&phase("C")), Some(PhaseState::Blocked));
}

#[test]
fn set_no_barrier_phases_idempotent_and_ignores_unknown() {
    // Calling twice has no further effect (the second call's `Blocked`
    // check fails — the phase is already `Active`). An unknown phase id
    // is silently ignored — defensive, since the barrier set and the
    // pool's phase set both derive from `get_phases()`.
    let mut p = pool_with(&["A", "B"], &[("B", &["A"])]);
    p.set_no_barrier_phases([phase("B"), phase("ZZZ")]);
    p.set_no_barrier_phases([phase("B")]);
    assert_eq!(p.phase_state(&phase("A")), Some(PhaseState::Active));
    assert_eq!(p.phase_state(&phase("B")), Some(PhaseState::Active));
    assert_eq!(p.phase_state(&phase("ZZZ")), None);
}

#[test]
fn set_no_barrier_does_not_disturb_done_or_active_phases() {
    // A phase that has been advanced past `Blocked` (e.g. legitimately
    // activated through `mark_phase_done` of its deps) is NOT regressed
    // by a later no-barrier flag — the setter only flips `Blocked`
    // states. Pins that a misordered manager call cannot rewind a phase
    // out of `Active`/`Draining`/`Done`.
    let mut p = pool_with(&["A", "B"], &[("B", &["A"])]);
    p.mark_phase_done(&phase("A")); // B flips Blocked → Active naturally.
    assert_eq!(p.phase_state(&phase("B")), Some(PhaseState::Active));
    p.set_no_barrier_phases([phase("B")]); // No-op.
    assert_eq!(p.phase_state(&phase("B")), Some(PhaseState::Active));
}
