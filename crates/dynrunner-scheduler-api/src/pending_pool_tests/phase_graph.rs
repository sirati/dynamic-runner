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
