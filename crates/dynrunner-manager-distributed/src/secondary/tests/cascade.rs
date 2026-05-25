//! Pool phase-cascade drain test.
//!
//! Verifies `cascade_drain_done` transitions phases whose only items
//! pre-completed elsewhere from Active to Done, unblocking dependents.

#![cfg(test)]

use super::super::test_helpers::TestId;

/// Regression: a promoted secondary's `populate_primary_from_cluster_state`
/// must transition phases whose ONLY items are pre-completed
/// elsewhere from Active to Done at construction time. Without the
/// cascade in `primary.rs`, dependent phases stay Blocked forever and
/// the primary hands out "no tasks" to every request — even
/// though the dependents have queued items.
///
/// Scenario mirrors dataset-peer's stuck-dispatch bug from
/// 2026-05-04 (b7fjzaqcg): two-phase graph, phase-A has 1 item that
/// already completed elsewhere (so the kept-set filters it out and
/// the pool's phase-A has 0 items + 0 in-flight), phase-B depends
/// on phase-A and has 1 queued item. Expected: phase-B is Active
/// after `cascade_drain_done`, so a downstream `take_first_match`
/// call would find the queued item dispatchable.
#[test]
fn cascade_drain_done_unblocks_dependent_when_parent_phase_is_empty() {
    use dynrunner_core::{PhaseId, SoftPreferredSecondaries, TaskInfo, TypeId};
    use dynrunner_scheduler_api::{PendingPool, PhaseState};
    use std::collections::{HashMap, HashSet};
    use std::path::PathBuf;

    let phase_a = PhaseId::from("phase-a");
    let phase_b = PhaseId::from("phase-b");
    let mut phase_ids = HashSet::new();
    phase_ids.insert(phase_a.clone());
    phase_ids.insert(phase_b.clone());
    let mut deps = HashMap::new();
    deps.insert(phase_b.clone(), vec![phase_a.clone()]);

    let mut pool = PendingPool::<TestId>::new(phase_ids, deps).expect("graph valid");

    // Phase-A's only item completed elsewhere → not in `items` (the
    // post-filter set passed to `extend`). Phase-B's queued item
    // mirrors the variant the dataset peer expected to dispatch.
    let item = TaskInfo {
        path: PathBuf::from("/some/binary"),
        size: 0,
        identifier: TestId("variant".into()),
        phase_id: phase_b.clone(),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: "variant".into(),
        task_depends_on: vec![],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        resolved_path: None,
    };
    pool.extend(vec![item]).expect("valid extend");

    // Pre-cascade: phase-A is Active (no deps, default), phase-B is
    // Blocked (waits for phase-A).
    assert_eq!(pool.phase_state(&phase_a), Some(PhaseState::Active));
    assert_eq!(pool.phase_state(&phase_b), Some(PhaseState::Blocked));

    super::super::primary::cascade_drain_done(&mut pool);

    // Post-cascade: phase-A is Done (0 queued, 0 in_flight ⇒ Drained
    // ⇒ Done) and phase-B is Active (parent is Done).
    assert_eq!(pool.phase_state(&phase_a), Some(PhaseState::Done));
    assert_eq!(pool.phase_state(&phase_b), Some(PhaseState::Active));
    assert_eq!(pool.len(), 1, "phase-B's variant must remain queued");
}
