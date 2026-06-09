//! Phase-state machine tests: `Active → Draining → Drained → Done`
//! transitions through `on_item_finished`, `requeue`, `release_worker`,
//! `poll_drain_transitions`, `mark_phase_done`, the empty-phase
//! drain helper, `reinject`, `drain_queued`, and the dependent-phase
//! activation cascade.

use super::{PhaseState, phase, pool_with, t};

#[test]
fn on_item_finished_drains_phase() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t("P", "T", "alpha", 1)]).expect("valid extend");
    let _ = p.pop_for_worker(1).unwrap();
    // Phase is Draining now (queue empty, in_flight = 1).
    assert_eq!(p.phase_state(&phase("P")), Some(PhaseState::Draining));
    p.on_item_finished(&phase("P"), None);
    assert_eq!(p.phase_state(&phase("P")), Some(PhaseState::Drained));
    assert_eq!(p.in_flight(&phase("P")), 0);
}

#[test]
fn requeue_inserts_at_front_and_flips_draining_back_to_active() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t("P", "T", "alpha", 1)]).expect("valid extend");
    let item = p.pop_for_worker(1).unwrap();
    assert_eq!(p.phase_state(&phase("P")), Some(PhaseState::Draining));
    p.requeue(item);
    assert_eq!(p.phase_state(&phase("P")), Some(PhaseState::Active));
    // Front of bucket is the requeued item.
    let again = p.pop_for_worker(1).unwrap();
    assert_eq!(again.size, 1);
}

#[test]
fn release_worker_unpins_only_if_last_pin() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t("P", "T", "alpha", 1),
        t("P", "T", "alpha", 2),
        t("P", "T", "alpha", 3),
    ])
    .expect("valid extend");
    // Worker 1 claims alpha. Worker 2 also picks (co-pin via step 4 after
    // the only typed bucket is already pinned).
    let _ = p.pop_for_worker(1).unwrap();
    let _ = p.pop_for_worker(2).unwrap();
    // Release worker 1 — bucket still has items, worker 2 still pinned.
    p.release_worker(1);
    // Worker 2's next pop should still come from alpha.
    let it = p.pop_for_worker(2).unwrap();
    assert_eq!(it.affinity_id.as_ref().unwrap().as_str(), "alpha");
}

#[test]
fn poll_drain_transitions_is_one_shot() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t("P", "T", "alpha", 1)]).expect("valid extend");
    let _ = p.pop_for_worker(1).unwrap();
    p.on_item_finished(&phase("P"), None);
    let first = p.poll_drain_transitions();
    assert_eq!(first, vec![phase("P")]);
    let second = p.poll_drain_transitions();
    assert!(second.is_empty());
}

#[test]
fn mark_phase_done_activates_dependents() {
    let mut p = pool_with(&["A", "B"], &[("B", &["A"])]);
    assert_eq!(p.phase_state(&phase("B")), Some(PhaseState::Blocked));
    p.mark_phase_done(&phase("A"));
    assert_eq!(p.phase_state(&phase("A")), Some(PhaseState::Done));
    assert_eq!(p.phase_state(&phase("B")), Some(PhaseState::Active));
}

/// `seed_completed_phases` (the failover-promotion hydration seeder) marks a
/// SET of phases `Done` at construction time WITHOUT a `Drained` edge, and
/// activates a live dependent whose deps are ALL in that set — regardless of
/// iteration order. Pins the two properties that distinguish it from a
/// per-phase `mark_phase_done` loop:
///   (1) NO `poll_drain_transitions` edge ever fires for the seeded phases
///       (a phase that starts `Done` is never `Drained`, so the manager's
///       cascade never re-fires `on_phase_end` for it); and
///   (2) the convergent activation pass flips a multi-dep dependent even
///       when the set is iterated worst-case (the dep marked last).
#[test]
fn seed_completed_phases_marks_done_without_drain_edge_and_activates_dependent() {
    // A,B,C complete; D (live) depends on BOTH B and C — the multi-dep case
    // a single ordering-naive `mark_phase_done` per phase could leave Blocked.
    let mut p = pool_with(&["A", "B", "C", "D"], &[("D", &["B", "C"])]);
    assert_eq!(p.phase_state(&phase("D")), Some(PhaseState::Blocked));

    // Seed completed phases in an order where D's deps are split (C last).
    p.seed_completed_phases([phase("A"), phase("B"), phase("C")]);

    for ph in ["A", "B", "C"] {
        assert_eq!(
            p.phase_state(&phase(ph)),
            Some(PhaseState::Done),
            "seeded phase {ph} must be Done"
        );
    }
    // D's deps (B, C) are both Done → convergent activation flips it Active.
    assert_eq!(
        p.phase_state(&phase("D")),
        Some(PhaseState::Active),
        "a live dependent whose ALL deps were seeded Done must activate"
    );
    // NO drain edge was ever recorded for the seeded phases — the manager's
    // cascade would never observe them via poll_drain_transitions, so
    // on_phase_end does NOT re-fire.
    assert!(
        p.poll_drain_transitions().is_empty(),
        "seeding Done must NOT push a Drained transition (no on_phase_end re-fire)"
    );
}

/// Empty `Active` phase transitions to `Drained` after
/// `drain_empty_active_phases`, and `poll_drain_transitions` reports
/// it. Without this, an empty phase-0 in a multi-phase chain would
/// never trigger `mark_phase_done` and dependents would stay
/// `Blocked` forever.
#[test]
fn drain_empty_active_phases_marks_empty_phase_drained() {
    let mut p = pool_with(&["A", "B"], &[("B", &["A"])]);
    // No items added — phase A is Active but empty.
    assert_eq!(p.phase_state(&phase("A")), Some(PhaseState::Active));
    p.drain_empty_active_phases();
    assert_eq!(p.phase_state(&phase("A")), Some(PhaseState::Drained));
    let drained = p.poll_drain_transitions();
    assert_eq!(drained, vec![phase("A")]);
}

/// Cascade: phase chain 0→1→2→3 with items only in phase 3 still
/// needs every empty intermediate phase to drain so view_for_worker
/// can see the phase-3 items. Mirrors the manager's
/// `process_phase_lifecycle` loop: drain empties, mark each done,
/// then re-drain the freshly-Active dependents until the chain
/// reaches the populated phase.
#[test]
fn drain_empty_active_phases_cascades_to_first_populated_phase() {
    let mut p = pool_with(
        &["P0", "P1", "P2", "P3"],
        &[("P1", &["P0"]), ("P2", &["P1"]), ("P3", &["P2"])],
    );
    p.extend([t("P3", "T", "", 1)]).expect("valid extend");
    // Initial state: only P0 Active (no deps); P1..P3 all Blocked.
    assert_eq!(p.phase_state(&phase("P0")), Some(PhaseState::Active));
    assert_eq!(p.phase_state(&phase("P3")), Some(PhaseState::Blocked));
    // view_for_worker on a fresh worker sees nothing — P3 isn't Active.
    assert!(p.view_for_worker(1, None).is_empty());

    // Cascade: drain P0 → mark Done → P1 Active → drain → ... → P3 Active.
    loop {
        p.drain_empty_active_phases();
        let drained = p.poll_drain_transitions();
        if drained.is_empty() {
            break;
        }
        for ph in &drained {
            p.mark_phase_done(ph);
        }
    }

    assert_eq!(p.phase_state(&phase("P0")), Some(PhaseState::Done));
    assert_eq!(p.phase_state(&phase("P1")), Some(PhaseState::Done));
    assert_eq!(p.phase_state(&phase("P2")), Some(PhaseState::Done));
    assert_eq!(p.phase_state(&phase("P3")), Some(PhaseState::Active));
    // Now phase-3 item is reachable.
    assert_eq!(p.view_for_worker(1, None).len(), 1);
}

/// `drain_empty_active_phases` must be a no-op when the Active
/// phase has queued items — wouldn't want to incorrectly drain
/// an in-use phase.
#[test]
fn drain_empty_active_phases_skips_phase_with_items() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t("P", "T", "", 1)]).expect("valid extend");
    p.drain_empty_active_phases();
    assert_eq!(p.phase_state(&phase("P")), Some(PhaseState::Active));
    assert!(p.poll_drain_transitions().is_empty());
}

#[test]
fn reinject_revives_drained_phase_to_active() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t("P", "T", "alpha", 1)]).expect("valid extend");
    let item = p.pop_for_worker(1).unwrap();
    p.on_item_finished(&phase("P"), None);
    assert_eq!(p.phase_state(&phase("P")), Some(PhaseState::Drained));
    p.reinject(item);
    assert_eq!(p.phase_state(&phase("P")), Some(PhaseState::Active));
    // No drained notification leaks through since reinject cleared it.
    assert!(p.poll_drain_transitions().is_empty());
    // The reinjected item is at the back of its bucket and dispatchable.
    let again = p.pop_for_worker(1).unwrap();
    assert_eq!(again.size, 1);
}

#[test]
fn drain_queued_empties_buckets_without_touching_inflight() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t("P", "T", "alpha", 1),
        t("P", "T", "beta", 2),
        t("P", "T", "", 3),
    ])
    .expect("valid extend");
    // Take one to bump in-flight.
    let _ = p.pop_for_worker(1).unwrap();
    let in_flight_before = p.in_flight(&phase("P"));
    let drained = p.drain_queued();
    assert_eq!(drained.len(), 2, "two queued items expected");
    assert_eq!(p.in_flight(&phase("P")), in_flight_before);
    // Bucket totals are now zero queued.
    assert_eq!(p.iter().count(), 0);
}

#[test]
fn activation_cascade_through_chain() {
    let mut p = pool_with(&["A", "B", "C"], &[("B", &["A"]), ("C", &["B"])]);
    p.mark_phase_done(&phase("A"));
    assert_eq!(p.phase_state(&phase("B")), Some(PhaseState::Active));
    assert_eq!(p.phase_state(&phase("C")), Some(PhaseState::Blocked));
    p.mark_phase_done(&phase("B"));
    assert_eq!(p.phase_state(&phase("C")), Some(PhaseState::Active));
}
