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

// =============================================================================
// #588 — phase-boundary gate on the drain transition.
//
// A barrier=False phase whose predecessor has not yet reached `Done` MUST NOT
// transition `Active → Drained` on an empty pool. Background:
// `set_no_barrier_phases` flips dependents `Blocked → Active` BEFORE the
// predecessor completes (the consumer's "streamed-injected" dep_graph pattern:
// tasks for the dependent are produced by the predecessor's `on_phase_end`
// hook or by tasks the predecessor schedules). The dependent's pool is
// legitimately empty for the gap between "phase activated" and "predecessor's
// hook injected the spawn batches". Transitioning `Drained` in that gap feeds
// the manager's drain-guard (`phase_can_proceed → RunShouldFail`) with a
// zero-tally phase and aborts the run before any work was even attempted —
// the consumer-reported regression of run_20260615_213039 (BUILD phase
// "reached drain with no terminal outcome (0 completed, 0 failed)" within ~5s
// of init).
//
// The fix gates `(0,0,0) → Drained` on the pool's `Done` view of `phase_deps`
// — the per-phase counterpart of `cluster_state.phases_ended`, paired at the
// same site via `mark_phase_done`. Empty + deps-done = legitimate drain;
// empty + deps-pending = hold at current state and re-evaluate when the
// predecessor edge re-triggers via the cascade's `drain_empty_active_phases`.

/// T1 — the consumer's exact regression scenario. A barrier=False BUILD phase
/// (`Active` from init because `set_no_barrier_phases` flipped its initial
/// `Blocked`) with an empty pool whose predecessor A has NOT yet ended must
/// NOT transition `Drained` — that would feed the manager's drain-guard with
/// a zero-tally phase and abort the run before A even started injecting.
/// Once A is `Done`, the re-trigger (the manager's cascade re-runs
/// `drain_empty_active_phases`) lets BUILD transition `Drained` legitimately
/// (the genuine "empty leaf with no injection" case the drain-guard surfaces
/// to the consumer as a fail-loud at the legitimate edge).
#[test]
fn empty_barrier_false_phase_with_pending_predecessor_does_not_drain_588() {
    // A → B, with B opted into barrier=False — A is the streamed-injection
    // predecessor (the consumer's DEPENDENCY_GRAPH role). B starts Active
    // because of `set_no_barrier_phases` even though A hasn't completed.
    let mut p = pool_with(&["A", "B"], &[("B", &["A"])]);
    p.set_no_barrier_phases([phase("B")]);
    assert_eq!(p.phase_state(&phase("A")), Some(PhaseState::Active));
    assert_eq!(
        p.phase_state(&phase("B")),
        Some(PhaseState::Active),
        "barrier=False phase starts Active per set_no_barrier_phases"
    );

    // The drain-empty pass runs at init (in the manager's hydrate +
    // post-extend cascade). With pre-#588 behaviour BOTH A AND B would
    // flip Drained here — A legitimately (no deps), B spuriously (deps
    // pending). POST-#588 only A drains; B is held at Active.
    p.drain_empty_active_phases();

    assert_eq!(
        p.phase_state(&phase("B")),
        Some(PhaseState::Active),
        "B must stay Active while predecessor A has not reached Done"
    );
    let drained = p.poll_drain_transitions();
    assert_eq!(
        drained,
        vec![phase("A")],
        "only A (no deps) drains; B held back (drained = {drained:?})"
    );

    // Mark A done — its lifecycle edge (the manager's
    // process_phase_lifecycle for A) flips it to Done.
    p.mark_phase_done(&phase("A"));
    assert_eq!(p.phase_state(&phase("A")), Some(PhaseState::Done));

    // The cascade's post-mark `drain_empty_active_phases` re-evaluates B.
    // B is STILL empty AND now its predecessor is Done — the legitimate
    // empty-leaf edge fires.
    p.drain_empty_active_phases();
    assert_eq!(
        p.phase_state(&phase("B")),
        Some(PhaseState::Drained),
        "B drains legitimately once predecessor is Done"
    );
    let drained_b = p.poll_drain_transitions();
    assert_eq!(drained_b, vec![phase("B")]);
}

/// T2 — the LEGITIMATE empty-phase case the gate must NOT regress. A
/// dependent phase whose predecessor is already `Done` (the standard cascade
/// shape after a barrier=True chain) drains normally on an empty pool — this
/// is the path `drain_empty_active_phases_cascades_to_first_populated_phase`
/// (above) already exercises, but pinned here as a regression guard for the
/// gate's "predecessors_done → drain proceeds" arm.
#[test]
fn empty_phase_with_completed_predecessor_drains_normally_588() {
    let mut p = pool_with(&["A", "B"], &[("B", &["A"])]);
    // A drains immediately (no items, no deps).
    p.drain_empty_active_phases();
    let _ = p.poll_drain_transitions(); // A
    p.mark_phase_done(&phase("A"));
    // B is now Active (the activation pass flipped it after A → Done).
    assert_eq!(p.phase_state(&phase("B")), Some(PhaseState::Active));

    p.drain_empty_active_phases();
    assert_eq!(
        p.phase_state(&phase("B")),
        Some(PhaseState::Drained),
        "empty phase with Done predecessor drains normally"
    );
    assert_eq!(p.poll_drain_transitions(), vec![phase("B")]);
}

/// T3 — barrier=False phase that receives streamed-injected items while its
/// predecessor is still active. The injection arrives via `extend`; the
/// phase's queued count goes (0 → ≥1) and the phase stays Active (never
/// touched the Drained edge). Then dispatch + complete drains it, and the
/// predecessor's PhaseEnded eventually arrives. This is the consumer's
/// SUCCESS path — tasks arrive in time, no spurious drain.
#[test]
fn barrier_false_phase_receives_injection_before_predecessor_ends_588() {
    let mut p = pool_with(&["A", "B"], &[("B", &["A"])]);
    p.set_no_barrier_phases([phase("B")]);

    // Init cascade — A drains immediately (no deps, empty); B is empty
    // + predecessor pending → held at Active. Mimic the manager's
    // process_phase_lifecycle but DO NOT yet mark A done (predecessor's
    // hook is still "running" — about to inject into B).
    p.drain_empty_active_phases();
    assert_eq!(p.poll_drain_transitions(), vec![phase("A")]);
    assert_eq!(p.phase_state(&phase("B")), Some(PhaseState::Active));

    // Streamed injection from A's hook lands while A is still in the
    // "drained-not-yet-done" window (the consumer's on_phase_end queues
    // SpawnTasks before mark_phase_done fires).
    p.extend([t("B", "T", "alpha", 1)]).expect("valid extend");
    assert_eq!(p.phase_state(&phase("B")), Some(PhaseState::Active));

    // Worker grabs B's item; the queue empties (in_flight = 1) so B
    // transitions Active → Draining. The Draining arm is unaffected by
    // the gate (live in-flight work, not a Drained edge).
    let item = p.pop_for_worker(1).unwrap();
    assert_eq!(p.in_flight(&phase("B")), 1);
    assert_eq!(p.phase_state(&phase("B")), Some(PhaseState::Draining));

    // B's task finishes BEFORE A ends. WITHOUT the gate the phase would
    // flip Drained immediately and fire the drain-guard against a phase
    // whose predecessor hasn't ended — the V-A2b race the manager-side
    // #584 gate catches via `phase_boundary_open`. The pool-side gate
    // holds it at the CURRENT state (`Draining` here) on the (0,0,0)
    // arm until A is Done — no spurious Drained edge.
    p.on_item_finished(&phase("B"), Some(&item.task_id));
    assert_eq!(
        p.phase_state(&phase("B")),
        Some(PhaseState::Draining),
        "post-finish empty-B held at Draining while predecessor A pending"
    );
    assert!(
        p.poll_drain_transitions().is_empty(),
        "no Drained edge until predecessor's PhaseEnded"
    );

    // A's lifecycle edge completes (mark_phase_done).
    p.mark_phase_done(&phase("A"));

    // Re-trigger via the manager's cascade — B now sees deps_done and
    // its post-finish (0,0,0) finally resolves to Drained.
    p.drain_empty_active_phases();
    assert_eq!(p.phase_state(&phase("B")), Some(PhaseState::Drained));
    assert_eq!(p.poll_drain_transitions(), vec![phase("B")]);
}

/// T4 — a non-leaf phase with no injection AND its predecessor has ended
/// SHOULD still drain through to the manager's drain-guard (which then
/// surfaces the legitimate "non-leaf phase that was never injected /
/// discovered" warning to the consumer). The gate must not suppress this:
/// once the predecessor is `Done`, the empty drain is no longer "by design
/// pending injection" — it is the real wedge the drain-guard is for.
#[test]
fn non_leaf_phase_with_no_injection_and_done_predecessor_still_drains_588() {
    // C depends on B which depends on A. C has dependents (a dummy D)
    // to make it "non-leaf" — but D's "blocked" floor is not the
    // discriminator here; the drain-guard's "non-leaf never injected"
    // surface is the manager's concern. The pool-level concern is just
    // that the empty drain still REACHES the manager.
    let mut p = pool_with(
        &["A", "B", "C", "D"],
        &[("B", &["A"]), ("C", &["B"]), ("D", &["C"])],
    );
    // Cascade A → B → C (all empty, no tasks anywhere).
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
    // All four phases drained + marked Done — the manager's drain-guard
    // would have seen each empty edge and (for a phase NOT declared
    // `may_be_empty`) surfaced RunShouldFail. The pool's only job is to
    // EMIT each Drained edge so the manager can apply policy; the gate
    // does NOT prevent that once predecessors are Done.
    for ph in ["A", "B", "C", "D"] {
        assert_eq!(p.phase_state(&phase(ph)), Some(PhaseState::Done));
    }
}

/// T5 — barrier=False phase with NON-empty pool at init must continue to
/// behave as before: queued items keep the phase Active, the gate is a
/// no-op on the live-work arms.
#[test]
fn barrier_false_phase_with_items_unaffected_by_gate_588() {
    let mut p = pool_with(&["A", "B"], &[("B", &["A"])]);
    p.set_no_barrier_phases([phase("B")]);
    // B has an item at init (e.g. the consumer eager-injected a sentinel
    // task — not the streamed pattern, but a legal barrier=False shape).
    p.extend([t("B", "T", "alpha", 1)]).expect("valid extend");

    p.drain_empty_active_phases();
    assert_eq!(
        p.phase_state(&phase("B")),
        Some(PhaseState::Active),
        "non-empty barrier=False phase stays Active (live queued work)"
    );
    // A (empty, no deps) drained on that pass; B is unaffected.
    assert_eq!(p.poll_drain_transitions(), vec![phase("A")]);

    // Now drain B's work fully WHILE A is still pending (not yet marked
    // Done — the hook window). Same hold as T3: pop flips Active →
    // Draining, then on_item_finished hits (0,0,0) but the gate holds
    // the state at Draining until A is Done.
    let item = p.pop_for_worker(1).unwrap();
    assert_eq!(p.phase_state(&phase("B")), Some(PhaseState::Draining));
    p.on_item_finished(&phase("B"), Some(&item.task_id));
    assert_eq!(
        p.phase_state(&phase("B")),
        Some(PhaseState::Draining),
        "post-finish empty B held at Draining while A still pending"
    );
    assert!(p.poll_drain_transitions().is_empty());

    // A's lifecycle edge completes — cascade lets B drain.
    p.mark_phase_done(&phase("A"));
    p.drain_empty_active_phases();
    assert_eq!(p.phase_state(&phase("B")), Some(PhaseState::Drained));
}
