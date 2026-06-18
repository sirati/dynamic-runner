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

/// Model-B AFFINE-ONLY PHASE drain gate (#affine-only-phase-drain).
///
/// Topology: an "import" phase whose ONLY content is a no-dep
/// `SecondaryAffine` import, with the dependent build in a SEPARATE downstream
/// "build" phase. The affine import is uncounted for phase drain
/// (`counts_for_phase_drain == false`), never `mark_in_flight`'d, and not
/// blocked — so the `(queued, in_flight, blocked)` counters
/// `maybe_transition_drain` keys on are all ZERO from SEED time, BEFORE the
/// import has been placed/dispatched/run. Pre-fix the import phase flipped
/// `Drained` immediately and the manager's drain-edge false-failed the run
/// against a still-live import.
///
/// Asserts the gate: the import phase must NOT drain while its import is live,
/// then MUST drain once `note_affine_terminal` records the import's first
/// per-secondary terminal — at which point the build phase activates.
#[test]
fn affine_only_phase_does_not_drain_until_import_terminals() {
    use dynrunner_core::{PhaseId, TaskDep, TaskInfo, TaskKind};

    // Build an affine import (no-dep) in the "import" phase.
    let mut import: TaskInfo<()> = t("import", "T", "", 1);
    import.task_id = "import-id".to_string();
    import.kind = TaskKind::SecondaryAffine;

    // A work dependent in the "build" phase depending on the import. Under
    // Model B the affine dep is excluded from the build's pool-blocking set
    // (the bitvector governs its dispatch), so the build is pool-ready on its
    // (empty) non-affine deps once "build" activates.
    let mut build: TaskInfo<()> = t("build", "T", "", 1);
    build.task_id = "build-id".to_string();
    build.kind = TaskKind::Work;
    build.task_depends_on = vec![TaskDep {
        task_id: "import-id".to_string(),
        phase_id: PhaseId::from("import"),
        inherit_outputs: false,
        def_id: None,
    }];

    let mut p = pool_with(&["import", "build"], &[("build", &["import"])]);
    // Mark the affine prereq BEFORE extend (the spawn/hydrate ordering the
    // manager guarantees) so the build's affine dep is excluded from its
    // blocking set and the drain guard recognises the token.
    p.mark_affine_prereqs(["import-id".to_string()]);
    p.extend([import, build]).expect("valid extend");

    // The import is a queued bucket token (uncounted), so the counters are
    // (0,0,0) for the import phase. WITHOUT the affine guard this would flip
    // Drained at seed; WITH it the phase stays Active (live import).
    p.drain_empty_active_phases();
    assert_eq!(
        p.phase_state(&phase("import")),
        Some(PhaseState::Active),
        "import phase must NOT drain while its affine import is live"
    );
    assert!(
        p.poll_drain_transitions().is_empty(),
        "no drain edge emitted while the import is live"
    );

    // The import's FIRST per-secondary terminal records its terminal in the
    // pool (phase-neutral: no in_flight decrement, no dependent unblock) and
    // re-runs the drain transition.
    p.note_affine_terminal(&phase("import"), "import-id");
    assert_eq!(
        p.phase_state(&phase("import")),
        Some(PhaseState::Drained),
        "import phase drains once its import terminals"
    );
    let drained = p.poll_drain_transitions();
    assert!(
        drained.contains(&phase("import")),
        "the drain edge for the import phase is now emitted"
    );

    // The manager's drain-edge marks the import phase Done; that activates the
    // build phase (dep satisfied). NON-globally-unblocking holds: the build was
    // never unblocked by the affine terminal itself (the bitvector governs that
    // — here we only assert the phase-level cascade).
    p.mark_phase_done(&phase("import"));
    assert_eq!(
        p.phase_state(&phase("build")),
        Some(PhaseState::Active),
        "build phase activates once the import phase is Done"
    );
}

/// SAME-PHASE topology (work + affine in ONE phase) must be UNCHANGED by the
/// affine guard: the WORK task is the drain gate, and the import's terminal
/// precedes the work's, so the guard is already `false` by the time the work
/// drains. Asserts the phase does NOT prematurely drain while the work is in
/// flight, and drains on the work's completion (after the import terminaled).
#[test]
fn same_phase_work_plus_affine_drains_on_work_completion() {
    use dynrunner_core::{PhaseId, TaskDep, TaskInfo, TaskKind};

    let mut import: TaskInfo<()> = t("P", "T", "", 1);
    import.task_id = "imp".to_string();
    import.kind = TaskKind::SecondaryAffine;

    let mut work: TaskInfo<()> = t("P", "T", "", 1);
    work.task_id = "wrk".to_string();
    work.kind = TaskKind::Work;
    work.task_depends_on = vec![TaskDep {
        task_id: "imp".to_string(),
        phase_id: PhaseId::from("P"),
        inherit_outputs: false,
        def_id: None,
    }];

    let mut p = pool_with(&["P"], &[]);
    p.mark_affine_prereqs(["imp".to_string()]);
    p.extend([import, work]).expect("valid extend");

    // The import terminals first (its per-secondary run precedes the work).
    p.note_affine_terminal(&phase("P"), "imp");
    // The work is still queued (counted), so the phase is Active — NOT drained.
    assert_eq!(p.phase_state(&phase("P")), Some(PhaseState::Active));

    // Dispatch + complete the work — the work is the drain gate. An affine-dep
    // work task is withheld from the global `pop_for_worker` view (the
    // bitvector governs its dispatch), so the manager dispatches it through the
    // per-secondary path: take it out of its bucket by hash + `mark_in_flight`,
    // exactly as `affine_dispatch::dispatch_affine_unit` does.
    let item = p
        .take_first_match(|item| item.task_id == "wrk")
        .expect("the work item is in its bucket");
    assert_eq!(item.task_id, "wrk");
    // `mark_in_flight` is a counter-only bump (the phase transition fires on
    // the cluster's finish report, not at dispatch), so the phase stays
    // `Active` while the work is in flight — unchanged by the affine guard.
    p.mark_in_flight(&phase("P"));
    assert_eq!(p.phase_state(&phase("P")), Some(PhaseState::Active));
    p.on_item_finished(&phase("P"), Some("wrk"));
    assert_eq!(
        p.phase_state(&phase("P")),
        Some(PhaseState::Drained),
        "same-phase work+affine drains on the work's completion (unchanged)"
    );
}

/// AFFINE-TERMINAL MIRROR — OPT-2 (sticky global terminal): an affine import's
/// GLOBAL terminal must NOT be un-completed by a reinject. A `SecondaryAffine`
/// token has no regenerable global output (its dependents' readiness is the
/// per-secondary bitvector), so `reinject` must leave its `completed_tasks`
/// entry intact and NOT re-block its dependents — the
/// `reblock_dependents_on_uncompleted` skip for `affine_prereq_ids`.
///
/// Pre-fix: the reinject un-completed the affine, Gate B
/// (`phase_has_live_affine_prereq`) flipped back TRUE, and the import phase was
/// held live forever (the lost-mirror strand). Post-fix Gate B stays FALSE.
#[test]
fn affine_global_terminal_is_sticky_across_reinject() {
    use dynrunner_core::{TaskInfo, TaskKind};

    let mut import: TaskInfo<()> = t("import", "T", "", 1);
    import.task_id = "import-id".to_string();
    import.kind = TaskKind::SecondaryAffine;

    let mut p = pool_with(&["import"], &[]);
    p.mark_affine_prereqs(["import-id".to_string()]);
    p.extend([import.clone()]).expect("valid extend");

    // The import terminals globally — recorded in the pool's completed set, so
    // Gate B is FALSE for the import phase.
    p.note_affine_terminal(&phase("import"), "import-id");
    assert!(
        !p.phase_has_live_affine_prereq_for_test(&phase("import")),
        "Gate B is clear after the import's global terminal"
    );

    // Reinject the SAME affine token (an operator/spawn-batch reinject funnels
    // through `reblock_dependents_on_uncompleted`). OPT-2 skips the
    // un-complete for the affine id, so Gate B MUST stay FALSE.
    p.reinject(std::sync::Arc::new(import));
    assert!(
        !p.phase_has_live_affine_prereq_for_test(&phase("import")),
        "OPT-2: an affine global terminal is sticky — reinject must NOT \
         un-complete it (Gate B stays clear)"
    );
}

/// AFFINE-TERMINAL MIRROR — #617 preserved: a genuinely NON-terminal affine
/// import (never run, neither completed nor failed) must KEEP Gate B held. The
/// mirror fixes only converge the pool to the GLOBAL terminal truth; they must
/// not relax the guard for an import that has not reached any terminal.
#[test]
fn affine_non_terminal_holds_gate_b() {
    use dynrunner_core::{TaskInfo, TaskKind};

    let mut import: TaskInfo<()> = t("import", "T", "", 1);
    import.task_id = "import-id".to_string();
    import.kind = TaskKind::SecondaryAffine;

    let mut p = pool_with(&["import"], &[]);
    p.mark_affine_prereqs(["import-id".to_string()]);
    p.extend([import]).expect("valid extend");

    // No terminal recorded — the import is genuinely live.
    assert!(
        p.phase_has_live_affine_prereq_for_test(&phase("import")),
        "#617: a never-run affine import holds Gate B (genuinely non-terminal)"
    );
}

/// AFFINE-TERMINAL MIRROR — failed-path twin: a genuinely-FAILED affine import
/// (recorded via `note_affine_failed`, the global all-`Failed` terminal) must
/// clear Gate B so its phase can drain past it. Pre-fix the failed path
/// recorded NOTHING in the pool, so a globally-failed affine held Gate B
/// forever (the failed-terminal twin of the missing complete-path mirror).
#[test]
fn affine_global_failure_clears_gate_b_and_drains() {
    use dynrunner_core::{TaskInfo, TaskKind};

    let mut import: TaskInfo<()> = t("import", "T", "", 1);
    import.task_id = "import-id".to_string();
    import.kind = TaskKind::SecondaryAffine;

    let mut p = pool_with(&["import"], &[]);
    p.mark_affine_prereqs(["import-id".to_string()]);
    p.extend([import]).expect("valid extend");

    // The import phase is held Active by Gate B while the import is live.
    p.drain_empty_active_phases();
    assert_eq!(p.phase_state(&phase("import")), Some(PhaseState::Active));
    assert!(p.phase_has_live_affine_prereq_for_test(&phase("import")));

    // The import reaches its GLOBAL terminal-FAILURE (failed on every eligible
    // secondary). `note_affine_failed` records it in the pool's failed set and
    // re-runs the drain transition.
    p.note_affine_failed(&phase("import"), "import-id");
    assert!(
        !p.phase_has_live_affine_prereq_for_test(&phase("import")),
        "failed-path: a globally-failed affine clears Gate B"
    );
    assert_eq!(
        p.phase_state(&phase("import")),
        Some(PhaseState::Drained),
        "the import phase drains once its import reaches a global terminal failure"
    );
    assert!(
        p.poll_drain_transitions().contains(&phase("import")),
        "the drain edge for the now-failed import phase is emitted"
    );
}

/// PHASE-DRAIN LEVEL-NET (a): a phase driven genuinely all-clear (terminal
/// work, no live affine, predecessors done) whose drain SURFACE was lost — the
/// `Drained` transition fired and was consumed off `drained_pending` by
/// `poll_drain_transitions` WITHOUT the manager's cascade completing
/// `mark_phase_done`. The ordinary machinery cannot re-surface it (the next
/// event-driven `poll_drain_transitions` returns empty; `maybe_transition_drain`
/// early-returns for an already-`Drained` phase). `phases_stuck_drainable` must
/// report it, and `resurface_drained_pending` must re-queue it so the manager's
/// ordinary drain-edge block runs unchanged.
///
/// Pre-fix: stranded forever (no event re-runs the check, the phase never
/// reaches `Done`, its dependents never activate — the matrix_eval freeze).
#[test]
fn stuck_drained_phase_resurfaces_after_lost_surface() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t("P", "T", "alpha", 1)]).expect("valid extend");
    let item = p.pop_for_worker(1).unwrap();
    p.on_item_finished(&phase("P"), Some(&item.task_id));
    assert_eq!(p.phase_state(&phase("P")), Some(PhaseState::Drained));

    // Simulate the LOST surface: the manager's `poll_drain_transitions`
    // consumed the phase off `drained_pending` (mem::take) but its cascade did
    // not reach `mark_phase_done` (a flipped-then-consumed race) — the phase
    // is `Drained`-but-not-`Done`, no longer queued, with no event to revive it.
    let consumed = p.poll_drain_transitions();
    assert_eq!(consumed, vec![phase("P")]);
    assert!(!p.has_drained_pending(), "surface consumed, none queued");
    assert_eq!(p.phase_state(&phase("P")), Some(PhaseState::Drained));

    // The level-net query reports the stranded phase (Drained-but-not-Done).
    assert_eq!(
        p.phases_stuck_drainable(),
        vec![phase("P")],
        "a Drained-but-not-Done phase with a lost surface is stuck-drainable"
    );

    // The drain-edge re-surface (the empty-active re-run cannot reach a
    // non-Active/Draining phase, so the re-push is the operative half here)
    // re-queues it for the manager's ordinary cascade.
    p.drain_empty_active_phases();
    assert!(
        p.resurface_drained_pending(),
        "the stranded Drained phase is re-pushed onto drained_pending"
    );
    assert_eq!(
        p.poll_drain_transitions(),
        vec![phase("P")],
        "the re-pushed phase is now observable by the manager's drain-edge block"
    );
}

/// PHASE-DRAIN LEVEL-NET (b): the never-flipped transient. A phase left
/// `Active` (or `Draining`) and all-clear — its last `maybe_transition_drain`
/// ran at an instant a counter was momentarily unsettled, so it never flipped
/// `Drained` and was never pushed onto `drained_pending`. Nothing re-runs the
/// check once the event stream stops. `phases_stuck_drainable` reports it, and
/// `drain_empty_active_phases` re-flips it.
#[test]
fn stuck_active_all_clear_phase_resurfaces() {
    // A zero-dep phase seeds `Active` and, with no items ever committed, is
    // (0,0,0) all-clear immediately — but `maybe_transition_drain` only runs
    // on a pool mutation, so without an explicit `drain_empty_active_phases`
    // call nothing flips it. This is the never-flipped transient: an Active
    // all-clear phase with no event to surface it.
    let mut p = pool_with(&["P"], &[]);
    assert_eq!(p.phase_state(&phase("P")), Some(PhaseState::Active));
    assert_eq!(p.queued_count(&phase("P")), 0);
    assert_eq!(p.in_flight(&phase("P")), 0);
    assert!(!p.has_drained_pending(), "never flipped → nothing queued");

    assert_eq!(
        p.phases_stuck_drainable(),
        vec![phase("P")],
        "an Active all-clear phase that never flipped is stuck-drainable"
    );
    p.drain_empty_active_phases();
    assert_eq!(
        p.phase_state(&phase("P")),
        Some(PhaseState::Drained),
        "drain_empty_active_phases re-flips the never-flipped phase"
    );
    assert!(
        p.has_drained_pending(),
        "the re-flipped phase is queued for the manager's drain-edge block"
    );
}

/// PHASE-DRAIN LEVEL-NET (d) / #617: the re-surface query RE-EVALUATES the
/// same gates, RELAXING none — an affine-only phase whose import has NOT run
/// reads `phase_has_live_affine_prereq == true` and must NOT be reported as
/// stuck-drainable (surfacing it would false-drain a phase with live work).
/// After the import terminals it becomes genuinely drainable and IS reported.
#[test]
fn affine_only_phase_not_stuck_drainable_until_import_terminals() {
    use dynrunner_core::{PhaseId, TaskDep, TaskInfo, TaskKind};

    let mut import: TaskInfo<()> = t("import", "T", "", 1);
    import.task_id = "import-id".to_string();
    import.kind = TaskKind::SecondaryAffine;

    let mut build: TaskInfo<()> = t("build", "T", "", 1);
    build.task_id = "build-id".to_string();
    build.kind = TaskKind::Work;
    build.task_depends_on = vec![TaskDep {
        task_id: "import-id".to_string(),
        phase_id: PhaseId::from("import"),
        inherit_outputs: false,
        def_id: None,
    }];

    let mut p = pool_with(&["import", "build"], &[("build", &["import"])]);
    p.mark_affine_prereqs(["import-id".to_string()]);
    p.extend([import, build]).expect("valid extend");

    // The import phase reads (0,0,0) on the raw counters but holds a LIVE
    // affine import → the level-net query must NOT surface it (#617: no
    // premature drain). The build phase is Blocked (its dep is not Done).
    assert!(
        !p.phases_stuck_drainable().contains(&phase("import")),
        "an affine-only phase with a live import is NOT stuck-drainable (#617)"
    );

    // After the import's first per-secondary terminal, the phase is genuinely
    // all-clear and IS reported as stuck-drainable (so the level-net surfaces
    // it should its ordinary drain event be missed).
    p.note_affine_terminal(&phase("import"), "import-id");
    // `note_affine_terminal` re-runs the transition, so the phase is already
    // Drained-and-queued here; consume the surface to model a lost-surface
    // race and confirm the query still reports it.
    let _ = p.poll_drain_transitions();
    assert!(
        p.phases_stuck_drainable().contains(&phase("import")),
        "once the import terminals the phase is genuinely stuck-drainable"
    );
}
