//! Task-level dependency tests (`task_id` + `task_depends_on`):
//! extend-time validation (unknown deps, cycles, duplicate ids),
//! blocked-then-unblocked dispatch, FRONT-of-bucket unblocking
//! position, pre-seeded completion via `mark_tasks_completed`, the
//! permanent-failure cascade, and the `update_first_match_in_place`
//! primitive (which scans both queued buckets and the blocked map).

use dynrunner_core::{PhaseId, SoftPreferredSecondaries, TaskDep, TaskInfo};

use super::{PendingPoolError, phase, pool_with, t};

/// Test fixture variant carrying a caller-chosen task_id and
/// (optional) deps. Overrides the synthetic id `t(...)` assigns so
/// tests that assert against the id can pin a stable value.
fn t_with_id(
    phase: &str,
    ty: &str,
    affinity: &str,
    size: u64,
    id: &str,
    deps: &[&str],
) -> TaskInfo<()> {
    let mut item = t(phase, ty, affinity, size);
    item.task_id = id.to_string();
    item.task_depends_on = deps
        .iter()
        .map(|d| TaskDep {
            task_id: d.to_string(),
            phase_id: PhaseId::from(phase),
            inherit_outputs: false,
        })
        .collect();
    item
}

#[test]
fn task_deps_unknown_id_fails_extend() {
    let mut p = pool_with(&["P"], &[]);
    let res = p.extend([t_with_id("P", "T", "", 1, "child", &["nope"])]);
    match res {
        Err(PendingPoolError::UnknownTaskDep {
            task,
            referenced_by,
        }) => {
            assert_eq!(task, "nope");
            assert_eq!(referenced_by, "child");
        }
        other => panic!("expected UnknownTaskDep, got {:?}", other),
    }
}

#[test]
fn task_deps_cycle_fails_extend() {
    let mut p = pool_with(&["P"], &[]);
    let res = p.extend([
        t_with_id("P", "T", "", 1, "a", &["b"]),
        t_with_id("P", "T", "", 1, "b", &["a"]),
    ]);
    assert!(matches!(res, Err(PendingPoolError::TaskDepCycle(_))));
}

#[test]
fn task_deps_duplicate_id_fails_extend() {
    let mut p = pool_with(&["P"], &[]);
    let res = p.extend([
        t_with_id("P", "T", "", 1, "dup", &[]),
        t_with_id("P", "T", "", 1, "dup", &[]),
    ]);
    assert!(matches!(res, Err(PendingPoolError::DuplicateTaskId(_))));
}

#[test]
fn task_deps_blocked_until_dep_completes() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t_with_id("P", "T", "", 1, "a", &[]),
        t_with_id("P", "T", "", 1, "b", &["a"]),
    ])
    .expect("valid extend");
    // Bucket iteration only sees A; B is in the blocked map.
    let queued_ids: Vec<_> = p.iter().map(|i| i.task_id.clone()).collect();
    assert_eq!(queued_ids, vec!["a".to_string()]);
    let first = p.pop_for_worker(1).expect("a is dispatchable");
    assert_eq!(first.task_id, "a");
    // No more queued items; B still blocked, phase still has work.
    assert!(p.pop_for_worker(1).is_none());
    p.on_item_finished(&phase("P"), Some("a"));
    // Now B is unblocked.
    let second = p.pop_for_worker(1).expect("b unblocked");
    assert_eq!(second.task_id, "b");
}

#[test]
fn task_deps_unblocked_lands_at_bucket_front() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t_with_id("P", "T", "alpha", 1, "a", &[]),
        t_with_id("P", "T", "alpha", 1, "b", &["a"]),
        t_with_id("P", "T", "alpha", 1, "c", &[]),
    ])
    .expect("valid extend");
    // A is dispatched first (it's in front of C; B is blocked).
    let a = p.pop_for_worker(1).expect("a");
    assert_eq!(a.task_id, "a");
    // Finish A → B unblocks and lands at the FRONT of the bucket,
    // ahead of C which has been queued behind A all along.
    p.on_item_finished(&phase("P"), Some("a"));
    let next = p.pop_for_worker(1).expect("b before c");
    assert_eq!(next.task_id, "b");
    let last = p.pop_for_worker(1).expect("c last");
    assert_eq!(last.task_id, "c");
}

#[test]
fn task_deps_seeded_completed_id_resolves_unknown_dep() {
    // Failover-resume regression: a promoted secondary rebuilds its
    // pool from a wire snapshot that has dropped pre-completed items
    // from the items vec. Surviving items that declared
    // `task_depends_on` against those completions used to fail
    // `extend()` with `UnknownTaskDep`. `mark_tasks_completed`
    // seeds the completion set so validation resolves.
    let mut p = pool_with(&["P"], &[]);
    p.mark_tasks_completed(["toolchain".to_string()]);
    p.extend([t_with_id("P", "T", "", 1, "variant", &["toolchain"])])
        .expect("variant resolves against pre-seeded toolchain completion");
    // Variant is dispatchable immediately because its only dep is
    // already considered complete.
    let item = p.pop_for_worker(1).expect("variant ready");
    assert_eq!(item.task_id, "variant");
}

#[test]
fn task_deps_cascade_fail_on_permanent_prereq_failure() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t_with_id("P", "T", "", 1, "a", &[]),
        t_with_id("P", "T", "", 1, "b", &["a"]),
        t_with_id("P", "T", "", 1, "c", &["b"]),
    ])
    .expect("valid extend");
    let a = p.pop_for_worker(1).expect("a");
    assert_eq!(a.task_id, "a");
    let cascaded = p.on_item_failed_permanent(&phase("P"), "a");
    let mut cascaded_ids: Vec<_> = cascaded.iter().map(|i| i.task_id.clone()).collect();
    cascaded_ids.sort();
    assert_eq!(cascaded_ids, vec!["b".to_string(), "c".to_string()]);
    // No queued items remain — B and C never made it into a bucket.
    assert!(p.pop_for_worker(1).is_none());
}

#[test]
fn update_first_match_in_place_mutates_queued_match() {
    // Three items in two buckets; predicate matches the middle one.
    // The closure flips `preferred_secondaries` on the matched
    // entry and leaves the rest alone.
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t_with_id("P", "T", "", 1, "a", &[]),
        t_with_id("P", "T", "", 1, "b", &[]),
        t_with_id("P", "T", "", 1, "c", &[]),
    ])
    .expect("valid extend");
    let updated = p.update_first_match_in_place(
        |t| t.task_id == "b",
        |t| {
            t.preferred_secondaries = SoftPreferredSecondaries::new(vec!["sec-x".into()]);
        },
    );
    assert!(updated, "predicate must match `b`");
    let prefs: Vec<_> = p
        .iter()
        .map(|t| {
            (
                t.task_id.clone(),
                t.preferred_secondaries.as_slice().to_vec(),
            )
        })
        .collect();
    // Only `b` mutated; `a` and `c` stay default-empty.
    let by_id: std::collections::HashMap<_, _> = prefs.into_iter().collect();
    assert!(by_id["a"].is_empty());
    assert_eq!(by_id["b"], vec!["sec-x".to_string()]);
    assert!(by_id["c"].is_empty());
}

#[test]
fn update_first_match_in_place_visits_blocked_items() {
    // `b` depends on the still-pending `a`, so `b` lives in
    // `blocked` (not in any bucket). The update primitive must
    // still find and mutate it — operator-side preference updates
    // should land on blocked entries the moment they're queued
    // back into a bucket, not on a stale clone.
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t_with_id("P", "T", "", 1, "a", &[]),
        t_with_id("P", "T", "", 1, "b", &["a"]),
    ])
    .expect("valid extend");
    // Sanity: `b` is not in the bucket-side iter (it's in `blocked`).
    let ids_in_buckets: Vec<_> = p.iter().map(|t| t.task_id.clone()).collect();
    assert_eq!(ids_in_buckets, vec!["a".to_string()]);
    let updated = p.update_first_match_in_place(
        |t| t.task_id == "b",
        |t| {
            t.preferred_secondaries = SoftPreferredSecondaries::new(vec!["sec-y".into()]);
        },
    );
    assert!(updated, "predicate must match blocked `b`");
    // Pop `a` to unblock `b`, then verify `b`'s preference survived.
    p.pop_for_worker(1).expect("a");
    p.on_item_finished(&phase("P"), Some("a"));
    let b = p.pop_for_worker(1).expect("b unblocked");
    assert_eq!(b.task_id, "b");
    assert_eq!(
        b.preferred_secondaries.as_slice(),
        &["sec-y".to_string()][..]
    );
}

#[test]
fn update_first_match_in_place_returns_false_on_no_match() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t_with_id("P", "T", "", 1, "a", &[])])
        .expect("valid");
    let updated = p.update_first_match_in_place(
        |t| t.task_id == "nonexistent",
        |t| {
            t.preferred_secondaries = SoftPreferredSecondaries::new(vec!["never".into()]);
        },
    );
    assert!(!updated, "no match → false");
    // `a` untouched.
    let a = p.pop_for_worker(1).expect("a");
    assert!(a.preferred_secondaries.as_slice().is_empty());
}

/// Fixture variant carrying a caller-chosen `(phase, task_id)` and a
/// list of fully-qualified `(dep_phase, dep_task_id)` deps so
/// cross-phase identity can be expressed (the shared `t_with_id` pins
/// each dep's phase to the item's own phase).
fn t_cross(phase: &str, id: &str, deps: &[(&str, &str)]) -> TaskInfo<()> {
    let mut item = t(phase, "T", "", 1);
    item.task_id = id.to_string();
    item.task_depends_on = deps
        .iter()
        .map(|(dp, dt)| TaskDep {
            task_id: dt.to_string(),
            phase_id: PhaseId::from(*dp),
            inherit_outputs: false,
        })
        .collect();
    item
}

/// `extend` keys duplicate detection on the FULL `(phase_id, task_id)`
/// identity (agreeing with `partition_ingest`): the SAME `task_id` in
/// two DIFFERENT phases is a DISTINCT task, NOT a duplicate. Pre-fix
/// `extend` dedup'd on the bare `task_id` and FALSE-rejected this batch
/// with `DuplicateTaskId`.
#[test]
fn extend_cross_phase_same_task_id_is_not_a_duplicate() {
    let mut p = pool_with(&["A", "B"], &[]);
    p.extend([t_cross("A", "shared", &[]), t_cross("B", "shared", &[])])
        .expect("cross-phase same task_id must NOT be a duplicate");
    // Both landed (one per phase bucket).
    let ids: Vec<_> = p.iter().map(|i| i.task_id.clone()).collect();
    assert_eq!(ids, vec!["shared".to_string(), "shared".to_string()]);
}

/// `extend`'s within-batch duplicate detection still fires when the
/// FULL identity collides (same phase AND task_id).
#[test]
fn extend_same_phase_same_task_id_is_a_duplicate() {
    let mut p = pool_with(&["A"], &[]);
    let res = p.extend([t_cross("A", "dup", &[]), t_cross("A", "dup", &[])]);
    assert!(matches!(res, Err(PendingPoolError::DuplicateTaskId(_))));
}

/// `extend`'s dep-existence check keys on the FULL `(phase_id, task_id)`:
/// a dep naming a phase where the `task_id` is absent is an
/// `UnknownTaskDep`, even though a same-named `task_id` exists in
/// another phase. Pre-fix the bare-id resolution accepted it.
#[test]
fn extend_cross_phase_missing_dep_in_named_phase_is_unknown() {
    let mut p = pool_with(&["A", "B"], &[]);
    // `parent` exists only in phase A; `child` in B depends on
    // (phase=B, parent) — absent in the named phase.
    let res = p.extend([
        t_cross("A", "parent", &[]),
        t_cross("B", "child", &[("B", "parent")]),
    ]);
    match res {
        Err(PendingPoolError::UnknownTaskDep {
            task,
            referenced_by,
        }) => {
            assert_eq!(task, "parent");
            assert_eq!(referenced_by, "child");
        }
        other => panic!("expected UnknownTaskDep, got {:?}", other),
    }
}

/// A cross-phase dep that names the RIGHT phase resolves under
/// `extend`'s full-identity rule.
#[test]
fn extend_cross_phase_dep_in_named_phase_resolves() {
    let mut p = pool_with(&["A", "B"], &[]);
    p.extend([
        t_cross("A", "parent", &[]),
        t_cross("B", "child", &[("A", "parent")]),
    ])
    .expect("cross-phase dep naming the right phase resolves");
}

/// The cycle check keys on full `(phase_id, task_id)` node identity: a
/// same-`task_id`-different-phase pair that depend on each other along
/// their NAMED phases is a genuine cycle. (A regression here would let
/// a phase-blind node-collapse hide or fabricate a cycle.)
#[test]
fn extend_cross_phase_cycle_uses_full_identity_nodes() {
    let mut p = pool_with(&["A", "B"], &[]);
    // (A,x) → (B,x) → (A,x): a real cross-phase cycle.
    let res = p.extend([
        t_cross("A", "x", &[("B", "x")]),
        t_cross("B", "x", &[("A", "x")]),
    ]);
    assert!(matches!(res, Err(PendingPoolError::TaskDepCycle(_))));
}

// ─────────────────────────────────────────────────────────────────────────
// Soft (retry-pending) failures + the doomed-blocked drain gate.
//
// The wire-terminal failure path marks a failed task SOFT
// (`on_item_failed_pending_retry`) instead of cascading immediately —
// the per-phase retry buckets at the DRAIN EDGE own the permanence
// decision. These tests pin the three legs of that machine: the drain
// gate discounts dependents doomed by a same-phase soft root (the edge
// is reachable — pre-fix the phase wedged in `Draining` forever and the
// run hung), `reinject` revives the root (the marker clears and the
// dependents go back to legitimately waiting), and
// `finalize_soft_failures` promotes + cascades at the declined edge.
// ─────────────────────────────────────────────────────────────────────────

/// A terminally-failed (soft) prereq whose dependents are the ONLY
/// remaining items of the phase must NOT hold the phase in `Draining`:
/// the drain edge fires (the retry-or-cascade decision lives there).
/// Doom propagates transitively (`c` is doomed via doomed `b`).
#[test]
fn soft_failed_prereq_opens_drain_edge_with_doomed_dependents() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t_with_id("P", "T", "", 1, "a", &[]),
        t_with_id("P", "T", "", 1, "b", &["a"]),
        t_with_id("P", "T", "", 1, "c", &["b"]),
    ])
    .expect("valid extend");
    let a = p.pop_for_worker(1).expect("a dispatchable");
    assert_eq!(a.task_id, "a");
    // The failure is terminal at the manager but its permanence is
    // pending the drain edge's retry decision.
    p.on_item_failed_pending_retry(&phase("P"), "a");
    // Pre-fix: blocked {b, c} kept the phase `Draining` and this poll
    // returned empty forever — the run-wedge.
    assert_eq!(
        p.poll_drain_transitions(),
        vec![phase("P")],
        "doomed blocked dependents must not hold the drain edge hostage"
    );
}

/// `reinject` (the retry bucket granting the root another pass) clears
/// the soft marker: the phase flips back to `Active` and a later
/// SUCCESS resolves the dependents normally — no premature cascade.
#[test]
fn reinject_revives_soft_failed_root_and_dependents_resolve() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t_with_id("P", "T", "", 1, "a", &[]),
        t_with_id("P", "T", "", 1, "b", &["a"]),
    ])
    .expect("valid extend");
    let a = p.pop_for_worker(1).expect("a dispatchable");
    p.on_item_failed_pending_retry(&phase("P"), "a");
    assert_eq!(p.poll_drain_transitions(), vec![phase("P")]);
    // The drain-edge bucket reinjects the root: revival.
    p.reinject(a);
    // The phase is Active again with a queued; nothing is drained.
    assert!(p.poll_drain_transitions().is_empty());
    let a2 = p.pop_for_worker(1).expect("a re-dispatchable");
    assert_eq!(a2.task_id, "a");
    // Retry succeeds → the dependent unblocks normally.
    p.on_item_finished(&phase("P"), Some("a"));
    let b = p
        .pop_for_worker(1)
        .expect("b unblocked after revived success");
    assert_eq!(b.task_id, "b");
}

/// `finalize_soft_failures` at the declined drain edge promotes the
/// phase's soft roots to permanent and cascade-fails their transitive
/// dependents, returning `(root, cascaded)` for the caller's ledgers.
#[test]
fn finalize_soft_failures_cascades_transitive_dependents() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t_with_id("P", "T", "", 1, "a", &[]),
        t_with_id("P", "T", "", 1, "b", &["a"]),
        t_with_id("P", "T", "", 1, "c", &["b"]),
    ])
    .expect("valid extend");
    let _a = p.pop_for_worker(1).expect("a dispatchable");
    p.on_item_failed_pending_retry(&phase("P"), "a");
    assert_eq!(p.poll_drain_transitions(), vec![phase("P")]);
    // The buckets declined; the caller finalizes.
    let finalized = p.finalize_soft_failures(&phase("P"));
    assert_eq!(finalized.len(), 1, "one soft root");
    let (root, cascaded) = &finalized[0];
    assert_eq!(root, "a");
    let mut ids: Vec<_> = cascaded.iter().map(|i| i.task_id.clone()).collect();
    ids.sort();
    assert_eq!(ids, vec!["b".to_string(), "c".to_string()]);
    // The pool is empty and the phase stays Drained — the run can end.
    assert!(p.is_empty(), "no blocked stragglers survive finalization");
    // Idempotent: a second finalize has nothing left.
    assert!(p.finalize_soft_failures(&phase("P")).is_empty());
}

/// A dependent blocked on ANOTHER phase's soft-failed root stays LIVE
/// for ITS phase's drain gate (that root's retry decision belongs to
/// the OTHER phase's drain edge); the other phase's finalization then
/// cascades it and re-runs this phase's drain transition.
#[test]
fn cross_phase_soft_root_does_not_doom_for_foreign_drain_gate() {
    let mut p = pool_with(&["Q", "P"], &[]);
    p.extend([
        t_cross("Q", "root", &[]),
        t_cross("P", "leaf", &[("Q", "root")]),
    ])
    .expect("valid extend");
    let root = p.pop_for_worker(1).expect("root dispatchable");
    assert_eq!(root.task_id, "root");
    p.on_item_failed_pending_retry(&phase("Q"), "root");
    // Q's gate fires (its own soft root dooms nothing else there);
    // P stays open — `leaf` is live for P's gate (the root's decision
    // is Q's drain edge, not P's).
    assert_eq!(p.poll_drain_transitions(), vec![phase("Q")]);
    // Q's edge declines → finalize: the cascade crosses phases and
    // resolves P's straggler, then P's drain transition fires.
    let finalized = p.finalize_soft_failures(&phase("Q"));
    assert_eq!(finalized.len(), 1);
    assert_eq!(finalized[0].1.len(), 1, "leaf cascaded");
    assert_eq!(finalized[0].1[0].task_id, "leaf");
    assert_eq!(
        p.poll_drain_transitions(),
        vec![phase("P")],
        "the cascade's drain transition releases the dependent's phase"
    );
}

// ---------------------------------------------------------------------------
// Hydration pre-seeds for terminal-FAILURE classes (fix/hydrate-failed-deps):
// `mark_tasks_failed_pending_retry` (Failed roots — drain-edge decision
// pending) and `mark_tasks_dormant` (Unfulfillable roots — operator-
// reinjectable dormancy). Both make the seeded id KNOWN to `extend` so a
// dependent lands in `blocked` instead of failing `UnknownTaskDep`, and
// NEITHER satisfies the dep (the `mark_tasks_completed` contract) nor
// cascade-fails it at extend time (the `mark_tasks_failed` contract).
// ---------------------------------------------------------------------------

#[test]
fn seeded_soft_failed_id_resolves_dep_and_blocks_dependent() {
    let mut p = pool_with(&["P"], &[]);
    p.mark_tasks_failed_pending_retry([("root".to_string(), phase("P"))]);
    p.extend([t_with_id("P", "T", "", 1, "child", &["root"])])
        .expect("a soft-failed prereq id must be KNOWN to extend");
    // The dependent is BLOCKED: not dispatchable, not cascade-failed.
    assert!(p.iter().next().is_none(), "child must not be queued");
    assert_eq!(p.blocked_len(), 1);
    // Drain gate (#382 shape): the doomed dependent does not hold the
    // phase open — the drain edge is reachable, where the retry-or-
    // cascade decision lives.
    p.drain_empty_active_phases();
    assert_eq!(
        p.poll_drain_transitions(),
        vec![phase("P")],
        "the seeded soft root's phase reaches its drain edge"
    );
    // Finalize promotes the seeded root and cascades the dependent —
    // identical to the live `on_item_failed_pending_retry` flow.
    let finalized = p.finalize_soft_failures(&phase("P"));
    assert_eq!(finalized.len(), 1);
    assert_eq!(finalized[0].0, "root");
    let cascaded_ids: Vec<_> = finalized[0].1.iter().map(|t| t.task_id.clone()).collect();
    assert_eq!(cascaded_ids, vec!["child".to_string()]);
    assert_eq!(p.blocked_len(), 0);
}

#[test]
fn seeded_soft_failed_root_revived_by_reinject_keeps_dependent_blocked() {
    let mut p = pool_with(&["P"], &[]);
    p.mark_tasks_failed_pending_retry([("root".to_string(), phase("P"))]);
    p.extend([t_with_id("P", "T", "", 1, "child", &["root"])])
        .expect("dependent blocks on the seeded soft root");
    // Revival (the retry bucket's seam): reinject clears the soft marker;
    // the dependent stays blocked until the root actually completes.
    p.reinject(t_with_id("P", "T", "", 1, "root", &[]));
    assert_eq!(p.blocked_len(), 1, "child still waits for the retry");
    let root = p.pop_for_worker(1).expect("revived root dispatches");
    assert_eq!(root.task_id, "root");
    p.on_item_finished(&phase("P"), Some("root"));
    let child = p.pop_for_worker(1).expect("child unblocks on completion");
    assert_eq!(child.task_id, "child");
    // The cleared marker means a later finalize has nothing to promote.
    assert!(p.finalize_soft_failures(&phase("P")).is_empty());
}

#[test]
fn seeded_dormant_id_resolves_dep_and_blocks_dependent_until_revival() {
    let mut p = pool_with(&["P"], &[]);
    p.mark_tasks_dormant(["root".to_string()]);
    p.extend([t_with_id("P", "T", "", 1, "child", &["root"])])
        .expect("a dormant prereq id must be KNOWN to extend");
    assert!(p.iter().next().is_none(), "child must not be queued");
    assert_eq!(p.blocked_len(), 1);
    // A dormant root does NOT doom its dependent: the dependent counts as
    // LIVE blocked work, so the phase holds open (Draining), never
    // reaching the drain edge's cascade — the dormancy contract.
    p.drain_empty_active_phases();
    assert!(
        p.poll_drain_transitions().is_empty(),
        "a live-blocked dependent holds the phase open"
    );
    assert_eq!(
        p.phase_state(&phase("P")),
        Some(crate::PhaseState::Draining)
    );
    assert!(p.finalize_soft_failures(&phase("P")).is_empty());
    // Revival: reinject the root; complete it; the dependent unblocks.
    p.reinject(t_with_id("P", "T", "", 1, "root", &[]));
    let root = p.pop_for_worker(1).expect("revived root dispatches");
    assert_eq!(root.task_id, "root");
    p.on_item_finished(&phase("P"), Some("root"));
    let child = p.pop_for_worker(1).expect("child unblocks");
    assert_eq!(child.task_id, "child");
}

#[test]
fn seeded_failure_ids_reject_duplicate_task_id_on_extend() {
    // Both seed classes claim the identity: a later batch reusing the
    // task_id is a producer-side bug, same as reusing a completed id.
    let mut p = pool_with(&["P"], &[]);
    p.mark_tasks_failed_pending_retry([("soft".to_string(), phase("P"))]);
    p.mark_tasks_dormant(["dormant".to_string()]);
    assert!(matches!(
        p.extend([t_with_id("P", "T", "", 1, "soft", &[])]),
        Err(PendingPoolError::DuplicateTaskId(_))
    ));
    assert!(matches!(
        p.extend([t_with_id("P", "T", "", 1, "dormant", &[])]),
        Err(PendingPoolError::DuplicateTaskId(_))
    ));
}

// ─────────────────────────────────────────────────────────────────────────
// Re-block on reinject of an ALREADY-COMPLETED dep (the inverse of
// completion's unblock).
//
// When a dep `a` completes, its dependent `b` is unblocked `blocked → ready`.
// If `a` is then REINJECTED (a finished task re-run, its output regenerated),
// a still-READY `b` must be RE-BLOCKED — it must not dispatch against the
// stale/torn predecessor output. `reinject` inverts the unblock by
// re-deriving the lingering dependents from the queued items (the consumed
// reverse index is gone) and re-routing each through `commit_item`. The
// boundary: a dependent already DISPATCHED cannot be un-run, and `requeue`
// (which never completed the task) is unaffected.
// ─────────────────────────────────────────────────────────────────────────

/// THE HEADLINE: complete `a` → `b` ready; reinject `a` → `b` RE-BLOCKED.
#[test]
fn reinject_completed_dep_reblocks_ready_dependent() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t_with_id("P", "T", "", 1, "a", &[]),
        t_with_id("P", "T", "", 1, "b", &["a"]),
    ])
    .expect("valid extend");
    assert_eq!(p.blocked_len(), 1, "b starts blocked on a");
    let a = p.pop_for_worker(1).expect("a dispatchable");
    assert_eq!(a.task_id, "a");
    p.on_item_finished(&phase("P"), Some("a"));
    // b is now READY (queued, not blocked).
    assert_eq!(p.blocked_len(), 0);
    assert_eq!(
        p.iter().map(|i| i.task_id.clone()).collect::<Vec<_>>(),
        vec!["b".to_string()],
    );
    // Reinject a (the finished task is re-run, regenerating its output).
    p.reinject(a);
    // b must be RE-BLOCKED — not dispatchable against a's regenerating output.
    assert_eq!(p.blocked_len(), 1, "b re-blocked on the reinjected a");
    let queued: Vec<_> = p.iter().map(|i| i.task_id.clone()).collect();
    assert_eq!(queued, vec!["a".to_string()], "only a is queued; b is blocked");
    // A worker can pick up a, but NOT b (b is invisible while blocked).
    let picked = p.pop_for_worker(1).expect("a re-dispatchable");
    assert_eq!(picked.task_id, "a");
    assert!(p.pop_for_worker(1).is_none(), "b stays blocked, not dispatchable");
}

/// Round-trip: after the re-block, re-completing `a` re-unblocks `b`.
#[test]
fn reinject_then_recomplete_reunblocks_dependent() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t_with_id("P", "T", "", 1, "a", &[]),
        t_with_id("P", "T", "", 1, "b", &["a"]),
    ])
    .expect("valid extend");
    let a = p.pop_for_worker(1).expect("a");
    p.on_item_finished(&phase("P"), Some("a"));
    p.reinject(a);
    assert_eq!(p.blocked_len(), 1, "b re-blocked");
    // Re-run a to completion; b unblocks again exactly as the first time.
    let a2 = p.pop_for_worker(1).expect("a re-dispatchable");
    assert_eq!(a2.task_id, "a");
    p.on_item_finished(&phase("P"), Some("a"));
    assert_eq!(p.blocked_len(), 0, "b unblocked on re-completion");
    let b = p.pop_for_worker(1).expect("b re-unblocked");
    assert_eq!(b.task_id, "b");
}

/// Diamond `a → b`, `a → c`: reinjecting `a` re-blocks BOTH dependents.
#[test]
fn reinject_completed_dep_reblocks_diamond_dependents() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t_with_id("P", "T", "", 1, "a", &[]),
        t_with_id("P", "T", "", 1, "b", &["a"]),
        t_with_id("P", "T", "", 1, "c", &["a"]),
    ])
    .expect("valid extend");
    assert_eq!(p.blocked_len(), 2, "b and c blocked on a");
    let a = p.pop_for_worker(1).expect("a");
    p.on_item_finished(&phase("P"), Some("a"));
    assert_eq!(p.blocked_len(), 0, "both unblocked");
    p.reinject(a);
    assert_eq!(p.blocked_len(), 2, "both b and c re-blocked");
    let queued: Vec<_> = p.iter().map(|i| i.task_id.clone()).collect();
    assert_eq!(queued, vec!["a".to_string()], "only a queued");
}

/// The BLOCKED-shape hole (must NOT regress): `b` depends on `a` AND `x`.
/// Completing only `a` leaves `b` BLOCKED on `x`, but `a` was silently
/// dropped from `b`'s unmet set. Reinjecting `a` MUST re-add that dep —
/// otherwise, when `x` later completes, `b` would unblock and dispatch
/// against the regenerating `a`. The re-route covers the blocked side, so
/// `b` stays blocked until BOTH `a` (re-completed) and `x` are met — and
/// is never double-counted (`blocked_len` stays 1, not 2).
#[test]
fn reinject_completed_dep_reblocks_blocked_dependent_with_other_unmet_deps() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t_with_id("P", "T", "", 1, "a", &[]),
        t_with_id("P", "T", "", 1, "x", &[]),
        t_with_id("P", "T", "", 1, "b", &["a", "x"]),
    ])
    .expect("valid extend");
    assert_eq!(p.blocked_len(), 1, "b blocked on a and x");
    // Complete a only; b stays blocked on x (never queued).
    let a = p.pop_for_worker(1).expect("a");
    assert_eq!(a.task_id, "a");
    p.on_item_finished(&phase("P"), Some("a"));
    assert_eq!(p.blocked_len(), 1, "b still blocked on x");
    // Reinject a: b is BLOCKED (not queued), but the re-route still finds
    // it via its declared task_depends_on and re-adds the now-unmet a.
    p.reinject(a);
    assert_eq!(p.blocked_len(), 1, "b re-blocked on {{a, x}}, never double-counted");
    // Completing x must NOT unblock b — a is unmet again post-reinject.
    let x = p.pop_for_worker(1).expect("x dispatchable");
    assert_eq!(x.task_id, "x");
    p.on_item_finished(&phase("P"), Some("x"));
    // b is still blocked because a (reinjected, not yet re-completed) is unmet.
    assert_eq!(p.blocked_len(), 1, "b waits on the reinjected a");
    // Re-complete a → b unblocks.
    let a2 = p.pop_for_worker(1).expect("a re-dispatchable");
    p.on_item_finished(&phase("P"), Some("a"));
    assert_eq!(p.blocked_len(), 0);
    assert_eq!(a2.task_id, "a");
    let b = p.pop_for_worker(1).expect("b unblocks once both deps met");
    assert_eq!(b.task_id, "b");
}

/// Boundary: a dependent already DISPATCHED (in flight, gone from every
/// bucket) when its dep is reinjected cannot be un-run — it is left
/// alone (no panic, no spurious re-block).
#[test]
fn reinject_completed_dep_leaves_inflight_dependent_alone() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t_with_id("P", "T", "", 1, "a", &[]),
        t_with_id("P", "T", "", 1, "b", &["a"]),
    ])
    .expect("valid extend");
    let a = p.pop_for_worker(1).expect("a");
    p.on_item_finished(&phase("P"), Some("a"));
    // Dispatch b — it is now in flight, gone from its bucket.
    let b = p.pop_for_worker(1).expect("b ready");
    assert_eq!(b.task_id, "b");
    assert_eq!(p.in_flight(&phase("P")), 1, "b is in flight");
    // Reinject a: b cannot be un-run; the re-block finds nothing queued.
    p.reinject(a);
    assert_eq!(p.blocked_len(), 0, "in-flight b is not re-blocked");
    assert_eq!(p.in_flight(&phase("P")), 1, "b stays in flight, untouched");
    // a is re-queued and dispatchable; b's in-flight slot is preserved.
    let a2 = p.pop_for_worker(2).expect("a re-dispatchable");
    assert_eq!(a2.task_id, "a");
}

/// The CONTRAST: `requeue` (a never-completed task back to the queue) does
/// NOT trigger any re-block — it never unblocked dependents in the first
/// place, so there is nothing to invert. `b` stays blocked throughout.
#[test]
fn requeue_does_not_reblock_dependents() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t_with_id("P", "T", "", 1, "a", &[]),
        t_with_id("P", "T", "", 1, "b", &["a"]),
    ])
    .expect("valid extend");
    assert_eq!(p.blocked_len(), 1, "b blocked on a");
    let a = p.pop_for_worker(1).expect("a");
    // a was dispatched but NEVER completed — requeue (worker death / transient).
    p.requeue(a);
    // b was never unblocked, so it is still blocked — requeue did not touch it.
    assert_eq!(p.blocked_len(), 1, "b stays blocked; requeue invented no re-block");
    let queued: Vec<_> = p.iter().map(|i| i.task_id.clone()).collect();
    assert_eq!(queued, vec!["a".to_string()]);
}

/// Transitive scope: `a → b → c`. After all complete and `b`, `c` are
/// ready, reinjecting `a` re-blocks ONLY the DIRECT dependent `b`; `c`
/// (which names `b`, not `a`) is NOT re-blocked by this reinject —
/// matching `resolve_completed_dependents`' own direct-dependent scope.
#[test]
fn reinject_completed_dep_reblocks_only_direct_dependents() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t_with_id("P", "T", "", 1, "a", &[]),
        t_with_id("P", "T", "", 1, "b", &["a"]),
        t_with_id("P", "T", "", 1, "c", &["b"]),
    ])
    .expect("valid extend");
    // Drive a → b → c all to completion so b and c become ready in turn.
    let a = p.pop_for_worker(1).expect("a");
    p.on_item_finished(&phase("P"), Some("a"));
    let b = p.pop_for_worker(1).expect("b ready");
    assert_eq!(b.task_id, "b");
    p.on_item_finished(&phase("P"), Some("b"));
    // c is now ready (queued). b is in flight; complete it back to queued state
    // by re-extending? No — instead leave c ready and reinject a.
    assert_eq!(
        p.iter().map(|i| i.task_id.clone()).collect::<Vec<_>>(),
        vec!["c".to_string()],
        "c is ready; a and b already dispatched"
    );
    // Reinject a: only DIRECT dependents of a are re-derived. b is in flight
    // (gone), c names b (not a) — so c is NOT re-blocked here.
    p.reinject(a);
    assert_eq!(p.blocked_len(), 0, "c is not a direct dependent of a");
    let queued: Vec<_> = p.iter().map(|i| i.task_id.clone()).collect();
    let mut sorted = queued.clone();
    sorted.sort();
    assert_eq!(sorted, vec!["a".to_string(), "c".to_string()], "a re-queued; c still ready");
}

/// Cross-phase re-block: `a` in phase Q, `b` in phase P depends on it.
/// Completing `a` unblocks `b` into P's bucket; reinjecting `a` re-blocks
/// `b` and re-evaluates P's drain state (P had only `b` queued).
#[test]
fn reinject_completed_dep_reblocks_cross_phase_dependent() {
    let mut p = pool_with(&["Q", "P"], &[]);
    p.extend([
        t_cross("Q", "a", &[]),
        t_cross("P", "b", &[("Q", "a")]),
    ])
    .expect("valid extend");
    assert_eq!(p.blocked_len(), 1, "b blocked on Q's a");
    let a = p.pop_for_worker(1).expect("a");
    assert_eq!(a.task_id, "a");
    p.on_item_finished(&phase("Q"), Some("a"));
    assert_eq!(p.blocked_len(), 0, "b unblocked into P");
    assert_eq!(p.phase_state(&phase("P")), Some(crate::PhaseState::Active));
    // Reinject a into Q; b in P must re-block, and P (now queue-empty with
    // a live blocked b) must reflect Draining, not Drained.
    p.reinject(a);
    assert_eq!(p.blocked_len(), 1, "b re-blocked in P");
    assert_eq!(
        p.phase_state(&phase("P")),
        Some(crate::PhaseState::Draining),
        "P holds open with live-blocked b, queue empty"
    );
    assert!(
        p.poll_drain_transitions().is_empty(),
        "P is not Drained — b is live-blocked work"
    );
}
