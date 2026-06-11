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
    let b = p.pop_for_worker(1).expect("b unblocked after revived success");
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
