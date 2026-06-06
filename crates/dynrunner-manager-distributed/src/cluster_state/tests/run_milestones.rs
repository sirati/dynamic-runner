//! `ClusterMutation::RunMilestone` (A7) apply + convergence semantics.
//!
//! The milestone fact is a MONOTONE grow-only set keyed by
//! `(RunMilestoneKind, PhaseId)`. These tests pin the three CRDT
//! properties the observer-from-CRDT projection (Wave 3b) relies on:
//!
//!   * apply is idempotent (re-reaching a milestone is a NoOp) and the
//!     `run_milestones()` accessor exposes the converged set the narrator
//!     diffs;
//!   * the set converges across replicas via snapshot/restore — order-
//!     independent, idempotent, and union-merged (each replica keeps every
//!     element either holds);
//!   * the digest folds the set so a divergent replica is `is_behind`,
//!     pulls, and heals (detect-WITH-heal), then quiesces.

use super::*;

fn ms(
    kind: RunMilestoneKind,
    phase: &str,
) -> ClusterMutation<RunnerIdentifier> {
    ClusterMutation::RunMilestone {
        kind,
        phase: PhaseId::from(phase),
    }
}

/// A first apply for a `(kind, phase)` records the milestone (`Applied`);
/// a re-apply for the SAME pair is a NoOp — the grow-only set's idempotent
/// insert. Distinct `(kind, phase)` pairs are independent entries.
#[test]
fn run_milestone_apply_is_idempotent_per_kind_phase() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    assert!(s.run_milestones().is_empty(), "fresh state has no milestones");

    assert_eq!(
        s.apply(ms(RunMilestoneKind::PhaseTaskSpawning, "compile")),
        ApplyOutcome::Applied
    );
    // Re-reaching the same milestone is a NoOp (grow-only, idempotent).
    assert_eq!(
        s.apply(ms(RunMilestoneKind::PhaseTaskSpawning, "compile")),
        ApplyOutcome::NoOp
    );
    assert_eq!(s.run_milestones().len(), 1);

    // A different kind for the same phase is a SEPARATE element.
    assert_eq!(
        s.apply(ms(RunMilestoneKind::ErrorRetryPassStart, "compile")),
        ApplyOutcome::Applied
    );
    // A different phase for the same kind is also a separate element.
    assert_eq!(
        s.apply(ms(RunMilestoneKind::PhaseTaskSpawning, "link")),
        ApplyOutcome::Applied
    );
    assert_eq!(s.run_milestones().len(), 3);

    // The accessor surfaces exactly the reached `(kind, phase)` pairs —
    // the edge-set the Wave-3b narrator diffs.
    let set = s.run_milestones();
    assert!(set.contains(&(RunMilestoneKind::PhaseTaskSpawning, PhaseId::from("compile"))));
    assert!(set.contains(&(RunMilestoneKind::ErrorRetryPassStart, PhaseId::from("compile"))));
    assert!(set.contains(&(RunMilestoneKind::PhaseTaskSpawning, PhaseId::from("link"))));
    assert!(!set.contains(&(RunMilestoneKind::OomRetryPassStart, PhaseId::from("compile"))));
}

/// The milestone set is order-independent: applying the same set of
/// milestones in a different order converges to the same set (and the same
/// digest fold).
#[test]
fn run_milestone_set_is_order_independent() {
    let milestones = [
        (RunMilestoneKind::PhaseTaskSpawning, "a"),
        (RunMilestoneKind::ErrorRetryPassStart, "a"),
        (RunMilestoneKind::OomRetryPassStart, "b"),
        (RunMilestoneKind::PhaseTaskSpawning, "b"),
    ];

    let mut a = ClusterState::<RunnerIdentifier>::new();
    for (k, p) in milestones {
        a.apply(ms(k, p));
    }
    let mut b = ClusterState::<RunnerIdentifier>::new();
    for (k, p) in milestones.iter().rev() {
        b.apply(ms(*k, p));
    }

    assert_eq!(a.run_milestones(), b.run_milestones());
    // The XOR-fold is order-independent, so the digests match too.
    assert_eq!(a.digest(), b.digest());
}

/// Two replicas with DISJOINT milestone subsets converge via snapshot/
/// restore to the UNION of both — the grow-only set merge. Each side keeps
/// every element either held; the restore is idempotent (re-restore adds
/// nothing) and order-independent (restoring either direction converges).
#[test]
fn run_milestone_set_converges_via_snapshot_union() {
    let mut a = ClusterState::<RunnerIdentifier>::new();
    let mut b = ClusterState::<RunnerIdentifier>::new();

    // `a` reached the compile phase-start + its error retry; `b` reached
    // the link phase-start + its OOM retry. Disjoint subsets.
    a.apply(ms(RunMilestoneKind::PhaseTaskSpawning, "compile"));
    a.apply(ms(RunMilestoneKind::ErrorRetryPassStart, "compile"));
    b.apply(ms(RunMilestoneKind::PhaseTaskSpawning, "link"));
    b.apply(ms(RunMilestoneKind::OomRetryPassStart, "link"));

    // Pull both ways: each side merges the other's snapshot.
    a.restore(b.snapshot());
    b.restore(a.snapshot());

    // Both converge to the union of all four milestones.
    assert_eq!(a.run_milestones().len(), 4);
    assert_eq!(a.run_milestones(), b.run_milestones());
    assert_eq!(a.digest(), b.digest());

    // Idempotent: a re-restore of the same snapshot adds nothing.
    a.restore(b.snapshot());
    assert_eq!(a.run_milestones().len(), 4);
}

/// A milestone that `a` holds and `b` does not makes `b` `is_behind` `a`
/// (the digest fold is summarised); pulling `a`'s snapshot converges the
/// set and the digest, after which neither is behind (detect → pull →
/// converge → quiesce).
#[test]
fn run_milestone_divergence_detected_and_heals() {
    let mut a = ClusterState::<RunnerIdentifier>::new();
    let mut b = ClusterState::<RunnerIdentifier>::new();
    // Same task ledger so ONLY the milestone set diverges.
    for s in [&mut a, &mut b] {
        s.apply(ClusterMutation::TaskAdded {
            hash: "t".into(),
            task: mk_task("t"),
        });
    }
    // `a` reached a milestone `b` has not seen.
    a.apply(ms(RunMilestoneKind::PhaseTaskSpawning, "compile"));

    // The digests DIFFER and `b` is behind `a` — the missing milestone
    // drives a pull.
    assert_ne!(a.digest(), b.digest());
    assert!(b.digest().is_behind(&a.digest()));
    // `a` is NOT behind `b` (it holds strictly more).
    assert!(!a.digest().is_behind(&b.digest()));

    // Pull: the snapshot's milestone set unions into `b` and converges.
    b.restore(a.snapshot());
    assert_eq!(a.digest(), b.digest());
    assert!(!b.digest().is_behind(&a.digest()));
    assert!(!a.digest().is_behind(&b.digest()));
    assert!(b
        .run_milestones()
        .contains(&(RunMilestoneKind::PhaseTaskSpawning, PhaseId::from("compile"))));
}

/// A milestone advancing the set (same task count) changes the digest
/// fold — so a replica that has NOT seen the new milestone is detected as
/// behind even though the TASK count is unchanged. Pins that the milestone
/// fold is wired into the digest independently of the task fold.
#[test]
fn run_milestone_count_drives_behind_at_equal_tasks() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "t".into(),
        task: mk_task("t"),
    });
    let stale = s.digest();
    s.apply(ms(RunMilestoneKind::OomRetryPassStart, "p"));
    let advanced = s.digest();

    // Task count unchanged; the milestone count + fold advanced.
    assert_eq!(stale.tasks_count, advanced.tasks_count);
    assert_eq!(advanced.run_milestones_count, 1);
    assert_ne!(stale.run_milestones_hash, advanced.run_milestones_hash);
    // The replica still missing the milestone is behind the advanced one.
    assert!(stale.is_behind(&advanced));
}
