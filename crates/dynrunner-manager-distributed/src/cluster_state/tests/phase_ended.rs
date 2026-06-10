//! `PhaseEnded` CRDT semantics (#343): the replicated grow-only per-phase
//! "the `on_phase_end` edge completed" fact (join = OR / set union).
//!
//! Pins:
//!   * the default is the EMPTY set (no phase is known to have ended — a
//!     cold run replays every end edge through the live cascade);
//!   * the apply rule is a set-insert — `Applied` iff newly inserted,
//!     NoOp on re-application (idempotent under at-least-once delivery),
//!     and nothing ever removes a phase (sticky — the no-redo decision
//!     can never regress to re-firing a hook that already fired, #326);
//!   * the snapshot/restore join is set UNION — facts survive promotion
//!     and overlapping snapshots converge regardless of pull order, and a
//!     stale peer's snapshot can never un-end a phase;
//!   * a pre-field snapshot (no `phases_ended` key) decodes as the empty
//!     set (the conservative replay-the-edge shape);
//!   * the AE digest folds the set (count + key XOR), so a replica
//!     missing a fact detects it is behind and the snapshot pull heals it.

use super::*;

#[test]
fn fresh_state_has_no_ended_phases() {
    let s = ClusterState::<RunnerIdentifier>::new();
    assert!(
        !s.phase_ended(&PhaseId::from("build")),
        "a cold ledger knows no ended phase — every end edge replays \
         through the live cascade"
    );
}

#[test]
fn phase_ended_apply_is_insert_then_idempotent_noop() {
    let mut s = ClusterState::<RunnerIdentifier>::new();

    // First apply: inserts the fact (Applied → broadcast-worthy).
    assert_eq!(
        s.apply(ClusterMutation::PhaseEnded {
            phase: PhaseId::from("build"),
        }),
        ApplyOutcome::Applied
    );
    assert!(s.phase_ended(&PhaseId::from("build")));
    // Another phase is independent.
    assert!(!s.phase_ended(&PhaseId::from("ship")));

    // Re-application (at-least-once delivery / snapshot replay) is a NoOp
    // and the fact stays latched.
    assert_eq!(
        s.apply(ClusterMutation::PhaseEnded {
            phase: PhaseId::from("build"),
        }),
        ApplyOutcome::NoOp
    );
    assert!(s.phase_ended(&PhaseId::from("build")));
}

#[test]
fn phase_ended_facts_survive_snapshot_restore() {
    // The promotion path: a promoted primary restoring a snapshot must
    // inherit exactly which phases already fired `on_phase_end`, so its
    // hydrate seeds those Done-without-firing (#326) and replays the rest.
    let mut original = ClusterState::<RunnerIdentifier>::new();
    original.apply(ClusterMutation::PhaseEnded {
        phase: PhaseId::from("build"),
    });

    let mut promoted = ClusterState::<RunnerIdentifier>::new();
    promoted.restore(original.snapshot());
    assert!(
        promoted.phase_ended(&PhaseId::from("build")),
        "the PhaseEnded fact must survive snapshot/restore (promotion)"
    );
    assert!(
        !promoted.phase_ended(&PhaseId::from("ship")),
        "a never-ended phase stays absent — its end edge replays"
    );
}

#[test]
fn phase_ended_restore_is_union_and_never_removes() {
    // Overlapping snapshots converge by UNION regardless of pull order,
    // and a stale peer that lacks a fact can never un-end a phase.
    let mut a = ClusterState::<RunnerIdentifier>::new();
    a.apply(ClusterMutation::PhaseEnded {
        phase: PhaseId::from("p1"),
    });
    let mut b = ClusterState::<RunnerIdentifier>::new();
    b.apply(ClusterMutation::PhaseEnded {
        phase: PhaseId::from("p2"),
    });

    // Bilateral pull: both converge to {p1, p2}.
    let snap_a = a.snapshot();
    let snap_b = b.snapshot();
    a.restore(snap_b);
    b.restore(snap_a);
    for s in [&a, &b] {
        assert!(s.phase_ended(&PhaseId::from("p1")));
        assert!(s.phase_ended(&PhaseId::from("p2")));
    }

    // A stale EMPTY snapshot removes nothing (grow-only).
    let empty = ClusterState::<RunnerIdentifier>::new();
    a.restore(empty.snapshot());
    assert!(a.phase_ended(&PhaseId::from("p1")));
    assert!(a.phase_ended(&PhaseId::from("p2")));
}

/// Backward-compat: a snapshot from a sender that PREDATES the
/// `phases_ended` field (its JSON omits the key) must decode as the EMPTY
/// set (`#[serde(default)]`) — "no hook is known to have fired", the
/// conservative replay-the-edge shape — not a missing-field error.
#[test]
fn legacy_snapshot_without_phases_ended_decodes_empty() {
    let legacy = serde_json::json!({
        "tasks": {},
        "current_primary": "primary-x",
        "primary_epoch": 4,
        "phase_deps": {},
        "peer_holdings": {},
        "task_outputs": {},
        "secondary_capacities": {},
        "alive_members": [],
        "run_complete": false,
        "run_aborted": null
    });
    let decoded: crate::cluster_state::ClusterStateSnapshot<RunnerIdentifier> =
        serde_json::from_str(&legacy.to_string()).unwrap();
    assert!(
        decoded.phases_ended.is_empty(),
        "a pre-field snapshot must decode phases_ended as the empty set"
    );
}

/// Anti-entropy: a replica missing a `PhaseEnded` fact reads BEHIND the
/// peer that holds it (count-OR-hash compare on the digest), so the
/// snapshot pull's union-merge heals it — detect-WITH-heal, and the
/// promoted-primary no-redo decision converges cluster-wide. Converged
/// replicas produce equal digests (self-quiescing).
#[test]
fn digest_detects_missing_phase_ended_fact() {
    let mut holder = ClusterState::<RunnerIdentifier>::new();
    holder.apply(ClusterMutation::PhaseEnded {
        phase: PhaseId::from("build"),
    });
    let mut lagger = ClusterState::<RunnerIdentifier>::new();

    assert!(
        lagger.digest().is_behind(&holder.digest()),
        "a replica missing the PhaseEnded fact must detect it is behind"
    );
    assert!(
        !holder.digest().is_behind(&lagger.digest()),
        "the holder is not behind the lagger (grow-only, one direction)"
    );

    // The flagged divergence is one the pull actually heals (no no-op loop).
    lagger.restore(holder.snapshot());
    assert!(lagger.phase_ended(&PhaseId::from("build")));
    assert!(!lagger.digest().is_behind(&holder.digest()));
    assert!(!holder.digest().is_behind(&lagger.digest()));
}
