//! Tests for the three grow-only-MAX replicated counters (F4 + P3):
//! `phase_event_tallies`, `retry_passes_used`, `unfulfillable_reinject_used`.
//!
//! Pins, mirroring the `secondary_capacities` / `task_outputs` grow-only
//! patterns:
//!
//!   - The originator max-bumps the local count; the accessor reads it back
//!     (0 for a never-bumped key).
//!   - The counts round-trip through snapshot/restore so a promoted primary
//!     inherits them; the merge is per-key grow-only MAX, so a stale peer's
//!     LOWER count can never resurrect a higher local one (the property that
//!     makes the run-start clear unnecessary AND safe — promotion inherits,
//!     cold start sees 0).
//!   - The digest folds each field (count + value), so a replica that bumped
//!     a count past a peer is detected as ahead via `is_behind`, and the two
//!     go quiescent once a snapshot/restore converges them.

use super::*;
use crate::cluster_state::PhaseTally;
use crate::primary::retry_bucket::BucketKind;

fn phase(name: &str) -> PhaseId {
    PhaseId::from(name)
}

// === phase_event_tallies (F4) ===

/// The originator max-bumps; the accessor reads back. A never-incremented
/// key reads 0 (no pre-seed needed) and the two `PhaseTally` keys are
/// independent (one field, two keys).
#[test]
fn phase_event_tally_originate_and_read() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let p = phase("build");
    assert_eq!(
        s.phase_event_tally_for(&(p.clone(), PhaseTally::Completed)),
        0
    );
    assert_eq!(s.phase_event_tally_for(&(p.clone(), PhaseTally::Failed)), 0);

    // Event-shaped: a fail then a (retry) success bumps BOTH keys.
    s.record_phase_event_tally((p.clone(), PhaseTally::Failed), 1);
    s.record_phase_event_tally((p.clone(), PhaseTally::Completed), 1);
    assert_eq!(s.phase_event_tally_for(&(p.clone(), PhaseTally::Failed)), 1);
    assert_eq!(
        s.phase_event_tally_for(&(p.clone(), PhaseTally::Completed)),
        1
    );

    // A lower re-bump is a no-op (grow-only MAX); a higher one ratchets.
    s.record_phase_event_tally((p.clone(), PhaseTally::Completed), 1);
    assert_eq!(
        s.phase_event_tally_for(&(p.clone(), PhaseTally::Completed)),
        1
    );
    s.record_phase_event_tally((p.clone(), PhaseTally::Completed), 3);
    assert_eq!(s.phase_event_tally_for(&(p, PhaseTally::Completed)), 3);
}

/// The event tallies round-trip through snapshot/restore so a promoted
/// primary inherits the SAME event-shaped per-phase numbers (a fail →
/// reinject → succeed task contributed to BOTH).
#[test]
fn phase_event_tallies_snapshot_round_trip() {
    let mut live = ClusterState::<RunnerIdentifier>::new();
    let p = phase("work");
    live.record_phase_event_tally((p.clone(), PhaseTally::Failed), 2);
    live.record_phase_event_tally((p.clone(), PhaseTally::Completed), 5);

    let mut promoted = ClusterState::<RunnerIdentifier>::new();
    promoted.restore(live.snapshot());

    assert_eq!(
        promoted.phase_event_tally_for(&(p.clone(), PhaseTally::Failed)),
        2
    );
    assert_eq!(
        promoted.phase_event_tally_for(&(p, PhaseTally::Completed)),
        5
    );
}

/// Grow-only-MAX merge on restore: a stale peer's LOWER count never
/// resurrects (regresses) a higher local one; a peer's HIGHER count
/// ratchets the local up. This is the clear-gating property — a promotion
/// inherits via max, a cold start (empty map) reads 0, and a stale snapshot
/// can't re-grant a budget.
#[test]
fn phase_event_tallies_restore_is_grow_only_max() {
    let p = phase("work");

    let mut local = ClusterState::<RunnerIdentifier>::new();
    local.record_phase_event_tally((p.clone(), PhaseTally::Completed), 5);

    // A STALE peer holds a LOWER count for the same key — restore must NOT
    // regress the local max.
    let mut stale = ClusterState::<RunnerIdentifier>::new();
    stale.record_phase_event_tally((p.clone(), PhaseTally::Completed), 2);
    local.restore(stale.snapshot());
    assert_eq!(
        local.phase_event_tally_for(&(p.clone(), PhaseTally::Completed)),
        5,
        "a stale lower count must not regress the local max"
    );

    // A peer with a HIGHER count ratchets the local up.
    let mut ahead = ClusterState::<RunnerIdentifier>::new();
    ahead.record_phase_event_tally((p.clone(), PhaseTally::Completed), 9);
    local.restore(ahead.snapshot());
    assert_eq!(
        local.phase_event_tally_for(&(p, PhaseTally::Completed)),
        9,
        "a higher peer count ratchets the local up"
    );
}

/// Digest convergence: a replica that bumped a phase tally past a peer is
/// detected as ahead; after the peer pulls + restores, the two are
/// quiescent (neither behind). Mirrors the `secondary_capacities` /
/// `task_outputs` digest tests.
#[test]
fn phase_event_tallies_digest_detect_then_quiesce() {
    let p = phase("work");
    let mut ahead = ClusterState::<RunnerIdentifier>::new();
    ahead.record_phase_event_tally((p.clone(), PhaseTally::Completed), 4);

    let behind = ClusterState::<RunnerIdentifier>::new();
    // The empty replica is behind the one holding a tally.
    assert!(behind.digest().is_behind(&ahead.digest()));
    assert!(!ahead.digest().is_behind(&behind.digest()));

    // Pull + restore converges them — quiescent both ways.
    let mut healed = behind;
    healed.restore(ahead.snapshot());
    assert_eq!(healed.digest(), ahead.digest());
    assert!(!healed.digest().is_behind(&ahead.digest()));
    assert!(!ahead.digest().is_behind(&healed.digest()));

    // Same-key DIVERGENT count (each bumped to a different value) is
    // detected both ways via the value-folding digest.
    let mut a = ClusterState::<RunnerIdentifier>::new();
    a.record_phase_event_tally((p.clone(), PhaseTally::Completed), 3);
    let mut b = ClusterState::<RunnerIdentifier>::new();
    b.record_phase_event_tally((p, PhaseTally::Completed), 7);
    assert!(a.digest().is_behind(&b.digest()));
    assert!(b.digest().is_behind(&a.digest()));
}

// === retry_passes_used (P3-pass) ===

/// The retry-pass used counter originates + reads + round-trips; the
/// (phase, bucket) keys are independent and the merge is grow-only MAX
/// (a promotion inherits the consumed budget, not re-granted).
#[test]
fn retry_passes_used_originate_round_trip_and_max() {
    let p = phase("work");
    let mut live = ClusterState::<RunnerIdentifier>::new();
    assert_eq!(
        live.retry_pass_used_for(&(p.clone(), BucketKind::Recoverable)),
        0
    );

    live.record_retry_pass_used((p.clone(), BucketKind::Recoverable), 1);
    live.record_retry_pass_used((p.clone(), BucketKind::Oom), 2);
    assert_eq!(
        live.retry_pass_used_for(&(p.clone(), BucketKind::Recoverable)),
        1
    );
    assert_eq!(live.retry_pass_used_for(&(p.clone(), BucketKind::Oom)), 2);

    // Promotion inherits the used budget via max-merge.
    let mut promoted = ClusterState::<RunnerIdentifier>::new();
    promoted.restore(live.snapshot());
    assert_eq!(
        promoted.retry_pass_used_for(&(p.clone(), BucketKind::Recoverable)),
        1
    );
    assert_eq!(
        promoted.retry_pass_used_for(&(p.clone(), BucketKind::Oom)),
        2
    );

    // A stale peer's lower count can't re-grant the budget.
    let mut stale = ClusterState::<RunnerIdentifier>::new();
    stale.record_retry_pass_used((p.clone(), BucketKind::Recoverable), 0);
    promoted.restore(stale.snapshot());
    assert_eq!(
        promoted.retry_pass_used_for(&(p, BucketKind::Recoverable)),
        1,
        "a stale snapshot must not reset the consumed retry budget"
    );
}

/// Digest detect-then-quiesce for the retry-pass counter.
#[test]
fn retry_passes_used_digest_detect_then_quiesce() {
    let p = phase("work");
    let mut ahead = ClusterState::<RunnerIdentifier>::new();
    ahead.record_retry_pass_used((p, BucketKind::Oom), 3);

    let behind = ClusterState::<RunnerIdentifier>::new();
    assert!(behind.digest().is_behind(&ahead.digest()));

    let mut healed = behind;
    healed.restore(ahead.snapshot());
    assert_eq!(healed.digest(), ahead.digest());
    assert!(!healed.digest().is_behind(&ahead.digest()));
}

// === unfulfillable_reinject_used (P3-reinject) ===

/// The reinject used counter originates + reads + round-trips; the merge is
/// grow-only MAX so a promotion inherits the consumed reinject budget.
#[test]
fn unfulfillable_reinject_used_originate_round_trip_and_max() {
    let mut live = ClusterState::<RunnerIdentifier>::new();
    assert_eq!(live.unfulfillable_reinject_used_for("hash-1"), 0);

    live.record_unfulfillable_reinject_used("hash-1".to_string(), 1);
    live.record_unfulfillable_reinject_used("hash-2".to_string(), 2);
    assert_eq!(live.unfulfillable_reinject_used_for("hash-1"), 1);
    assert_eq!(live.unfulfillable_reinject_used_for("hash-2"), 2);

    let mut promoted = ClusterState::<RunnerIdentifier>::new();
    promoted.restore(live.snapshot());
    assert_eq!(promoted.unfulfillable_reinject_used_for("hash-1"), 1);
    assert_eq!(promoted.unfulfillable_reinject_used_for("hash-2"), 2);

    // A stale lower count can't re-grant the budget.
    let mut stale = ClusterState::<RunnerIdentifier>::new();
    stale.record_unfulfillable_reinject_used("hash-1".to_string(), 0);
    promoted.restore(stale.snapshot());
    assert_eq!(
        promoted.unfulfillable_reinject_used_for("hash-1"),
        1,
        "a stale snapshot must not reset the consumed reinject budget"
    );
}

/// Digest detect-then-quiesce for the reinject used counter.
#[test]
fn unfulfillable_reinject_used_digest_detect_then_quiesce() {
    let mut ahead = ClusterState::<RunnerIdentifier>::new();
    ahead.record_unfulfillable_reinject_used("h".to_string(), 2);

    let behind = ClusterState::<RunnerIdentifier>::new();
    assert!(behind.digest().is_behind(&ahead.digest()));

    let mut healed = behind;
    healed.restore(ahead.snapshot());
    assert_eq!(healed.digest(), ahead.digest());
    assert!(!healed.digest().is_behind(&ahead.digest()));
}
