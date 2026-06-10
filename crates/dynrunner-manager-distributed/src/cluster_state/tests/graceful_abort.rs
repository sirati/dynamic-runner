//! `ClusterMutation::GracefulAbortRequested` CRDT semantics — the
//! replicated dispatch-freeze latch behind the observer-requested
//! graceful abort.
//!
//! Pins:
//!   * the apply rule is a sticky false→true latch (`Applied` once,
//!     `NoOp` on re-application — the `RunComplete` shape);
//!   * the latch is independent of the two run terminals (a freeze is
//!     not a completion and not a hard abort);
//!   * the latch survives the snapshot round-trip (the no-redo law: a
//!     failover-promoted primary restoring a frozen snapshot inherits
//!     the freeze) and the restore merge is the one-directional `|=`
//!     ratchet (an un-frozen snapshot never clears a local freeze);
//!   * the digest carries the latch so the AE detector pulls the
//!     freeze across (`is_behind` one-directionally);
//!   * `inflight_count_for_secondary` projects per-secondary occupancy
//!     off the replicated `InFlight` entries (the
//!     `MostActiveWorkers` relocation-policy input).

use super::*;

#[test]
fn graceful_abort_latch_is_sticky_and_idempotent() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    assert!(!s.graceful_abort_requested(), "fresh state is not frozen");
    assert_eq!(
        s.apply(ClusterMutation::GracefulAbortRequested),
        ApplyOutcome::Applied
    );
    assert!(s.graceful_abort_requested());
    // Sticky monotonic: a re-applied request (operator re-trigger /
    // at-least-once delivery) is a NoOp — never re-broadcast-amplified.
    assert_eq!(
        s.apply(ClusterMutation::GracefulAbortRequested),
        ApplyOutcome::NoOp
    );
    assert!(s.graceful_abort_requested());
}

#[test]
fn graceful_abort_independent_of_run_terminals() {
    // The freeze is neither a completion nor a hard abort — all three
    // latches are orthogonal facts (the graceful VERDICT is the
    // composition `run_complete ∧ graceful_abort`, derived by readers).
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::GracefulAbortRequested);
    assert!(s.graceful_abort_requested());
    assert!(!s.run_complete());
    assert!(s.run_aborted().is_none());

    s.apply(ClusterMutation::RunComplete);
    assert!(s.run_complete() && s.graceful_abort_requested());
}

#[test]
fn graceful_abort_latch_survives_snapshot_restore() {
    // The no-redo law end to end: a frozen primary's snapshot restored
    // into a fresh replica (the promotion seed path) carries the freeze.
    let mut frozen = ClusterState::<RunnerIdentifier>::new();
    frozen.apply(ClusterMutation::GracefulAbortRequested);

    let mut promoted = ClusterState::<RunnerIdentifier>::new();
    promoted.restore(frozen.snapshot());
    assert!(
        promoted.graceful_abort_requested(),
        "a promoted primary restoring a frozen snapshot must inherit the freeze"
    );

    // One-directional ratchet: an UN-frozen snapshot never clears a
    // local freeze (sticky `|=`, mirroring run_complete).
    let unfrozen = ClusterState::<RunnerIdentifier>::new();
    promoted.restore(unfrozen.snapshot());
    assert!(
        promoted.graceful_abort_requested(),
        "an un-frozen snapshot must never regress the latch"
    );
}

#[test]
fn graceful_abort_latch_is_carried_in_the_digest() {
    // The AE detector sees the freeze: an un-latched replica is behind a
    // latched peer (and only in that direction), so the snapshot pull
    // propagates the freeze to a replica that missed the live broadcast.
    let mut frozen = ClusterState::<RunnerIdentifier>::new();
    frozen.apply(ClusterMutation::GracefulAbortRequested);
    let cold = ClusterState::<RunnerIdentifier>::new();
    assert!(cold.digest().is_behind(&frozen.digest()));
    assert!(!frozen.digest().is_behind(&cold.digest()));
}

#[test]
fn inflight_count_for_secondary_projects_replicated_occupancy() {
    // The MostActiveWorkers relocation input: per-secondary `InFlight`
    // counts off the replicated ledger, dropping as terminals land.
    let mut s = ClusterState::<RunnerIdentifier>::new();
    for (hash, secondary) in [("h1", "sec-a"), ("h2", "sec-a"), ("h3", "sec-b")] {
        s.apply(ClusterMutation::TaskAdded {
            hash: hash.into(),
            task: mk_task(hash),
        });
        s.apply(ClusterMutation::TaskAssigned {
            hash: hash.into(),
            secondary: secondary.into(),
            worker: 0,
            version: TaskVersion {
                primary_epoch: 1,
                seq: 1,
            },
            attempt: 0,
        });
    }
    assert_eq!(s.inflight_count_for_secondary("sec-a"), 2);
    assert_eq!(s.inflight_count_for_secondary("sec-b"), 1);
    assert_eq!(s.inflight_count_for_secondary("sec-c"), 0);

    // A terminal landing drops the occupancy — the drain the graceful
    // protocol watches.
    s.apply(ClusterMutation::TaskCompleted {
        hash: "h1".into(),
        result_data: None,
        attempt: 0,
    });
    assert_eq!(s.inflight_count_for_secondary("sec-a"), 1);
}
