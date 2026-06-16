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

/// #358 real-time mirror exactness: the F4 tally bumps on the WINNING
/// `TaskCompleted` APPLY itself (the `merge_task_state` join), so a mirror
/// fed only the per-completion mutation broadcasts — NO snapshot, NO
/// anti-entropy round — holds the exact event count the moment the task
/// states land. This is the wire shape of the failover-lag bug: pre-fix
/// the mirror held N completed STATES but a 0 tally until the next
/// snapshot merge.
#[test]
fn tally_bumps_on_task_completed_apply_in_real_time() {
    let p = phase("p0"); // mk_task's phase
    let mut mirror = ClusterState::<RunnerIdentifier>::new();
    for (hash, name) in [("ha", "a"), ("hb", "b"), ("hc", "c")] {
        mirror.apply(ClusterMutation::TaskAdded {
            hash: hash.into(),
            task: mk_task(name),
            def_id: None,
        });
        assert_eq!(
            mirror.apply(ClusterMutation::TaskCompleted {
                hash: hash.into(),
                result_data: None,
                attempt: 0,
            }),
            ApplyOutcome::Applied
        );
    }
    assert_eq!(
        mirror.phase_event_tally_for(&(p.clone(), PhaseTally::Completed)),
        3,
        "the mirror's tally must be exact in real time, from broadcasts alone"
    );
    // At-least-once redelivery of an already-won completion NoOps the join
    // and must NOT re-bump.
    assert_eq!(
        mirror.apply(ClusterMutation::TaskCompleted {
            hash: "ha".into(),
            result_data: None,
            attempt: 0,
        }),
        ApplyOutcome::NoOp
    );
    assert_eq!(
        mirror.phase_event_tally_for(&(p, PhaseTally::Completed)),
        3,
        "an idempotent redelivery must not double-count"
    );
}

/// #358 convergence proof (the apply ∘ snapshot idempotence): a replica
/// that already applied N `TaskCompleted` broadcasts (apply-side bumps →
/// tally N) and THEN merges the originator's snapshot carrying the SAME N
/// events (its task states AND its tally field) converges to N, not 2N —
/// the in-snapshot state transitions NoOp the join (no re-bump) and the
/// grow-MAX field merge aliases the already-counted events.
#[test]
fn tally_apply_then_snapshot_merge_converges_without_double_count() {
    let p = phase("p0");
    let mutations = |state: &mut ClusterState<RunnerIdentifier>| {
        for (hash, name) in [("ha", "a"), ("hb", "b"), ("hc", "c")] {
            state.apply(ClusterMutation::TaskAdded {
                hash: hash.into(),
                task: mk_task(name),
                def_id: None,
            });
            state.apply(ClusterMutation::TaskCompleted {
                hash: hash.into(),
                result_data: None,
                attempt: 0,
            });
        }
    };
    let mut originator = ClusterState::<RunnerIdentifier>::new();
    mutations(&mut originator);
    assert_eq!(
        originator.phase_event_tally_for(&(p.clone(), PhaseTally::Completed)),
        3
    );

    let mut replica = ClusterState::<RunnerIdentifier>::new();
    mutations(&mut replica); // saw every broadcast → tally already 3
    replica.restore(originator.snapshot());
    assert_eq!(
        replica.phase_event_tally_for(&(p, PhaseTally::Completed)),
        3,
        "apply-side bumps + the snapshot's tally must alias under grow-MAX \
         (3), never sum (6)"
    );
}

/// #358 restore-side exactness, cold + partial-knowledge:
///   * a COLD replica restoring a snapshot with N completed states + tally
///     N lands on N (the restore-side transition bumps and the co-present
///     field max-merge alias, states-before-fields order);
///   * a replica that observed a DIFFERENT event via broadcast (task b)
///     and restores a snapshot covering only task a converges to the
///     UNION coverage (2) — the field merge alone (max(1, 1) = 1) could
///     not express it; the restore-side bump is what makes the tally
///     exact.
#[test]
fn tally_restore_covers_cold_and_partial_knowledge_union() {
    let p = phase("p0");
    // Originator saw ONLY task a complete.
    let mut originator = ClusterState::<RunnerIdentifier>::new();
    for (hash, name) in [("ha", "a"), ("hb", "b")] {
        originator.apply(ClusterMutation::TaskAdded {
            hash: hash.into(),
            task: mk_task(name),
            def_id: None,
        });
    }
    originator.apply(ClusterMutation::TaskCompleted {
        hash: "ha".into(),
        result_data: None,
        attempt: 0,
    });

    // Cold replica: restore alone yields the exact count, not 2x.
    let mut cold = ClusterState::<RunnerIdentifier>::new();
    cold.restore(originator.snapshot());
    assert_eq!(
        cold.phase_event_tally_for(&(p.clone(), PhaseTally::Completed)),
        1,
        "cold restore: the state-merge bump and the snapshot tally alias"
    );

    // Partial-knowledge replica: saw ONLY task b's completion broadcast.
    let mut partial = ClusterState::<RunnerIdentifier>::new();
    for (hash, name) in [("ha", "a"), ("hb", "b")] {
        partial.apply(ClusterMutation::TaskAdded {
            hash: hash.into(),
            task: mk_task(name),
            def_id: None,
        });
    }
    partial.apply(ClusterMutation::TaskCompleted {
        hash: "hb".into(),
        result_data: None,
        attempt: 0,
    });
    partial.restore(originator.snapshot());
    assert_eq!(
        partial.phase_event_tally_for(&(p, PhaseTally::Completed)),
        2,
        "restore must add the snapshot's UNSEEN event (a) on top of the \
         locally-observed one (b) — union coverage, not per-replica max"
    );
}

/// #358 failed-side twin, EVENT-shaped on the apply path: each WINNING
/// failure-terminal apply bumps the Failed tally (a higher-attempt
/// re-failure counts again — B1 cadence), an idempotent redelivery NoOps
/// (no double-count), and the eventual retry-success bumps Completed — a
/// fail → retry → fail → retry → succeed task reports failed=2,
/// completed=1 on EVERY node that applied the broadcasts.
#[test]
fn tally_failed_twin_is_event_shaped_on_apply() {
    let p = phase("p0");
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "ha".into(),
        task: mk_task("a"),
        def_id: None,
    });
    let fail = |attempt: u32, seq: u32| ClusterMutation::TaskFailed {
        hash: "ha".into(),
        kind: ErrorType::Recoverable,
        error: "transient".into(),
        version: TaskVersion {
            primary_epoch: 0,
            seq,
        },
        attempt,
    };
    // First failure (attempt 0) wins → Failed = 1.
    assert_eq!(s.apply(fail(0, 1)), ApplyOutcome::Applied);
    assert_eq!(s.phase_event_tally_for(&(p.clone(), PhaseTally::Failed)), 1);
    // At-least-once redelivery of the SAME failure NoOps → still 1.
    assert_eq!(s.apply(fail(0, 1)), ApplyOutcome::NoOp);
    assert_eq!(s.phase_event_tally_for(&(p.clone(), PhaseTally::Failed)), 1);
    // Retry reset (rank-DROP arm, not a join) — no tally movement.
    assert_eq!(
        s.apply(ClusterMutation::TaskRetried {
            hash: "ha".into(),
            attempt: 1,
            version: TaskVersion {
                primary_epoch: 0,
                seq: 2,
            },
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(s.phase_event_tally_for(&(p.clone(), PhaseTally::Failed)), 1);
    // Re-failure under the new generation wins → Failed = 2.
    assert_eq!(s.apply(fail(1, 3)), ApplyOutcome::Applied);
    assert_eq!(s.phase_event_tally_for(&(p.clone(), PhaseTally::Failed)), 2);
    // Second retry + eventual success: Completed = 1, Failed stays 2 —
    // event counts, not a projection of the single terminal state.
    s.apply(ClusterMutation::TaskRetried {
        hash: "ha".into(),
        attempt: 2,
        version: TaskVersion {
            primary_epoch: 0,
            seq: 4,
        },
    });
    assert_eq!(
        s.apply(ClusterMutation::TaskCompleted {
            hash: "ha".into(),
            result_data: None,
            attempt: 2,
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        s.phase_event_tally_for(&(p.clone(), PhaseTally::Completed)),
        1
    );
    assert_eq!(s.phase_event_tally_for(&(p, PhaseTally::Failed)), 2);
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
