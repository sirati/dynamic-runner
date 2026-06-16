//! Tests for the incremental outcome tally (`outcome_counts()` served O(1))
//! and its correctness invariant: the maintained tally must NEVER drift from
//! the full-walk oracle.
//!
//! The single load-bearing check (`outcome_tally_matches_scan`): the
//! incrementally-maintained `outcome_counts()` equals the `#[cfg(test)]`
//! `outcome_counts_by_scan()` full-walk over the LOGICAL ledger (fat `tasks`
//! ∪ spilled `settled`) after EVERY mutation across a sequence that exercises
//! every transition class — a create, a join win to terminal, a terminal→
//! non-terminal DECREMENT (retry reset), a terminal→different-terminal, the
//! success-like terminals (skip / setup / affine), a cascade Blocked→Pending→
//! Completed, AND a SPILL crossing (fat→settled). A missed increment or
//! decrement at any write path makes the two diverge at the exact mutation
//! that missed it.

use super::*;

/// Assert the tally invariant on `s`: the O(1) maintained `outcome_counts()`
/// is byte-identical to the full-walk `outcome_counts_by_scan()` oracle, for
/// EVERY bucket. THE load-bearing check — a single missed increment/decrement
/// silently desyncs the tally → a wrong operator-facing partition and a wrong
/// finalize stranded-accounting.
fn assert_tally_matches_scan(s: &ClusterState<RunnerIdentifier>) {
    let tally = s.outcome_counts();
    let scan = s.outcome_counts_by_scan();
    assert_eq!(
        tally, scan,
        "the O(1) outcome tally diverged from the full-walk scan — a mutation \
         site failed to increment/decrement the outcome partition"
    );
}

/// THE tally invariant pin (mirrors `range_digest_memo_matches_fresh_fold`):
/// the incrementally-maintained tally equals a fresh scan after EVERY mutation
/// across a sequence covering every transition class — INCLUDING a terminal→
/// non-terminal decrement (retry reset) and a fat→settled SPILL crossing.
#[test]
fn outcome_tally_matches_scan() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut s = ClusterState::<RunnerIdentifier>::new();
    // Detach the per-coordinator production spill writer so the test attaches
    // its OWN writer over a unique path via `test_spill_all` (isolated).
    s.detach_spill_writer_for_test();
    assert_tally_matches_scan(&s); // empty ledger: all buckets zero

    // --- creates (TaskAdded vacant insert → None, non-terminal) ---
    for i in 0..20 {
        s.apply(ClusterMutation::TaskAdded {
            hash: format!("t{i:02}"),
            task: mk_task(&format!("t{i:02}")),
            def_id: None,
        });
        assert_tally_matches_scan(&s);
    }

    // --- Pending→InFlight→Completed (the success path; the increment is at
    //     the terminal transition, NOT the assign) ---
    s.apply(ClusterMutation::TaskAssigned {
        hash: "t00".into(),
        secondary: "s1".into(),
        worker: 0,
        version: Default::default(),
        attempt: 0,
    });
    assert_tally_matches_scan(&s);
    s.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "t00".into(),
        result_data: None,
    });
    assert_tally_matches_scan(&s);
    assert_eq!(s.outcome_counts().succeeded, 1);

    // --- Failed{Recoverable} → retry reset (Failed→Pending) → re-Completed:
    //     the terminal→NON-terminal DECREMENT, then a fresh terminal. After
    //     the reset `fail_retry` must drop back to 0; after the re-complete
    //     `succeeded` must rise. ---
    s.apply(ClusterMutation::TaskFailed {
        hash: "t01".into(),
        kind: ErrorType::Recoverable,
        error: "boom".into(),
        version: Default::default(),
        attempt: 0,
    });
    assert_tally_matches_scan(&s);
    assert_eq!(s.outcome_counts().fail_retry, 1);
    // Retry reset: Failed{Recoverable} → Pending (DECREMENT fail_retry).
    s.apply(ClusterMutation::TaskRetried {
        hash: "t01".into(),
        attempt: 1,
        version: Default::default(),
    });
    assert_tally_matches_scan(&s);
    assert_eq!(
        s.outcome_counts().fail_retry,
        0,
        "a retry reset (Failed→Pending) must DECREMENT fail_retry"
    );
    // Re-completes after the retry → succeeded rises to 2.
    s.apply(ClusterMutation::TaskCompleted {
        attempt: 1,
        hash: "t01".into(),
        result_data: None,
    });
    assert_tally_matches_scan(&s);
    assert_eq!(s.outcome_counts().succeeded, 2);

    // --- Failed{NonRecoverable} → fail_final (a terminal failure) ---
    s.apply(ClusterMutation::TaskFailed {
        hash: "t02".into(),
        kind: ErrorType::NonRecoverable,
        error: "fatal".into(),
        version: Default::default(),
        attempt: 0,
    });
    assert_tally_matches_scan(&s);
    assert_eq!(s.outcome_counts().fail_final, 1);

    // --- Failed{ResourceExhausted("memory")} → fail_oom ---
    s.apply(ClusterMutation::TaskFailed {
        hash: "t03".into(),
        kind: ErrorType::ResourceExhausted("memory".into()),
        error: "oom".into(),
        version: Default::default(),
        attempt: 0,
    });
    assert_tally_matches_scan(&s);
    assert_eq!(s.outcome_counts().fail_oom, 1);

    // --- Blocked→Pending→Completed (cascade resume): block t05 on t04,
    //     complete t04 → t05 auto-resumes Blocked→Pending, then complete t05.
    //     Blocked is non-terminal throughout, so the only increment is at t05's
    //     terminal completion (and t04's). ---
    s.apply(ClusterMutation::TaskBlocked {
        hash: "t05".into(),
        on: "t04".into(),
    });
    assert_tally_matches_scan(&s);
    s.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "t04".into(),
        result_data: None,
    });
    assert_tally_matches_scan(&s); // t04 terminal + t05 resumed to Pending
    s.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "t05".into(),
        result_data: None,
    });
    assert_tally_matches_scan(&s);

    // --- the success-like terminals in their OWN buckets ---
    s.apply(ClusterMutation::TaskSkippedAlreadyDone { hash: "t06".into() });
    assert_tally_matches_scan(&s);
    assert_eq!(s.outcome_counts().skipped, 1);
    s.apply(ClusterMutation::SetupCompleted { hash: "t07".into() });
    assert_tally_matches_scan(&s);
    assert_eq!(s.outcome_counts().setup_succeeded, 1);
    s.apply(ClusterMutation::AffineReady { hash: "t08".into() });
    assert_tally_matches_scan(&s);
    assert_eq!(s.outcome_counts().affine_ready, 1);

    // Complete a spread of the remaining Pending tasks so the spill has a
    // representative slice of settle-eligible terminals to move.
    for i in 9..20 {
        s.apply(ClusterMutation::TaskCompleted {
            attempt: 0,
            hash: format!("t{i:02}"),
            result_data: None,
        });
        assert_tally_matches_scan(&s);
    }

    // Snapshot the partition just before the spill so we can prove the SPILL
    // is tally-NEUTRAL (the counts must be byte-identical after the crossing).
    let before_spill = s.outcome_counts();

    // --- SPILL crossing: a slice of settle-eligible terminals moves
    //     fat→settled. The tally must NOT change (the entry's outcome class is
    //     unchanged by the move; it was counted while fat and stays counted
    //     while settled). The oracle now walks BOTH halves, so the invariant
    //     proves the maintained tally already counts the settled half. ---
    let evicted = s.test_spill_all(&dir.path().join("spill.cbor"));
    assert!(
        evicted > 0,
        "the fixture must actually spill some terminals for the crossing to mean anything"
    );
    assert_eq!(s.tasks_in_memory(), s.task_count() - evicted);
    assert_tally_matches_scan(&s);
    assert_eq!(
        s.outcome_counts(),
        before_spill,
        "a fat→settled spill must be tally-NEUTRAL — the outcome partition is \
         unchanged by moving a terminal's body between the fat and settled halves"
    );

    // --- a post-spill terminal still lands in the tally (the seam is live
    //     after the crossing) ---
    s.apply(ClusterMutation::TaskAdded {
        hash: "post-spill".into(),
        task: mk_task("post-spill"),
        def_id: None,
    });
    assert_tally_matches_scan(&s);
    s.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "post-spill".into(),
        result_data: None,
    });
    assert_tally_matches_scan(&s);

    // --- restore chokepoint: merge a divergent peer snapshot (many per-task
    //     joins routing through merge_task_state → set_task_state) ---
    let mut peer = ClusterState::<RunnerIdentifier>::new();
    for i in 30..40 {
        peer.apply(ClusterMutation::TaskAdded {
            hash: format!("p{i}"),
            task: mk_task(&format!("p{i}")),
            def_id: None,
        });
        peer.apply(ClusterMutation::TaskCompleted {
            attempt: 0,
            hash: format!("p{i}"),
            result_data: None,
        });
    }
    s.restore(peer.snapshot());
    assert_tally_matches_scan(&s);
    // A second restore (NoOp merge) must leave the tally intact.
    s.restore(peer.snapshot());
    assert_tally_matches_scan(&s);

    // Final cross-check: the maintained partition's totals match the scan's
    // (and a non-trivial number of terminals were tallied through all paths).
    assert_eq!(s.outcome_counts(), s.outcome_counts_by_scan());
    assert!(s.outcome_counts().total_terminal() > 20);
}
