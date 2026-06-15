//! Tests for the anti-entropy `digest()` projection.
//!
//! Covers: a converged replica produces an equal digest; the digest is
//! order-independent (insertion order does not change the fold); the
//! `is_behind` detector fires when a replica is missing data and goes
//! quiescent once a snapshot/restore converges the two — the
//! detect → pull → converge → quiesce cycle the cadence relies on.

use super::*;

/// Two replicas that converged to the same state (one built directly, one
/// built then snapshot/restored onto a fresh replica) produce IDENTICAL
/// digests. This is the self-quiescing precondition: a converged mesh
/// exchanges matching digests and pulls nothing.
#[test]
fn converged_replicas_produce_equal_digests() {
    let mut a = ClusterState::<RunnerIdentifier>::new();
    a.apply(ClusterMutation::TaskAdded {
        hash: "p".into(),
        task: mk_task("p"),
    });
    a.apply(ClusterMutation::TaskAdded {
        hash: "c".into(),
        task: mk_task("c"),
    });
    a.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "c".into(),
        result_data: None,
    });
    a.apply(ClusterMutation::PrimaryChanged {
        new: "s1".into(),
        epoch: 4,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });

    let mut b = ClusterState::<RunnerIdentifier>::new();
    b.restore(a.snapshot());

    // The two replicas hold the same replicated ledger, so their digests
    // are byte-identical and neither is "behind" the other.
    assert_eq!(a.digest(), b.digest());
    assert!(!a.digest().is_behind(&b.digest()));
    assert!(!b.digest().is_behind(&a.digest()));
}

/// The task fold is order-independent: applying the same set of
/// `TaskAdded`s in a different order yields the same digest.
#[test]
fn digest_is_order_independent() {
    let mut a = ClusterState::<RunnerIdentifier>::new();
    for name in ["alpha", "beta", "gamma", "delta"] {
        a.apply(ClusterMutation::TaskAdded {
            hash: name.into(),
            task: mk_task(name),
        });
    }
    let mut b = ClusterState::<RunnerIdentifier>::new();
    for name in ["delta", "alpha", "gamma", "beta"] {
        b.apply(ClusterMutation::TaskAdded {
            hash: name.into(),
            task: mk_task(name),
        });
    }
    assert_eq!(a.digest(), b.digest());
}

/// A task advancing to a stronger state (same key, same count) changes
/// the fold — so a replica that has NOT seen the advance is detected as
/// behind even though the task COUNT is unchanged.
#[test]
fn same_count_state_advance_changes_fold() {
    let mut a = ClusterState::<RunnerIdentifier>::new();
    a.apply(ClusterMutation::TaskAdded {
        hash: "t".into(),
        task: mk_task("t"),
    });
    let stale = a.digest();
    a.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "t".into(),
        result_data: None,
    });
    let advanced = a.digest();
    // Count unchanged (one task), but the fold differs because the entry
    // advanced Pending → Completed.
    assert_eq!(stale.tasks_count, advanced.tasks_count);
    assert_ne!(stale.tasks_hash, advanced.tasks_hash);
    // The replica still on `Pending` is behind the one that saw Completed.
    assert!(stale.is_behind(&advanced));
}

/// The retry-attempt generation (F2) participates in the `tasks_hash` fold
/// directly: a `Failed { attempt: 0 }` and a `Pending { attempt: 1 }` for
/// the SAME task hash produce DIFFERENT digests even though the task count
/// is unchanged. This pins the attempt-prepended leg of `hashable_join_key`
/// — the retry reset is at a lower band/rank than the Failed it supersedes,
/// so without the prepended `attempt` the fold could collapse them; with it,
/// the higher-attempt replica is detected as ahead and drives the heal.
#[test]
fn retry_attempt_advance_changes_fold() {
    // Replica `a`: task added then failed → `Failed { attempt: 0 }`.
    let mut a = ClusterState::<RunnerIdentifier>::new();
    a.apply(ClusterMutation::TaskAdded {
        hash: "t".into(),
        task: mk_task("t"),
    });
    a.apply(ClusterMutation::TaskFailed {
        hash: "t".into(),
        kind: ErrorType::Recoverable,
        error: "boom".into(),
        version: Default::default(),
        attempt: 0,
    });

    // Replica `b`: same up to the failure, then the F2 retry reset
    // `Failed { attempt: 0 } → Pending { attempt: 1 }`.
    let mut b = ClusterState::<RunnerIdentifier>::new();
    b.apply(ClusterMutation::TaskAdded {
        hash: "t".into(),
        task: mk_task("t"),
    });
    b.apply(ClusterMutation::TaskFailed {
        hash: "t".into(),
        kind: ErrorType::Recoverable,
        error: "boom".into(),
        version: Default::default(),
        attempt: 0,
    });
    b.apply(ClusterMutation::TaskRetried {
        hash: "t".into(),
        attempt: 1,
        version: Default::default(),
    });

    // Same single task on each replica, but the prepended `attempt` makes
    // the per-entry fold differ.
    assert_eq!(a.digest().tasks_count, b.digest().tasks_count);
    assert_ne!(a.digest().tasks_hash, b.digest().tasks_hash);
    // The `Failed { attempt: 0 }` replica is behind the `Pending {
    // attempt: 1 }` one — the divergence drives a pull of the higher
    // generation.
    assert!(a.digest().is_behind(&b.digest()));
}

/// A divergence in the role-capability 2P-set IS detected by the
/// anti-entropy detector (C6 — reversed from the pre-C6 detector, which
/// excluded role sets). The capability lattice is a proper CRDT (merged
/// monotonically by `merge_capability` in `restore`), so folding it is
/// detect-WITH-heal: a flagged divergence is one a snapshot pull resolves,
/// NOT the R2 no-op loop the old projection-only design had to avoid.
#[test]
fn observer_capability_divergence_detected_and_heals() {
    // Two replicas with the SAME task ledger.
    let mut a = ClusterState::<RunnerIdentifier>::new();
    let mut b = ClusterState::<RunnerIdentifier>::new();
    for s in [&mut a, &mut b] {
        s.apply(ClusterMutation::TaskAdded {
            hash: "t".into(),
            task: mk_task("t"),
        });
    }
    // `a` knows an observer capability that `b` does not — a capability
    // 2P-set divergence with identical tasks.
    a.apply(ClusterMutation::PeerJoined {
        peer_id: "obs-x".into(),
        is_observer: true,
        can_be_primary: false,
        cap_version: Default::default(),
        member_gen: 0,
    });

    // The digests DIFFER (the capability fold is summarised) and `b` is
    // behind `a` — the missing capability drives a pull.
    assert_ne!(a.digest(), b.digest());
    assert!(b.digest().is_behind(&a.digest()));

    // Pull: the snapshot's capability 2P-set merges into `b` and converges
    // the digest (detect-WITH-heal — no permanent no-op loop).
    b.restore(a.snapshot());
    assert_eq!(a.digest(), b.digest());
    assert!(!b.digest().is_behind(&a.digest()));
}

/// A divergence in the `can_be_primary` capability is detected and heals,
/// the same way the observer capability does (C6) — the capability 2P-set
/// is the SINGLE replicated source and is snapshot-healable.
#[test]
fn can_be_primary_capability_divergence_detected_and_heals() {
    let mut a = ClusterState::<RunnerIdentifier>::new();
    let mut b = ClusterState::<RunnerIdentifier>::new();
    for s in [&mut a, &mut b] {
        s.apply(ClusterMutation::TaskAdded {
            hash: "t".into(),
            task: mk_task("t"),
        });
    }
    // `a` granted a peer primary-capability that `b` has not seen — a
    // capability 2P-set divergence with identical tasks.
    a.apply(ClusterMutation::SetCanBePrimary {
        peer_id: "cap-x".into(),
        can_be_primary: true,
        cap_version: Default::default(),
    });

    assert_ne!(a.digest(), b.digest());
    assert!(b.digest().is_behind(&a.digest()));

    b.restore(a.snapshot());
    assert_eq!(a.digest(), b.digest());
    assert!(!b.digest().is_behind(&a.digest()));
}

/// The residual LIVENESS divergence after a capability tombstone converges
/// does NOT drive further pulls — honest-liveness (C6). A `PeerRemoved`
/// writes BOTH a capability `Departed` tombstone (a converging fact, folded
/// into the digest) AND a `peer_state` Dead bit (node-local, NOT folded).
/// Once the tombstone converges via pull, the two replicas still disagree
/// on the `peer_state` alive/dead bit — `a` holds w1 Alive, `b` holds it
/// Dead — and anti-entropy must NEVER force-converge that (it must never
/// resurrect a peer `b` correctly buried). So post-convergence the digests
/// match and neither is behind, even though the liveness views diverge.
#[test]
fn residual_liveness_divergence_is_not_behind() {
    let mut a = ClusterState::<RunnerIdentifier>::new();
    let mut b = ClusterState::<RunnerIdentifier>::new();
    for s in [&mut a, &mut b] {
        s.apply(ClusterMutation::TaskAdded {
            hash: "t".into(),
            task: mk_task("t"),
        });
        // Both saw the peer join (Alive + an Advertised capability entry).
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "w1".into(),
            is_observer: false,
            can_be_primary: false,
            cap_version: Default::default(),
            member_gen: 0,
        });
    }
    // `b` locally detected `w1` as dead and buried it: this writes a
    // capability `Departed` tombstone AND marks `peer_state` Dead.
    b.apply(ClusterMutation::PeerRemoved {
        id: "w1".into(),
        cause: RemovalCause::KeepaliveMiss,
        member_gen: 0,
    });
    // The capability tombstone is a converging fact, so `a` IS behind `b`
    // until it pulls (detect-WITH-heal).
    assert!(a.digest().is_behind(&b.digest()));
    a.restore(b.snapshot());

    // After convergence: capability matches (both hold w1 Departed). The
    // LIVENESS still diverges — `a` holds w1 Alive (restore never buries),
    // `b` holds it Dead — but liveness is NOT folded, so the digests match
    // and neither drives a further pull.
    assert!(a.is_peer_alive("w1"), "restore must NOT bury a's live peer");
    assert!(!b.is_peer_alive("w1"), "b correctly holds w1 dead");
    assert_eq!(a.digest(), b.digest());
    assert!(!a.digest().is_behind(&b.digest()));
    assert!(!b.digest().is_behind(&a.digest()));
}

/// The full detect → pull → converge → quiesce cycle the cadence drives:
/// an incomplete replica is detected as behind a complete one; pulling +
/// restoring the snapshot converges it; the SECOND digest comparison now
/// matches and triggers no further pull.
#[test]
fn divergence_detected_then_quiescent_after_restore() {
    // Complete replica.
    let mut complete = ClusterState::<RunnerIdentifier>::new();
    complete.apply(ClusterMutation::TaskAdded {
        hash: "a".into(),
        task: mk_task("a"),
    });
    complete.apply(ClusterMutation::TaskAdded {
        hash: "b".into(),
        task: mk_task("b"),
    });
    complete.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "b".into(),
        result_data: None,
    });

    // Incomplete replica: only saw task "a".
    let mut incomplete = ClusterState::<RunnerIdentifier>::new();
    incomplete.apply(ClusterMutation::TaskAdded {
        hash: "a".into(),
        task: mk_task("a"),
    });

    // First round: the incomplete replica is behind the complete one.
    assert!(incomplete.digest().is_behind(&complete.digest()));

    // Pull: the existing snapshot RPC + restore lattice converges it.
    incomplete.restore(complete.snapshot());

    // Second round: digests now match → no further pull (self-quiescing).
    assert_eq!(incomplete.digest(), complete.digest());
    assert!(!incomplete.digest().is_behind(&complete.digest()));
}

/// The discovery-debt lattice height threads through `digest()` and drives
/// the AE detector by STRICT lattice-height compare across all three states
/// `Undeclared < Owed < Settled`. Pins the case the single-bool projection
/// could NOT carry: an `Undeclared` local (missed the live `Declared`) is
/// BEHIND an `Owed` peer and pulls it via the snapshot `max`-join, then the
/// pair quiesces. Also pins the `Owed`-behind-`Settled` heal and the
/// one-directional reverse (a higher local is never behind a lower peer).
#[test]
fn discovery_debt_divergence_detected_and_heals() {
    // A replica that declared discovery and settled it.
    let mut settled = ClusterState::<RunnerIdentifier>::new();
    settled.apply(ClusterMutation::DiscoveryDebtDeclared);
    settled.apply(ClusterMutation::DiscoverySettled);

    // A replica still owing discovery (declared, not yet settled).
    let mut owed = ClusterState::<RunnerIdentifier>::new();
    owed.apply(ClusterMutation::DiscoveryDebtDeclared);

    // A replica that missed the `Declared` broadcast entirely (BOTTOM).
    let undeclared = ClusterState::<RunnerIdentifier>::new();

    // Strict lattice-height: a lower local is behind a higher peer, in both
    // adjacent steps. THE bool-missed case is Undeclared-behind-Owed.
    assert!(
        undeclared.digest().is_behind(&owed.digest()),
        "an Undeclared local must be behind an Owed peer (the bool-missed case)"
    );
    assert!(
        owed.digest().is_behind(&settled.digest()),
        "an Owed local must be behind a Settled peer"
    );
    // Reverse: a higher local is never behind a lower peer.
    assert!(
        !owed.digest().is_behind(&undeclared.digest()),
        "an Owed local must NOT be behind an Undeclared peer"
    );
    assert!(
        !settled.digest().is_behind(&owed.digest()),
        "a Settled local must NOT be behind an Owed peer (top-wins)"
    );

    // Pull: restoring the Owed snapshot ratchets the Undeclared local up to
    // Owed (the convergence the bool could not detect), then it quiesces vs
    // that peer.
    let mut healing = ClusterState::<RunnerIdentifier>::new();
    healing.restore(owed.snapshot());
    assert_eq!(
        healing.digest().discovery_debt,
        owed.digest().discovery_debt,
        "the Undeclared local must now match the Owed peer"
    );
    assert!(
        !healing.digest().is_behind(&owed.digest()),
        "converged → no further pull (self-quiescing)"
    );

    // And the Owed → Settled heal likewise quiesces.
    owed.restore(settled.snapshot());
    assert_eq!(owed.digest().discovery_debt, settled.digest().discovery_debt);
    assert!(!owed.digest().is_behind(&settled.digest()));
}

// === digest memoization (the dirty-flag memo) ===

use crate::cluster_state::{PhaseTally, RespawnEventRecord};
use crate::primary::retry_bucket::BucketKind;
use dynrunner_protocol_primary_secondary::RemovalCause;
use std::time::SystemTime;

/// THE critical pin: the memoized `digest()` is byte-identical to a fresh
/// un-memoized fold after EVERY mutation, across a sequence that exercises
/// every invalidation seam — the apply chokepoint (many arms), the four
/// grow-max originators (which mutate folded fields OUTSIDE apply), and the
/// restore chokepoint. A missed invalidation would let the memo serve a
/// stale digest, silently breaking anti-entropy convergence (a replica that
/// never detects it is behind, or one falsely flagged), so this is the
/// property the memo must never violate.
#[test]
fn digest_memo_matches_fresh_fold() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let t0 = SystemTime::UNIX_EPOCH;

    // After every mutation: the memo (digest) must equal a fresh fold. We
    // call `digest()` FIRST so the memo is populated, THEN `fresh_digest_fold()`
    // (which recomputes unconditionally) — if a seam failed to invalidate,
    // the populated memo would carry the PRE-mutation value and diverge.
    macro_rules! assert_memo_fresh {
        () => {{
            let memo = s.digest();
            let fresh = s.fresh_digest_fold();
            assert_eq!(
                memo, fresh,
                "memoized digest diverged from a fresh fold — a mutation seam \
                 failed to invalidate the digest memo"
            );
        }};
    }

    // Pre-warm the memo on the empty ledger, then mutate and re-check.
    assert_memo_fresh!();

    // --- apply-chokepoint arms (tasks / outputs / primary / phase graph /
    //     peers+capabilities / custom messages / latches) ---
    for i in 0..8 {
        s.apply(ClusterMutation::TaskAdded {
            hash: format!("t{i}"),
            task: mk_task(&format!("t{i}")),
        });
        assert_memo_fresh!();
    }
    s.apply(ClusterMutation::TaskAssigned {
        hash: "t0".into(),
        secondary: "s1".into(),
        worker: 0,
        version: Default::default(),
        attempt: 0,
    });
    assert_memo_fresh!();
    s.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "t0".into(),
        // A non-None payload exercises the task_outputs fold via the
        // internal `record_task_outputs_value` (reached through apply).
        result_data: Some(serde_json::to_vec(&serde_json::json!({"out": 1})).unwrap()),
    });
    assert_memo_fresh!();
    s.apply(ClusterMutation::TaskFailed {
        hash: "t1".into(),
        kind: ErrorType::Recoverable,
        error: "boom".into(),
        version: Default::default(),
        attempt: 0,
    });
    assert_memo_fresh!();
    s.apply(ClusterMutation::TaskRetried {
        hash: "t1".into(),
        attempt: 1,
        version: Default::default(),
    });
    assert_memo_fresh!();
    s.apply(ClusterMutation::PrimaryChanged {
        new: "s1".into(),
        epoch: 3,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    assert_memo_fresh!();
    s.apply(ClusterMutation::PeerJoined {
        peer_id: "obs".into(),
        is_observer: true,
        can_be_primary: false,
        cap_version: Default::default(),
        member_gen: 0,
    });
    assert_memo_fresh!();
    s.apply(ClusterMutation::SecondaryCapacity {
        secondary: "s1".into(),
        worker_count: 4,
        resources: Default::default(),
    });
    assert_memo_fresh!();
    s.apply(ClusterMutation::CustomMessagePosted {
        origin: "s1".into(),
        seq: 1,
        topic: "topic".into(),
        data: b"data".to_vec(),
        is_high_volume: false,
    });
    assert_memo_fresh!();
    s.apply(ClusterMutation::CustomMessageHandled {
        origin: "s1".into(),
        seq: 1,
    });
    assert_memo_fresh!();
    s.apply(ClusterMutation::PhaseEnded {
        phase: PhaseId::from("p0"),
    });
    assert_memo_fresh!();
    s.apply(ClusterMutation::DiscoveryDebtDeclared);
    assert_memo_fresh!();
    s.apply(ClusterMutation::RunComplete { counts: Default::default() });
    assert_memo_fresh!();

    // --- the four grow-max originators (mutate folded fields OUTSIDE the
    //     apply chokepoint — each must invalidate on its own) ---
    s.record_phase_event_tally((PhaseId::from("p0"), PhaseTally::Completed), 5);
    assert_memo_fresh!();
    s.record_retry_pass_used((PhaseId::from("p0"), BucketKind::Recoverable), 2);
    assert_memo_fresh!();
    s.record_unfulfillable_reinject_used("t2".to_string(), 1);
    assert_memo_fresh!();
    s.record_respawn_event(
        "secondary-1".to_string(),
        RespawnEventRecord {
            original_id: "secondary-0".to_string(),
            cause: RemovalCause::KeepaliveMiss,
            at: t0,
        },
    );
    assert_memo_fresh!();

    // --- the restore chokepoint: merging a divergent peer snapshot must
    //     invalidate (it mutates many folded fields at once) ---
    let mut peer = ClusterState::<RunnerIdentifier>::new();
    for i in 8..12 {
        peer.apply(ClusterMutation::TaskAdded {
            hash: format!("t{i}"),
            task: mk_task(&format!("t{i}")),
        });
    }
    peer.record_phase_event_tally((PhaseId::from("p9"), PhaseTally::Failed), 9);
    s.restore(peer.snapshot());
    assert_memo_fresh!();

    // A second restore of the SAME snapshot is a NoOp merge, but still
    // invalidates (unconditional at the seam) — the re-fold yields the same
    // value, so the memo stays correct.
    s.restore(peer.snapshot());
    assert_memo_fresh!();
}

/// A clean read serves the memo WITHOUT re-running the O(ledger) fold:
/// repeated `digest()` calls on an unchanged ledger run the fold exactly
/// once (the production waste was one full fold per inbound `StateDigest`
/// frame), and the next mutation invalidates so the following read folds
/// again.
#[test]
fn digest_memo_hit_skips_the_fold() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    for i in 0..16 {
        s.apply(ClusterMutation::TaskAdded {
            hash: format!("t{i}"),
            task: mk_task(&format!("t{i}")),
        });
    }

    // First read folds once and populates the memo.
    let before = s.digest_fold_count();
    let first = s.digest();
    assert_eq!(
        s.digest_fold_count(),
        before + 1,
        "the first read after a mutation must run the fold"
    );

    // A burst of reads against the unchanged ledger — the AE receive-cadence
    // shape — folds ZERO more times (all served from the memo).
    for _ in 0..50 {
        assert_eq!(s.digest(), first, "every clean read returns the memo");
    }
    assert_eq!(
        s.digest_fold_count(),
        before + 1,
        "clean reads must NOT re-run the fold (the CPU-waste fix)"
    );

    // A mutation invalidates; the next read folds exactly once more.
    s.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "t0".into(),
        result_data: None,
    });
    let _ = s.digest();
    assert_eq!(
        s.digest_fold_count(),
        before + 2,
        "a mutation must invalidate so the next read re-folds"
    );
}
