//! Range-scoped (Merkle-lite) projection of the task ledger — the P1
//! "resume from last good state" DELTA on top of the scalar
//! [`StateDigest`](dynrunner_protocol_primary_secondary::StateDigest) task
//! fold.
//!
//! # Single concern
//!
//! Build a per-bucket
//! [`RangeDigest`](dynrunner_protocol_primary_secondary::RangeDigest) of the
//! task ledger: split the keyspace into [`RANGE_COUNT`] fixed hash-prefix
//! buckets ([`super::keyspace::range_index`]) and, per bucket, count the
//! entries + XOR-fold the SAME per-entry term the scalar
//! [`super::digest`] folds into `tasks_hash`. A behind node compares its
//! buckets to a peer's ([`RangeDigest::divergent_ranges`]) and pulls ONLY
//! the divergent ones.
//!
//! This is a PURE PROJECTION — read-only over the same `tasks` + `settled`
//! the digest reads, no mutation, no merge logic. It is a sibling to
//! [`super::digest`] (the scalar fold) and bound by the SAME two ledger
//! halves: the in-memory `tasks` (folded via
//! [`super::keyspace::task_digest_term`]) and the spilled `settled` entries
//! (folded via their persisted `digest_contribution`, the identical term
//! stamped at spill-commit). Folding both keeps the cross-bucket sum equal
//! to the scalar `tasks_hash` across the fat/settled split.
//!
//! # The two correctness invariants (pinned by the tests below)
//!
//! - `XOR(range-folds) == StateDigest::tasks_hash` and
//!   `sum(counts) == StateDigest::tasks_count` — by construction (every
//!   entry's term lands in exactly one bucket; XOR is associative +
//!   commutative). The `range_digest_folds_match_scalar` test pins it.
//! - A one-task change moves exactly one bucket (the changed key's), so a
//!   delta pull re-streams ~one bucket. The `one_task_change_isolates_to_
//!   one_range` test pins it.
//!
//! # Incremental memo (#492 P2 — built)
//!
//! [`ClusterState::tasks_range_digest`] is served from the node-local
//! [`super::range_fold_memo::RangeFoldMemo`], maintained INCREMENTALLY at the
//! task-state mutation sites (the seam this module + [`super::keyspace`]
//! documented): each mutation XORs the per-entry term keyed by `range_index`
//! out/in, so the read is O(buckets), not the O(ledger) fold a probe storm
//! would otherwise run on every inbound `PullProbe` (the #504 oploop wedge).
//! The one-pass fold remains as the `#[cfg(test)]`
//! [`ClusterState::fresh_tasks_range_digest`] the differential invariant test
//! recomputes against.

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::RangeDigest;

use super::ClusterState;
use super::TaskState;
use super::keyspace::task_digest_term;
#[cfg(test)]
use super::keyspace::range_index;

impl<I: Identifier> ClusterState<I> {
    // ─── Incremental memo maintenance ───
    //
    // The range-fold memo is touched ONLY through the `range_fold_memo`
    // primitives (`add` / `swap`), always with the per-entry term computed via
    // the SHARED `keyspace::task_digest_term` (the SAME term `digest()` folds
    // into `tasks_hash`), so the memo and the scalar fold can never disagree.
    // The eight authoritative-rank-drop arms share the rewrite tail through
    // the one `rewrite_task_state` seam below; the create / merge-win / resume
    // sites compute the term inline (the value is moved into the map on the
    // very next line, so the term must be captured first) but route through
    // the same two primitives. The memo stays equal to a fresh fold by
    // construction — the `range_digest_memo_matches_fresh_fold` invariant.

    /// The memo-aware in-place state rewrite the authoritative-rank-drop arms
    /// share (the 8 `*state = ...` sites): look the entry up, XOR the old
    /// term out + the new term in, then store the new state. Returns `false`
    /// (a NoOp, no memo touch) when the hash is absent — the same absent-slot
    /// NoOp the arms' bare `get_mut` took. The arms keep their own
    /// variant-precondition match (which extracts task/attempt) BEFORE
    /// calling this; this funnels the common rewrite tail through ONE
    /// memo-maintaining seam so no `*state =` site can silently skip the memo.
    pub(super) fn rewrite_task_state(&mut self, hash: &str, new: TaskState<I>) -> bool {
        let Some(old) = self.tasks.get(hash) else {
            return false;
        };
        let old_term = task_digest_term(hash, old);
        let new_term = task_digest_term(hash, &new);
        self.range_fold_memo.swap(hash, old_term, new_term);
        // The entry is present (checked above); overwrite its state.
        if let Some(slot) = self.tasks.get_mut(hash) {
            *slot = new;
        }
        true
    }

    /// Build the range-scoped [`RangeDigest`] of the task ledger: per
    /// hash-prefix bucket, the entry count + the XOR-fold of the per-entry
    /// term. The cross-bucket fold reconstructs the scalar
    /// `StateDigest::tasks_hash` exactly (and the cross-bucket count sum the
    /// scalar `tasks_count`) — the invariant the delta's correctness rests
    /// on.
    ///
    /// O(buckets): served from the incrementally-maintained
    /// [`super::range_fold_memo::RangeFoldMemo`] (a cheap array clone), NOT a
    /// per-call O(ledger) fold. The memo is XOR-maintained over the LOGICAL
    /// ledger (fat `tasks` ∪ spilled `settled`) at every task-state mutation,
    /// so a probe storm at a 66k-task phase START reads it instantly instead
    /// of folding 66k entries per inbound probe (#504). A SETTLED entry stays
    /// counted in the memo — a spill MOVES the entry's term between the fat
    /// and settled halves the logical fold sums over, never changing it.
    ///
    /// Returns a `Box`: a `RangeDigest` is ~3 KiB, and every consumer (the
    /// probe reply, the candidate list, the pull directive) keeps it boxed to
    /// stay off the by-value stack-move paths through the hot dispatch loop,
    /// so building it on the heap from the start avoids a transient 3 KiB
    /// stack copy at the read site.
    pub fn tasks_range_digest(&self) -> Box<RangeDigest> {
        self.range_fold_memo.to_range_digest()
    }

    /// Test seam: the un-memoized O(ledger) one-pass fold the incremental
    /// [`Self::tasks_range_digest`] memo must always equal. The differential
    /// invariant pin (`range_digest_memo_matches_fresh_fold`) asserts the
    /// memo-served digest equals this fresh fold after every mutation; a
    /// missed XOR site at any mutation path makes them diverge.
    ///
    /// Read-only over the LOGICAL ledger (fat `tasks` ∪ spilled `settled`),
    /// the same universe `digest()` / `snapshot()` / the stream plan iterate.
    /// A SETTLED entry folds its persisted `digest_contribution` (the term
    /// stamped at spill-commit, identical to a live fold) into its bucket.
    #[cfg(test)]
    pub(crate) fn fresh_tasks_range_digest(&self) -> Box<RangeDigest> {
        // Count this full O(ledger) fold for the bound-per-iteration test
        // (mirroring `digest.rs`'s `digest_fold_count`): the PRODUCTION
        // `tasks_range_digest` read serves the memo and NEVER calls this, so
        // a probe storm can be proved to run ZERO full re-folds. A process-
        // wide thread-local keeps the counter test-scoped (no struct field,
        // no exhaustive-guard churn) — the tests run single-threaded per case.
        RANGE_FRESH_FOLD_COUNT.with(|c| c.set(c.get() + 1));
        let mut digest = Box::new(RangeDigest::default());
        // Fat (in-memory) entries: fold the per-entry term into the key's
        // bucket — the SAME term `digest()`'s live loop folds into
        // `tasks_hash`.
        for (key, state) in &self.tasks {
            let r = range_index(key);
            digest.counts[r] = digest.counts[r].saturating_add(1);
            digest.folds[r] ^= task_digest_term(key, state);
        }
        // Settled (spilled) entries: fold their persisted contribution (the
        // identical term, moved into the settled accumulator at spill-commit)
        // into the key's bucket. Settled bodies live on disk; the term is the
        // only thing the fold needs, so no per-entry file read here.
        for (key, term) in self.settled.digest_contributions() {
            let r = range_index(key);
            digest.counts[r] = digest.counts[r].saturating_add(1);
            digest.folds[r] ^= term;
        }
        digest
    }
}

#[cfg(test)]
thread_local! {
    /// Process-wide (per-thread) count of full O(ledger) range folds run via
    /// [`ClusterState::fresh_tasks_range_digest`]. The bound-per-iteration
    /// test reads it to prove the PRODUCTION `tasks_range_digest` read serves
    /// the memo and runs ZERO full re-folds under a probe burst.
    static RANGE_FRESH_FOLD_COUNT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
impl<I: Identifier> ClusterState<I> {
    /// Test seam: snapshot the full-range-fold counter (the count of O(ledger)
    /// folds run so far on this thread).
    pub(crate) fn range_fresh_fold_count() -> u64 {
        RANGE_FRESH_FOLD_COUNT.with(|c| c.get())
    }

    /// Test seam: reset the counter so a test isolates its own probe-burst
    /// window (the counter is thread-local and tests run single-threaded per
    /// case, but a reset makes the assertion robust to prior reads in-case).
    pub(crate) fn reset_range_fresh_fold_count() {
        RANGE_FRESH_FOLD_COUNT.with(|c| c.set(0));
    }
}
