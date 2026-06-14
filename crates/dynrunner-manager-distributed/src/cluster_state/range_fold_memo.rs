//! Incremental memo of the per-bucket range fold (#492 P2) — the O(1)-read
//! maintained twin of the O(ledger) [`super::range_digest`] projection.
//!
//! # Single concern
//!
//! "Keep a per-bucket XOR-fold + count of the LOGICAL task ledger so
//! [`ClusterState::tasks_range_digest`](super::ClusterState::tasks_range_digest)
//! reads in O(buckets), not O(ledger)." Nothing else: this type holds NO
//! bucketing rule of its own (it calls [`super::keyspace::range_index`]) and
//! NO term rule of its own (the caller hands it the term
//! [`super::keyspace::task_digest_term`] already computed) — it is purely the
//! running accumulator the [`super::range_digest`] one-pass fold would
//! otherwise recompute on every probe.
//!
//! # Why incremental, not invalidate-and-recompute
//!
//! The scalar `digest_cache` (#480) memo can afford invalidate-on-mutation
//! because a probe storm reads it far more often than mutations clear it.
//! The RANGE fold cannot: at a 66k-task phase START every behind secondary
//! probes at once, so the read path itself is the hot path the wedge (#504)
//! froze. Invalidating would still cost one O(66k) fold on the next probe —
//! one per concurrent probe in the same inbox batch — which is exactly the
//! synchronous CPU burst that wedged the single-threaded oploop. So the fold
//! is maintained INCREMENTALLY: each task-state mutation XORs the old term
//! OUT and the new term IN (the XOR-fold is commutative + associative — the
//! seam [`super::keyspace`] documents), and the read is a cheap clone.
//!
//! # The correctness invariant (the load-bearing one)
//!
//! `XOR(maintained range-folds) == fresh full-fold == StateDigest::tasks_hash`
//! and `sum(maintained counts) == tasks_count`. A SINGLE missed mutation site
//! silently desyncs the memo → wrong divergent-range set → a delta pull drops
//! CRDT entries (data divergence). The
//! `range_digest_memo_matches_fresh_fold` differential test pins it, and the
//! convergence/apply/settle/hydrate suites assert it after every mutation
//! path so a missed XOR site is caught. Every site that XOR-maintains this
//! memo is the SAME site the [`super::range_digest`] one-pass fold would have
//! visited — they are kept in lockstep by routing both through
//! [`super::keyspace::task_digest_term`].
//!
//! # Logical-ledger scope (fat ∪ settled)
//!
//! The memo tracks the LOGICAL ledger (fat in-memory `tasks` ∪ spilled
//! `settled`), the same universe [`super::range_digest`] folds. A spill
//! (commit) / unsettle (rehydrate) MOVES an entry between the two halves but
//! does NOT change its term or its bucket, so it is memo-NEUTRAL — exactly as
//! it is `tasks_hash`-neutral (the term is XORed out of the live `tasks_hash`
//! and into `settled.tasks_hash_acc` at the same instant, value-preserving).
//! Only a LOGICAL create ([`RangeFoldMemo::add`]: spawn / TaskAdded) and a
//! LOGICAL state CHANGE under a fixed key ([`RangeFoldMemo::swap`]: the join
//! win + the authoritative rank-drops + the cascade resume/block) update the
//! memo. The logical ledger never shrinks (a terminal entry stays counted; a
//! spill only MOVES it), so there is no count-decrementing path — the count
//! is monotone.

use dynrunner_protocol_primary_secondary::{RANGE_COUNT, RangeDigest};

use super::keyspace::range_index;

/// The maintained per-bucket fold. Mirrors the on-the-wire
/// [`RangeDigest`]'s two arrays (`folds` + `counts`) so a read is a direct
/// clone, but kept as a distinct node-local type (NOT a `RangeDigest`) so it
/// carries no wire-serialization concern and the read site is the one place
/// the two cross.
#[derive(Clone)]
pub(super) struct RangeFoldMemo {
    /// Per-bucket XOR-accumulator of the per-entry term (the SAME term
    /// [`super::digest`] folds into `tasks_hash`). `XOR(folds) == tasks_hash`.
    folds: [u64; RANGE_COUNT],
    /// Per-bucket count of logical-ledger entries. `sum(counts) ==
    /// tasks_count`.
    counts: [u32; RANGE_COUNT],
}

impl Default for RangeFoldMemo {
    fn default() -> Self {
        Self {
            folds: [0u64; RANGE_COUNT],
            counts: [0u32; RANGE_COUNT],
        }
    }
}

impl RangeFoldMemo {
    /// A logical entry CAME INTO BEING (or moved into a bucket): XOR its term
    /// into the key's bucket and bump the count. Used for a spawn / TaskAdded
    /// and for the IN half of a state swap.
    pub(super) fn add(&mut self, key: &str, term: u64) {
        let r = range_index(key);
        self.folds[r] ^= term;
        self.counts[r] = self.counts[r].saturating_add(1);
    }

    /// A logical entry CHANGED STATE under a FIXED key: XOR the old term out
    /// and the new term in, leaving the count unchanged (the entry stayed in
    /// the ledger, only its fold term moved). One call so a state rewrite
    /// can never half-update (out without in) the memo. A no-op term change
    /// (old == new) cancels to identity, which is correct.
    pub(super) fn swap(&mut self, key: &str, old_term: u64, new_term: u64) {
        let r = range_index(key);
        self.folds[r] ^= old_term ^ new_term;
    }

    /// Snapshot the maintained fold as the wire [`RangeDigest`] —
    /// O(buckets), the cheap read that replaces the O(ledger) fold. A `Box`
    /// for the same reason [`super::range_digest`] boxes its result: every
    /// consumer keeps the ~3 KiB digest off the by-value stack-move paths.
    /// The differential invariant test reads `XOR(folds)` / `sum(counts)`
    /// through this returned `RangeDigest` (its arrays are `pub`), so the memo
    /// needs no test-only accessors of its own.
    pub(super) fn to_range_digest(&self) -> Box<RangeDigest> {
        Box::new(RangeDigest {
            folds: self.folds,
            counts: self.counts,
        })
    }
}
