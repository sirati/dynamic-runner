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
//! # P2 memo seam (do NOT build now)
//!
//! This computes the folds in one O(ledger) pass per call. Owner-confirmed
//! P1-acceptable (probes are single-flight + cooldown-bounded). The seam for
//! a future incremental memo (P2/#492) is the per-entry term keyed by
//! `range_index` in [`super::keyspace`]: a memo can XOR exactly that term
//! in/out at each mutation. Left clean; not built here.

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{RANGE_COUNT, RangeDigest};

use super::ClusterState;
use super::keyspace::{range_index, task_digest_term};

impl<I: Identifier> ClusterState<I> {
    /// Build the range-scoped [`RangeDigest`] of the task ledger: per
    /// hash-prefix bucket, the entry count + the XOR-fold of the per-entry
    /// term. The cross-bucket fold reconstructs the scalar
    /// `StateDigest::tasks_hash` exactly (and the cross-bucket count sum the
    /// scalar `tasks_count`) — the invariant the delta's correctness rests
    /// on.
    ///
    /// Read-only over the LOGICAL ledger (fat `tasks` ∪ spilled `settled`),
    /// the same universe `digest()` / `snapshot()` / the stream plan iterate.
    /// A SETTLED entry folds its persisted `digest_contribution` (the term
    /// stamped at spill-commit, identical to a live fold) into its bucket, so
    /// the digest is whole regardless of which entries are spilled.
    ///
    /// Returns a `Box`: a `RangeDigest` is ~3 KiB, and every consumer (the
    /// probe reply, the candidate list, the pull directive) keeps it boxed to
    /// stay off the by-value stack-move paths through the hot dispatch loop,
    /// so building it on the heap from the start avoids a transient 3 KiB
    /// stack copy at the fold site.
    pub fn tasks_range_digest(&self) -> Box<RangeDigest> {
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
        debug_assert_eq!(
            RANGE_COUNT,
            digest.counts.len(),
            "RangeDigest array length tracks RANGE_COUNT"
        );
        digest
    }
}
