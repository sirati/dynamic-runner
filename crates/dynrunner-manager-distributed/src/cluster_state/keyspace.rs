//! Task-keyspace bucketing — the ONE source of truth for the P1
//! range-digest delta's range assignment + per-entry fold term.
//!
//! # Single concern
//!
//! "Which fixed bucket does a task key belong to, and what `u64` term does
//! its ledger entry contribute to that bucket's fold." Both the
//! [`RangeDigest`](dynrunner_protocol_primary_secondary::RangeDigest)
//! projection ([`super::range_digest`]) AND the snapshot-stream key filter
//! ([`super::stream::SnapshotStreamPlan`]) bucket keys the SAME way, and the
//! per-entry fold term is the SAME one [`super::digest`] folds into
//! `tasks_hash`. Defining both here ONCE is what makes the two correctness
//! invariants hold BY CONSTRUCTION rather than by two implementations
//! happening to agree:
//!
//! 1. `XOR(range-folds) == StateDigest::tasks_hash` — every entry's term
//!    ([`task_digest_term`]) lands in exactly one bucket and XOR is
//!    associative + commutative, so the cross-bucket fold reconstructs the
//!    scalar fold the digest already exposes.
//! 2. The streamed key-set ⊆ the divergent ranges — the stream filter
//!    buckets keys with the SAME [`range_index`] the digest comparison used,
//!    so a key the requester asked for (its bucket is divergent) and a key
//!    the responder streams are bucketed identically.
//!
//! # Stability + replica-independence
//!
//! [`range_index`] is a HASH-PREFIX of the key (`hash_one(key) %
//! RANGE_COUNT`), NOT its sorted position: a sorted-index split would
//! re-bucket every key whenever two ledgers differ in count — precisely the
//! divergence case the delta exists for — so the same key would land in
//! different buckets on two replicas and the comparison would be
//! meaningless. A hash-prefix puts the SAME key in the SAME bucket on every
//! replica running the SAME binary (the digest's existing cross-build
//! assumption — the wire protocol is version-locked per run), independent of
//! what other keys exist. It is also order-independent: the bucket is a pure
//! function of the key.
//!
//! # P2 memo seam (do NOT build now)
//!
//! [`super::range_digest`] computes the folds in one O(ledger) pass per
//! probe reply. That is fine for P1 (probes are single-flight +
//! cooldown-bounded), but the per-entry term is keyed by [`range_index`], so
//! a future incremental memo (invalidate-on-mutation, like the #480 digest
//! memo) can maintain a per-bucket running fold by XOR-ing exactly this term
//! in/out at each task mutation — the seam is this module's two pure
//! functions. The memo is P2/#492 scope; this module deliberately holds no
//! state.

use std::hash::Hash;

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::RANGE_COUNT;

use super::TaskState;
use super::merge::hashable_join_key;

/// Hash a single hashable value to a `u64`. Mirrors the per-module
/// `hash_one` helpers (`digest`, `merge`, `settled`) — the digest is only
/// ever compared between peers running the SAME binary (the wire protocol is
/// version-locked per run), so cross-build hash-stability is not required;
/// only determinism + order-independence WITHIN the run, which the standard
/// library's default hasher provides.
///
/// NOTE (flagged to the owner): there are already four near-identical
/// private `hash_one` copies across the `cluster_state` sub-modules
/// (`digest`, `merge`, `settled`, and an inline pair in `grow_max`). This is
/// the fifth call-site shape; consolidating them into one shared helper is a
/// pre-existing cleanup that should NOT be expanded by P1 — this module
/// reuses `merge::hashable_join_key` (which already routes through `merge`'s
/// copy) for the join-key half, and needs `hash_one` only for the outer
/// `(key, join)` combine + the [`range_index`] prefix.
fn hash_one<H: Hash>(value: H) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

/// The fixed bucket a task key belongs to: a hash-prefix of the key,
/// `0..RANGE_COUNT`. STABLE (same key → same bucket on every replica) and
/// COUNT-INDEPENDENT (does not shift when the ledger gains/loses other
/// keys), the two properties the range-digest comparison depends on. The
/// SAME function the digest projection AND the stream key filter call, so a
/// key the requester flagged divergent and a key the responder streams are
/// bucketed identically.
pub(super) fn range_index(key: &str) -> usize {
    (hash_one(key) % RANGE_COUNT as u64) as usize
}

/// The `u64` term a LIVE task entry contributes to its bucket's fold —
/// EXACTLY the per-entry term [`super::digest`] folds into `tasks_hash`
/// (`hash_one((key, hashable_join_key(state)))`). Defined here so the
/// range-fold and the scalar fold use one expression: that is what makes
/// `XOR(range-folds) == tasks_hash` hold by construction. A SETTLED entry's
/// term is the same value, persisted as `SettledEntry::digest_contribution`
/// at spill-commit time (see `super::settled::commit_spill`), folded into
/// the bucket via [`super::settled::SettledStore`]'s own accessor — settled
/// entries are not re-derived here (their fat body is on disk).
pub(super) fn task_digest_term<I: Identifier>(key: &str, state: &TaskState<I>) -> u64 {
    hash_one((key, hashable_join_key(state)))
}

/// Test seam: the bucket a key maps to, so the range-digest tests can assert
/// a changed key landed in the bucket they expect WITHOUT re-deriving the
/// hash-prefix rule (which would just re-implement the function under test).
#[cfg(test)]
pub(crate) fn range_index_for_test(key: &str) -> usize {
    range_index(key)
}
