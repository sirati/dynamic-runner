//! The grow-only-MAX-map CRDT primitive.
//!
//! Single concern: a `HashMap<K, u32>` whose value is a monotone
//! non-decreasing per-key count (an *event* count or a *used* count) that
//! replicates by taking the per-key MAX of two replicas' counts. The join
//! is commutative / associative / idempotent (MAX is), so the map is
//! snapshot-healable — it rides the snapshot + anti-entropy digest path
//! with ZERO wire-protocol surface (no new `ClusterMutation` variant), the
//! same channel `secondary_capacities` / `task_outputs` use.
//!
//! **Merge rule (DRAWN ONCE here):** grow-only MAX of a monotone used/event
//! count. Converges under per-key `max`; NEVER LWW, NEVER decrement. A
//! stale peer can never resurrect a lower count via a snapshot (max never
//! decreases), which is exactly the property that lets a promoted primary
//! inherit the counts (max-merge from the restored snapshot) and a cold
//! start see 0 (empty map) — with no run-start `clear()` needed.
//!
//! Three fields decompose to this ONE primitive (one concern, one merge
//! rule, three key types): F4 `phase_event_tallies`
//! (`(PhaseId, PhaseTally)`), P3 `retry_passes_used`
//! (`(PhaseId, BucketKind)`), P3 `unfulfillable_reinject_used` (`String`).
//! Each field's struct-decl / snapshot-out / restore-in / digest-fold /
//! Clone / Default / Debug exhaustive-destructure sites live with the field
//! (the cookbook's structural-completeness guards); the *behaviour* (merge,
//! fold, read, bump) lives here so it is spelled exactly once.

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use dynrunner_core::{Identifier, PhaseId};

use crate::primary::retry_bucket::BucketKind;

use super::ClusterState;
use super::types::PhaseTally;

/// Grow-only-MAX merge of an `incoming` map into `local` (the restore-in
/// merge loop). For each incoming `(k, v)` the local entry ratchets up to
/// `max(local, v)`; a key only in `incoming` is inserted at its value (an
/// `or_insert(0)` then `max` = the incoming value). NEVER replaces,
/// `or_insert`s, or `+=`s — the merge is idempotent and order-independent.
pub(super) fn merge_grow_max<K: Eq + Hash>(local: &mut HashMap<K, u32>, incoming: HashMap<K, u32>) {
    for (k, v) in incoming {
        let e = local.entry(k).or_insert(0);
        *e = (*e).max(v);
    }
}

/// Order-independent XOR-fold of a grow-only-MAX map for the digest. Folds
/// the `(k, v)` PAIR (not key-only) because the count diverges before
/// convergence — same shape as `task_outputs_hash`, so a same-key
/// divergent-count entry is detected by `field_behind` and pulled. The
/// per-entry hash is XOR-folded so the result is invariant under iteration
/// order.
pub(super) fn fold_grow_max<K: Hash>(map: &HashMap<K, u32>) -> u64 {
    let mut h = 0u64;
    for (k, v) in map {
        let mut hasher = DefaultHasher::new();
        (k, v).hash(&mut hasher);
        h ^= hasher.finish();
    }
    h
}

/// Read accessor: the monotone count for `k`, or 0 for a never-bumped key
/// (`.copied().unwrap_or(0)`). The reader derives `remaining = budget −
/// used` (or reports the event count) LOCALLY from this.
pub(super) fn read_grow_max<K: Eq + Hash>(map: &HashMap<K, u32>, k: &K) -> u32 {
    map.get(k).copied().unwrap_or(0)
}

/// Originator: local max-bump of `k` to at least `count`
/// (`max(existing, count)`). The live primary (sole writer) holds the
/// authoritative running max; the snapshot / anti-entropy path replicates
/// it. Idempotent — re-bumping with an equal-or-lower count is a no-op.
pub(super) fn bump_grow_max<K: Eq + Hash>(map: &mut HashMap<K, u32>, k: K, count: u32) {
    let e = map.entry(k).or_insert(0);
    *e = (*e).max(count);
}

impl<I: Identifier> ClusterState<I> {
    // === phase_event_tallies (F4) ===

    /// The replicated per-phase EVENT tally for `(phase, tally)`, or 0 for a
    /// never-incremented key (F4). Read by `on_phase_end` and the
    /// `phase_can_proceed` / `RunShouldFail` gate — identical numbers to the
    /// old node-local maps on the live path, CORRECT on the promoted path
    /// (the events were replicated).
    pub fn phase_event_tally_for(&self, key: &(PhaseId, PhaseTally)) -> u32 {
        read_grow_max(&self.phase_event_tallies, key)
    }

    /// Originate a per-phase EVENT tally (F4): max-bump the local count for
    /// `key` to at least `count`. Called by `note_item_completed` /
    /// `note_item_failed` with the new running event count. Grow-only MAX —
    /// replicated via snapshot + AE.
    pub(crate) fn record_phase_event_tally(&mut self, key: (PhaseId, PhaseTally), count: u32) {
        bump_grow_max(&mut self.phase_event_tallies, key, count);
    }

    // === retry_passes_used (P3-pass) ===

    /// The replicated per-(phase, bucket) retry-pass USED count for `key`,
    /// or 0 for a never-bumped key (P3). The budget check derives
    /// `used >= max_passes` against this.
    pub(crate) fn retry_pass_used_for(&self, key: &(PhaseId, BucketKind)) -> u32 {
        read_grow_max(&self.retry_passes_used, key)
    }

    /// Originate a retry-pass USED count (P3): max-bump the local count for
    /// `key` to at least `used`. Called by the async retry-bucket caller
    /// with the new used count the pure core returned. Grow-only MAX —
    /// replicated via snapshot + AE.
    pub(crate) fn record_retry_pass_used(&mut self, key: (PhaseId, BucketKind), used: u32) {
        bump_grow_max(&mut self.retry_passes_used, key, used);
    }

    // === unfulfillable_reinject_used (P3-reinject) ===

    /// The replicated per-hash unfulfillable-reinject USED count for `hash`,
    /// or 0 for a never-bumped hash (P3). The handler derives
    /// `remaining = cap − used` against this.
    pub(crate) fn unfulfillable_reinject_used_for(&self, hash: &str) -> u32 {
        self.unfulfillable_reinject_used
            .get(hash)
            .copied()
            .unwrap_or(0)
    }

    /// Originate an unfulfillable-reinject USED count (P3): max-bump the
    /// local count for `hash` to at least `used`. Called by the reinject
    /// handler on a successful reinject WHEN a cap is set (an unbounded
    /// `None` cap originates nothing — there is no budget to enforce).
    /// Grow-only MAX — replicated via snapshot + AE.
    pub(crate) fn record_unfulfillable_reinject_used(&mut self, hash: String, used: u32) {
        bump_grow_max(&mut self.unfulfillable_reinject_used, hash, used);
    }

    /// Test-only sum of the retry-pass USED counts across every
    /// (phase, bucket) key. Lets coordinator tests assert "how many retry
    /// passes were consumed in total" against the replicated field after the
    /// counter moved off the coordinator into the CRDT (P3).
    #[cfg(test)]
    pub(crate) fn retry_passes_used_total(&self) -> u32 {
        self.retry_passes_used.values().sum()
    }
}
