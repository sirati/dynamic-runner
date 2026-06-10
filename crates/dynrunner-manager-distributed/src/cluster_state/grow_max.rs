//! The grow-only-MAX-map CRDT primitive.
//!
//! Single concern: a `HashMap<K, u32>` whose value is a monotone
//! non-decreasing per-key count (an *event* count or a *used* count) that
//! replicates by taking the per-key MAX of two replicas' counts. The join
//! is commutative / associative / idempotent (MAX is), so the map is
//! snapshot-healable — it rides the snapshot + anti-entropy digest path
//! with ZERO wire-protocol surface (no new `ClusterMutation` variant), the
//! same channel `secondary_capacities` / `task_outputs` use. F4
//! additionally bumps LOCALLY on every winning `TaskCompleted`/`TaskFailed`
//! apply (`merge_task_state`, #358) — still no wire surface of its own;
//! the count derives from the task mutations every node already applies,
//! and the field merge only heals transitions a node never observed.
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
use super::types::{PhaseTally, RespawnEventRecord};

/// Grow-only-MAX merge of an `incoming` map into `local` (the restore-in
/// merge loop). For each incoming `(k, v)` the local entry ratchets up to
/// `max(local, v)`; a key only in `incoming` is inserted at its value.
/// NEVER replaces downward or `+=`s — the merge is idempotent and
/// order-independent. Generic over the monotone value (`u32` event/used
/// counts; the F5 `u64` per-origin watermark) — ONE merge rule, one
/// place.
pub(super) fn merge_grow_max<K: Eq + Hash, V: Ord>(
    local: &mut HashMap<K, V>,
    incoming: HashMap<K, V>,
) {
    for (k, v) in incoming {
        match local.entry(k) {
            std::collections::hash_map::Entry::Occupied(mut e) => {
                if v > *e.get() {
                    e.insert(v);
                }
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(v);
            }
        }
    }
}

/// Order-independent XOR-fold of a grow-only-MAX map for the digest. Folds
/// the `(k, v)` PAIR (not key-only) because the count diverges before
/// convergence — same shape as `task_outputs_hash`, so a same-key
/// divergent-count entry is detected by `field_behind` and pulled. The
/// per-entry hash is XOR-folded so the result is invariant under iteration
/// order.
pub(super) fn fold_grow_max<K: Hash, V: Hash>(map: &HashMap<K, V>) -> u64 {
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
/// (`max(existing, count)`). For the P3 *used* counts the live primary
/// (sole writer) holds the authoritative running max and the snapshot /
/// anti-entropy path replicates it; for the F4 *event* tallies EVERY node
/// bumps locally as it applies the winning task mutation (#358) and the
/// field merge heals unobserved transitions. Idempotent — re-bumping with
/// an equal-or-lower count is a no-op.
pub(super) fn bump_grow_max<K: Eq + Hash>(map: &mut HashMap<K, u32>, k: K, count: u32) {
    let e = map.entry(k).or_insert(0);
    *e = (*e).max(count);
}

// === grow-only-SET (F7) ===
//
// The grow-only SET is the SET twin of the MAX-map above (one concern —
// a snapshot-healable grow-only map — two value shapes). The value is an
// opaque RECORD written exactly once per globally-unique key (the
// `new_id` of an accepted respawn event), NOT a monotone counter, so the
// merge is UNION-BY-KEY (`entry.or_insert(record)`) rather than per-key
// MAX. The set never removes a key and never mutates a value, so it is
// commutative / associative / idempotent under union-by-key (the same
// CRDT properties as MAX), hence equally snapshot-healable with ZERO wire
// surface.

/// Grow-only-SET union of an `incoming` map into `local` (the restore-in
/// merge loop). For each incoming `(k, v)` the local entry is inserted
/// ONLY when absent (`or_insert`); an existing key keeps the local value.
/// Correct + idempotent because a key (`new_id`) is globally unique per
/// event and its value is written exactly once, so local and incoming
/// agree on the value of any shared key — union-by-key never has to pick
/// between divergent values. NEVER replaces, NEVER removes.
pub(super) fn merge_grow_set<K: Eq + Hash, V>(local: &mut HashMap<K, V>, incoming: HashMap<K, V>) {
    for (k, v) in incoming {
        local.entry(k).or_insert(v);
    }
}

/// Order-independent XOR-fold of a grow-only SET for the digest. Folds
/// the `(k, v)` PAIR so a (would-be-impossible) same-key divergent value
/// is still detected by `field_behind` and a missing key is caught by the
/// count + fold — mirroring `fold_grow_max`'s KEY+VALUE shape. Requires
/// `V: Hash` (the respawn record is `Hash` via its `String` /
/// `RemovalCause` / `SystemTime` fields).
pub(super) fn fold_grow_set<K: Hash, V: Hash>(map: &HashMap<K, V>) -> u64 {
    let mut h = 0u64;
    for (k, v) in map {
        let mut hasher = DefaultHasher::new();
        (k, v).hash(&mut hasher);
        h ^= hasher.finish();
    }
    h
}

/// Originator: union-insert `(k, v)` into the set — the live primary (sole
/// writer) records an accepted event under its unique key. Idempotent: a
/// re-insert of an already-present key is a no-op (`or_insert`), so an
/// at-least-once re-origination never duplicates or mutates the record.
pub(super) fn insert_grow_set<K: Eq + Hash, V>(map: &mut HashMap<K, V>, k: K, v: V) {
    map.entry(k).or_insert(v);
}

impl<I: Identifier> ClusterState<I> {
    // === phase_event_tallies (F4) ===

    /// The replicated per-phase EVENT tally for `(phase, tally)`, or 0 for a
    /// never-incremented key (F4). Read by the `on_phase_end` hook (the
    /// `completed` / `failed` numbers it hands the consumer) — identical
    /// numbers to the old node-local maps on the live path, CORRECT on the
    /// promoted path (the events were replicated).
    pub fn phase_event_tally_for(&self, key: &(PhaseId, PhaseTally)) -> u32 {
        read_grow_max(&self.phase_event_tallies, key)
    }

    /// Originate a per-phase EVENT tally (F4): max-bump the local count for
    /// `key` to at least `count`. SINGLE caller: the `merge_task_state`
    /// join, on every winning `Completed` / failure-terminal transition
    /// (#358) — so the bump lands wherever the replicated event itself
    /// lands (originator apply-locally, mirror broadcast-apply, snapshot
    /// restore) and every node's tally is exact in real time. Grow-only
    /// MAX — the snapshot + AE field merge additionally heals any
    /// transition a node never observed.
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

    /// Iterate every `((phase, bucket), used)` entry of the replicated
    /// retry-pass USED map (P3). The read-seam the run-narrator scans to
    /// surface error- / OOM-retry-pass-start milestones from the converged
    /// CRDT. The narrator derives a once-per-`(phase, bucket)` PRESENCE
    /// milestone — it emits the moment a key first appears here with a
    /// positive count (`used >= 1`) and never again for that key, no matter
    /// how high the count climbs. It is NOT a per-step count diff: the
    /// `used` value is read only for the `>= 1` presence test, not differenced
    /// against a last-seen count. Presence is the only failover-consistent
    /// derivation — a live primary watching a count step 1→2→3 and a
    /// promoted/observing node fed the already-converged count 3 both see the
    /// SAME presence and emit the SAME single line, whereas a count diff would
    /// make them narrate differently from the one converged CRDT. A pass that
    /// opened on a promoted primary (a different node) is surfaced here purely
    /// via replication, no per-node authority. Unlike `retry_pass_used_for`
    /// (a single-key budget read) the narrator must discover WHICH keys exist,
    /// so it borrows the whole map. (See `run_narrator::retry_passes_emitted`.)
    pub(crate) fn retry_passes_used(&self) -> impl Iterator<Item = (&(PhaseId, BucketKind), u32)> {
        self.retry_passes_used.iter().map(|(k, v)| (k, *v))
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

    // === respawn_events (F7 grow-only SET) ===

    /// Read accessor over the replicated respawn ledger (F7): the whole
    /// grow-only SET keyed by `new_id`. The respawn budget
    /// (`RespawnBudget::should_respawn`) walks the `(new_id, record)`
    /// pairs for the family-chain count, the total count, and the
    /// per-family cooldown — all value-shaped / order-independent, so a
    /// `HashMap` value iterator is exactly the right view. A promoted
    /// primary inherits the FULL ledger via union-merge on restore, so
    /// the admission budget + cooldown are NOT re-granted on failover.
    pub(crate) fn respawn_events(&self) -> &HashMap<String, RespawnEventRecord> {
        &self.respawn_events
    }

    /// Originate a respawn event (F7): union-insert the accepted event's
    /// `record` under its unique `new_id`. Called by
    /// `dispatch_respawn_request` on the ACCEPTED path (the sole writer);
    /// the snapshot + AE digest path replicates it. Grow-only SET —
    /// idempotent re-insert, never removes, never mutates a value.
    pub(crate) fn record_respawn_event(&mut self, new_id: String, record: RespawnEventRecord) {
        insert_grow_set(&mut self.respawn_events, new_id, record);
    }
}
