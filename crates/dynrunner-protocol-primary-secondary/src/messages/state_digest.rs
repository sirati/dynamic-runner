//! `StateDigest` — a compact, order-independent fingerprint of a
//! replicated cluster ledger, exchanged periodically for anti-entropy.
//!
//! Single concern: a fixed-size summary (per-field counts + `u64`
//! content hashes) of a `ClusterState`, plus the pure field-by-field
//! "is the local replica missing data the peer holds?" comparison the
//! anti-entropy cadence uses to decide *whether* to pull a snapshot.
//!
//! The digest carries NO task payloads and NO identifier-typed data —
//! every member is a `u64` hash or a `usize`/`u64` scalar — so the frame
//! is `I`-erased exactly like the wire envelope it rides in. The hashes
//! summarise SET/MAP membership (XOR-fold of per-entry hashes), never
//! iteration order, so two replicas that converged to the same state
//! always produce byte-identical digests regardless of insertion order.
//!
//! This type holds NO merge logic and NO knowledge of the CRDT lattice.
//! It is a read-only projection (built by `ClusterState::digest`) plus a
//! monotone comparison; the actual reconciliation is the existing
//! snapshot RPC + `ClusterState::restore`. The digest only answers
//! "when to pull".

use serde::{Deserialize, Serialize};

use crate::cluster_mutation::DiscoveryDebt;

/// Compact fingerprint of a replicated `ClusterState`, exchanged on the
/// anti-entropy cadence. Every field pairs a COUNT (how many entries the
/// replica holds for that part of the ledger) with a `u64` content HASH
/// (an order-independent fold of the entries' identities/values), so a
/// receiver can tell — per field — both whether the sender holds MORE
/// entries and whether the same-count sets DIVERGE.
///
/// Determinism + order-independence: each map/set hash is the XOR-fold of
/// a per-entry hash, so it is invariant under iteration order and
/// re-computing it on a converged replica yields the same value. The
/// scalar fields (`primary_epoch`, the run latches) are carried verbatim.
///
/// Wire-compat: the whole struct is `#[serde(default)]`-friendly via
/// per-field defaults so a future field added here decodes as its zero
/// value on a peer that predates it (a missing count/hash reads as "this
/// peer holds nothing for that field", the conservative
/// never-claims-to-be-ahead shape).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct StateDigest {
    /// Number of task entries in the ledger.
    #[serde(default)]
    pub tasks_count: u64,
    /// Order-independent fold over the task ledger: XOR of a per-entry
    /// hash that combines the task's wire hash key with its
    /// state-rank, so a replica whose entry advanced to a stronger
    /// terminal (e.g. `Pending` → `Completed` for the same key) produces
    /// a different fold even at the same count.
    #[serde(default)]
    pub tasks_hash: u64,
    /// Number of per-secondary capacity records.
    #[serde(default)]
    pub secondary_capacities_count: u64,
    /// XOR-fold over the per-secondary capacity record keys (the
    /// secondary ids). Capacity is set-once/static, so the key-set
    /// identity is sufficient to detect a missing entry.
    #[serde(default)]
    pub secondary_capacities_hash: u64,
    /// Number of keyed task-output cache entries.
    #[serde(default)]
    pub task_outputs_count: u64,
    /// XOR-fold over the keyed-output cache KEY+VALUE pairs (AE-5): a
    /// divergent output value at an equal key produces a different fold,
    /// so a value split is detected (was key-only).
    #[serde(default)]
    pub task_outputs_hash: u64,
    /// Number of phases in the static dependency graph.
    #[serde(default)]
    pub phase_deps_count: u64,
    /// Order-independent canonical content hash of the static
    /// phase-dependency graph (CRD-3/D-G). Distinct from the count: a
    /// divergent-but-equal-count graph (e.g. two replicas that diverged
    /// across a partition) has the SAME `phase_deps_count` but a DIFFERENT
    /// hash, which the count-only compare could not see (R5).
    #[serde(default)]
    pub phase_deps_hash: u64,
    /// Hash of the `current_primary` identity (CRD-2/D-P). A same-epoch
    /// DIFFERENT-identity split carries the SAME `primary_epoch`, so only
    /// this hash distinguishes the two replicas; the restore lower-id-wins
    /// rule then converges both in one round.
    #[serde(default)]
    pub current_primary_hash: u64,
    /// Number of entries in the role-capability 2P-set (C6).
    #[serde(default)]
    pub capabilities_count: u64,
    /// XOR-fold over the role-capability 2P-set entries
    /// `(id, is_observer, can_be_primary, cap_version, is_departed)` (C6).
    /// The 2P-set IS snapshot-healable, so a flagged divergence is one a
    /// snapshot pull's `restore` actually heals (detect-WITH-heal).
    #[serde(default)]
    pub capabilities_hash: u64,
    /// Replicated primary epoch (monotone scalar; higher wins).
    #[serde(default)]
    pub primary_epoch: u64,
    /// Sticky-monotone run-completion latch.
    #[serde(default)]
    pub run_complete: bool,
    /// Sticky-monotone run-abort latch (carried as a presence bit — the
    /// reason string is not needed to detect divergence).
    #[serde(default)]
    pub run_aborted: bool,
    /// Sticky-monotone discovery-debt lattice height, carried VERBATIM (the
    /// full three-state [`DiscoveryDebt`], NOT a bool). A bool is provably
    /// insufficient for a 3-state lattice: it would map both `Undeclared`
    /// (BOTTOM) and `Settled` (TOP) to the same value, so a replica that
    /// missed the `Declared` broadcast (`Undeclared`) could never detect it
    /// is behind an `Owed` peer and would never pull — silent
    /// non-convergence (the mode-2 stall). `is_behind` compares lattice
    /// height directly (`self.discovery_debt < other.discovery_debt`).
    /// `#[serde(default)]` decodes a pre-field peer as `Undeclared` = the
    /// never-declared BOTTOM = the conservative never-claims-ahead shape
    /// (it loses to any peer's higher state, so a legacy peer never drags a
    /// declared run down).
    #[serde(default)]
    pub discovery_debt: DiscoveryDebt,
    /// Number of per-phase EVENT-tally entries (F4 grow-only-MAX map).
    #[serde(default)]
    pub phase_event_tallies_count: u64,
    /// XOR-fold over the per-phase EVENT-tally `(key, value)` pairs (F4): a
    /// divergent count at an equal key produces a different fold, so a
    /// promoted-vs-stale count split is detected and healed via snapshot.
    #[serde(default)]
    pub phase_event_tallies_hash: u64,
    /// Number of per-(phase, bucket) retry-pass USED entries (P3
    /// grow-only-MAX map).
    #[serde(default)]
    pub retry_passes_used_count: u64,
    /// XOR-fold over the retry-pass USED `(key, value)` pairs (P3): a
    /// divergent used-count at an equal key is detected (was invisible to a
    /// count-only compare).
    #[serde(default)]
    pub retry_passes_used_hash: u64,
    /// Number of per-hash unfulfillable-reinject USED entries (P3
    /// grow-only-MAX map).
    #[serde(default)]
    pub unfulfillable_reinject_used_count: u64,
    /// XOR-fold over the unfulfillable-reinject USED `(key, value)` pairs
    /// (P3): a divergent used-count at an equal hash is detected.
    #[serde(default)]
    pub unfulfillable_reinject_used_hash: u64,
    /// Number of respawn-ledger entries (F7 grow-only SET).
    #[serde(default)]
    pub respawn_events_count: u64,
    /// XOR-fold over the respawn-ledger `(new_id, record)` pairs (F7): a
    /// peer that recorded a respawn event this replica lacks makes the
    /// replica behind, so the snapshot pull's union-merge heals it (and the
    /// admission budget / cooldown converge cluster-wide).
    #[serde(default)]
    pub respawn_events_hash: u64,
}

impl StateDigest {
    /// Does `other` (a peer's digest) hold ledger data this local digest
    /// is MISSING — i.e. would pulling+`restore()`ing the peer's snapshot
    /// change local state? This is the anti-entropy detector: the cadence
    /// pulls a snapshot iff this returns `true`.
    ///
    /// Per-field rule, faithful to the monotone `restore()` lattice:
    ///
    /// - Count-bearing fields (`tasks`, `secondary_capacities`,
    ///   `task_outputs`, `phase_deps`): the peer is ahead iff it holds
    ///   strictly MORE entries (`peer.count > local.count`) OR holds the
    ///   SAME number but a DIFFERENT fold (`count ==`, `hash !=`) — same-
    ///   count-different-members means the peer holds at least one entry the
    ///   local replica lacks (and the merge is a no-op for the entries they
    ///   share, so pulling is always safe). A peer with FEWER entries is
    ///   behind, not ahead, so it never triggers a local pull (the peer's
    ///   own digest round will pull from us).
    /// - `phase_deps` (CRD-3/D-G): ahead iff the peer holds MORE phases OR
    ///   the same count with a DIVERGENT content hash (the count-OR-hash
    ///   compare — R5: the count-only line left a divergent-but-equal-count
    ///   graph digest-invisible). The restore content-hash merge reconciles
    ///   it deterministically (lower hash wins) regardless of pull order.
    /// - `capabilities` (C6): the role-capability 2P-set IS snapshot-
    ///   healable, so it is COMPARED here (detect-WITH-heal). Ahead iff the
    ///   peer holds MORE entries OR the same count with a divergent fold.
    ///   A flagged divergence is one a snapshot pull's `restore` of the
    ///   2P-set actually resolves — no R2 no-op loop.
    /// - `primary_epoch` + `current_primary_hash` (CRD-2/D-P): ahead iff
    ///   the peer's epoch is strictly higher, OR the epochs are EQUAL but
    ///   the `current_primary_hash` DIFFERS (a same-epoch identity split).
    ///   Restore's deterministic lower-id-wins converges both replicas in
    ///   one round; both sides pulling on the equal-epoch divergence is the
    ///   intended bilateral convergence (C5).
    /// - `run_complete` / `run_aborted`: ahead iff the peer's latch is set
    ///   while ours is not (`false → true` ratchet; the reason string is
    ///   irrelevant to the detector).
    /// - `discovery_debt`: the peer is ahead iff it is STRICTLY HIGHER in the
    ///   three-state lattice `Undeclared < Owed < Settled` —
    ///   `self.discovery_debt < other.discovery_debt`. This is a direct
    ///   lattice-height compare (NOT a bool mirror): a bool projection would
    ///   conflate `Undeclared` and `Settled`, so an `Undeclared` replica
    ///   (missed the `Declared` broadcast) could never detect it is behind an
    ///   `Owed` peer → never pulls → on promotion reads `!= Owed` → skips
    ///   discovery → the run stalls / false-completes. The restore `max`-join
    ///   then heals the lower side. One-directional by construction: a higher
    ///   local is never behind a lower peer.
    ///
    /// The detector compares the monotone, snapshot-healable ledger
    /// fields INCLUDING the capability 2P-set (C6 — it heals via snapshot).
    /// The alive-member LIVENESS set stays DELIBERATELY EXCLUDED:
    ///
    /// 1. The alive-member divergence is INTENTIONAL per the honest-
    ///    liveness design — each node owns its own liveness view, so two
    ///    nodes legitimately disagree on whether a peer is alive; anti-
    ///    entropy must NEVER force-converge that (it must never resurrect a
    ///    peer a node correctly buried as dead). Dead ids are not
    ///    snapshotted, so a Dead-set divergence is not snapshot-healable.
    /// 2. The `RoleTable.observers`/`can_be_primary` SETS are node-local
    ///    PROJECTIONS of `capabilities × peer_state-alive` — the capability
    ///    convergence is captured by `capabilities_hash`, and the alive
    ///    composition is the node-local liveness above, so there is no
    ///    separate role-set digest field.
    ///
    /// Symmetric quiescence: when the two replicas are converged every
    /// compared field is equal and this returns `false`, so a steady-state
    /// digest exchange triggers ZERO pulls — the self-quiescing property.
    pub fn is_behind(&self, other: &StateDigest) -> bool {
        field_behind(self.tasks_count, self.tasks_hash, other.tasks_count, other.tasks_hash)
            || field_behind(
                self.secondary_capacities_count,
                self.secondary_capacities_hash,
                other.secondary_capacities_count,
                other.secondary_capacities_hash,
            )
            || field_behind(
                self.task_outputs_count,
                self.task_outputs_hash,
                other.task_outputs_count,
                other.task_outputs_hash,
            )
            // CRD-3/D-G: count-OR-hash (R5 — the count-only compare left a
            // divergent-but-equal-count graph invisible).
            || field_behind(
                self.phase_deps_count,
                self.phase_deps_hash,
                other.phase_deps_count,
                other.phase_deps_hash,
            )
            // C6: the snapshot-healable capability 2P-set (detect-WITH-heal).
            || field_behind(
                self.capabilities_count,
                self.capabilities_hash,
                other.capabilities_count,
                other.capabilities_hash,
            )
            // CRD-2/D-P: higher epoch, OR equal epoch with a divergent
            // current-primary identity (the same-epoch split).
            || other.primary_epoch > self.primary_epoch
            || (other.primary_epoch == self.primary_epoch
                && other.current_primary_hash != self.current_primary_hash)
            || (other.run_complete && !self.run_complete)
            || (other.run_aborted && !self.run_aborted)
            // discovery_debt: behind iff the peer is STRICTLY HIGHER in the
            // lattice `Undeclared < Owed < Settled` (a direct lattice-height
            // compare on the full enum). Covers ALL three states: an
            // Undeclared local is behind an Owed-or-Settled peer; an Owed
            // local is behind a Settled peer; a Settled local is behind
            // nothing. The restore `max`-join heals the lower side.
            || self.discovery_debt < other.discovery_debt
            // F4 + P3 grow-only-MAX maps: count-OR-hash compare, same shape
            // as the other count-bearing fields. A promoted primary that
            // bumped a count past a stale peer's snapshot makes the peer
            // behind; the snapshot pull's per-key max-merge heals it.
            || field_behind(
                self.phase_event_tallies_count,
                self.phase_event_tallies_hash,
                other.phase_event_tallies_count,
                other.phase_event_tallies_hash,
            )
            || field_behind(
                self.retry_passes_used_count,
                self.retry_passes_used_hash,
                other.retry_passes_used_count,
                other.retry_passes_used_hash,
            )
            || field_behind(
                self.unfulfillable_reinject_used_count,
                self.unfulfillable_reinject_used_hash,
                other.unfulfillable_reinject_used_count,
                other.unfulfillable_reinject_used_hash,
            )
            // F7 grow-only SET: count-OR-hash compare, same shape as the
            // other count-bearing fields. A peer that recorded a respawn
            // event this replica lacks makes the replica behind; the
            // snapshot pull's union-merge heals it.
            || field_behind(
                self.respawn_events_count,
                self.respawn_events_hash,
                other.respawn_events_count,
                other.respawn_events_hash,
            )
    }
}

/// A single count+hash field: the peer is ahead iff it holds strictly
/// more entries, or the same number with a divergent fold (so it holds at
/// least one entry the local replica lacks). See [`StateDigest::is_behind`].
fn field_behind(local_count: u64, local_hash: u64, peer_count: u64, peer_hash: u64) -> bool {
    peer_count > local_count || (peer_count == local_count && peer_hash != local_hash)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_digests_are_not_behind() {
        let d = StateDigest {
            tasks_count: 3,
            tasks_hash: 0xABCD,
            primary_epoch: 2,
            ..Default::default()
        };
        assert!(!d.is_behind(&d));
    }

    #[test]
    fn more_tasks_on_peer_is_behind() {
        let local = StateDigest {
            tasks_count: 1,
            tasks_hash: 0x11,
            ..Default::default()
        };
        let peer = StateDigest {
            tasks_count: 2,
            tasks_hash: 0x22,
            ..Default::default()
        };
        assert!(local.is_behind(&peer));
        // The peer with fewer entries is NOT behind the lesser local.
        assert!(!peer.is_behind(&local));
    }

    #[test]
    fn same_count_divergent_fold_is_behind_both_ways() {
        // Equal counts, different folds: each side may hold an entry the
        // other lacks, so both pull (idempotent restore reconciles).
        let a = StateDigest {
            tasks_count: 2,
            tasks_hash: 0x1,
            ..Default::default()
        };
        let b = StateDigest {
            tasks_count: 2,
            tasks_hash: 0x2,
            ..Default::default()
        };
        assert!(a.is_behind(&b));
        assert!(b.is_behind(&a));
    }

    #[test]
    fn higher_epoch_and_set_latches_are_behind() {
        let local = StateDigest::default();
        let peer = StateDigest {
            primary_epoch: 5,
            run_complete: true,
            run_aborted: true,
            ..Default::default()
        };
        assert!(local.is_behind(&peer));
        // Latch ratchet is one-directional: a set-local vs unset-peer is
        // NOT behind.
        assert!(!peer.is_behind(&local));
    }

    /// discovery_debt lattice-height direction across ALL THREE states
    /// `Undeclared < Owed < Settled`: a replica is behind iff the peer is
    /// STRICTLY higher. Pins every adjacent transition in BOTH directions —
    /// the case the single-bool projection could not carry (it would
    /// conflate `Undeclared` and `Settled`, so an `Undeclared` replica would
    /// never detect it is behind an `Owed` peer → the mode-2 stall).
    #[test]
    fn discovery_debt_behind_iff_peer_strictly_higher() {
        let undeclared = StateDigest {
            discovery_debt: DiscoveryDebt::Undeclared,
            ..Default::default()
        };
        let owed = StateDigest {
            discovery_debt: DiscoveryDebt::Owed,
            ..Default::default()
        };
        let settled = StateDigest {
            discovery_debt: DiscoveryDebt::Settled,
            ..Default::default()
        };

        // Undeclared is behind Owed and Settled (pull up the lattice).
        // THIS is the case a bool would miss: Undeclared-behind-Owed.
        assert!(undeclared.is_behind(&owed));
        assert!(undeclared.is_behind(&settled));
        // Owed is behind Settled.
        assert!(owed.is_behind(&settled));

        // Reverse direction: a higher local is NEVER behind a lower peer.
        assert!(!owed.is_behind(&undeclared));
        assert!(!settled.is_behind(&undeclared));
        assert!(!settled.is_behind(&owed));

        // Equal states → neither behind (no divergence on this field).
        assert!(!undeclared.is_behind(&undeclared));
        assert!(!owed.is_behind(&owed));
        assert!(!settled.is_behind(&settled));
    }

    /// CRD-2/D-P: at EQUAL epoch a DIFFERENT `current_primary_hash` makes
    /// BOTH sides behind (the same-epoch identity split — bilateral, C5).
    #[test]
    fn equal_epoch_divergent_primary_hash_is_behind_both_ways() {
        let a = StateDigest {
            primary_epoch: 9,
            current_primary_hash: 0xAAAA,
            ..Default::default()
        };
        let b = StateDigest {
            primary_epoch: 9,
            current_primary_hash: 0xBBBB,
            ..Default::default()
        };
        assert!(a.is_behind(&b));
        assert!(b.is_behind(&a));
        // A higher epoch dominates the hash compare (epoch wins outright).
        let higher = StateDigest {
            primary_epoch: 10,
            current_primary_hash: 0xAAAA,
            ..Default::default()
        };
        assert!(a.is_behind(&higher));
        assert!(!higher.is_behind(&a));
    }

    /// CRD-3/D-G (R5): a divergent-but-equal-COUNT phase graph is detected
    /// via the `phase_deps_hash` — the count-only compare could not.
    #[test]
    fn equal_count_divergent_phase_deps_hash_is_behind() {
        let a = StateDigest {
            phase_deps_count: 1,
            phase_deps_hash: 0x1111,
            ..Default::default()
        };
        let b = StateDigest {
            phase_deps_count: 1,
            phase_deps_hash: 0x2222,
            ..Default::default()
        };
        assert!(a.is_behind(&b));
        assert!(b.is_behind(&a));
        // Equal count AND equal hash → not behind.
        let same = StateDigest {
            phase_deps_count: 1,
            phase_deps_hash: 0x1111,
            ..Default::default()
        };
        assert!(!a.is_behind(&same));
    }

    /// C6: a divergent capability 2P-set fold makes the lagging side
    /// behind (detect-WITH-heal — the snapshot pull reconciles it). Both
    /// count-ahead and equal-count-divergent-fold trigger.
    #[test]
    fn capability_divergence_is_behind() {
        let local = StateDigest {
            capabilities_count: 1,
            capabilities_hash: 0xCAFE,
            ..Default::default()
        };
        // Peer holds MORE capability entries → local behind.
        let more = StateDigest {
            capabilities_count: 2,
            capabilities_hash: 0xBEEF,
            ..Default::default()
        };
        assert!(local.is_behind(&more));
        assert!(!more.is_behind(&local));
        // Equal count, divergent fold → both behind.
        let diverged = StateDigest {
            capabilities_count: 1,
            capabilities_hash: 0xDEAD,
            ..Default::default()
        };
        assert!(local.is_behind(&diverged));
        assert!(diverged.is_behind(&local));
    }
}
